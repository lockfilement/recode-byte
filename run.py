#!/usr/bin/env python3
import sys
import time
import logging
import subprocess
import signal
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import threading
import json
import asyncio
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('supervisor')

class CodeChangeHandler(FileSystemEventHandler):
    def __init__(self, restart_callback, reload_callback):
        self.restart_callback = restart_callback
        self.reload_callback = reload_callback
        self.last_reload = 0
        self.cooldown = 2  # Minimum seconds between reloads
        # Files that require full restart when changed
        self.core_files = {
            'main.py', 
            'run.py',
            'token_api.py',
            os.path.join('utils', 'config_manager.py'),
        }
        
    def should_ignore(self, event):
        """Check if the file change should be ignored"""
        # Ignore these files completely
        if (event.src_path.endswith('config.json') or        # Skip config.json as we don't want restarts when updating user tokens
            event.src_path.endswith('.pyc') or              # Skip compiled Python files
            '__pycache__' in event.src_path or              # Skip cache directories
            '.git' in event.src_path):                      # Skip git files
            return True
        return False
            
    def should_reload(self, event):
        """Helper to check if file change should trigger reload"""
        # Skip ignored files
        if self.should_ignore(event):
            return False
            
        # Only reload for .py files and database changes
        return event.src_path.endswith('.py') or event.src_path.endswith('config/database.json')
        
    def needs_full_restart(self, path):
        """Check if the file needs a full restart or if it can be hot-reloaded"""
        # Convert path to use correct path separator
        norm_path = os.path.normpath(path)
        
        # Check if it's a core file that requires restart
        return any(os.path.normpath(core_file) in norm_path for core_file in self.core_files)
        
    def try_reload(self, event):
        """Attempt to reload with cooldown check"""
        current_time = time.time()
        if current_time - self.last_reload > self.cooldown:
            logger.info(f"Detected change in {event.src_path} (event type: {event.event_type})")
            self.last_reload = current_time
            
            # Determine if we need a full restart or just hot reload
            if self.needs_full_restart(event.src_path):
                logger.info(f"Core file changed, performing full restart")
                self.restart_callback()
            else:
                # Hot reload only the changed file
                file_path = event.src_path
                logger.info(f"Performing hot reload for: {file_path}")
                self.reload_callback(file_path)
            
    def on_modified(self, event):
        if self.should_reload(event):
            self.try_reload(event)
            
    def on_created(self, event):
        if self.should_reload(event):
            self.try_reload(event)

class BotSupervisor:
    def __init__(self):
        self.process = None
        self.should_run = True
        self.observer = None
        self.output_thread = None
        self.ipc_socket = None

    def monitor_output(self, process):
        """Monitor process output in a separate thread"""
        while process.poll() is None:
            line = process.stdout.readline()
            if line:
                print(line.rstrip())

    def start_bot(self):
        """Start the bot process"""
        try:
            logger.info("Starting bot process...")
            self.process = subprocess.Popen(
                [sys.executable, 'main.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                env={**os.environ, 'HOT_RELOAD_ENABLED': '1'}  # Set env var to enable hot reload
            )
            
            # Start output monitoring in a separate thread
            self.output_thread = threading.Thread(
                target=self.monitor_output,
                args=(self.process,),
                daemon=True
            )
            self.output_thread.start()
            
        except Exception as e:
            logger.error(f"Error starting bot: {e}")
            self.should_run = False

    def restart_bot(self):
        """Restart the bot process"""
        try:
            logger.info("Restarting bot...")
            if self.process:
                # Send SIGTERM to allow graceful shutdown
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)  # Wait up to 10 seconds
                except subprocess.TimeoutExpired:
                    logger.warning("Bot didn't terminate gracefully, forcing shutdown")
                    self.process.kill()  # Force kill if it doesn't shut down
                    self.process.wait()
            
            self.start_bot()
            
        except Exception as e:
            logger.error(f"Error during restart: {e}")

    def hot_reload(self, file_path):
        """Send a hot reload signal to the bot for a specific file"""
        try:
            # Don't reload if the process isn't running
            if not self.process or self.process.poll() is not None:
                logger.warning("Cannot hot reload: Bot process not running")
                return False
                
            # Normalize file path to use forward slashes for consistency
            file_path = file_path.replace('\\', '/')
            
            # Decide what to reload based on file path
            if '/cogs/' in file_path:
                # Extract cog name
                cog_name = os.path.basename(file_path)[:-3]  # Remove .py extension
                reload_msg = f"HOT_RELOAD:COG:{cog_name}\n"
                logger.info(f"Hot reloading cog: {cog_name}")
            elif '/utils/' in file_path:
                # Extract utils module name
                if 'utils/database/' in file_path:
                    module_path = f"utils.database.{os.path.basename(file_path)[:-3]}"
                else:
                    module_path = f"utils.{os.path.basename(file_path)[:-3]}"
                reload_msg = f"HOT_RELOAD:MODULE:{module_path}\n"
                logger.info(f"Hot reloading module: {module_path}")
            else:
                # For other files we don't handle specially, send the full path
                reload_msg = f"HOT_RELOAD:FILE:{file_path}\n"
                logger.info(f"Hot reloading file: {file_path}")
            
            # Write to a file that will be watched by the bot
            with open('.hot_reload_signal', 'w') as f:
                f.write(reload_msg)
            
            return True
            
        except Exception as e:
            logger.error(f"Error during hot reload: {e}")
            return False

    def setup_file_watcher(self):
        """Set up the file system watcher"""
        self.observer = Observer()
        event_handler = CodeChangeHandler(self.restart_bot, self.hot_reload)
        
        # Watch the main bot directory and all subdirectories
        paths_to_watch = [
            '.',  # Main directory
            './cogs',  # Cogs directory
            './utils',  # Utils directory
            './config' # Config directory
        ]
        
        for path in paths_to_watch:
            if os.path.exists(path):
                self.observer.schedule(event_handler, path, recursive=True)
                logger.info(f"Watching directory: {path}")
        
        self.observer.start()

    def run(self):
        """Main supervisor loop"""
        try:
            # Set up signal handlers
            signal.signal(signal.SIGINT, self.handle_shutdown)
            signal.signal(signal.SIGTERM, self.handle_shutdown)
            
            logger.info("Starting bot supervisor...")
            self.setup_file_watcher()
            self.start_bot()
            
            # Keep the supervisor running
            while self.should_run:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.handle_shutdown(None, None)
        except Exception as e:
            logger.error(f"Supervisor error: {e}")
        finally:
            self.cleanup()

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Shutdown signal received")
        self.should_run = False
        self.cleanup()
        sys.exit(0)

    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up...")
        if self.observer:
            self.observer.stop()
            self.observer.join()
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
        # Clean up any signal files
        if os.path.exists('.hot_reload_signal'):
            try:
                os.remove('.hot_reload_signal')
            except:
                pass

if __name__ == "__main__":
    supervisor = BotSupervisor()
    supervisor.run()
