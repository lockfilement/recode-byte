import discord
from discord.ext import commands
import json
import asyncio
import os
import logging
import traceback
import collections

# 1.7.3 / Python 3.13 Compatibility Patch
if not hasattr(collections, 'Iterable'):
    import collections.abc
    collections.Iterable = collections.abc.Iterable
    collections.Mapping = collections.abc.Mapping
    collections.MutableMapping = collections.abc.MutableMapping
    collections.Sequence = collections.abc.Sequence

# Import CurlError for proper exception handling
try:
    from curl_cffi.curl import CurlError
except ImportError:
    CurlError = Exception 

# ... (Voice patches and other imports remain the same)

# Apply voice patches early, before any Discord connections
try:
    from utils.voice_patches import initialize_patches
    initialize_patches()
    logging.getLogger(__name__).info("Voice patches initialized in main.py")
except Exception as e:
    logging.getLogger(__name__).warning(f"Could not initialize voice patches: {e}")

from utils.database.manager import DatabaseManager
from utils.config_manager import ConfigManager
from utils.general import detect_message_limit
from typing import Dict, Set, Optional
import time
import signal
import sys
import aiohttp
import ssl
import importlib
import threading
import deepl
import string
import random
from typing import Union, Optional
from websockets.client import WebSocketClientProtocol, connect
from websockets.exceptions import ConnectionClosed, WebSocketException, ConnectionClosedOK, ConnectionClosedError
from discord.gateway import WebSocketClosure

class MockCurl:
    """Mock curl object to satisfy discord.py-self expectations."""
    
    def __init__(self, websocket):
        if websocket is None:
            raise ValueError("WebSocket reference cannot be None")
        self._websocket = websocket
        self._curl_value = self  # Internal storage for _curl value
    
    @property 
    def _curl(self):
        """Return _curl value, considering connection state."""
        try:
            if self._websocket is None or self._websocket.closed:
                return None
            return self._curl_value
        except Exception as e:
            logging.getLogger('bot').debug(
                "Error in MockCurl._curl getter: %s | repr: %r",
                e,
                e,
                exc_info=True,
            )
            return None
    
    @_curl.setter
    def _curl(self, value):
        """Allow setting _curl value."""
        try:
            self._curl_value = value
        except Exception as e:
            logging.getLogger('bot').debug(
                "Error in MockCurl._curl setter: %s | repr: %r",
                e,
                e,
                exc_info=True,
            )
            # Don't raise, just log and continue
    
    def __getattr__(self, name):
        """Catch any missing methods/properties that discord.py-self might expect on curl object."""
        logging.getLogger('bot').debug("MockCurl.__getattr__ missing attribute: %s", name, stack_info=False)
        raise AttributeError(f"MockCurl has no attribute '{name}'")

class AsyncWebSocket:
    """
    Wrapper for WebSocket connections that mimics aiohttp's ClientWebSocketResponse interface
    but uses the standard websockets library internally.
    """
    
    def __init__(self, ws: WebSocketClientProtocol):
        if ws is None:
            raise ValueError("WebSocket connection cannot be None")
        
        self.ws = ws
        self._closed = False
        self._close_code = None
        self._close_reason = None
        
        # Add curl attribute that discord.py-self expects
        self.curl = MockCurl(self)
    
    @property
    def closed(self) -> bool:
        """Returns True if the WebSocket connection is closed."""
        return self.ws.closed or self._closed
    
    @property
    def close_code(self) -> Optional[int]:
        """Returns the close code if the connection was closed."""
        return self._close_code if self._close_code is not None else (self.ws.close_code if hasattr(self.ws, 'close_code') else None)
    
    @property  
    def close_reason(self) -> Optional[str]:
        """Returns the close reason if the connection was closed."""
        return self._close_reason if self._close_reason is not None else (self.ws.close_reason if hasattr(self.ws, 'close_reason') else None)
    
    @property
    def open(self) -> bool:
        return not self.closed
    
    async def send_str(self, data: str) -> None:
        """Send a text message."""
        if self.closed:
            raise RuntimeError("Cannot send to closed WebSocket")
        await self.ws.send(data)
    
    async def send_bytes(self, data: bytes) -> None:
        """Send binary data."""
        if self.closed:
            raise RuntimeError("Cannot send to closed WebSocket")
        await self.ws.send(data)
    
    async def send(self, data: Union[str, bytes]) -> None:
        """Send data (text or binary)."""
        if isinstance(data, str):
            await self.send_str(data)
        else:
            await self.send_bytes(data)
    
    async def receive(self):
        """Receive a message from the WebSocket."""
        if self.closed:
            raise RuntimeError("Cannot receive from closed WebSocket")
        
        try:
            msg = await self.ws.recv()
            # Create a mock message object similar to aiohttp's WSMessage
            return MockWSMessage(msg)
        except Exception as e:
            self._closed = True
            raise e
    
    async def recv(self):
        """Return (message, flags) for gateway; raise WebSocketClosure carrying this ws wrapper on close."""
        try:
            msg = await self.ws.recv()
        except (ConnectionClosedOK, ConnectionClosedError, ConnectionClosed) as e:
            # Capture close details for discord.py-self which may inspect ws.close_code/close_reason
            try:
                if getattr(e, 'code', None) is not None:
                    self._close_code = e.code
                if getattr(e, 'reason', None) is not None:
                    # websockets 'reason' can be str
                    self._close_reason = e.reason
            except Exception:
                pass
            # Hand control back to the gateway by raising with the ws-like object
            raise WebSocketClosure(self)
        
        flags = 1 if isinstance(msg, str) else 2
        return msg, flags
    
    async def ping(self, data: bytes = b'') -> None:
        """Send a ping frame."""
        if self.closed:
            raise RuntimeError("Cannot ping closed WebSocket")
        await self.ws.ping(data)
    
    async def pong(self, data: bytes = b'') -> None:
        """Send a pong frame."""
        if self.closed:
            raise RuntimeError("Cannot pong closed WebSocket")
        await self.ws.pong(data)
    
    async def close(self, code: int = 1000, reason: Union[str, bytes] = "", *, message: Union[str, bytes, None] = None) -> None:
        """Close the WebSocket connection with proper state management for discord.py-self."""
        logger.debug("AsyncWebSocket.close() called with code=%s, reason=%s", code, reason)
        
        # Set close code and reason for discord.py-self's reconnection logic
        self._close_code = code
        # Support both 'reason' and legacy 'message' kwarg
        close_text: Union[str, bytes] = message if message is not None else reason
        if isinstance(close_text, bytes):
            self._close_reason = close_text.decode('utf-8', errors='ignore')
        else:
            self._close_reason = close_text
        
        if not self.closed:
            # Defer setting _closed until after attempting underlying close
            try:
                if hasattr(self.ws, 'close') and callable(self.ws.close):
                    logger.debug("Closing underlying websocket with code %s", code)
                    await self.ws.close(code=code, reason=self._close_reason or "")
                    logger.debug("Underlying websocket closed successfully")
                else:
                    logger.debug("Underlying websocket has no close method")
            except Exception as e:
                logger.debug("Error closing underlying websocket: %s: %s", type(e).__name__, e, exc_info=True)
            finally:
                self._closed = True
        else:
            logger.debug("AsyncWebSocket.close() called but already closed")
        
        logger.debug("AsyncWebSocket.close() completed, closed=%s, close_code=%s", self.closed, self._close_code)
    
    # Add any other methods that might be expected
    @property
    def protocol(self):
        """Return the websocket protocol."""
        return getattr(self.ws, 'protocol', None)
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    def __getattr__(self, name):
        """Catch any missing methods/properties that discord.py-self might expect."""
        logger.debug("AsyncWebSocket.__getattr__ missing attribute: %s", name, stack_info=False)
        
        # Try to delegate to the underlying websocket
        if hasattr(self.ws, name):
            attr = getattr(self.ws, name)
            logger.debug("Delegating '%s' to underlying websocket: %r", name, attr)
            return attr
        
        # If not found, raise AttributeError with detailed info
        raise AttributeError(f"AsyncWebSocket has no attribute '{name}' (also not found on underlying websocket)")

class MockWSMessage:
    """Mock WebSocket message to mimic aiohttp's WSMessage interface."""
    
    def __init__(self, data: Union[str, bytes]):
        if isinstance(data, str):
            self.type = 'text'
            self.data = data
        else:
            self.type = 'binary' 
            self.data = data
    
    def json(self, **kwargs):
        """Parse message data as JSON."""
        import json
        if isinstance(self.data, str):
            return json.loads(self.data, **kwargs)
        else:
            return json.loads(self.data.decode('utf-8'), **kwargs)

async def ws_connect(
    url: str,
    headers: Optional[dict] = None,
    proxy: Optional[str] = None,
    proxy_auth: Optional[tuple] = None,
    timeout: float = 30.0,
    interface: Optional[str] = None,  # Accept but ignore this parameter
    **kwargs
) -> AsyncWebSocket:
    """
    Connect to a WebSocket using the standard websockets library
    but return an interface compatible with aiohttp's ClientWebSocketResponse.
    """
    try:
        # Build extra headers
        extra_headers = {}
        if headers:
            extra_headers.update(headers)
        
        # Add default Discord headers that are usually expected
        default_headers = {
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd', 
            'Origin': 'https://discord.com',
            'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits',
        }
        
        for key, value in default_headers.items():
            if key not in extra_headers:
                extra_headers[key] = value
        
        # Filter out parameters that websockets library doesn't understand
        filtered_kwargs = {}
        supported_params = {
            'origin', 'extensions', 'subprotocols', 'extra_headers', 
            'user_agent_header', 'compression', 'ping_interval', 
            'ping_timeout', 'close_timeout', 'max_size', 'max_queue',
            'read_limit', 'write_limit'
        }
        
        for key, value in kwargs.items():
            if key in supported_params:
                filtered_kwargs[key] = value
            else:
                # Log unsupported parameters for debugging
                logger.debug("Ignoring unsupported websocket parameter: %s=%r", key, value)
        
        # Handle proxy if provided (websockets library has different proxy support)
        # Note: Standard websockets library doesn't support HTTP proxies directly
        # For full proxy support, you'd need to use a different approach
        if proxy:
            logger.debug("Proxy support not fully implemented in websockets library: %s", proxy)
        
        # Create SSL context for secure connections
        ssl_context = ssl.create_default_context()
        # Discord should have valid certificates, so keep default verification
        
        # IMPORTANT: Discord.py-self handles compression at APPLICATION level using _decompressor
        # We must NOT use transport-level compression in the websocket library
        # Discord sends compressed bytes over uncompressed websocket, then decompresses in application
        logger.debug("Disabling transport-level compression (discord.py-self handles compression)")
        
        # Connect using websockets library with filtered parameters
        ws = await connect(
            url,
            extra_headers=extra_headers,
            ssl=ssl_context if url.startswith('wss://') else None,
            ping_interval=None,  # Disable automatic ping to match aiohttp behavior
            ping_timeout=None,
            close_timeout=timeout,
            max_size=2**23,  # 8MB max message size (Discord can send very large messages)
            max_queue=2**8,  # 256 message queue size
            compression=None,  # MUST be None - discord.py-self handles compression itself
            read_limit=2**16,  # 64KB read buffer
            write_limit=2**16,  # 64KB write buffer
            **filtered_kwargs
        )
        
        # Validate connection is actually open before returning
        if ws.closed:
            raise RuntimeError("WebSocket connection was closed immediately after creation")
        
        wrapper = AsyncWebSocket(ws)
        logger.debug("Created AsyncWebSocket wrapper: %r (type=%s)", wrapper, type(wrapper))
        return wrapper
        
    except Exception as e:
        error_msg = str(e).strip()
        logger.debug(
            "ws_connect failed: %s: '%s' | repr: %r | empty: %s",
            type(e).__name__,
            e,
            e,
            not error_msg,
            exc_info=True,
        )
        
        if error_msg:
            raise RuntimeError(f"Failed to connect to WebSocket: {e}") from e
        else:
            raise RuntimeError(f"Failed to connect to WebSocket: {type(e).__name__} (empty error message)") from e

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('bot')
logger.setLevel(logging.INFO)

# Configure logging for different components
logging.getLogger('discord').setLevel(logging.WARNING)
logging.getLogger('cogs').setLevel(logging.INFO)  # Reduce chatter from cogs
logging.getLogger('utils').setLevel(logging.INFO)
logging.getLogger('discord.gateway').setLevel(logging.WARNING)

# Extra gateway tracing: log resume vs identify decisions
try:
    _gw_logger = logging.getLogger('discord.gateway')
    from discord.state import ConnectionState

    if not hasattr(ConnectionState, '_copilot_original__identify'):
        ConnectionState._copilot_original__identify = ConnectionState.identify
        ConnectionState._copilot_original__resume = ConnectionState.resume

        async def _copilot_identify(self, *args, **kwargs):
            _gw_logger.debug('[gateway] IDENTIFY: no valid session, starting new connection')
            return await ConnectionState._copilot_original__identify(self, *args, **kwargs)

        async def _copilot_resume(self, *args, **kwargs):
            _gw_logger.debug('[gateway] RESUME: attempting to resume session_id=%s seq=%s', getattr(self, 'session_id', None), getattr(self, 'sequence', None))
            return await ConnectionState._copilot_original__resume(self, *args, **kwargs)

        ConnectionState.identify = _copilot_identify
        ConnectionState.resume = _copilot_resume
        _gw_logger.debug('[gateway] Patched ConnectionState.identify/resume for debug logs')
except Exception as _e:
    logging.getLogger(__name__).debug(f"Gateway debug patch failed: {_e}")

# Allow individual loggers to be configured by name
def get_cog_logger(cog_name):
    """Get a logger for a specific cog that inherits the global config"""
    cog_logger = logging.getLogger(f'cogs.{cog_name}')
    return cog_logger

# Load configuration
with open('config.json') as config_file:
    config = json.load(config_file)

class HotReloader:
    """Handles hot reloading of modules and cogs without restarting the bot."""
    
    def __init__(self, bot_manager):
        self.bot_manager = bot_manager
        self.signal_file = '.hot_reload_signal'
        self.last_check_time = 0
        self.check_interval = 1.0  # Check every second
        self.running = True
        self.monitor_thread = None
        self.event_loop = None  # Store a reference to the event loop
    
    def start_monitoring(self):
        """Start monitoring for hot reload signals in a separate thread."""
        # Store the event loop reference from the main thread
        self.event_loop = asyncio.get_event_loop()
        
        # Start the monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Hot reload monitor started")
    
    def stop_monitoring(self):
        """Stop monitoring for hot reload signals."""
        self.running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2.0)
        logger.info("Hot reload monitor stopped")
    
    def _monitor_loop(self):
        """Monitor for hot reload signals in a loop."""
        while self.running:
            try:
                self._check_signal_file()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in hot reload monitor: {e}")
                traceback.print_exc()
    
    def _check_signal_file(self):
        """Check if a hot reload signal file exists and process it."""
        if not os.path.exists(self.signal_file):
            return
        
        try:
            # Get file modification time
            mod_time = os.path.getmtime(self.signal_file)
            
            # Skip if we've already processed this signal
            if mod_time <= self.last_check_time:
                return
            
            self.last_check_time = mod_time
            
            # Read signal file
            with open(self.signal_file, 'r') as f:
                signal_data = f.read().strip()
            
            # Process signal
            if signal_data.startswith('HOT_RELOAD:') and self.event_loop:
                _, signal_type, target = signal_data.split(':', 2)
                
                # Schedule the coroutine in the event loop and wait for it to complete
                future = asyncio.run_coroutine_threadsafe(
                    self._process_reload_signal(signal_type, target), 
                    self.event_loop
                )
                
                # Optionally wait for result with timeout (prevents blocking the thread indefinitely)
                try:
                    # Wait for up to 30 seconds for the reload to complete
                    result = future.result(30)
                    logger.info(f"Hot reload completed: {result}")
                except asyncio.TimeoutError:
                    logger.error("Hot reload timed out")
                except Exception as e:
                    logger.error(f"Exception during hot reload: {e}")
        
        except Exception as e:
            logger.error(f"Error processing hot reload signal: {e}")
            traceback.print_exc()
    
    async def _process_reload_signal(self, signal_type, target):
        """Process a hot reload signal to reload code without restarting."""
        try:
            logger.info(f"Processing hot reload signal: {signal_type}:{target}")
            
            if signal_type == 'COG':
                return await self._reload_cog(target)
            elif signal_type == 'MODULE':
                return await self._reload_module(target)
            elif signal_type == 'FILE':
                return await self._reload_file(target)
            else:
                logger.warning(f"Unknown hot reload signal type: {signal_type}")
                return f"Unknown signal type: {signal_type}"
        
        except Exception as e:
            logger.error(f"Error processing hot reload: {e}")
            traceback.print_exc()
            return f"Error: {str(e)}"
    async def _reload_cog(self, cog_name):
        """Reload a cog across all bot instances with retry mechanism."""
        logger.info(f"Hot reloading cog: {cog_name}")
        success_count = 0
        failed_bots = []
        
        for bot in self.bot_manager.bots.values():
            success = await self._reload_cog_with_retry(bot, cog_name)
            if success:
                success_count += 1
            else:
                failed_bots.append(bot.user.name if hasattr(bot, 'user') and bot.user else 'Unknown')
        
        error_count = len(failed_bots)
        if failed_bots:
            logger.warning(f"Hot reload of cog {cog_name} failed for bots: {', '.join(failed_bots)}")
        
        logger.info(f"Hot reload of cog {cog_name} complete: {success_count} successful, {error_count} failed")
        return f"Cog {cog_name}: {success_count} successful, {error_count} failed"
    
    async def _reload_cog_with_retry(self, bot, cog_name, max_retries=3):
        """Reload a cog for a single bot instance with retry mechanism."""
        extension_name = f"cogs.{cog_name}"
        
        for attempt in range(max_retries):
            try:
                if extension_name in bot.extensions:
                    # Reload the cog
                    await bot.reload_extension(extension_name)
                else:
                    # Try to load it if it's not loaded
                    await bot.load_extension(extension_name)
                return True
                
            except Exception as e:
                wait_time = min(1.0 * (2 ** attempt), 5.0)  # Exponential backoff, max 5 seconds
                
                if attempt < max_retries - 1:
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for cog {cog_name} on {bot.user.name if hasattr(bot, 'user') and bot.user else 'Unknown'}: {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All {max_retries} attempts failed for cog {cog_name} on {bot.user.name if hasattr(bot, 'user') and bot.user else 'Unknown'}: {e}")
                    
        return False
    
    async def _reload_module(self, module_name):
        """Reload a module and any cogs that depend on it."""
        logger.info(f"Hot reloading module: {module_name}")
        affected_cogs = 0
        
        try:
            # Find the module
            if module_name in sys.modules:
                # Reload the module
                old_module = sys.modules[module_name]
                importlib.reload(old_module)
                logger.info(f"Reloaded module: {module_name}")
                
                # Find any cogs that might depend on this module
                for bot in self.bot_manager.bots.values():
                    reloaded_cogs = set()
                    for cog_name, cog in list(bot.cogs.items()):
                        # Check if cog might depend on this module
                        cog_module = cog.__module__
                        
                        if cog_name.lower() in reloaded_cogs:
                            continue
                            
                        needs_reload = False
                        
                        # Check direct module dependency
                        if cog_module.startswith(module_name):
                            needs_reload = True
                        
                        # Check if the module is used in the cog's module
                        cog_module_obj = sys.modules.get(cog.__module__)
                        if cog_module_obj and hasattr(cog_module_obj, '__file__'):
                            if module_name in getattr(cog_module_obj, '__file__', ''):
                                needs_reload = True
                          # If dependent, reload the cog
                        if needs_reload:
                            logger.info(f"Reloading dependent cog {cog_name} after module change")
                            # Use retry mechanism for dependent cog reloads too
                            success = await self._reload_cog_with_retry(bot, cog_name.lower())
                            if success:
                                reloaded_cogs.add(cog_name.lower())
                                affected_cogs += 1
                            else:
                                logger.error(f"Failed to reload dependent cog {cog_name} after all retries")
            else:
                # Try to import it if not already loaded
                try:
                    importlib.import_module(module_name)
                    logger.info(f"Imported new module: {module_name}")
                    return f"Imported new module: {module_name}"
                except ImportError as e:
                    logger.error(f"Could not import module {module_name}: {e}")
                    return f"Error importing module: {str(e)}"
        
            return f"Reloaded module {module_name}, affected {affected_cogs} cogs"
        except Exception as e:
            logger.error(f"Error reloading module {module_name}: {e}")
            traceback.print_exc()
            return f"Error: {str(e)}"
    
    async def _reload_file(self, file_path):
        """Handle reloading a file by determining its type."""
        try:
            # Normalize path for better comparison
            norm_path = os.path.normpath(file_path).replace('\\', '/')
            
            # Determine what kind of file this is
            if '/cogs/' in norm_path or '\\cogs\\' in norm_path:
                # This is a cog
                cog_name = os.path.basename(norm_path)[:-3]  # Remove .py extension
                return await self._reload_cog(cog_name)
            
            elif '/utils/' in norm_path or '\\utils\\' in norm_path:
                # This is a utility module
                if 'utils/database/' in norm_path or 'utils\\database\\' in norm_path:
                    module_name = f"utils.database.{os.path.basename(norm_path)[:-3]}"
                else:
                    module_name = f"utils.{os.path.basename(norm_path)[:-3]}"
                return await self._reload_module(module_name)
            
            else:
                message = f"Unsupported file for hot reload: {file_path}"
                logger.warning(message)
                return message
        
        except Exception as e:
            logger.error(f"Error processing file {file_path} for hot reload: {e}")
            traceback.print_exc()
            return f"Error: {str(e)}"

class BotManager:
    """Manages multiple bot instances and shared resources."""
    
    def __init__(self):
        self.bots: Dict[str, commands.Bot] = {}
        self._instance_locks: Dict[str, asyncio.Lock] = {}
        self._startup_complete: asyncio.Event = asyncio.Event()
        self._cleanup_tasks: Set[asyncio.Task] = set()
        self._config_path: str = 'config.json'
        self.hot_reloader: Optional[HotReloader] = None
        
        # Add shared ConfigManager instance for optimized operations
        self._shared_config_manager: Optional[ConfigManager] = None
        
        # Initialize shared data collections with type hints
        self.shared_insults: list = []
        self.shared_words: set = set()
        self.shared_rizz_lines: list = []
        self.shared_skibidi_lines: list = []
        
        # Add shared DeepL translator
        self.shared_deepl_translator = None
        self.deepl_source_languages = {}
        self.deepl_target_languages = {}
        self.deepl_language_code_map = {
            # Common language code mappings
            "EN": "EN-US",  # Default English to US English
            "PT": "PT-BR",  # Default Portuguese to Brazilian Portuguese
            "ZH": "ZH",     # Default Chinese (already exists as is)
        }
        
        # Load all shared data
        self._load_shared_data("config/insults.txt", "list", "shared_insults")
        self._load_shared_data("config/blacktea.txt", "set", "shared_words", lower=True)
        self._load_shared_data("config/rizz.txt", "list", "shared_rizz_lines")
        self._load_shared_data("config/skibidi.txt", "list", "shared_skibidi_lines")
        
        # Initialize shared DeepL translator
        self._initialize_shared_deepl()
        
        # Set up hot reloader if enabled
        if os.environ.get('HOT_RELOAD_ENABLED') == '1':
            self.hot_reloader = HotReloader(self)

    def _load_shared_data(self, file_path: str, collection_type: str, attribute_name: str, lower: bool = False):
        """Generic method to load shared data from files at manager level
        
        Args:
            file_path: Path to the data file
            collection_type: Type of collection to use ('list' or 'set')
            attribute_name: Name of the attribute to store the data in
            lower: Whether to lowercase the data (default: False)
        """
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = [line.strip() for line in f if line.strip()]
                    if lower:
                        lines = [line.lower() for line in lines]
                    
                    # Convert to appropriate collection type
                    data = set(lines) if collection_type == "set" else lines
                    setattr(self, attribute_name, data)
                    
                logger.info(f"Loaded {len(data)} items from {file_path}")
            else:
                logger.error(f"File not found: {file_path}")
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")

    def _initialize_shared_deepl(self):
        """Initialize a shared DeepL translator instance for all bots to use"""
        try:
            # Get the DeepL API key from configuration
            # Use shared config manager to get the key
            if not self._shared_config_manager:
                self._shared_config_manager = ConfigManager(token=None)
            
            config = self._shared_config_manager._get_cached_config()
            api_key = config.get('deepl_api_key')
            
            if api_key and api_key != "YOUR-API-KEY-HERE":
                self.shared_deepl_translator = deepl.Translator(api_key)
                
                # Fetch and cache available languages
                self.deepl_source_languages = {lang.code: lang.name for lang in self.shared_deepl_translator.get_source_languages()}
                self.deepl_target_languages = {lang.code: lang.name for lang in self.shared_deepl_translator.get_target_languages()}
                
                logger.info("Shared DeepL translator initialized successfully")
                logger.info(f"Available target languages: {', '.join(self.deepl_target_languages.keys())}")
            else:                
                logger.warning("No DeepL API key set or using placeholder key, shared translator not initialized")
        except ImportError:
            logger.warning("DeepL library not installed, shared translator not initialized")
        except Exception as e:
            logger.error(f"Failed to initialize shared DeepL translator: {e}")

    def load_tokens(self) -> list[str]:
        """Load active tokens from config file using optimized ConfigManager.
        
        Returns:
            list[str]: List of valid, connected user tokens
        """
        try:
            # Use shared ConfigManager instance for optimized caching
            if not self._shared_config_manager:
                self._shared_config_manager = ConfigManager(token=None)
            
            config = self._shared_config_manager._get_cached_config()
            # Only return tokens for connected users
            return [
                token for token, settings in config.get('user_settings', {}).items()
                if settings.get('connected', True) and token in config.get('tokens', [])
            ]
        except Exception as e:
            logger.error(f"Error loading tokens: {e}")
            return []

    async def start_bot(self, token: str) -> None:
        """Start a single bot instance with proper rate limiting.
        
        Args:
            token (str): Discord user token to authenticate with
            
        Raises:
            Exception: If bot initialization or startup fails
        """
        if token not in self._instance_locks:
            self._instance_locks[token] = asyncio.Lock()
            
        async with self._instance_locks[token]:
            try:
                # Use shared ConfigManager instance for token validation
                if not self._shared_config_manager:
                    self._shared_config_manager = ConfigManager(token=None)
                
                if not await self._shared_config_manager.validate_token_api(token):
                    logger.warning(f"Skipping invalid token: {token[:10]}...")
                    return
                    
                bot = MyBot(token, manager=self)
                self.bots[token] = bot
                
                # Start bot task but wait for it to be ready before proceeding
                try:
                    task = asyncio.create_task(bot.start(token))
                    task.set_name(f"Bot_{token[:10]}")
                    self._cleanup_tasks.add(task)
                    task.add_done_callback(lambda t: self._handle_task_completion(token, t))
                    
                    
                except Exception as e:
                    logger.error(f"Error starting bot task for {token[:10]}: {e}")
                    if token in self.bots:
                        del self.bots[token]
                
                
            except Exception as e:
                logger.error(f"Failed to start bot {token[:10]}: {e}")
                if token in self.bots:
                    del self.bots[token]

    def _handle_task_completion(self, token: str, task: asyncio.Task) -> None:
        """Handle completion of bot tasks, including error cases.
        
        Args:
            token: Bot token for identification
            task: The completed asyncio task
        """
        try:
            self._cleanup_tasks.discard(task)
            
            # Check if task completed with exception
            if task.done() and not task.cancelled():
                try:
                    task.result()  # This will raise if there was an exception
                except CurlError as e:
                    logger.error(f"Bot {token[:10]} failed with CurlError: {e}")
                    # Only mark as disconnected for authentication failures
                    mark_disconnected = "Authentication" in str(e)
                    asyncio.create_task(self._cleanup_failed_bot(token, f"CurlError: {e}", mark_disconnected))
                except discord.LoginFailure as e:
                    logger.error(f"Bot {token[:10]} failed with LoginFailure: {e}")
                    # LoginFailure is always a token issue
                    asyncio.create_task(self._cleanup_failed_bot(token, f"LoginFailure: {e}", True))
                except Exception as e:
                    logger.error(f"Bot {token[:10]} failed with error: {e}")
                    # Don't mark as disconnected for general errors
                    asyncio.create_task(self._cleanup_failed_bot(token, f"General error: {e}", False))
        except Exception as e:
            logger.error(f"Error handling task completion: {e}")

    async def _cleanup_failed_bot(self, token: str, reason: str, mark_disconnected: bool = False) -> None:
        """Async cleanup for failed bot instances."""
        try:
            if token in self.bots:
                bot = self.bots[token]
                logger.info(f"Cleaning up failed bot {token[:10]} - {reason}")
                
                # Only mark as disconnected if explicitly requested (for actual token failures)
                if mark_disconnected and hasattr(bot, 'config_manager'):
                    try:
                        await bot.config_manager.reload_config_async()
                        await bot.config_manager.set_user_connected_async(bot.config_manager.uid, False)
                        logger.info(f"Marked token as disconnected for {token[:10]} due to: {reason}")
                    except Exception as e:
                        logger.error(f"Error marking bot as disconnected: {e}")
                elif hasattr(bot, 'config_manager'):
                    logger.info(f"Cleaning up {token[:10]} without marking as permanently disconnected")
                
                # Let the bot handle its own cleanup if it's not already closed
                if not bot.is_closed():
                    try:
                        await bot.close()
                    except Exception as e:
                        logger.error(f"Error during bot cleanup: {e}")
                
                # Remove from manager
                del self.bots[token]
                logger.info(f"Successfully cleaned up failed bot {token[:10]}")
                
        except Exception as e:
            logger.error(f"Error in async bot cleanup for {token[:10]}: {e}")

    async def start_all(self) -> None:
        """Start all bot instances concurrently.
        
        Initializes global database connection and starts all valid bot instances.
        Handles cleanup if initialization fails.
        
        Raises:
            Exception: If database initialization or bot startup fails
        """
        try:
            # Initialize global database FIRST, before any bot instances
            logger.info("Initializing global database connection...")
            from utils.database.global_manager import initialize_global_database
            await initialize_global_database()
            logger.info("Global database connection established")
            
            tokens = self.load_tokens()
            
            if not tokens:
                logger.error("No valid tokens found to start bots")
                return
        
            # Start all bots concurrently using asyncio.gather
            logger.info(f"Starting {len(tokens)} bot instances concurrently...")
            tasks = [self.start_bot(token) for token in tokens]
            await asyncio.gather(*tasks, return_exceptions=True)
                
            logger.info(f"Started {len(self.bots)} bot instances")
            
            # Start hot reload monitor if enabled
            if self.hot_reloader:
                self.hot_reloader.start_monitoring()
            
        except Exception as e:
            logger.error(f"Error in start_all: {e}")
            await self.cleanup()
            raise

    async def cleanup(self) -> None:
        """Graceful shutdown of all bot instances and cleanup of resources.
        
        This method ensures all bot instances are properly closed,
        ongoing tasks are cancelled, and database connections are terminated.
        """
        logger.info("Starting cleanup...")
        
        # Stop hot reload monitoring if active
        if self.hot_reloader:
            self.hot_reloader.stop_monitoring()
            
        tasks = []
        
        # Close all bot instances
        for bot in self.bots.values():
            if not bot.is_closed():
                tasks.append(asyncio.create_task(bot.close()))
        
        # Cancel any remaining tasks
        for task in self._cleanup_tasks:
            task.cancel()
          # Wait for all cleanup tasks to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
        # Clean up shared config manager
        if self._shared_config_manager:
            self._shared_config_manager.cleanup()
            self._shared_config_manager = None
        
        # Clean up all config manager caches
        ConfigManager.cleanup_all_caches()
        
        # Shutdown global database connection
        try:
            logger.info("Shutting down global database connection...")
            from utils.database.global_manager import shutdown_global_database
            await shutdown_global_database()
            logger.info("Global database connection closed")
        except Exception as e:
            logger.error(f"Error shutting down global database: {e}")
        
        logger.info("Cleanup completed")

class MyBot(commands.Bot):
    """Custom Discord bot implementation with enhanced functionality."""

    def __init__(self, token: str, manager: BotManager = None, *args, **kwargs):
        self.token: str = token
        self.config_manager: ConfigManager = ConfigManager(token)
        self._manager: BotManager = manager
        self.config_manager._bot_ref = self
        self.db: DatabaseManager = None

        self.current_browser = "Discord Web"
        self.context: ssl.SSLContext = self._create_ssl_context()
        
        # Safe Prefix
        prefix = ">"
        try:
            prefix = getattr(self.config_manager, 'command_prefix', '>')
        except:
            pass

        # --- FIX FOR 1.7.3 INTENTS & FLAGS ---
        # We must define intents for the flags to work
        intents = discord.Intents.default()
        intents.members = True 

        super().__init__(
            command_prefix=prefix,
            self_bot=True,
            intents=intents, # Add this
            # Set online=False to fix the error you just got
            member_cache_flags=discord.MemberCacheFlags(joined=True, voice=True, online=False),
            sync_presence=False,
            *args, **kwargs
        )

        # Android browser patch
        original = self._connection._update_references
        def update_references(*args, **kwargs):
            if hasattr(args[0], '_headers') and hasattr(args[0]._headers, 'super_properties'):
                args[0]._headers.super_properties["browser"] = self.current_browser
            original(*args, **kwargs)
        self._connection._update_references = update_references

        try:
            self.auto_delete = getattr(self.config_manager, 'auto_delete', False)
        except:
            self.auto_delete = False
            
        self.start_time = time.time()
        self._last_heartbeat_send = 0.0
        self._last_heartbeat_ack = 0.0
        self._last_sequence = None
        self._watchdog_task: Optional[asyncio.Task] = None

    async def start(self, token, *, reconnect=True):
        """Override start method to handle errors gracefully and coordinate with event loop."""
        try:
            await super().start(token, reconnect=reconnect)
        except CurlError as e:
            logger.error(f"CurlError during bot start for UID {self.config_manager.uid}: {e}")
            # Check if it's a token-related error
            if "Authentication" in str(e):
                logger.error(f"Token became invalid during start for UID: {self.config_manager.uid}")
                await self.handle_invalid_token(mark_permanently_disconnected=True)
            elif any(err_str in str(e) for err_str in ["Broken pipe", "Send failure", "WS_SEND"]):
                logger.error(f"Network error during start for UID: {self.config_manager.uid}, not marking as invalid token")
                await self.handle_invalid_token(mark_permanently_disconnected=False)
                # Don't re-raise here, let the task completion handler manage cleanup
                return
            else:
                # For other CurlErrors, try to reconnect once before giving up
                logger.warning(f"Non-token related CurlError for UID {self.config_manager.uid}, attempting recovery: {e}")
                await asyncio.sleep(2)  # Brief delay before retry
                raise  # Let the library's reconnect logic handle this
        except discord.LoginFailure as e:
            logger.error(f"LoginFailure during bot start for UID {self.config_manager.uid}: {e}")
            await self.handle_invalid_token()
            return  # Don't re-raise, cleanup handled
        except Exception as e:
            logger.error(f"Unexpected error during bot start for UID {self.config_manager.uid}: {e}")
            raise

    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create a secure SSL context for API connections.
        
        Returns:
            ssl.SSLContext: Configured SSL context using TLS 1.3
        """
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        return context

    async def update_browser_property(self, new_browser: str) -> bool:
        """Update the browser property in the client's connection.
        
        Args:
            new_browser (str): New browser value to set (e.g. "Discord Android", "Discord Embedded")
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Update the instance variable
            self.current_browser = new_browser
            
            # Remember current status and activities to restore after reconnect
            current_status = self.status
            current_activities = self.activities
            
            # The monkey-patched method will use the updated self.current_browser
            # on the next reconnect
            logger.info(f"Updated browser property to: {new_browser}")
            
            # Force a reconnection to apply the change
            if self.ws and self.ws.open:
                logger.info("Closing WebSocket connection to apply browser property change...")
                await self.ws.close(code=1000)
                
            # Wait for reconnection
            await asyncio.sleep(2)
            
            # Restore presence after reconnection
            await self.change_presence(status=current_status, activities=current_activities)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update browser property: {e}")
            traceback.print_exc()
            return False

    async def GetUser(self, user_id: str | int) -> discord.User | None:
        """Fetch a Discord user by their ID.
        
        Args:
            user_id (Union[str, int]): Discord user ID to lookup
            
        Returns:
            Optional[discord.User]: User object if found, None otherwise
        """
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.context)) as session:
                request = await session.get(
                    discord.http.Route('GET', '/users/{user_id}', user_id=user_id).url,
                    headers={
                        "Authorization": self.http.token,
                        "User-Agent": self.http.user_agent,
                        "X-Super-Properties": self.http.headers.encoded_super_properties
                    }
                )
                
                if request.status == 200:
                    data = await request.json()
                    return discord.User(state=self._connection, data=data)
                return None
                
        except aiohttp.ClientError as e:
            logger.error(f"Failed to fetch user {user_id}: {e}")
            return None


    def patch_ws_connect(self):
        """Patch the HTTP session's ws_connect method to use aiohttp instead of curl_cffi."""
        try:
            session = self.http._HTTPClient__session
            original_ws_connect = getattr(session, 'ws_connect', None)
            
            async def patched_ws_connect(*args, **kwargs):
                uuid = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
                logger.debug("Connecting to websocket via aiohttp... | %s", uuid)
                start = time.perf_counter()
                
                try:
                    # Use our custom ws_connect function instead of the session's method
                    logger.debug("Calling ws_connect with args: %r..., kwargs keys: %s", args[:1], list(kwargs.keys()))
                    ws = await ws_connect(*args, **kwargs)
                    
                    duration = time.perf_counter() - start
                    logger.debug("Connected to websocket via aiohttp in %.2f seconds | %s", duration, uuid)
                    logger.debug("Returning websocket type: %s | %r", type(ws), ws)
                    return ws
                except Exception as e:
                    logger.debug("WebSocket connection failed (%s): %s: %s", uuid, type(e).__name__, e, exc_info=True)
                    # If our custom connection fails, we could fallback to original if available
                    # But for now, re-raise the error
                    raise

            session.ws_connect = patched_ws_connect
            logger.info("WebSocket connect patched to use aiohttp instead of curl_cffi.")
        except Exception as e:
            logger.error("Failed to patch ws_connect: %s", e, exc_info=True)

    async def setup_hook(self) -> None:
        try:
            # DO NOT call patch_ws_connect here
            
            logger.info(f"Connecting to global database for instance...")
            self.db = DatabaseManager()
            await self.db.initialize()
            
            await self.http.static_login(self.token)
            
            # Initialize configurations
            await self.config_manager.update_username_async(self.http.token, self.user.name)
            await detect_message_limit(self)
            
            await self.load_cogs()
        except discord.LoginFailure:
            logger.error(f"Invalid token detected")
            await self.handle_invalid_token()
            return
        except Exception as e:
            logger.error(f"Error in setup: {e}")
            # If database initialization fails, don't crash the entire bot
            if "database" in str(e).lower() or "mongo" in str(e).lower():
                logger.warning(f"Database error during setup - continuing without database for UID: {self.config_manager.uid}")
                self.db = None
                # Try to continue with other setup steps
                try:
                    await self.load_cogs()
                except Exception as cog_error:
                    logger.error(f"Failed to load cogs after database error: {cog_error}")
                    raise
            else:
                raise

    async def close(self) -> None:
        """Perform cleanup operations when bot is shutting down.
        
        Cancels all running tasks, unloads cogs, and closes connections.
        Ensures graceful shutdown even if errors occur.
        """
        logger.info("Starting bot shutdown...")
        try:
            # Mark as closed first to prevent new operations
            self._closed = True
            
            # Cancel watchdog first
            if self._watchdog_task and not self._watchdog_task.done():
                self._watchdog_task.cancel()
                try:
                    await self._watchdog_task
                except asyncio.CancelledError:
                    pass

            # Cancel any event tasks first
            tasks = [t for t in asyncio.all_tasks() 
                    if t is not asyncio.current_task() and
                    t._coro.__name__.startswith('_run_event') and 
                    t.get_coro().cr_frame.f_locals.get('self') == self]
                    
            for task in tasks:
                task.cancel()
                
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    
            # Unload all cogs before closing DB
            for cog_name in list(self.cogs.keys()):
                try:
                    await self.unload_extension(f"cogs.{cog_name}")
                    logger.info(f"Unloaded cog: {cog_name}")
                except Exception as e:
                    logger.error(f"Error unloading cog {cog_name}: {e}")
    
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        finally:            # Close database adapter for this instance only
            if self.db:
                try:
                    await self.db.close()
                    logger.info(f"Closed database adapter for instance {self.user.id if self.user else 'unknown'}")
                except Exception as e:
                    logger.error(f"Error closing database adapter: {e}")

            # Clean up config manager for this instance
            if hasattr(self, 'config_manager') and self.config_manager:
                self.config_manager.cleanup()

            # Call parent close method
            try:
                await super().close()
            except Exception as e:
                logger.error(f"Error in parent close: {e}")
    
        logger.info("Bot shutdown complete")

    async def _gateway_watchdog(self) -> None:
        """Periodically check gateway liveness and force reconnect if heartbeats stall."""
        HEARTBEAT_STALL_SECONDS = 90
        CHECK_INTERVAL = 30
        try:
            while not self.is_closed():
                await asyncio.sleep(CHECK_INTERVAL)
                now = time.time()
                # If no ACK for a long time while websocket is open, force a clean reconnect
                if self.ws and getattr(self.ws, 'open', False):
                    last_ack = self._last_heartbeat_ack or 0
                    last_send = self._last_heartbeat_send or 0
                    stalled_by_ack = last_ack and (now - last_ack) > HEARTBEAT_STALL_SECONDS
                    stalled_no_ack_after_send = last_send and (last_ack < last_send) and (now - last_send) > HEARTBEAT_STALL_SECONDS
                    if stalled_by_ack or stalled_no_ack_after_send:
                        lag = int(now - (last_ack or last_send))
                        logger.warning(f"Gateway heartbeat stalled ({lag}s). Forcing reconnect.")
                        try:
                            await self.ws.close(code=1011, message=b'watchdog reconnect')
                        except Exception as e:
                            logger.debug(f"Watchdog close error: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Watchdog task error: {e}")

    async def load_cogs(self) -> None:
        """Load all cogs from the cogs directory with EventManager first.
        
        Ensures EventManager is loaded before other cogs since it's a dependency.
        Logs success/failure for each cog loading attempt.
        
        Returns:
            None: Returns early if EventManager fails to load
        """
        # First load EventManager
        try:
            await self.load_extension("utils.services.event_manager")
            logger.info("Loaded EventManager")
        except Exception as e:
            logger.error(f"Failed to load EventManager: {str(e)}")
            traceback.print_exc()
            return  # Don't load other cogs if EventManager fails

        # Then load all other cogs
        cogs_dir = "cogs"
        for filename in os.listdir(cogs_dir):
            if filename.endswith(".py"):
                try:
                    cog_name = f"cogs.{filename[:-3]}"
                    await self.load_extension(cog_name)
                    logger.info(f"Loaded extension: {cog_name}")
                except Exception as e:
                    logger.error(f"Failed to load extension {filename}: {str(e)}")
                    traceback.print_exc()

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: Exception) -> None:
        if isinstance(error, discord.errors.LoginFailure) or "Authentication failed" in str(error):
            logger.error(f"Token became invalid for UID: {self.config_manager.uid}")
            await self.handle_invalid_token()
        elif isinstance(error, CurlError):
            logger.error(f"CurlError in command for UID {self.config_manager.uid}: {error}")
            # Check if it's a connection failure that indicates invalid token
            if "Authentication" in str(error):
                logger.error(f"Token became invalid due to CurlError for UID: {self.config_manager.uid}")
                await self.handle_invalid_token(mark_permanently_disconnected=True)
            elif "Broken pipe" in str(error) or "Send failure" in str(error):
                logger.error(f"Network error in command for UID: {self.config_manager.uid}, not marking as invalid token")
                await self.handle_invalid_token(mark_permanently_disconnected=False)

    @commands.Cog.listener() 
    async def on_error(self, event: str, *args: any, **kwargs: any) -> None:
        """Handle websocket/gateway errors.
        
        Args:
            event: The name of the event that raised the error
            args: Positional arguments that were passed to the event
            kwargs: Keyword arguments that were passed to the event
        """
        error = sys.exc_info()[1]
        if isinstance(error, (discord.GatewayNotFound, discord.ConnectionClosed)):
            if str(error).startswith('Authentication failed'):
                logger.error(f"Token became invalid for UID: {self.config_manager.uid}")
                await self.handle_invalid_token()
                return
        elif isinstance(error, CurlError):
            logger.error(f"CurlError in event '{event}' for UID {self.config_manager.uid}: {error}")
            # Check if it's a connection failure that indicates invalid token
            if "Authentication" in str(error):
                logger.error(f"Token became invalid due to CurlError for UID: {self.config_manager.uid}")
                await self.handle_invalid_token(mark_permanently_disconnected=True)
            elif "Broken pipe" in str(error) or "Send failure" in str(error) or "WS_SEND" in str(error):
                logger.error(f"Network error in event for UID: {self.config_manager.uid}, not marking as invalid token")
                await self.handle_invalid_token(mark_permanently_disconnected=False)
                return
            
    async def handle_invalid_token(self, mark_permanently_disconnected: bool = True) -> None:
        """Handle invalid token by gracefully stopping the instance and cleaning up.
        
        Args:
            mark_permanently_disconnected: If True, marks token as connected=false in config.
                                         If False, only closes connection (for temporary issues).
        
        Performs coordinated cleanup when a token becomes invalid:
        - Signals to stop all bot activities
        - Gracefully closes connections
        - Updates configuration (only if permanently disconnected)
        - Lets the task completion handler manage final cleanup
        """
        if self._closed:
            return  # Already handling or handled
            
        try:
            if mark_permanently_disconnected:
                logger.warning(f"Handling invalid token for UID: {self.config_manager.uid}")
            else:
                logger.warning(f"Handling connection error for UID: {self.config_manager.uid} (not marking as permanently disconnected)")
            
            # Mark as closed first to prevent new operations
            self._closed = True
            
            # Only mark as disconnected in config if this is a permanent token issue
            if mark_permanently_disconnected:
                try:
                    await self.config_manager.reload_config_async()
                    await self.config_manager.set_user_connected_async(self.config_manager.uid, False)
                    logger.info(f"Marked token as permanently disconnected for UID: {self.config_manager.uid}")
                except Exception as e:
                    logger.error(f"Error updating connection status: {e}")
            else:
                logger.info(f"Closing connection for UID {self.config_manager.uid} without marking as permanently disconnected")
            
            # Signal all cogs that we're shutting down - unload them gracefully
            cog_names = list(self.cogs.keys())
            for cog_name in cog_names:
                try:
                    await self.unload_extension(f"cogs.{cog_name}")
                    logger.debug(f"Unloaded cog: {cog_name}")
                except Exception as e:
                    logger.debug(f"Error unloading cog {cog_name}: {e}")
            
            # Close database connection for this instance
            if self.db:
                try:
                    await self.db.close()
                    self.db = None
                    logger.info(f"Closed database adapter for invalid token UID: {self.config_manager.uid}")
                except Exception as e:
                    logger.error(f"Error closing database adapter: {e}")
            
            # Clean close of WebSocket - use proper close code
            if hasattr(self, 'ws') and self.ws and not self.ws.closed:
                try:
                    # Use a more appropriate close code for authentication failure
                    await self.ws.close(code=1000, message=b'Authentication invalid')
                    logger.info(f"Closed WebSocket gracefully for invalid token UID: {self.config_manager.uid}")
                except Exception as e:
                    logger.debug(f"Error closing WebSocket: {e}")
            
            # Let the main event loop and task completion handler manage the final cleanup
            # This approach is more coordinated with the overall bot lifecycle
            logger.info(f"Completed invalid token cleanup for UID: {self.config_manager.uid}")
                    
        except Exception as e:
            logger.error(f"Error handling invalid token: {e}")
            traceback.print_exc()
        finally:
            # Ensure bot is marked as closed
            self._closed = True

    async def on_socket_raw_send(self, data):
        # data can be str or bytes
        try:
            import json as _json
            payload = _json.loads(data) if isinstance(data, str) else _json.loads(data.decode("utf-8", errors="ignore"))
            op = payload.get("op")
            if op == 1:  # HEARTBEAT
                self._last_heartbeat_send = time.time()
                self._last_sequence = payload.get("d")
                logging.getLogger('discord.gateway').debug(
                    "[hb] send seq=%s t=%.3f", self._last_sequence, self._last_heartbeat_send
                )
        except Exception:
            pass

    async def on_socket_raw_receive(self, data):
        try:
            import json as _json
            payload = _json.loads(data) if isinstance(data, str) else _json.loads(data.decode("utf-8", errors="ignore"))
            op = payload.get("op")
            if op == 11:  # HEARTBEAT ACK
                self._last_heartbeat_ack = time.time()
                lag = self._last_heartbeat_ack - (self._last_heartbeat_send or self._last_heartbeat_ack)
                logging.getLogger('discord.gateway').debug(
                    "[hb] ack dt=%.3fs last_seq=%s", max(lag, 0.0), self._last_sequence
                )
        except Exception:
            pass

async def main() -> None:
    bot_manager = BotManager()
    
    try:
        await bot_manager.start_all()
        
        # Keep running and handle signals
        stop = asyncio.Event()
        
        # Windows-friendly signal handling
        if os.name != 'nt':  # 'nt' means Windows
            loop = asyncio.get_running_loop()
            def signal_handler():
                logger.info("Received shutdown signal")
                stop.set()
                
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, signal_handler)
        else:
            logger.info("Running on Windows: Use Ctrl+C to shut down.")

        # This will keep the script alive until interrupted
        while not stop.is_set():
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except KeyboardInterrupt:
                break
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        traceback.print_exc()
    finally:
        logger.info("Initiating shutdown sequence")
        await bot_manager.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
