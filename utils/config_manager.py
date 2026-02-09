import json
import asyncio
import base64
import os
import tempfile
import shutil
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Set
from types import SimpleNamespace
import aiofiles
import aiohttp
import re
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

@dataclass
class CacheEntry:
    """Cache entry with timestamp for cleanup"""
    data: dict
    timestamp: float = field(default_factory=time.time)
    dirty: bool = False

@dataclass
class AutoDeleteConfig:
    enabled: bool = True
    delay: int = 120

@dataclass
class UserConfig:
    token: str
    command_prefix: str = ";"
    leakcheck_api_key: str = ""
    auto_delete: AutoDeleteConfig = field(default_factory=lambda: AutoDeleteConfig(enabled=True, delay=120))
    presence: dict = field(default_factory=dict)
    connected: bool = True
    uid: Optional[int] = None
    discord_id: Optional[int] = None
    username: Optional[str] = None
    nitro_sniper: dict = field(default_factory=dict)
    running_commands: set = field(default_factory=set)

@dataclass
class UserSettings:
    command_prefix: str = ";"
    auto_delete: dict = field(default_factory=lambda: {'enabled': True, 'delay': 120})
    presence: dict = None
    discord_id: Optional[int] = None
    uid: Optional[int] = None
    username: Optional[str] = None
    nitro_sniper: dict = None

class ConfigManager:
    def fix_all_uids(self) -> bool:
        """Reassign UIDs to fill gaps, keeping developers at negative/0 UIDs, and compact others from 1 up."""
        try:
            config = self._get_cached_config()
            user_settings = config.get('user_settings', {})
            
            # First, fix any missing discord_ids
            self._fix_missing_discord_ids(config)
            
            # Separate developer and regular users
            dev_uid = 0
            dev_tokens = []
            other_tokens = []
            
            # Get developer ID for comparison
            developer_ids = self._get_developer_ids(config)
            
            # Process each token and categorize
            for token, settings in list(user_settings.items()):
                if not token or not isinstance(token, str) or token == 'null':
                    # Remove invalid tokens
                    del user_settings[token]
                    continue
                    
                # Check if this is the developer token
                discord_id = settings.get('discord_id')
                if discord_id in developer_ids:
                    dev_tokens.append(token)
                else:
                    other_tokens.append(token)
            
            # Sort other tokens by their current UIDs to maintain some consistency
            other_tokens.sort(key=lambda t: user_settings[t].get('uid', 999))
            
            # Assign UIDs compactly starting from 1 for non-dev users
            next_uid = 1
            changed = False
            
            for token in other_tokens:
                if not token or not isinstance(token, str):
                    continue
                if user_settings[token].get('uid') != next_uid:
                    user_settings[token]['uid'] = next_uid
                    changed = True
                    logger.info(f"Reassigned UID {next_uid} to user {user_settings[token].get('username', 'unknown')}")
                next_uid += 1
            
            # Ensure all dev tokens have proper developer UIDs
            for token in dev_tokens:
                if not token or not isinstance(token, str):
                    continue
                if user_settings[token].get('uid') != dev_uid:
                    user_settings[token]['uid'] = dev_uid
                    changed = True
                    logger.info(f"Set developer UID {dev_uid} for user {user_settings[token].get('username', 'unknown')}")
            
            if changed:
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                self._safe_write_to_file(self.config_path, config)
                logger.info(f"Fixed UIDs for {len(user_settings)} users (dev: {len(dev_tokens)}, others: {len(other_tokens)})")
                
            self._update_uid_tracking(config)
            return changed
        except Exception as e:
            logger.error(f"Error fixing all UIDs: {e}")
            return False
    # Class-level variables for optimized management
    _locks: Dict[str, asyncio.Lock] = {}
    _config_cache: Dict[str, CacheEntry] = {}
    _token_update_cache = {}
    _cache_lock = asyncio.Lock()
    _uid_counter: int = 1
    _used_uids: Set[int] = set()
    
    # Cache cleanup settings
    CACHE_CLEANUP_INTERVAL = 300  # 5 minutes
    CACHE_MAX_AGE = 1800  # 30 minutes
    
    def __init__(self, token: str = None, config_path: str = 'config.json'):
        self.token = token
        self.config_path = config_path
        self._last_cleanup = time.time()
        # Initialize async lock for this token
        if token and token not in self._locks:
            self._locks[token] = asyncio.Lock()
        # Load initial config
        self._load_initial_config()
        # Initialize instance variables
        self.name = None
        self.developer_ids = []
        self.version = '1.3'
        self._developer_name = None
        # Load and cache config
        self._sync_load_config()
        # Comprehensive validation and repair on every selfbot start
        self.validate_and_repair_config()
        # Migrate developer UIDs if needed (one-time migration)
        self.migrate_developer_uids()

    def _get_developer_ids(self, config):
        """Get developer IDs supporting both old and new format"""
        # Support new developer_ids array format
        if 'developer_ids' in config:
            return [int(dev_id) for dev_id in config['developer_ids'] if dev_id]
        # Fallback to old developer_id format
        elif 'developer_id' in config and config['developer_id']:
            return [int(config['developer_id'])]
        return []

    def _load_initial_config(self):
        """Load initial config and assign proper developer UIDs"""
        try:
            with open(self.config_path) as f:
                config = json.load(f)
                
            # Cache the initial config
            self._config_cache[self.config_path] = CacheEntry(data=config.copy())
            
            if self.token and self.token in config.get('tokens', []):
                try:
                    user_id = int(base64.b64decode(self.token.split('.')[0] + "==").decode('utf-8'))
                    # Support both old developer_id and new developer_ids format
                    developer_ids = self._get_developer_ids(config)
                    if user_id in developer_ids:
                        if self.token in config.get('user_settings', {}):
                            config['user_settings'][self.token]['uid'] = self._get_developer_uid(user_id, developer_ids)
                            self._safe_write_to_file(self.config_path, config)
                            # Update cache
                            self._config_cache[self.config_path].data = config.copy()
                            self._config_cache[self.config_path].timestamp = time.time()
                except Exception as e:
                    logger.error(f"Error processing developer token: {e}")
        except Exception as e:
            logger.error(f"Error loading initial config: {e}")
            # Create empty cache entry if file doesn't exist
            self._config_cache[self.config_path] = CacheEntry(data={
                'tokens': [],
                'user_settings': {},
                'name': 'Selfbot',
                'developer_ids': ['0'],
                'version': '1.3'
            })

    def _sync_load_config(self):
        """Synchronous config loading for initialization"""
        try:
            config = self._get_cached_config()
            
            # Load global settings
            self.name = config.get('name', 'Selfbot')
            self.developer_ids = self._get_developer_ids(config)
            self.version = config.get('version', '1.3')
            
            # Initialize user_settings if missing
            if 'user_settings' not in config:
                config['user_settings'] = {}
                self._mark_cache_dirty()

            # Update UID tracking first
            self._update_uid_tracking(config)
            
            # Clean up invalid tokens
            self.cleanup_invalid_tokens()
            
            # Fix missing discord IDs and null UIDs
            self.fix_null_uids()
            
            # Initialize or migrate user settings
            self._initialize_or_migrate_user(config)
            
            # Fix all UIDs to ensure proper sequencing
            self.fix_all_uids()
            
            # Load final config
            self.load_config()
        except Exception as e:
            logger.error(f"Error in sync_load_config: {e}")

    @property
    def developer_id(self):
        """Backward compatibility property - returns first developer ID"""
        return str(self.developer_ids[0]) if self.developer_ids else '0'
    
    def is_developer(self, user_id: int) -> bool:
        """Check if a user ID is in the developer list"""
        return user_id in self.developer_ids
    
    def is_developer_uid(self, uid: int) -> bool:
        """Check if a UID belongs to a developer (0 or negative)"""
        return uid <= 0
    
    def migrate_developer_uids(self) -> bool:
        """Migrate existing config to use new developer UID system
        
        This function should be called once to update existing configs where
        multiple developers might have uid=0 from the old system.
        
        Returns:
            bool: True if migration was performed, False if no changes needed
        """
        try:
            config = self._get_cached_config()
            user_settings = config.get('user_settings', {})
            developer_ids = self._get_developer_ids(config)
            
            if not developer_ids:
                logger.info("No developers found, skipping UID migration")
                return False
            
            changes_made = False
            developer_tokens = []
            
            # Find all tokens belonging to developers
            for token, settings in user_settings.items():
                discord_id = settings.get('discord_id')
                if discord_id and int(discord_id) in developer_ids:
                    developer_tokens.append((token, discord_id, settings))
            
            # Update developer UIDs using new system
            for token, discord_id, settings in developer_tokens:
                correct_uid = self._get_developer_uid(discord_id, developer_ids)
                current_uid = settings.get('uid')
                
                if current_uid != correct_uid:
                    settings['uid'] = correct_uid
                    changes_made = True
                    logger.info(f"Migrated developer UID: {settings.get('username', 'unknown')} "
                              f"from UID {current_uid} to UID {correct_uid}")
            
            if changes_made:
                self._safe_write_to_file(self.config_path, config)
                # Update cache
                self._config_cache[self.config_path].data = config.copy()
                self._config_cache[self.config_path].timestamp = time.time()
                logger.info(f"Developer UID migration completed - updated {len(developer_tokens)} developer accounts")
                return True
            else:
                logger.info("Developer UID migration not needed - all UIDs already correct")
                return False
                
        except Exception as e:
            logger.error(f"Error during developer UID migration: {e}")
            return False
    
    def add_developer(self, user_id: int):
        """Add a developer ID to the list"""
        if user_id not in self.developer_ids:
            self.developer_ids.append(user_id)
            self._update_developer_ids_in_config()
            self._refresh_all_instances()
    
    def remove_developer(self, user_id: int):
        """Remove a developer ID from the list"""
        if user_id in self.developer_ids:
            self.developer_ids.remove(user_id)
            self._update_developer_ids_in_config()
            self._refresh_all_instances()
    
    def _update_developer_ids_in_config(self):
        """Update the developer_ids in the config file"""
        try:
            config = self._get_cached_config()
            config['developer_ids'] = [str(dev_id) for dev_id in self.developer_ids]
            self._safe_write_to_file(self.config_path, config)
            self._config_cache[self.config_path].data = config.copy()
            self._config_cache[self.config_path].timestamp = time.time()
        except Exception as e:
            logger.error(f"Error updating developer IDs in config: {e}")
    
    def _refresh_all_instances(self):
        """Refresh developer_ids across all bot instances"""
        try:
            # Reload our own developer_ids from config
            config = self._get_cached_config()
            new_developer_ids = self._get_developer_ids(config)
            self.developer_ids = new_developer_ids
            
            # If we have access to bot manager, update all instances
            if hasattr(self, '_bot_ref') and self._bot_ref and hasattr(self._bot_ref, '_manager'):
                bot_manager = self._bot_ref._manager
                for token, bot_instance in bot_manager.bots.items():
                    if hasattr(bot_instance, 'config_manager'):
                        bot_instance.config_manager.developer_ids = new_developer_ids.copy()
                        logger.debug(f"Updated developer_ids for bot instance {bot_instance.user.id if bot_instance.user else 'unknown'}")
                logger.info(f"Refreshed developer_ids across {len(bot_manager.bots)} bot instances")
            else:
                logger.info("Refreshed developer_ids for current instance only")
        except Exception as e:
            logger.error(f"Error refreshing instances: {e}")
    
    def refresh_developer_ids(self):
        """Public method to manually refresh developer IDs across all instances"""
        self._refresh_all_instances()

    def _get_cached_config(self) -> dict:
        """Get config from cache or load from file"""
        cache_entry = self._config_cache.get(self.config_path)
        
        if cache_entry is None or time.time() - cache_entry.timestamp > self.CACHE_MAX_AGE:
            # Cache miss or expired, reload from file
            try:
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                self._config_cache[self.config_path] = CacheEntry(data=config.copy())
            except Exception as e:
                logger.error(f"Error loading config from file: {e}")
                # Return cached data if available, otherwise empty config
                if cache_entry:
                    return cache_entry.data
                return {'tokens': [], 'user_settings': {}}
        
        return self._config_cache[self.config_path].data.copy()

    async def _get_cached_config_async(self) -> dict:
        """Async version of _get_cached_config"""
        async with self._cache_lock:
            cache_entry = self._config_cache.get(self.config_path)
            
            if cache_entry is None or time.time() - cache_entry.timestamp > self.CACHE_MAX_AGE:
                try:
                    async with aiofiles.open(self.config_path, 'r') as f:
                        content = await f.read()
                        config = json.loads(content)
                    self._config_cache[self.config_path] = CacheEntry(data=config.copy())
                except Exception as e:
                    logger.error(f"Error loading config from file: {e}")
                    if cache_entry:
                        return cache_entry.data.copy()
                    return {'tokens': [], 'user_settings': {}}
            
            return self._config_cache[self.config_path].data.copy()

    def _mark_cache_dirty(self):
        """Mark cache as dirty (needs to be written to file)"""
        cache_entry = self._config_cache.get(self.config_path)
        if cache_entry:
            cache_entry.dirty = True
            cache_entry.timestamp = time.time()

    def _update_uid_tracking(self, config: dict):
        """Update UID tracking for efficient allocation"""
        self._used_uids.clear()
        max_uid = 0
        
        for token, settings in config.get('user_settings', {}).items():
            if not token or not isinstance(token, str) or token == 'null':
                continue
            uid = settings.get('uid')
            if uid is not None:
                self._used_uids.add(uid)
                max_uid = max(max_uid, uid)
        
        # Reserve negative UIDs for developers
        developer_ids = self._get_developer_ids(config)
        for i in range(len(developer_ids)):
            self._used_uids.add(-i)  # Reserve -0, -1, -2, etc.
        self._uid_counter = max_uid + 1
        
        logger.debug(f"Updated UID tracking: {len(self._used_uids)} UIDs in use, max: {max_uid}")

    def _get_developer_uid(self, user_id: int, developer_ids: list) -> int:
        """Get UID for a developer based on their position in developer_ids list
        
        Args:
            user_id: Discord user ID of the developer
            developer_ids: List of developer IDs from config
            
        Returns:
            int: Developer UID (0 for primary, negative for others)
        """
        try:
            index = developer_ids.index(user_id)
            if index == 0:
                return 0  # Primary developer gets UID 0
            else:
                return -index  # Secondary developers get negative UIDs
        except ValueError:
            # Fallback if user_id not found in list
            logger.warning(f"Developer ID {user_id} not found in developer_ids list")
            return 0

    async def _cleanup_cache(self):
        """Clean up old cache entries"""
        current_time = time.time()
        if current_time - self._last_cleanup < self.CACHE_CLEANUP_INTERVAL:
            return
            
        async with self._cache_lock:
            # Clean up token update cache
            expired_users = [
                user_id for user_id, data in self._token_update_cache.items()
                if current_time - data.get('timestamp', 0) > self.CACHE_MAX_AGE
            ]
            for user_id in expired_users:
                del self._token_update_cache[user_id]
            
            self._last_cleanup = current_time

    @asynccontextmanager
    async def _config_context(self):
        """Context manager for safe config operations"""
        if not self.token:
            raise ValueError("Token is required for config operations")
            
        lock = self._locks.setdefault(self.token, asyncio.Lock())
        async with lock:
            await self._cleanup_cache()
            try:
                config = await self._get_cached_config_async()
                yield config
            finally:
                # Auto-save if dirty
                cache_entry = self._config_cache.get(self.config_path)
                if cache_entry and cache_entry.dirty:
                    await self._safe_write_to_file_async(self.config_path, cache_entry.data)
                    cache_entry.dirty = False

    def _get_next_uid(self, config: dict = None) -> int:
        """Get the next available UID, filling gaps from 1 onwards (0 is reserved for developer)"""
        if config is None:
            config = self._get_cached_config()
            
        # Collect all currently used UIDs
        used_uids = set()
        for settings in config.get('user_settings', {}).values():
            uid = settings.get('uid')
            if uid is not None:
                used_uids.add(uid)
        
        # Always reserve 0 for developer
        used_uids.add(0)
        
        # Find the first available UID starting from 1
        candidate = 1
        while candidate in used_uids:
            candidate += 1
            
        # Update the class tracking
        self._used_uids = used_uids
        self._used_uids.add(candidate)
        
        return candidate


    def _initialize_or_migrate_user(self, config):
        """Optimized user initialization with strict UID enforcement"""
        needs_save = False
        user_id = None
        # Only try to extract user_id if self.token is a valid string
        if self.token and isinstance(self.token, str):
            try:
                user_id = int(base64.b64decode(self.token.split('.')[0] + "==").decode('utf-8'))
            except Exception as e:
                logger.error(f"Error extracting user ID from token: {e}")
        else:
            logger.error("Error extracting user ID from token: token is None or not a string")

        # Check for cached settings from token update
        is_token_update = False
        if user_id and user_id in self._token_update_cache:
            is_token_update = True
            cached_settings = self._token_update_cache[user_id]
            logger.info(f"Found cached settings for user {user_id} during initialization")
            if self.token in config.get('tokens', []) and self.token not in config.get('user_settings', {}):
                # Always assign UID if missing
                if 'uid' not in cached_settings or cached_settings['uid'] is None:
                    developer_ids = self._get_developer_ids(config)
                    if user_id in developer_ids:
                        cached_settings['uid'] = self._get_developer_uid(user_id, developer_ids)
                    else:
                        cached_settings['uid'] = self._get_next_uid()
                config['user_settings'][self.token] = cached_settings.copy()
                logger.info(f"Restored cached settings for user {user_id}")
                needs_save = True
            del self._token_update_cache[user_id]

        # Fallback check for token update
        if not is_token_update and user_id:
            for other_token, settings in config.get('user_settings', {}).items():
                if (other_token != self.token and settings.get('discord_id') == user_id):
                    is_token_update = True
                    logger.info(f"Found existing settings for discord_id {user_id} under different token")
                    break

        # Handle new users
        if self.token and self.token in config.get('tokens', []) and self.token not in config.get('user_settings', {}) and not is_token_update:
            developer_ids = self._get_developer_ids(config)
            is_developer = self.token in config.get('tokens', []) and user_id in developer_ids if user_id else False
            if is_developer:
                assigned_uid = self._get_developer_uid(user_id, developer_ids)
            else:
                assigned_uid = self._get_next_uid()
            default_settings = {
                'uid': assigned_uid,
                'discord_id': user_id,
                'username': None,
                'command_prefix': config.get('command_prefix', '-'),
                'leakcheck_api_key': config.get('leakcheck_api_key', ''),
                'auto_delete': {'enabled': True, 'delay': 5},
                'presence': {},
                'connected': True,
                'nitro_sniper': {'enabled': False}
            }
            config['user_settings'][self.token] = default_settings
            needs_save = True

        # Migrate existing users if needed
        if self.token and self.token in config.get('user_settings', {}):
            user_settings = config['user_settings'][self.token]
            # Always assign UID if missing or None
            if 'uid' not in user_settings or user_settings['uid'] is None:
                developer_ids = self._get_developer_ids(config)
                if self.token in config.get('tokens', []) and user_id and user_id in developer_ids:
                    user_settings['uid'] = 0
                    self._used_uids.add(0)
                else:
                    user_settings['uid'] = self._get_next_uid()
                needs_save = True
            # Add missing fields with defaults
            defaults = {
                'leakcheck_api_key': config.get('leakcheck_api_key', ''),
                'connected': True,
                'presence': config.get('presence', {}).copy(),
                'auto_delete': config.get('auto_delete', {'enabled': True, 'delay': 5}).copy(),
                'nitro_sniper': {'enabled': False},
                'username': None
            }
            for key, default_value in defaults.items():
                if key not in user_settings:
                    user_settings[key] = default_value
                    needs_save = True
            # Add discord_id if missing
            if 'discord_id' not in user_settings and user_id:
                user_settings['discord_id'] = user_id
                needs_save = True

        # Final check: if UID is still missing, forcibly assign one
        if self.token and self.token in config.get('user_settings', {}):
            user_settings = config['user_settings'][self.token]
            if 'uid' not in user_settings or user_settings['uid'] is None:
                developer_ids = self._get_developer_ids(config)
                is_developer = user_id in developer_ids if user_id else False
                if is_developer:
                    user_settings['uid'] = self._get_developer_uid(user_id, developer_ids)
                else:
                    user_settings['uid'] = self._get_next_uid()
                needs_save = True

        if needs_save:
            self._safe_write_to_file(self.config_path, config)
            self._mark_cache_dirty()

    @property 
    def developer_name(self):
        """Get developer name with improved caching"""
        if self._developer_name:
            return self._developer_name
            
        try:
            config = self._get_cached_config()
            
            if self.developer_id:
                for token, settings in config.get('user_settings', {}).items():
                    if settings.get('discord_id') == int(self.developer_id) and settings.get('username'):
                        self._developer_name = settings.get('username')
                        return self._developer_name
        except Exception as e:
            logger.error(f"Error getting developer name from config: {e}")
        
        return "_z.z_"

    @developer_name.setter
    def developer_name(self, value: str):
        """Set developer name with cache update"""
        self._developer_name = value
        
        try:
            config = self._get_cached_config()
            
            if not self.developer_id:
                logger.warning("Cannot update developer name: No developer_id specified in config")
                return
                
            updated = False
            
            # Update current token if it belongs to developer
            if self.token:
                try:
                    token_id = int(base64.b64decode(self.token.split('.')[0] + "==").decode('utf-8'))
                    if token_id == int(self.developer_id) and self.token in config.get('user_settings', {}):
                        config['user_settings'][self.token]['username'] = value
                        updated = True
                except Exception as e:
                    logger.error(f"Error checking if token belongs to developer: {e}")
                    
            # Update any other matching entries
            for token, settings in config.get('user_settings', {}).items():
                if settings.get('discord_id') == int(self.developer_id):
                    settings['username'] = value
                    updated = True
                    
            if updated:
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                logger.info(f"Updated developer username to: {value}")
        except Exception as e:
            logger.error(f"Error updating developer name in config: {e}")

    def generate_uid(self, user_id: int) -> str:
        uid = hex(user_id)[2:].upper()[-6:]
        checksum = sum(int(c, 16) for c in uid) % 36
        checksum = hex(checksum)[2:].upper()
        return f"{uid}-{checksum}"

    def load_config(self):
        """Load config with caching"""
        config = self._get_cached_config()
        user_settings = config.get('user_settings', {}).get(self.token, {})
        
        # Load all settings
        self.command_prefix = user_settings.get('command_prefix', '-')
        self.leakcheck_api_key = user_settings.get('leakcheck_api_key', '')
        self.presence = user_settings.get('presence', {})
        self.connected = user_settings.get('connected', True)
        self.auto_delete = SimpleNamespace(
            enabled=user_settings.get('auto_delete', {}).get('enabled', True),
            delay=user_settings.get('auto_delete', {}).get('delay', 5)
        )
        self.uid = user_settings.get('uid')
        self.discord_id = user_settings.get('discord_id')
        self.nitro_sniper = user_settings.get('nitro_sniper', {})
        self.username = user_settings.get('username', None)

    async def save_config_async(self):
        """Optimized thread-safe async config saving"""
        if not self.token:
            logger.error("Cannot save config: No token provided")
            return False
            
        async with self._config_context() as config:
            if 'user_settings' not in config:
                config['user_settings'] = {}
                
            # Preserve existing UID and Discord ID
            existing_settings = config['user_settings'].get(self.token, {})
            uid = existing_settings.get('uid')
            discord_id = existing_settings.get('discord_id')
                
            # Update settings
            config['user_settings'][self.token] = {
                'command_prefix': self.command_prefix,
                'leakcheck_api_key': self.leakcheck_api_key,
                'auto_delete': {
                    'enabled': self.auto_delete.enabled,
                    'delay': self.auto_delete.delay
                },
                'presence': self.presence,
                'connected': self.connected,
                'uid': uid,
                'discord_id': discord_id,
                'username': self.username,
                'nitro_sniper': self.nitro_sniper
            }
            
            # Update cache
            self._config_cache[self.config_path].data = config
            self._mark_cache_dirty()
            return True

    def save_config(self):
        """Synchronous wrapper for save_config_async"""
        try:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self.save_config_async())
        except RuntimeError:
            # If no event loop is running, create a new one
            return asyncio.run(self.save_config_async())

    def save_user_config(self, user_config: UserConfig):
        """Optimized user config saving with caching"""
        try:
            config = self._get_cached_config()
            
            # Preserve existing UID and Discord ID if present
            existing_settings = config.get('user_settings', {}).get(user_config.token, {})
            if 'uid' in existing_settings:
                user_config.uid = existing_settings['uid']
            else:
                developer_ids = self._get_developer_ids(config)
                is_dev = user_config.discord_id in developer_ids
                if is_dev:
                    user_config.uid = self._get_developer_uid(user_config.discord_id, developer_ids)
                else:
                    user_config.uid = self._get_next_uid()
                
            if 'discord_id' in existing_settings:
                user_config.discord_id = existing_settings['discord_id']
            
            config['user_settings'] = config.get('user_settings', {})
            config['user_settings'][user_config.token] = {
                'command_prefix': user_config.command_prefix,
                'leakcheck_api_key': user_config.leakcheck_api_key,
                'auto_delete': user_config.auto_delete.__dict__,
                'presence': user_config.presence,
                'connected': user_config.connected,
                'uid': user_config.uid,
                'discord_id': user_config.discord_id,
                'username': user_config.username,
                'nitro_sniper': user_config.nitro_sniper
            }
            
            # Update cache and mark dirty
            self._config_cache[self.config_path].data = config
            self._mark_cache_dirty()
            return self._safe_write_to_file(self.config_path, config)
        except Exception as e:
            logger.error(f"Error saving user config: {e}")
            return False

    async def save_user_config_async(self, user_config: UserConfig) -> bool:
        """Async user config saving to avoid event-loop blocking.

        Uses the async config context and async file writer to persist changes
        without blocking heartbeats or gateway polling.
        """
        try:
            async with self._config_context() as config:
                existing_settings = config.get('user_settings', {}).get(user_config.token, {})

                # Preserve UID and discord_id or assign when missing
                if 'uid' in existing_settings and existing_settings['uid'] is not None:
                    user_config.uid = existing_settings['uid']
                else:
                    developer_ids = self._get_developer_ids(config)
                    is_dev = user_config.discord_id in developer_ids if user_config.discord_id is not None else False
                    user_config.uid = self._get_developer_uid(user_config.discord_id, developer_ids) if is_dev else self._get_next_uid(config)

                if 'discord_id' in existing_settings and existing_settings['discord_id'] is not None:
                    user_config.discord_id = existing_settings['discord_id']

                config['user_settings'] = config.get('user_settings', {})
                config['user_settings'][user_config.token] = {
                    'command_prefix': user_config.command_prefix,
                    'leakcheck_api_key': user_config.leakcheck_api_key,
                    'auto_delete': user_config.auto_delete.__dict__,
                    'presence': user_config.presence,
                    'connected': user_config.connected,
                    'uid': user_config.uid,
                    'discord_id': user_config.discord_id,
                    'username': user_config.username,
                    'nitro_sniper': user_config.nitro_sniper,
                }

                # Update cache and mark dirty; file write is handled by context exit
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                return True
        except Exception as e:
            logger.error(f"Error saving user config asynchronously: {e}")
            return False

    def save_user_settings(self, token: str, settings: UserSettings):
        """Optimized user settings saving"""
        if token is None or token == "null":
            logger.warning("Attempted to save settings for null token")
            return False

        try:
            config = self._get_cached_config()
            
            if 'user_settings' not in config:
                config['user_settings'] = {}
                
            config['user_settings'][token] = {
                'command_prefix': settings.command_prefix,
                'auto_delete': settings.auto_delete,
                'presence': settings.presence,
                'discord_id': settings.discord_id,
                'uid': settings.uid,
                'username': settings.username,
                'nitro_sniper': settings.nitro_sniper
            }
            
            # Update cache and mark dirty
            self._config_cache[self.config_path].data = config
            self._mark_cache_dirty()
            return self._safe_write_to_file(self.config_path, config)
        except Exception as e:
            logger.error(f"Error saving user settings: {e}")
            return False

    async def reload_config_async(self):
        """Optimized thread-safe async config reloading"""
        if not self.token:
            return
            
        async with self._config_context() as config:
            user_settings = config.get('user_settings', {}).get(self.token, {})
            
            # Update instance variables
            self.uid = user_settings.get('uid')
            self.discord_id = user_settings.get('discord_id')
            self.username = user_settings.get('username', None)
            self.command_prefix = user_settings.get('command_prefix', '-')
            self.leakcheck_api_key = user_settings.get('leakcheck_api_key', '')
            self.presence = user_settings.get('presence', {})
            self.connected = user_settings.get('connected', True)
            self.auto_delete = SimpleNamespace(
                enabled=user_settings.get('auto_delete', {}).get('enabled', True),
                delay=user_settings.get('auto_delete', {}).get('delay', 5)
            )
            self.nitro_sniper = user_settings.get('nitro_sniper', {})

    async def add_token_async(self, token: str) -> bool:
        """Add a token to the global tokens list without blocking the event loop."""
        try:
            async with self._config_context() as config:
                tokens = config.get('tokens', [])
                if token not in tokens:
                    tokens.append(token)
                    config['tokens'] = tokens
                    # update cache and mark dirty (file write handled by context)
                    self._config_cache[self.config_path].data = config
                    self._mark_cache_dirty()
                return True
        except Exception as e:
            logger.error(f"Error adding token asynchronously: {e}")
            return False

    def reload_config(self):
        """Synchronous wrapper for reload_config_async"""
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.reload_config_async())
        except RuntimeError:
            asyncio.run(self.reload_config_async())

    def remove_user(self, uid: int) -> bool:
        """Optimized user removal with caching"""
        try:
            config = self._get_cached_config()
            
            # Find token by UID
            token_to_remove = None
            for token, settings in config.get('user_settings', {}).items():
                if settings.get('uid') == uid:
                    token_to_remove = token
                    break
                    
            if token_to_remove:
                # Remove from tokens list
                if token_to_remove in config.get('tokens', []):
                    config['tokens'].remove(token_to_remove)
                
                # Remove user settings
                del config['user_settings'][token_to_remove]
                
                # Update UID tracking
                self._used_uids.discard(uid)
                
                # Update cache and save
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                return self._safe_write_to_file(self.config_path, config)
                
            return False
        except Exception as e:
            logger.error(f"Error removing user {uid}: {e}")
            return False

    def update_user_token(self, uid: int, new_token: str) -> bool:
        """Optimized token update with better caching"""
        try:
            config = self._get_cached_config()
            
            # Find old token by UID
            old_token = None
            for token, settings in config.get('user_settings', {}).items():
                if settings.get('uid') == uid:
                    old_token = token
                    break
                    
            if old_token:
                try:
                    # Validate new token format
                    user_id = int(base64.b64decode(new_token.split('.')[0] + "==").decode('utf-8'))
                except Exception as e:
                    logger.error(f"Invalid token format: {e}")
                    return False
                    
                # Copy settings to new token
                settings = config['user_settings'][old_token].copy()
                settings['discord_id'] = user_id
                settings['uid'] = uid
                settings['connected'] = True
                config['user_settings'][new_token] = settings
                
                # Cache settings for future use
                self._token_update_cache[user_id] = {
                    **settings.copy(),
                    'timestamp': time.time()
                }
                logger.info(f"Cached settings for user {user_id} during token update")
                
                # Update tokens list
                if old_token in config.get('tokens', []):
                    config['tokens'][config['tokens'].index(old_token)] = new_token
                
                # Remove old token settings
                del config['user_settings'][old_token]
                
                # Update cache and save
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                return self._safe_write_to_file(self.config_path, config)
                
            return False
        except Exception as e:
            logger.error(f"Error updating user token: {e}")
            return False

    def validate_token(self, token: str) -> bool:
        """Validate token format and check if it exists"""
        try:
            # Check basic token structure
            parts = token.split('.')
            if len(parts) != 3:
                return False
                
            # Try to decode the first part (user ID)
            padding_needed = len(parts[0]) % 4
            if padding_needed:
                parts[0] += '=' * (4 - padding_needed)
            int(base64.b64decode(parts[0]).decode('utf-8'))
            
            # Use a more lenient regex pattern for token validation
            if not re.match(r'^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$', token):
                return False
                
            return True
        except:
            return False

    async def validate_token_api(self, token: str) -> bool:
        """Validate token by making an API request"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    'https://discord.com/api/v9/users/@me',
                    headers={'Authorization': token}
                ) as resp:
                    return resp.status == 200
        except:
            return False

    def validate_uid(self, uid: int) -> bool:
        """Optimized UID validation with caching"""
        try:
            config = self._get_cached_config()
            return any(settings.get('uid') == uid 
                      for settings in config.get('user_settings', {}).values())
        except Exception as e:
            logger.error(f"Error validating UID: {e}")
            return False

    def is_user_connected(self, uid: int) -> bool:
        """Optimized connection check with caching"""
        try:
            config = self._get_cached_config()
            for settings in config.get('user_settings', {}).values():
                if settings.get('uid') == uid:
                    return settings.get('connected', False)
            return False
        except Exception as e:
            logger.error(f"Error checking user connection status: {e}")
            return False

    def set_user_connected(self, uid: int, connected: bool) -> bool:
        """Optimized connection status update"""
        try:
            config = self._get_cached_config()
            
            updated = False
            for settings in config.get('user_settings', {}).values():
                if settings.get('uid') == uid:
                    settings['connected'] = connected
                    updated = True
                    break
            
            if updated:
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                return self._safe_write_to_file(self.config_path, config)
            
            logger.warning(f"User with UID {uid} not found when setting connected status")
            return False
        except Exception as e:
            logger.error(f"Error setting user connected status: {e}")
            return False

    async def set_user_connected_async(self, uid: int, connected: bool) -> bool:
        """Async version of connection status update to avoid blocking the event loop"""
        try:
            async with self._config_context() as config:
                updated = False
                for settings in config.get('user_settings', {}).values():
                    if settings.get('uid') == uid:
                        settings['connected'] = connected
                        updated = True
                        break

                if updated:
                    # cache updated by context manager; mark dirty for any sync readers
                    self._config_cache[self.config_path].data = config
                    self._mark_cache_dirty()
                    return True

                logger.warning(f"User with UID {uid} not found when setting connected status (async)")
                return False
        except Exception as e:
            logger.error(f"Error setting user connected status (async): {e}")
            return False

    def fix_null_uids(self) -> bool:
        """Fix null UIDs and invalid tokens with improved validation"""
        try:
            config = self._get_cached_config()
            user_settings = config.get('user_settings', {})
            modified = False
    
            # Remove null entry if it exists
            if 'null' in user_settings:
                del user_settings['null']
                modified = True
                logger.info("Removed null token entry from user_settings")
            
            # Process each token
            for token, settings in list(user_settings.items()):
                if not token or token == "null":
                    if token in user_settings:
                        del user_settings[token]
                        modified = True
                        logger.info(f"Removed invalid token entry: {token}")
                    continue
                    
                # Fix missing or null discord_id
                if settings.get('discord_id') is None:
                    try:
                        # Extract discord_id from token
                        token_parts = token.split('.')
                        if len(token_parts) >= 1:
                            user_id_b64 = token_parts[0]
                            padding_needed = len(user_id_b64) % 4
                            if padding_needed:
                                user_id_b64 += '=' * (4 - padding_needed)
                            
                            decoded_bytes = base64.b64decode(user_id_b64)
                            try:
                                user_id = int(decoded_bytes.decode('utf-8'))
                            except UnicodeDecodeError:
                                user_id = int.from_bytes(decoded_bytes, byteorder='big')
                            
                            settings['discord_id'] = user_id
                            modified = True
                            logger.info(f"Fixed missing discord_id for {settings.get('username', 'unknown')}: {user_id}")
                    except Exception as e:
                        logger.error(f"Error extracting discord_id from token: {e}")
                        continue
                    
                # Fix missing or null UID
                if settings.get('uid') is None:
                    try:
                        user_id = settings.get('discord_id')
                        developer_ids = self._get_developer_ids(config)
                        
                        if user_id in developer_ids:
                            developer_uid = self._get_developer_uid(user_id, developer_ids)
                            settings['uid'] = developer_uid
                            self._used_uids.add(developer_uid)
                            logger.info(f"Set developer UID {developer_uid} for {settings.get('username', 'unknown')}")
                        else:
                            settings['uid'] = self._get_next_uid(config)
                            logger.info(f"Assigned UID {settings['uid']} to {settings.get('username', 'unknown')}")
                            
                        modified = True
                        
                    except Exception as e:
                        logger.error(f"Error fixing UID for token: {e}")
                        continue
            
            if modified:
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                return self._safe_write_to_file(self.config_path, config)
                
            return True
        except Exception as e:
            logger.error(f"Error fixing null UIDs: {e}")
            return False

    def update_username(self, token: str, new_username: str) -> bool:
        """Optimized username update with caching"""
        try:
            config = self._get_cached_config()
            
            if token in config.get('user_settings', {}):
                config['user_settings'][token]['username'] = new_username
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                return self._safe_write_to_file(self.config_path, config)
            
            logger.warning(f"Token {token[:10]}... not found when updating username")
            return False
        except Exception as e:
            logger.error(f"Error updating username: {e}")
            return False

    async def update_username_async(self, token: str, new_username: str) -> bool:
        """Optimized async username update"""
        try:
            async with self._config_context() as config:
                if token in config.get('user_settings', {}):
                    config['user_settings'][token]['username'] = new_username
                    self._config_cache[self.config_path].data = config
                    self._mark_cache_dirty()
                    return True
                
                logger.warning(f"Token {token[:10]}... not found when updating username asynchronously")
                return False
        except Exception as e:
            logger.error(f"Error updating username asynchronously: {e}")
            return False

    def set_initial_username(self, token: str, username: str) -> bool:
        """Optimized initial username setting"""
        try:
            config = self._get_cached_config()
            
            if token in config.get('user_settings', {}):
                if config['user_settings'][token].get('username') is None:
                    config['user_settings'][token]['username'] = username
                    self._config_cache[self.config_path].data = config
                    self._mark_cache_dirty()
                    return self._safe_write_to_file(self.config_path, config)
            return True
        except Exception as e:
            logger.error(f"Error setting initial username: {e}")
            return False

    @property
    def uid(self) -> Optional[int]:
        """Get UID for this token with caching"""
        try:
            config = self._get_cached_config()
            return config.get('user_settings', {}).get(self.token, {}).get('uid')
        except Exception as e:
            logger.error(f"Error getting UID: {e}")
            return None

    @uid.setter 
    def uid(self, value: Optional[int]):
        """Set UID for this token with caching"""
        try:
            config = self._get_cached_config()
                
            if 'user_settings' not in config:
                config['user_settings'] = {}
            if self.token not in config['user_settings']:
                config['user_settings'][self.token] = {}
                
            config['user_settings'][self.token]['uid'] = value
            
            # Update UID tracking
            if value is not None:
                self._used_uids.add(value)
            
            self._config_cache[self.config_path].data = config
            self._mark_cache_dirty()
            self._safe_write_to_file(self.config_path, config)
        except Exception as e:
            logger.error(f"Error setting UID: {e}")

    def _validate_json(self, data: Any) -> bool:
        """Validate that data can be properly serialized as JSON"""
        if data is None:
            logger.error("JSON validation error: Data is None")
            return False
        # Verify the top-level structure is a dictionary
        if not isinstance(data, dict):
            logger.error(f"JSON validation error: Top-level data must be a dictionary, got {type(data)}")
            return False
        # Check for required fields
        if 'tokens' not in data or not isinstance(data['tokens'], list):
            logger.error("JSON validation error: 'tokens' field missing or not a list")
            return False
        if 'user_settings' not in data or not isinstance(data['user_settings'], dict):
            logger.error("JSON validation error: 'user_settings' field missing or not a dictionary")
            return False
        # Look for invalid dictionary keys
        for key in data.keys():
            if key is None or key == "":
                logger.error("JSON validation error: Empty or None key found in top-level dictionary")
                return False
        # Check for invalid keys in user_settings
        for token, settings in list(data.get('user_settings', {}).items()):
            if token is None or token == "null" or token == "":
                logger.warning(f"Removing invalid token key from user_settings: {token}")
                del data['user_settings'][token]
                continue
            if not isinstance(settings, dict):
                logger.error(f"JSON validation error: User settings for {token} is not a dictionary")
                return False
        # Attempt serialization
        try:
            # Test serialization to make sure it's valid JSON
            json_str = json.dumps(data, indent=4)
            # Additional check for trailing characters
            if json_str.strip().endswith('}') is False and json_str.strip().endswith(']') is False:
                logger.error("JSON validation error: JSON string has trailing characters")
                return False
            return True
        except (TypeError, OverflowError, ValueError) as e:
            logger.error(f"JSON validation error: {e}")
            return False

    def _safe_write_to_file(self, file_path: str, data: Any) -> bool:
        """Safely write JSON data to a file using an atomic operation approach"""
        if not self._validate_json(data):
            logger.error("Failed to write file: Invalid JSON data")
            return False
            
        # Create a backup copy first
        backup_path = None
        try:
            if os.path.exists(file_path):
                backup_path = f"{file_path}.bak"
                shutil.copy2(file_path, backup_path)
                
            # Create a temporary file in the same directory as the target file
            dir_path = os.path.dirname(file_path) or '.'
            with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=dir_path) as temp_file:
                json_str = json.dumps(data, indent=4)
                temp_file.write(json_str)
                temp_file_name = temp_file.name
                
            # Replace the original file with the temp file atomically
            shutil.move(temp_file_name, file_path)
            
            # Remove backup if all went well
            if backup_path and os.path.exists(backup_path):
                os.remove(backup_path)
                
            return True
        except Exception as e:
            logger.error(f"Error writing to file {file_path}: {e}")
            # Restore backup if something went wrong and backup exists
            if backup_path and os.path.exists(backup_path):
                try:
                    shutil.copy2(backup_path, file_path)
                    os.remove(backup_path)
                except Exception as restore_error:
                    logger.error(f"Failed to restore from backup: {restore_error}")
            return False

    async def _safe_write_to_file_async(self, file_path: str, data: Any) -> bool:
        """Async version of safe file writing"""
        if not self._validate_json(data):
            logger.error("Failed to write file: Invalid JSON data")
            return False
            
        # Create a backup copy first
        backup_path = None
        try:
            if os.path.exists(file_path):
                backup_path = f"{file_path}.bak"
                shutil.copy2(file_path, backup_path)
                
            # Create a temporary file in the same directory as the target file
            dir_path = os.path.dirname(file_path) or '.'
            json_str = json.dumps(data, indent=4)
            
            # Write to temp file
            temp_fd, temp_path = tempfile.mkstemp(dir=dir_path)
            try:
                with os.fdopen(temp_fd, 'w') as temp_file:
                    temp_file.write(json_str)
                
                # Replace the original file with the temp file atomically
                shutil.move(temp_path, file_path)
            finally:
                # Ensure temp file is cleaned up if something goes wrong
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
            
            # Remove backup if all went well
            if backup_path and os.path.exists(backup_path):
                os.remove(backup_path)
                
            return True
        except Exception as e:
            logger.error(f"Error writing to file {file_path}: {e}")
            # Restore backup if something went wrong and backup exists
            if backup_path and os.path.exists(backup_path):
                try:
                    shutil.copy2(backup_path, file_path)
                    os.remove(backup_path)
                except Exception as restore_error:
                    logger.error(f"Failed to restore from backup: {restore_error}")
            return False

    def cleanup(self):
        """Clean up resources and caches"""
        try:
            # Clear token update cache
            self._token_update_cache.clear()
            
            # Clear config cache for this instance's config path
            if self.config_path in self._config_cache:
                del self._config_cache[self.config_path]
            
            # Clear UID tracking
            self._used_uids.clear()
            
            logger.info("ConfigManager cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    @classmethod
    def cleanup_all_caches(cls):
        """Class method to clean up all caches"""
        try:
            cls._token_update_cache.clear()
            cls._config_cache.clear()
            cls._locks.clear()
            logger.info("All ConfigManager caches cleared")
        except Exception as e:
            logger.error(f"Error cleaning up all caches: {e}")

    def __del__(self):
        """Destructor to ensure cleanup"""
        try:
            # Check if Python is shutting down by testing if builtins are still available
            if 'open' not in dir(__builtins__) if isinstance(__builtins__, dict) else not hasattr(__builtins__, 'open'):
                return  # Skip cleanup during interpreter shutdown
                
            # Save any pending changes before destruction
            cache_entry = self._config_cache.get(self.config_path)
            if cache_entry and cache_entry.dirty:
                self._safe_write_to_file(self.config_path, cache_entry.data)
        except Exception:
            pass  # Ignore errors during destruction

    def cleanup_invalid_tokens(self) -> bool:
        """Remove invalid tokens and settings from config"""
        try:
            config = self._get_cached_config()
            user_settings = config.get('user_settings', {})
            original_count = len(user_settings)
            
            # Remove invalid tokens
            invalid_tokens = []
            for token, settings in list(user_settings.items()):
                if not token or not isinstance(token, str) or token == 'null':
                    invalid_tokens.append(token)
                elif not self.validate_token(token):
                    invalid_tokens.append(token)
                    logger.warning(f"Removing invalid token for user {settings.get('username', 'unknown')}")
            
            for token in invalid_tokens:
                del user_settings[token]
                # Also remove from tokens list if present
                if token in config.get('tokens', []):
                    config['tokens'].remove(token)
            
            if invalid_tokens:
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                self._safe_write_to_file(self.config_path, config)
                logger.info(f"Cleaned up {len(invalid_tokens)} invalid tokens")
                return True
                
            return False
        except Exception as e:
            logger.error(f"Error cleaning up invalid tokens: {e}")
            return False

    def validate_and_repair_config(self) -> bool:
        """Comprehensive validation and repair of the configuration"""
        try:
            logger.info("Starting comprehensive config validation and repair...")
            config = self._get_cached_config()
            repairs_made = False
            
            # 1. Clean up invalid tokens
            if self.cleanup_invalid_tokens():
                repairs_made = True
                config = self._get_cached_config()  # Reload after cleanup
            
            # 2. Fix missing discord_ids
            if self._fix_missing_discord_ids(config):
                repairs_made = True
                self._config_cache[self.config_path].data = config
                self._mark_cache_dirty()
                self._safe_write_to_file(self.config_path, config)
            
            # 3. Fix null UIDs and validate all UIDs
            if self.fix_null_uids():
                repairs_made = True
                config = self._get_cached_config()  # Reload after fixes
            
            # 4. Fix UID sequencing
            if self.fix_all_uids():
                repairs_made = True
                config = self._get_cached_config()  # Reload after UID fixes
            
            # 5. Validate final state
            user_settings = config.get('user_settings', {})
            total_users = len(user_settings)
            
            # Check for any remaining issues
            issues = []
            for token, settings in user_settings.items():
                username = settings.get('username', 'unknown')
                if settings.get('discord_id') is None:
                    issues.append(f"User {username} has null discord_id")
                if settings.get('uid') is None:
                    issues.append(f"User {username} has null UID")
            
            if issues:
                logger.warning(f"Remaining issues after repair: {issues}")
            else:
                logger.info(f"Config validation complete: {total_users} users, all data valid")
            
            return repairs_made
            
        except Exception as e:
            logger.error(f"Error in comprehensive config validation: {e}")
            return False

    def _fix_missing_discord_ids(self, config: dict) -> bool:
        """Fix missing discord_ids by extracting them from tokens"""
        try:
            user_settings = config.get('user_settings', {})
            changed = False
            
            for token, settings in user_settings.items():
                if not token or not isinstance(token, str) or token == 'null':
                    continue
                    
                # If discord_id is missing or null, try to extract it from token
                if settings.get('discord_id') is None:
                    try:
                        # Extract user ID from token
                        token_parts = token.split('.')
                        if len(token_parts) >= 1:
                            # Add padding if needed
                            user_id_b64 = token_parts[0]
                            padding_needed = len(user_id_b64) % 4
                            if padding_needed:
                                user_id_b64 += '=' * (4 - padding_needed)
                            
                            # Decode the user ID
                            decoded_bytes = base64.b64decode(user_id_b64)
                            try:
                                user_id = int(decoded_bytes.decode('utf-8'))
                            except UnicodeDecodeError:
                                # Fallback for unusual encoding
                                user_id = int.from_bytes(decoded_bytes, byteorder='big')
                            
                            settings['discord_id'] = user_id
                            changed = True
                            logger.info(f"Fixed missing discord_id for user {settings.get('username', 'unknown')}: {user_id}")
                            
                    except Exception as e:
                        logger.error(f"Failed to extract discord_id from token for user {settings.get('username', 'unknown')}: {e}")
                        
            return changed
        except Exception as e:
            logger.error(f"Error fixing missing discord_ids: {e}")
            return False

# Create optimized global instance
config_manager = ConfigManager(token=None)
