import discord
from discord.ext import commands
from utils.config_manager import UserConfig, AutoDeleteConfig
from utils.general import format_message, quote_block, get_max_message_length
import json
import logging
import asyncio
import base64
import io
from curl_cffi import requests
from curl_cffi.requests import AsyncSession
import random
import time
from typing import Union, Optional
from datetime import datetime
import re
import unicodedata
import aiohttp

logger = logging.getLogger(__name__)

def developer_only(allow_auxiliary: bool = False):
    """
    Decorator to restrict commands.
    - Always allows the main developer.
    - If allow_auxiliary is True, also allows users in allowed_auxiliary_users config.
    - Hides commands from unauthorized users in help.
    """
    async def predicate(ctx):
        is_developer = ctx.bot.config_manager.is_developer(ctx.author.id)
        
        # Developer always has access
        if is_developer:
            return True
            
        # Check if auxiliary users are allowed for this command and if the user is one
        is_authorized_auxiliary = False
        if allow_auxiliary:
            # Get auxiliary users from config
            config = await ctx.bot.config_manager._get_cached_config_async()
            allowed_auxiliary_users = set(config.get('allowed_auxiliary_users', []))
            if ctx.author.id in allowed_auxiliary_users:
                is_authorized_auxiliary = True
            
        # Final authorization check
        is_authorized = is_developer or is_authorized_auxiliary
        
        # If this is a help command check, return the appropriate permission
        if ctx.command and ctx.command.qualified_name == 'help':
            return is_authorized
        
        # For actual command execution
        if not is_authorized:
            raise commands.CheckFailure("You don't have permission to use this command.")
        

        logger.info(f"Command '{ctx.command}' used by {ctx.author} (ID: {ctx.author.id})")
        
        return True
        
    return commands.check(predicate)
    
class Developer(commands.Cog):    
    def __init__(self, bot):
        self.bot = bot
        self.search_pages = {}  # Store search results for pagination
        self.search_page_size = 5 
        # Add mutual friends cache to avoid fetching repeatedly
        self.mutual_friends_cache = {}
        # Set cache expiry (10 minutes)
        self.cache_expiry = 600
        
        # Load config for API URLs
        # Note: We can't await here in __init__, so we'll access it when needed or use a property
        # But we can set defaults
        self._config_cache = {}
        
        # Phase 1 feature flag for improved alt analysis / search normalization
        self.use_alt_analysis_v2 = True
        # Phase 2 structures
        self._alt_total_user_cache = None  # (count, timestamp)
        self._alt_pattern_cache = {}       # pattern -> (count, timestamp)
        self._alt_cache_ttl = 300          # seconds
        self._alt_semaphore = asyncio.Semaphore(3)  # limit concurrent mutual enrichments
        # Accuracy enhancement flag & weights (Phase 3)
        self.use_alt_accuracy_enhanced = True
        self.alt_accuracy_weights = {
            'near_exact': 8.0,
            'near_edit2': 4.0,
            'token_jaccard': 18.0,  # multiplied by Jaccard (capped)
            'ngram_unit': 1.2,      # per distinct rare ngram overlap (capped total)
            'order_consistency': 12.0,
            'burst': 6.0,
            'base_numeric_bonus': 5.0,
            'affix_core_bonus': 6.0,
            'affix_substring_bonus': 4.0,
            'common_short_penalty': -12.0,
            'overgeneral_penalty': -8.0,
            'flood_penalty': -5.0
        }

    # Alt Analysis methods removed

    @commands.command(aliases=['hostuser'], hidden=True)
    @developer_only()
    async def authorize_host(self, ctx, action: str, user_id: int = None, limit: int = None):
        """Manage users authorized to host themselves on the selfbot and blacklist management
        
        Usage:
        ;hostuser add <user_id> - Add a user to authorized hosts list
        ;hostuser remove <user_id> - Remove a user from authorized hosts list
        ;hostuser list [page] - List all authorized users (with pagination)
        ;hostuser limit <user_id> <limit> - Set custom hosting limit for a user
        ;hostuser blacklist <user_id> - Add a user ID to the blacklist (cannot be hosted)
        ;hostuser unblacklist <user_id> - Remove a user ID from the blacklist
        ;hostuser listblacklist [page] - List all blacklisted user IDs (with pagination)
        """
        try:
            await self.safe_delete_message(ctx.message)
        except:
            pass

        # Normalize the action
        action = action.lower()
        
        if action == 'add' and user_id:
            # Check if user is already in authorized hosts
            existing = await self.bot.db.db.authorized_hosts.find_one({"user_id": user_id})
            if existing:
                await ctx.send(
                    quote_block(f"```ansi\n\u001b[1;33m⚠ User ID {user_id} is already in authorized hosts```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
                
            # Add user to authorized hosts
            try:
                # Try to validate the user by fetching their info
                user = await self.bot.GetUser(user_id)
                username = user.name if user else "Unknown"
                
                # Insert into MongoDB
                await self.bot.db.db.authorized_hosts.insert_one({
                    "user_id": user_id,
                    "username": username,
                    "hosting_limit": 5,  # Default limit
                    "added_at": datetime.utcnow(),
                    "added_by": ctx.author.id
                })
                
                await ctx.send(
                    quote_block(f"```ansi\n\u001b[1;32m✓ Added {username} ({user_id}) to authorized hosts```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            except Exception as e:
                logger.error(f"Error adding user to authorized hosts: {e}")
                await ctx.send(
                    quote_block(f"```ansi\n\u001b[1;31m✗ Error adding user: {e}```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                
        elif action == 'remove' and user_id:
            # Remove user from authorized hosts
            try:
                result = await self.bot.db.db.authorized_hosts.delete_one({"user_id": user_id})
                
                if result.deleted_count > 0:
                    await ctx.send(
                        quote_block(f"```ansi\n\u001b[1;32m✓ Removed user ID {user_id} from authorized hosts```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                else:
                    await ctx.send(
                        quote_block(f"```ansi\n\u001b[1;31m✗ User ID {user_id} not found in authorized hosts```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
            except Exception as e:
                logger.error(f"Error removing user from authorized hosts: {e}")
                await ctx.send(
                    quote_block(f"```ansi\n\u001b[1;31m✗ Error removing user: {e}```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
        elif action == 'list':
            # Parse page from user_id parameter
            page = user_id if user_id else 1
            
            # List all authorized hosts with pagination
            try:
                cursor = self.bot.db.db.authorized_hosts.find({})
                users = await cursor.to_list(length=1000)  # Get all users for pagination
                
                if not users:
                    await ctx.send(
                        quote_block("```ansi\n\u001b[1;31mNo authorized hosts found.```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
                
                # Sort users by user_id for consistent ordering
                users.sort(key=lambda x: x.get("user_id", 0))
                
                # Pagination
                items_per_page = 5
                total_pages = (len(users) + items_per_page - 1) // items_per_page
                page = min(max(1, page), total_pages)
                start_idx = (page - 1) * items_per_page
                page_users = users[start_idx:start_idx + items_per_page]
                
                message_parts = [
                    "```ansi\n" + \
                    "\u001b[30m\u001b[1m\u001b[4mAuthorized Hosts\u001b[0m\n" + \
                    f"\u001b[0;36mTotal Users: \u001b[0;37m{len(users)}\n" + \
                    f"\u001b[30m{'─' * 35}\u001b[0m\n"
                ]
                
                for user in page_users:
                    user_id = user.get("user_id", "Unknown")
                    username = user.get("username", "Unknown")
                    hosting_limit = user.get("hosting_limit", 5)
                    added_at = user.get("added_at", "Unknown")
    
                    # Ако username е Unknown, опитай да го вземеш с GetUser
                    if username == "Unknown" and user_id != "Unknown":
                        try:
                            fetched = await self.bot.GetUser(user_id)
                            if fetched:
                                username = fetched.name
                        except Exception:
                            pass
    
                    date_str = ""
                    if isinstance(added_at, datetime):
                        date_str = added_at.strftime("%Y-%m-%d")
    
                    message_parts[-1] += f"\u001b[0;36mUser: \u001b[1;37m{username} ({user_id})\n"
                    message_parts[-1] += f"\u001b[0;36mLimit: \u001b[1;33m{hosting_limit}\u001b[0m\n"
                    if date_str:
                        message_parts[-1] += f"\u001b[0;36mAdded: \u001b[0;37m{date_str}\n"
                    message_parts[-1] += f"\u001b[0;37m{'─' * 35}\n"
                
                message_parts[-1] += "```"
                
                # Add page counter
                page_info = f"```ansi\nPage \u001b[1m\u001b[37m{page}/{total_pages}\u001b[0m```"
                
                await ctx.send(
                    quote_block(''.join(message_parts) + page_info),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            except Exception as e:
                logger.error(f"Error listing authorized hosts: {e}")
                await self.send_with_auto_delete(ctx, f"Error listing users: {e}")
                
        elif action == 'limit' and user_id and limit is not None:
            # Set custom hosting limit for a user
            try:
                if limit < 1 or limit > 50:
                    await self.send_with_auto_delete(ctx, "Limit must be between 1 and 50")
                    return
                
                existing = await self.bot.db.db.authorized_hosts.find_one({"user_id": user_id})
                if not existing:
                    await self.send_with_auto_delete(ctx, f"User ID {user_id} is not in authorized hosts. Add them first.")
                    return
                
                # Update the hosting limit
                result = await self.bot.db.db.authorized_hosts.update_one(
                    {"user_id": user_id},
                    {"$set": {"hosting_limit": limit, "updated_at": datetime.utcnow(), "updated_by": ctx.author.id}}
                )
                
                if result.modified_count > 0:
                    username = existing.get("username", "Unknown")
                    await ctx.send(
                        quote_block(f"```ansi\n\u001b[1;32m✓ Updated hosting limit for {username} ({user_id}) to {limit}```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                else:
                    await self.send_with_auto_delete(ctx, f"Failed to update hosting limit for user ID {user_id}")
            except Exception as e:
                logger.error(f"Error setting hosting limit: {e}")
                await self.send_with_auto_delete(ctx, f"Error setting hosting limit: {e}")
                
        elif action == 'blacklist' and user_id:
            # Add user ID to blacklist
            try:
                existing = await self.bot.db.db.blacklisted_users.find_one({"user_id": user_id})
                if existing:
                    await ctx.send(
                        quote_block(f"```ansi\n\u001b[1;33m⚠ User ID {user_id} is already blacklisted```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
                
                # Try to get username for logging
                user = await self.bot.GetUser(user_id)
                username = user.name if user else "Unknown"
                
                # Insert into MongoDB
                await self.bot.db.db.blacklisted_users.insert_one({
                    "user_id": user_id,
                    "username": username,
                    "blacklisted_at": datetime.utcnow(),
                    "blacklisted_by": ctx.author.id
                })
                
                await self.send_with_auto_delete(ctx, f"Blacklisted {username} ({user_id}) from hosting")
            except Exception as e:
                logger.error(f"Error blacklisting user: {e}")
                await self.send_with_auto_delete(ctx, f"Error blacklisting user: {e}")
        elif action == 'unblacklist' and user_id:
            # Remove user ID from blacklist
            try:
                result = await self.bot.db.db.blacklisted_users.delete_one({"user_id": user_id})
                
                if result.deleted_count > 0:
                    await self.send_with_auto_delete(ctx, f"Removed user ID {user_id} from blacklist")
                else:
                    await self.send_with_auto_delete(ctx, f"User ID {user_id} not found in blacklist")
            except Exception as e:
                logger.error(f"Error removing user from blacklist: {e}")
                await self.send_with_auto_delete(ctx, f"Error removing user from blacklist: {e}")
                
        elif action == 'listblacklist':
            # Parse page from user_id parameter
            page = user_id if user_id else 1
            
            # List all blacklisted users with pagination
            try:
                cursor = self.bot.db.db.blacklisted_users.find({})
                users = await cursor.to_list(length=1000)  # Get all users for pagination
                
                if not users:
                    await ctx.send(
                        quote_block("```ansi\n\u001b[1;31mNo blacklisted users found.```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
                
                # Sort users by user_id for consistent ordering
                users.sort(key=lambda x: x.get("user_id", 0))
                
                # Pagination
                items_per_page = 5
                total_pages = (len(users) + items_per_page - 1) // items_per_page
                page = min(max(1, page), total_pages)
                start_idx = (page - 1) * items_per_page
                page_users = users[start_idx:start_idx + items_per_page]
                
                message_parts = [
                    "```ansi\n" + \
                    "\u001b[30m\u001b[1m\u001b[4mBlacklisted Users\u001b[0m\n" + \
                    f"\u001b[0;31mTotal Users: \u001b[0;37m{len(users)}\n" + \
                    f"\u001b[30m{'─' * 30}\u001b[0m\n"
                ]
                
                for user in page_users:
                    user_id = user.get("user_id", "Unknown")
                    username = user.get("username", "Unknown")
                    blacklisted_at = user.get("blacklisted_at", "Unknown")
    
                    # Ако username е Unknown, опитай да го вземеш с GetUser
                    if username == "Unknown" and user_id != "Unknown":
                        try:
                            fetched = await self.bot.GetUser(user_id)
                            if fetched:
                                username = fetched.name
                        except Exception:
                            pass
    
                    date_str = ""
                    if isinstance(blacklisted_at, datetime):
                        date_str = blacklisted_at.strftime("%Y-%m-%d")
    
                    message_parts[-1] += f"\u001b[0;31mUser: \u001b[1;37m{username} ({user_id})\n"
                    if date_str:
                        message_parts[-1] += f"\u001b[0;31mBlacklisted: \u001b[0;37m{date_str}\n"
                    message_parts[-1] += f"\u001b[0;37m{'─' * 30}\n"
                
                message_parts[-1] += "```"
                
                # Add page counter
                page_info = f"```ansi\nPage \u001b[1m\u001b[37m{page}/{total_pages}\u001b[0m```"
                
                await ctx.send(
                    quote_block(''.join(message_parts) + page_info),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            except Exception as e:
                logger.error(f"Error listing blacklisted users: {e}")
                await self.send_with_auto_delete(ctx, f"Error listing blacklisted users: {e}")
        
        else:
            # Show help if invalid action
            await self.send_with_auto_delete(ctx, "Invalid action. Use 'add', 'remove', 'list', 'blacklist', 'unblacklist', or 'listblacklist'.")
        
    # fetch_kilo_data removed
    
    async def safe_delete_message(self, message):
        """Helper method to safely delete messages"""
        try:
            await message.delete()
        except Exception as e:
            logger.debug(f"Error deleting message: {e}")
    
    async def send_with_auto_delete(self, ctx, content, **kwargs):
        """Helper method to send messages with auto-delete if enabled"""
        return await ctx.send(
            format_message(content),
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None,
            **kwargs
        )
    
    def get_token_by_uid(self, uid):
        """Helper method to get token by UID from config"""
        with open(self.bot.config_manager.config_path) as f:
            config = json.load(f)
            for token, settings in config.get('user_settings', {}).items():
                if settings.get('uid') == uid:
                    return token
        return None
    
    async def handle_server_verification(self, guild_id, invite_code, token, headers):
        """Handle server member verification if present"""
        try:
            # Check if there's member verification required for this guild
            verification_url = f'https://discord.com/api/v9/guilds/{guild_id}/member-verification?with_guild=false&invite_code={invite_code}'
            
            
            # Use aiohttp instead of requests
            async with aiohttp.ClientSession() as session:
                async with session.get(verification_url, headers=headers) as verification_response:
                    if verification_response.status != 200:
                        logger.debug(f"No member verification for guild {guild_id} or error: {verification_response.status}")
                        return None
                    
                    verification_data = await verification_response.json()
                    
                    # Check if there are form fields to agree to
                    if not verification_data.get('form_fields', []):
                        return None
                        
                    # Process the verification form - add response:true to each required field
                    form_fields = verification_data.get('form_fields', [])
                    version = verification_data.get('version', '')
                    
                    # Clone the form fields and add response:true to each required field
                    response_fields = []
                    for field in form_fields:
                        if field.get('required', False):
                            field_copy = field.copy()
                            field_copy['response'] = True
                            response_fields.append(field_copy)
                    
                    # Build payload
                    payload = {
                        "version": version,
                        "form_fields": response_fields
                    }
                    
                    # Submit verification response using aiohttp with proxy
                    submit_url = f'https://discord.com/api/v9/guilds/{guild_id}/requests/@me'
                    
                    # Use a new session for the submit request
                    async with aiohttp.ClientSession() as session:
                        async with session.put(submit_url, headers=headers, json=payload) as response:
                            
                            if response.status in [200, 201, 204]:
                                return {
                                    "status": True,
                                    "num_fields": len(response_fields)
                                }
                            else:
                                response_text = await response.text()
                                
                                # If verification failed with 410 (already verified), try onboarding instead
                                if response.status == 410:
                                    onboarding_result = await self.handle_server_onboarding(guild_id, token, headers)
                                    if onboarding_result and onboarding_result.get("status", False):
                                        return {
                                            "status": True,
                                            "num_fields": 0,  # No verification fields, but onboarding succeeded
                                            "onboarding_fallback": True,
                                            "onboarding_responses": onboarding_result.get("num_responses", 0)
                                        }
                                    else:
                                        return {
                                            "status": False,
                                            "error": f"Verification status {response.status}, onboarding fallback failed: {onboarding_result.get('error', 'Unknown error') if onboarding_result else 'No onboarding result'}"
                                        }
                                else:
                                    logger.error(f"Failed to submit verification: {response.status} - {response_text}")
                                    return {
                                        "status": False,
                                        "error": f"Status {response.status}"
                                    }
        except Exception as e:
            logger.error(f"Error handling verification: {e}")
            return {
                "status": False,
                "error": str(e)
            }    
        
    async def handle_server_onboarding(self, guild_id, token, headers):
        """Handle server onboarding if present"""
        try:
            # First, check if there's onboarding for this guild - use aiohttp
            async with aiohttp.ClientSession() as session:
                onboarding_url = f'https://discord.com/api/v9/guilds/{guild_id}/onboarding'
                
                async with session.get(onboarding_url, headers=headers) as onboarding_response:
                    if onboarding_response.status != 200:
                        logger.debug(f"No onboarding for guild {guild_id} or error: {onboarding_response.status}")
                        return None
                    
                    onboarding_data = await onboarding_response.json()
                    
                    if not onboarding_data.get('enabled', False):
                        return None
                
            # Prepare onboarding responses
            current_time_ms = int(time.time() * 1000)
            onboarding_responses = []
            onboarding_prompts_seen = {}
            onboarding_responses_seen = {}
            
            # Process each prompt and select random options
            for prompt in onboarding_data.get('prompts', []):
                if not prompt.get('in_onboarding', False):
                    continue
                    
                prompt_id = prompt['id']
                options = prompt.get('options', [])
                
                if not options:
                    continue
                    
                # For each prompt, select a random option
                if prompt.get('single_select', False):
                    # Single select: choose one random option
                    selected_option = random.choice(options)
                    selected_option_id = selected_option['id']
                    onboarding_responses.append(selected_option_id)
                    onboarding_responses_seen[selected_option_id] = current_time_ms
                else:
                    # Multi select: choose random number of options (1-3 or all if fewer)
                    num_to_select = min(random.randint(1, 3), len(options))
                    selected_options = random.sample(options, num_to_select)
                    
                    for option in selected_options:
                        option_id = option['id']
                        onboarding_responses.append(option_id)
                        onboarding_responses_seen[option_id] = current_time_ms
                
                # Mark prompt as seen
                onboarding_prompts_seen[prompt_id] = current_time_ms
            
            # Submit onboarding responses if we have any (moved outside the loop)
            if onboarding_responses:
                payload = {
                    "onboarding_responses": onboarding_responses,
                    "onboarding_prompts_seen": onboarding_prompts_seen,
                    "onboarding_responses_seen": onboarding_responses_seen
                }
                
                # Submit using aiohttp - create new session since previous one is closed
                submit_url = f'https://discord.com/api/v9/guilds/{guild_id}/onboarding-responses'
                
                async with aiohttp.ClientSession() as submit_session:
                    async with submit_session.post(submit_url, headers=headers, json=payload) as response:
                        if response.status in [200, 201, 204]:
                            return {
                                "status": True,
                                "num_prompts": len(onboarding_prompts_seen),
                                "num_responses": len(onboarding_responses)
                            }
                        else:
                            response_text = await response.text()
                            logger.error(f"Failed to submit onboarding responses: {response.status} - {response_text}")
                            return {
                                "status": False,
                                "error": f"Status {response.status}"
                            }
        except Exception as e:
            logger.error(f"Error handling onboarding: {e}")
            return {
                "status": False,
                "error": str(e)
            }
        
        return None    
    async def handle_server_reaction_verification(self, guild_id, token, headers):
        """Check channels for messages with reactions and add the same reactions to blend in - non-blocking version"""
        try:
            async with aiohttp.ClientSession() as session:
                # Get guild information using aiohttp
                async with session.get(
                    f'https://discord.com/api/v9/guilds/{guild_id}',
                    headers=headers
                ) as guild_response:
                    if guild_response.status != 200:
                        logger.debug(f"Could not get guild info for {guild_id}: {guild_response.status}")
                        return None
                
                # Get channels in guild using aiohttp
                async with session.get(
                    f'https://discord.com/api/v9/guilds/{guild_id}/channels',
                    headers=headers
                ) as channels_response:
                    if channels_response.status != 200:
                        logger.debug(f"Could not get channels for guild {guild_id}: {channels_response.status}")
                        return None
                        
                    channels = await channels_response.json()
            reactions_added = 0
            messages_processed = 0
            channels_checked = 0
            
            # Identify priority channels (rules, welcome, verification, info channels)
            priority_keywords = ['rule', 'welcome', 'verify', 'verification', 'info', 'read-me', 'readme', 'announcement', 'role']
            priority_channels = []
            regular_channels = []
            
            # Sort channels into priority and regular
            for channel in channels:
                if channel['type'] == 0:  # Text channel
                    channel_name = channel.get('name', '').lower()
                    if any(keyword in channel_name for keyword in priority_keywords):
                        priority_channels.append(channel)
                    else:
                        regular_channels.append(channel)
            
            # Create a list with priority channels first, then regular channels
            # Check up to 15 channels total (expanded from 5)
            text_channels = priority_channels + regular_channels
            text_channels = text_channels[:15]
                
            # Track which messages we've already reacted to, to avoid duplicates
            processed_message_ids = set()
            
            # Also track which emoji we've already added to each message
            message_emoji_map = {}
            
            for channel in text_channels:
                channel_id = channel['id']
                channels_checked += 1
                channel_name = channel.get('name', 'unknown')
                logger.debug(f"Checking channel: {channel_name} ({channel_id})")
                
                # Get messages in the channel - increased from 10 to 25 messages
                try:
                    # First try pinned messages - they often have important reactions using aiohttp
                    async with session.get(
                        f'https://discord.com/api/v9/channels/{channel_id}/pins',
                        headers=headers
                    ) as pinned_response:
                        pinned_messages = []
                        if pinned_response.status == 200:
                            pinned_messages = await pinned_response.json()
                            logger.debug(f"Found {len(pinned_messages)} pinned messages in {channel_name}")
                    
                    # Process pinned messages first
                    for message in pinned_messages:
                        messages_processed += 1
                        message_id = message['id']
                        
                        # Skip if we've already processed this message
                        if message_id in processed_message_ids:
                            continue
                            
                        processed_message_ids.add(message_id)
                        
                        # If message has reactions, add the same ones
                        if 'reactions' in message and message['reactions']:
                            logger.debug(f"Found message with {len(message['reactions'])} reactions in {channel_name}")
                            
                            # Process up to 5 reactions for pinned messages (increased from 3)
                            for reaction in message['reactions'][:5]:
                                emoji_data = reaction.get('emoji', {})
                                emoji_id = emoji_data.get('id')
                                emoji_name = emoji_data.get('name')
                                
                                # Add reaction to message - NON-BLOCKING VERSION
                                try:
                                    # Properly handle emoji formatting for API request
                                    if emoji_id:
                                        emoji_identifier = f"{emoji_name}:{emoji_id}"
                                    else:
                                        import urllib.parse
                                        emoji_identifier = urllib.parse.quote(emoji_name)
                                        
                                    # Make the request using aiohttp
                                    async with session.put(
                                        f'https://discord.com/api/v9/channels/{channel_id}/messages/{message_id}/reactions/{emoji_identifier}/@me',
                                        headers=headers
                                    ) as reaction_response:
                                        if reaction_response.status in (201, 204):
                                            reactions_added += 1
                                            logger.debug(f"Added reaction {emoji_name} to pinned message in {channel_name}")
                                        else:
                                            logger.debug(f"Failed to add reaction: Status {reaction_response.status}")
                                        
                                        # Vary delay to avoid patterns (0.3-0.7 seconds)
                                        await asyncio.sleep(0.3 + random.random() * 0.4)
                                    
                                except Exception as e:
                                    logger.debug(f"Failed to add reaction: {e}")
                    
                    # Now get regular messages - increased limit to 25 using aiohttp
                    async with session.get(
                        f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=25',
                        headers=headers
                    ) as messages_response:
                        if messages_response.status != 200:
                            logger.debug(f"Failed to get messages for channel {channel_name}: {messages_response.status}")
                            continue
                            
                        messages = await messages_response.json()
                        logger.debug(f"Found {len(messages)} messages in {channel_name}")
                    
                    # Look for messages with reactions
                    for message in messages:
                        messages_processed += 1
                        message_id = message['id']
                        
                        # Skip if we've already processed this message
                        if message_id in processed_message_ids:
                            continue
                            
                        processed_message_ids.add(message_id)
                        
                        # Import at top level
                        import urllib.parse
                        import re
                        
                        # Check if message contains verification keywords - expanded phrase matching
                        verification_message = False
                        verification_level = 0  # 0=not verification, 1=maybe, 2=likely, 3=definitely
                        
                        # Check message content - normalize with robust lowercasing
                        message_content = message.get('content', '').lower().strip()
                        verification_exact_phrases = [
                            'react to verify', 'react for verification', 
                            'react below to verify', 'react with', 'react to get',
                            'to gain access', 'to get access', 'verify yourself',
                            'verification', 'verify here', 'verify now',
                            'react for access', 'react for roles', 'react here'
                        ]
                        verification_keywords = [
                            'verify', 'verification', 'react', 'role', 'access',
                            'member', 'click', 'emoji', 'reaction', 'check mark',
                            'accept', 'agree', 'confirm', 'validate', 'join'
                        ]
                        
                        # Check for exact phrases first (strongest signal)
                        for phrase in verification_exact_phrases:
                            if phrase in message_content:
                                verification_message = True
                                verification_level = 3  # Definite verification message
                                logger.debug(f"Found exact verification phrase '{phrase}' in {channel_name}")
                                break
                        
                        # If no exact phrases, check for keyword combinations
                        if verification_level < 3:
                            keyword_count = sum(1 for keyword in verification_keywords if keyword in message_content)
                            if keyword_count >= 3:  # If 3+ verification keywords appear
                                verification_message = True
                                verification_level = 2  # Likely verification message
                                logger.debug(f"Found {keyword_count} verification keywords in {channel_name}")
                            elif keyword_count >= 1:  # If at least 1 keyword appears
                                verification_message = True
                                verification_level = 1  # Maybe verification message
                        
                        # Check embeds for verification content too
                        if verification_level < 3 and 'embeds' in message and message['embeds']:
                            for embed in message['embeds']:
                                # Check embed title
                                embed_title = embed.get('title', '').lower()
                                # Check embed description
                                embed_description = embed.get('description', '').lower()
                                
                                # Check all text fields in the embed
                                embed_texts = [embed_title, embed_description]
                                
                                # Also check fields
                                if 'fields' in embed:
                                    for field in embed['fields']:
                                        field_name = field.get('name', '').lower()
                                        field_value = field.get('value', '').lower()
                                        embed_texts.extend([field_name, field_value])
                                
                                # Check all embed texts for verification phrases
                                for text in embed_texts:
                                    if not text:
                                        continue
                                    
                                    # Check for exact phrases
                                    for phrase in verification_exact_phrases:
                                        if phrase in text:
                                            verification_message = True
                                            verification_level = max(verification_level, 3)
                                            logger.debug(f"Found verification phrase in embed: '{phrase}'")
                                    
                                    # Count keywords
                                    keyword_count = sum(1 for keyword in verification_keywords if keyword in text)
                                    if keyword_count >= 2:
                                        verification_message = True
                                        verification_level = max(verification_level, 2)
                                        logger.debug(f"Found {keyword_count} verification keywords in embed")
                        
                        if verification_message:
                            logger.debug(f"Verification message detected (level {verification_level}) in {channel_name}: {message_content[:50]}...")
                        
                        # Extract emoji mentions from the message content using regex
                        mentioned_emojis = []
                        # Look for custom emoji format like <:emoji_name:emoji_id>
                        custom_emoji_pattern = r'<a?:([a-zA-Z0-9_]+):(\d+)>'
                        for match in re.finditer(custom_emoji_pattern, message_content):
                            emoji_name = match.group(1)
                            emoji_id = match.group(2)
                            mentioned_emojis.append((emoji_name, emoji_id))
                        
                        # If message has reactions, add the same ones
                        if 'reactions' in message and message['reactions']:
                            # Process up to 4 reactions (increased from 3)
                            for reaction in message['reactions'][:4]:
                                emoji_data = reaction.get('emoji', {})
                                emoji_id = emoji_data.get('id')
                                emoji_name = emoji_data.get('name')
                                
                                # Add reaction to message - NON-BLOCKING VERSION
                                try:
                                    # Properly handle emoji formatting for API request
                                    if emoji_id:
                                        emoji_identifier = f"{emoji_name}:{emoji_id}"
                                    else:
                                        emoji_identifier = urllib.parse.quote(emoji_name)
                                        
                                    # Make the request using aiohttp
                                    async with session.put(
                                        f'https://discord.com/api/v9/channels/{channel_id}/messages/{message_id}/reactions/{emoji_identifier}/@me',
                                        headers=headers
                                    ) as reaction_response:
                                        if reaction_response.status in (201, 204):
                                            reactions_added += 1
                                            logger.debug(f"Added reaction {emoji_name} to message in {channel_name}")
                                        else:
                                            response_text = await reaction_response.text()
                                            logger.debug(f"Failed to add reaction: Status {reaction_response.status}, Response: {response_text[:100]}")
                                        
                                        # Vary delay to avoid patterns (0.3-0.7 seconds)
                                        await asyncio.sleep(0.3 + random.random() * 0.4)
                                    
                                except Exception as e:
                                    logger.debug(f"Failed to add reaction: {e}")
                                    
                        # Special handling for verification messages - more intelligent approach
                        # Handle verification messages, including those that already have reactions
                        if verification_message or ('reactions' in message and message['reactions']):
                            logger.debug(f"Processing verification/reaction message with level {verification_level}")
                            
                            emojis_to_try = []
                            has_existing_reactions = False
                            
                            # If message already has reactions, ONLY use those - no need for fallbacks
                            if 'reactions' in message and message['reactions']:
                                logger.debug(f"Message already has {len(message['reactions'])} reactions - using ONLY these")
                                has_existing_reactions = True
                                
                                # Use every reaction from the message - no filtering required
                                for reaction in message['reactions']:
                                    emoji_data = reaction.get('emoji', {})
                                    emoji_id = emoji_data.get('id')
                                    emoji_name = emoji_data.get('name')
                                    if emoji_name:  # Ensure emoji is valid
                                        emojis_to_try.append((emoji_name, emoji_id))
                            
                            # Only if no existing reactions are found, consider alternatives
                            if not has_existing_reactions:
                                # If it's a verification message but has no reactions yet, check for emojis mentioned in content
                                for emoji_name, emoji_id in mentioned_emojis:
                                    emojis_to_try.append((emoji_name, emoji_id))
                                    
                                # Only use fallback emojis if we have nothing else and this is definitely a verification message
                                if not emojis_to_try and verification_level >= 2:
                                    # Common verification emojis - last resort only
                                    common_verification_emojis = ["âœ…", "ðŸ‘", "âœ”ï¸"]
                                    for emoji in common_verification_emojis:
                                        emojis_to_try.append((emoji, None))
                            
                            # Try to add verification emojis - try more options for higher verification levels
                            attempts = min(5, max(2, verification_level + 1))
                            
                            # Initialize tracker for this message if needed
                            if message_id not in message_emoji_map:
                                message_emoji_map[message_id] = set()
                            
                            for emoji_name, emoji_id in emojis_to_try[:attempts]:
                                # Create a unique identifier for this emoji to track
                                emoji_tracking_id = f"{emoji_name}:{emoji_id}" if emoji_id else emoji_name
                                
                                # Skip if we've already added this emoji to this message
                                if emoji_tracking_id in message_emoji_map[message_id]:
                                    logger.debug(f"Skipping emoji {emoji_name} - already added to this message")
                                    continue
                                try:
                                    # Properly handle emoji formatting for API request
                                    if emoji_id:
                                        emoji_identifier = f"{emoji_name}:{emoji_id}"
                                    else:
                                        emoji_identifier = urllib.parse.quote(emoji_name)
                                    
                                    logger.debug(f"Removing any existing emoji {emoji_name} before adding it again")
                                    
                                    # First try to delete any existing reaction we might have using aiohttp
                                    try:
                                        # DELETE request to remove our existing reaction if it exists
                                        async with session.delete(
                                            f'https://discord.com/api/v9/channels/{channel_id}/messages/{message_id}/reactions/{emoji_identifier}/@me',
                                            headers=headers
                                        ) as delete_response:
                                            if delete_response.status in (204, 200):
                                                logger.debug(f"Successfully removed existing reaction {emoji_name}")
                                            elif delete_response.status != 404:  # 404 just means no reaction existed
                                                logger.debug(f"Response when removing reaction: {delete_response.status}")
                                                
                                            # Add a small delay between remove and add
                                            await asyncio.sleep(0.2)
                                    except Exception as e:
                                        logger.debug(f"Error removing existing reaction (continuing anyway): {e}")
                                    
                                    # Now add the reaction
                                    logger.debug(f"Trying to add emoji {emoji_name} to verification message")
                                    
                                    # Make the request using aiohttp
                                    async with session.put(
                                        f'https://discord.com/api/v9/channels/{channel_id}/messages/{message_id}/reactions/{emoji_identifier}/@me',
                                        headers=headers
                                    ) as reaction_response:
                                        if reaction_response.status in (201, 204):
                                            reactions_added += 1
                                            # Add to our tracking set to avoid duplicates
                                            message_emoji_map[message_id].add(emoji_tracking_id)
                                            logger.debug(f"Added verification emoji {emoji_name} to verification message")
                                            # For level 3 (definite) verification messages, add one more emoji as backup
                                            if verification_level < 3:
                                                break
                                        else:
                                            response_text = await reaction_response.text()
                                            logger.debug(f"Failed to add reaction: Status {reaction_response.status}, Response: {response_text[:100]}")
                                        
                                        # Vary delay to avoid patterns
                                        await asyncio.sleep(0.3 + random.random() * 0.4)
                                except Exception as e:
                                    logger.debug(f"Failed to add verification reaction {emoji_name}: {e}")
                            
                        # Stop processing messages in this channel if we've added plenty of reactions
                        if reactions_added >= 15:  # Increased threshold from 5 to 15
                            break
                                
                except Exception as e:
                    logger.debug(f"Error checking messages in channel {channel_id}: {e}")
                
                # Stop checking channels if we've added enough reactions in total
                if reactions_added >= 30:  # New global threshold
                    break
                
                # Randomized delay between channels (1-2 seconds) - NON-BLOCKING VERSION
                await asyncio.sleep(1 + random.random())
            
            # Return summary of reaction verification
            if reactions_added > 0:
                return {
                    "status": True,
                    "channels_checked": channels_checked,
                    "messages_processed": messages_processed,
                    "reactions_added": reactions_added
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error in reaction verification: {e}")
            return {
                "status": False,
                "error": str(e)
            }

    @commands.command(aliases=['tb'], hidden=True)
    @developer_only()
    async def transferboost(self, ctx, target: str, guild_id: int):
        """Transfer available boosts to a specific guild
        ;transferboost <uid/uids/all/others> <guild_id>
        Examples:
        ;transferboost 1 123456789 - Transfer boosts with UID 1
        ;transferboost 1,2,3 123456789 - Transfer boosts with multiple UIDs
        ;transferboost others 123456789 - Transfer boosts with all instances except developer"""
        await self.safe_delete_message(ctx.message)
        
        try:
            bot_manager = self.bot._manager
            
            # Status message
            status_msg = await ctx.send(
                f"```ansi\n\u001b[1;33mAttempting to transfer boosts to guild {guild_id}...\u001b[0m```",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            
            # Determine which instances to use
            selected_instances = []
            
            if target.lower() == 'all':
                for token, instance in bot_manager.bots.items():
                    if instance.is_ready() and not instance.config_manager.is_developer_uid(instance.config_manager.uid):  # Skip developer instances
                        selected_instances.append((instance.config_manager.uid, instance, token))
            
            elif target.lower() == 'others':
                for token, instance in bot_manager.bots.items():
                    if instance.is_ready() and not instance.config_manager.is_developer_uid(instance.config_manager.uid):  # Skip developer instances
                        selected_instances.append((instance.config_manager.uid, instance, token))
            
            else:
                # Handle comma-separated UIDs
                try:
                    target_uids = [int(uid.strip()) for uid in target.split(',')]
                    
                    for uid in target_uids:
                        # Skip developer instances
                        if self.bot.config_manager.is_developer_uid(uid):
                            continue
                            
                        token = self.get_token_by_uid(uid)
                        if not token:
                            continue
                            
                        bot_instance = bot_manager.bots.get(token)
                        if bot_instance and bot_instance.is_ready():
                            selected_instances.append((uid, bot_instance, token))
                
                except ValueError:
                    await status_msg.delete()
                    await self.send_with_auto_delete(ctx, "Invalid UID format. Use number(s) like '1' or '1,2,3', or use 'all' or 'others'")
                    return
            
            if not selected_instances:
                await status_msg.delete()
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mNo valid instances found to use```"), 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            
            # Process each selected instance using asyncio tasks
            results = []
            success_count = 0
            
            # Create async tasks for each instance
            tasks = []
            for uid, bot_instance, token in selected_instances:
                tasks.append(self.transfer_boost(uid, bot_instance, token, guild_id))
            
            # Run tasks concurrently and collect results
            if tasks:
                # Update status message to show work is happening in background
                await status_msg.edit(content=quote_block("```ansi\n\u001b[1;33mTransferring boosts in background...\u001b[0m```"))
                  # Create background task to process results without blocking the main event loop
                self.bot.loop.create_task(self._gather_boost_results(tasks, status_msg, len(tasks), guild_id))
                
                # Return immediately with a status message
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;33mBoost transfer requests are processing in background. Check logs for results.\u001b[0m```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            
            await status_msg.delete()
            
            if not results:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mNo results returned```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            
            # Format results into a nice message
            response_msg = (
                f"```ansi\n\u001b[1;33mBoost Transfer Results\u001b[0m\n" +
                f"\u001b[0;36mGuild ID: \u001b[0;37m{guild_id}\u001b[0m\n" +
                f"\u001b[0;36mSuccess: \u001b[0;37m{success_count}/{len(results)}\u001b[0m\n\n"
            )
            
            for result in results:
                if "✅" in result:
                    response_msg += f"\u001b[1;32m{result}\u001b[0m\n"
                else:
                    response_msg += f"\u001b[1;31m{result}\u001b[0m\n"
            
            response_msg += "```"
            
            await ctx.send(
                quote_block(response_msg),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
        
        except Exception as e:
            logger.error(f"Error in transferboost command: {e}")
            await self.send_with_auto_delete(ctx, f"Error: {str(e)}")

    async def _gather_boost_results(self, tasks, status_msg, total_tasks, guild_id):
        """Helper method to gather boost transfer results without blocking the main event loop"""
        try:
            results = []
            success_count = 0
            
            # Process tasks as they complete
            for task in asyncio.as_completed(tasks):
                result = await task
                if result:
                    results.append(result)
                    if "✅" in result:
                        success_count += 1
                    
                    # Update status message with partial results
                    try:
                        await status_msg.edit(content=quote_block(f"```ansi\n\u001b[1;33mTransferring boosts: {len(results)}/{total_tasks} complete...\u001b[0m```"))
                    except Exception as e:
                        logger.error(f"Error updating status message: {e}")
            
            # Format results into a nice message
            if results:
                response_msg = (
                    f"```ansi\n\u001b[1;33mBoost Transfer Results\u001b[0m\n" +
                    f"\u001b[0;36mGuild ID: \u001b[0;37m{guild_id}\u001b[0m\n" +
                    f"\u001b[0;36mSuccess: \u001b[0;37m{success_count}/{len(results)}\u001b[0m\n\n"
                )
                
                for result in results:
                    if "✅" in result:
                        response_msg += f"\u001b[1;32m{result}\u001b[0m\n"
                    else:
                        response_msg += f"\u001b[1;31m{result}\u001b[0m\n"
                
                response_msg += "```"
                
                # Try to update the status message with results
                try:
                    await status_msg.edit(content=quote_block(response_msg))
                    # Keep the message for the auto-delete time
                    await asyncio.sleep(self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else 120)
                    await status_msg.delete()
                except Exception as e:
                    logger.error(f"Error updating final status message: {e}")
                    # If we can't edit the message (e.g., it was deleted), log the results
                    logger.info(f"Boost transfer results: {success_count}/{len(results)} successful")
        except Exception as e:
            logger.error(f"Error in _gather_boost_results: {e}")

    async def transfer_boost(self, uid, bot_instance, token, guild_id):
        """Transfer available boosts to a specific guild"""
        try:
            # First, get user's current boost subscriptions
            headers = {
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.8',
                'authorization': token,
                'content-type': 'application/json',
                'origin': 'https://discord.com',
                'referer': 'https://discord.com/channels/@me',
                'sec-ch-ua': '"Not(A:Brand";v="99", "Brave";v="131", "Chromium";v="131"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'sec-gpc': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                'x-debug-options': 'bugReporterEnabled',
                'x-discord-locale': 'en-US',
                'x-discord-timezone': 'America/Los_Angeles',
                'x-super-properties': bot_instance.http.headers.encoded_super_properties
            }
            
            # Get user's premium subscription slots using aiohttp
            async with aiohttp.ClientSession() as session:
                # Use the subscription-slots endpoint which returns available boost slots
                async with session.get(
                    'https://discord.com/api/v9/users/@me/guilds/premium/subscription-slots',
                    headers=headers
                ) as slots_response:
                    if slots_response.status != 200:
                        return f"UID {uid}: Failed to get subscription slots: {slots_response.status}"

                    slots_data = await slots_response.json()

                    if not slots_data:
                        return f"UID {uid}: No boost slots available (no Nitro subscription?)"

                    # Find boosts that can be transferred (not on cooldown and not already on target guild)
                    available_boosts = []
                    from datetime import datetime, timezone
                    now = datetime.now(timezone.utc)

                    for slot in slots_data:
                        slot_id = slot.get('id')
                        cooldown_ends_at = slot.get('cooldown_ends_at')

                        # Check if already boosting target guild
                        if slot.get('premium_guild_subscription') and \
                           slot['premium_guild_subscription'].get('guild_id') == str(guild_id):
                            continue  # Skip, already boosting this guild

                        # Check cooldown
                        if cooldown_ends_at:
                            cooldown_time = datetime.fromisoformat(cooldown_ends_at.replace('Z', '+00:00'))
                            if cooldown_time > now:
                                # Still on cooldown
                                continue

                        # This boost is available for transfer
                        available_boosts.append(slot)

                    if not available_boosts:
                        return f"UID {uid}: No available boosts to transfer (all on cooldown or already boosting target)"

                    # Transfer the first available boost to the target guild
                    boost_to_transfer = available_boosts[0]
                    subscription_id = boost_to_transfer['id']
                    
                    # Make the transfer request using aiohttp
                    transfer_payload = {
                        'guild_id': str(guild_id)
                    }
                    
                    async with session.put(
                        f'https://discord.com/api/v9/users/@me/guilds/premium/subscriptions/{subscription_id}',
                        headers=headers,
                        json=transfer_payload
                    ) as transfer_response:
                        # Add small delay between requests to avoid rate limits
                        await asyncio.sleep(1)
                        
                        # Process result
                        if transfer_response.status == 200:
                            try:
                                # Try to get guild name for better reporting
                                guild = bot_instance.get_guild(guild_id)
                                guild_name = guild.name if guild else f"Guild {guild_id}"
                                return f"UID {uid}: ✅ Successfully transferred boost to {guild_name}"
                            except:
                                return f"UID {uid}: ✅ Successfully transferred boost to guild {guild_id}"
                        elif transfer_response.status == 204:
                            try:
                                guild = bot_instance.get_guild(guild_id)
                                guild_name = guild.name if guild else f"Guild {guild_id}"
                                return f"UID {uid}: ✅ Successfully transferred boost to {guild_name}"
                            except:
                                return f"UID {uid}: ✅ Successfully transferred boost to guild {guild_id}"
                        else:
                            error_msg = await transfer_response.text()
                            error_text = error_msg[:100] if error_msg else "Unknown error"
                            return f"UID {uid}: ❌ Failed to transfer boost ({transfer_response.status}): {error_text}"
                
        except Exception as e:
            logger.error(f"Error transferring boost for UID {uid}: {e}")
            return f"UID {uid}: ❌ Error: {str(e)}"

    @commands.command(aliases=['ji'], hidden=True)
    @developer_only()
    async def joininvite(self, ctx, target: str, invite_code: str):
        """Join a guild with reduced captcha detection
        ;joininvite <uid/uids/all/others> <invite_code>
        Examples:
        ;joininvite 1 invite - Join a server with UID 1
        ;joininvite 1,2,3 invite - Join with multiple UIDs
        ;joininvite others invite - Join with all instances except developer"""
        await self.safe_delete_message(ctx.message)
        
        try:
            bot_manager = self.bot._manager
            
            # Process invite code (if full URL was provided)
            if invite_code.startswith(('https://', 'http://', 'discord.gg/')):
                invite_code = invite_code.split('/')[-1]
            
            # Status message
            status_msg = await ctx.send(
                f"```ansi\n\u001b[1;33mAttempting to join invite {invite_code}...\u001b[0m```",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            
            # Determine which instances to use
            selected_instances = []
            
            if target.lower() == 'all':
                for token, instance in bot_manager.bots.items():
                    if instance.is_ready() and not instance.config_manager.is_developer_uid(instance.config_manager.uid):  # Skip developer instances
                        selected_instances.append((instance.config_manager.uid, instance, token))
            
            elif target.lower() == 'others':
                for token, instance in bot_manager.bots.items():
                    if instance.is_ready() and not instance.config_manager.is_developer_uid(instance.config_manager.uid):  # Skip developer instances
                        selected_instances.append((instance.config_manager.uid, instance, token))
            
            else:
                # Handle comma-separated UIDs
                try:
                    target_uids = [int(uid.strip()) for uid in target.split(',')]
                    
                    for uid in target_uids:
                        # Skip developer instances
                        if self.bot.config_manager.is_developer_uid(uid):
                            continue
                            
                        token = self.get_token_by_uid(uid)
                        if not token:
                            continue
                            
                        bot_instance = bot_manager.bots.get(token)
                        if bot_instance and bot_instance.is_ready():
                            selected_instances.append((uid, bot_instance, token))
                
                except ValueError:
                    await status_msg.delete()
                    await self.send_with_auto_delete(ctx, "Invalid UID format. Use number(s) like '1' or '1,2,3', or use 'all' or 'others'")
                    return
            
            if not selected_instances:
                await status_msg.delete()
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mNo valid instances found to use```"), 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            # Process each selected instance using asyncio tasks
            results = []
            success_count = 0
            
            # Create async tasks for each instance
            tasks = []
            for uid, bot_instance, token in selected_instances:
                tasks.append(self.join_guild(uid, bot_instance, token, invite_code))
            
            # Run tasks concurrently and collect results
            if tasks:
                # Update status message to show work is happening in background
                await status_msg.edit(content=quote_block("```ansi\n\u001b[1;33mJoining servers in background...\u001b[0m```"))
                  # Create background task to process results without blocking the main event loop
                self.bot.loop.create_task(self._gather_join_results(tasks, status_msg, len(tasks)))
                
                # Return immediately with a status message
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;33mJoin requests are processing in background. Check logs for results.\u001b[0m```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            
            await status_msg.delete()
            
            if not results:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mNo results returned```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            
            # Format results into a nice message
            response_msg = (
                f"```ansi\n\u001b[1;33mInvite Join Results\u001b[0m\n" +
                f"\u001b[0;36mInvite: \u001b[0;37m{invite_code}\u001b[0m\n" +
                f"\u001b[0;36mSuccess: \u001b[0;37m{success_count}/{len(results)}\u001b[0m\n\n"
            )
            
            for result in results:
                if "âœ…" in result:
                    response_msg += f"\u001b[1;32m{result}\u001b[0m\n"
                else:
                    response_msg += f"\u001b[1;31m{result}\u001b[0m\n"
            
            response_msg += "```"
            
            await ctx.send(
                quote_block(response_msg),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
        
        except Exception as e:
            logger.error(f"Error in joininvite command: {e}")
            await self.send_with_auto_delete(ctx, f"Error: {str(e)}")    
    async def _gather_join_results(self, tasks, status_msg, total_tasks):
        """Helper method to gather join results without blocking the main event loop"""
        try:
            results = []
            success_count = 0
            
            # Process tasks as they complete
            for task in asyncio.as_completed(tasks):
                result = await task
                if result:
                    results.append(result)
                    # Count as success if the user joined (regardless of verification/onboarding status)
                    if "Joined" in result and "Failed" not in result:
                        success_count += 1
                    
                    # Update status message with partial results
                    try:
                        await status_msg.edit(content=quote_block(f"```ansi\n\u001b[1;33mJoining in progress: {len(results)}/{total_tasks} complete...\u001b[0m```"))
                    except Exception as e:
                        logger.error(f"Error updating status message: {e}")
            
            # Format results into a nice message
            if results:
                response_msg = (
                    f"```ansi\n\u001b[1;33mInvite Join Results\u001b[0m\n" +
                    f"\u001b[0;36mSuccess: \u001b[0;37m{success_count}/{len(results)}\u001b[0m\n\n"
                )
                
                for result in results:
                    if "âœ…" in result:
                        response_msg += f"\u001b[1;32m{result}\u001b[0m\n"
                    else:
                        response_msg += f"\u001b[1;31m{result}\u001b[0m\n"
                
                response_msg += "```"
                
                # Try to update the status message with results
                try:
                    await status_msg.edit(content=quote_block(response_msg))
                    # Keep the message for the auto-delete time
                    await asyncio.sleep(self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else 120)
                    await status_msg.delete()
                except Exception as e:
                    logger.error(f"Error updating final status message: {e}")
                    # If we can't edit the message (e.g., it was deleted), log the results
                    logger.info(f"Join invite results: {success_count}/{len(results)} successful")
        except Exception as e:
            logger.error(f"Error in _gather_join_results: {e}")
            
    async def join_guild(self, uid, bot_instance, token, invite_code):
        try:
            session_id = bot_instance.ws._connection.session_id
            
            if not session_id:
                return f"UID {uid}: Failed to get session ID, instance not properly connected"
            
            # Headers for fingerprint request
            finger_headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.8',
                'cache-control': 'max-age=0',
                'priority': 'u=0, i',
                'sec-ch-ua': '"Not(A:Brand";v="99", "Brave";v="131", "Chromium";v="131"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'sec-fetch-user': '?1',
                'sec-gpc': '1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            }
            
            # Get fingerprint and cookies using curl_cffi AsyncSession with retry logic
            finger_response = None
            for attempt in range(3):  # Try up to 3 times
                try:
                    async with AsyncSession(impersonate="chrome131") as session:
                        finger_response = await session.get(
                            'https://discord.com/api/v10/experiments', 
                            headers=finger_headers
                        )
                    break  # Success, exit retry loop
                except Exception as e:
                    if attempt < 2:  # Not the last attempt
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
                        continue
                    else:
                        return f"UID {uid}: SSL/Connection error after retries: {str(e)[:100]}"
            
            if not finger_response or finger_response.status_code != 200:
                return f"UID {uid}: Failed to get fingerprint: {finger_response.status_code if finger_response else 'No response'}"
            
            fingerprint_data = finger_response.json()
            fingerprint = fingerprint_data.get('fingerprint')
            
            # Extract cookies
            dcfduid = finger_response.cookies.get('__dcfduid', '')
            sdcfduid = finger_response.cookies.get('__sdcfduid', '')
            cfruid = finger_response.cookies.get('__cfruid', '')
            
            vcookie = f"locale=en; __dcfduid={dcfduid}; __sdcfduid={sdcfduid}; __cfruid={cfruid}"
            
            # Join guild
            headers = {
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.8',
                'authorization': token,
                'content-type': 'application/json',
                'origin': 'https://discord.com',
                'priority': 'u=1, i',
                'sec-ch-ua': '"Not(A:Brand";v="99", "Brave";v="131", "Chromium";v="131"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'sec-gpc': '1',
                'cookie': vcookie,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
                'x-debug-options': 'bugReporterEnabled',
                'x-discord-locale': 'en-US',
                'x-discord-timezone': 'America/Los_Angeles',
                'x-super-properties': bot_instance.http.headers.encoded_super_properties
            }
            
            if fingerprint:
                headers['fingerprint'] = fingerprint
            
            json_data = {'session_id': session_id}
            
            # Make the POST request using curl_cffi AsyncSession with retry logic
            response = None
            for attempt in range(3):  # Try up to 3 times
                try:
                    async with AsyncSession(impersonate="chrome131") as session:
                        response = await session.post(
                            f'https://discord.com/api/v9/invites/{invite_code}', 
                            headers=headers, 
                            json=json_data
                        )
                    break  # Success, exit retry loop
                except Exception as e:
                    if attempt < 2:  # Not the last attempt
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
                        continue
                    else:
                        return f"UID {uid}: SSL/Connection error during join after retries: {str(e)[:100]}"
            
            if not response:
                return f"UID {uid}: No response received after retries"
            
            # Add small delay between requests to avoid rate limits
            await asyncio.sleep(1)
            
            # Process result
            if response.status_code == 200:
                json_response = response.json()
                guild_name = json_response.get('guild', {}).get('name', 'Unknown Server')
                guild_id = json_response.get('guild', {}).get('id')
                  # Handle verification if present
                verification_result = None
                onboarding_result = None
                reaction_verification_result = None
                
                if guild_id:
                    # First run the verification check (this needs to be done first)
                    verification_result = await self.handle_server_verification(guild_id, invite_code, token, headers)
                    
                    # If verification passed or wasn't needed, or if it failed with 410 (already verified), run onboarding and reaction verification
                    should_run_onboarding = (
                        not verification_result or 
                        verification_result.get("status", False) or 
                        (verification_result.get("error", "").startswith("Status 410"))
                    )
                    
                    if should_run_onboarding:
                        # Run onboarding and reaction verification concurrently for better performance
                        onboarding_task = asyncio.create_task(self.handle_server_onboarding(guild_id, token, headers))
                        reaction_verification_task = asyncio.create_task(self.handle_server_reaction_verification(guild_id, token, headers))
                        
                        # Wait for both tasks to complete
                        onboarding_result = await onboarding_task
                        reaction_verification_result = await reaction_verification_task
                
                # Format result message based on verification, onboarding, and reaction verification
                if verification_result and onboarding_result and reaction_verification_result:
                    if verification_result.get("status", False) and onboarding_result.get("status", False) and reaction_verification_result.get("status", False):
                        return f"UID {uid}: Joined {guild_name}, completed verification ({verification_result.get('num_fields', 0)} fields), onboarding ({onboarding_result.get('num_responses', 0)} responses), and reaction verification ({reaction_verification_result.get('reactions_added', 0)} reactions)"
                    elif verification_result.get("status", False) and onboarding_result.get("status", False):
                        return f"UID {uid}: Joined {guild_name}, completed verification and onboarding but reaction verification failed: {reaction_verification_result.get('error', 'Unknown error')}"
                    elif verification_result.get("status", False):
                        return f"UID {uid}: Joined {guild_name}, completed verification but onboarding and reaction verification failed: {onboarding_result.get('error', 'Unknown error')}, {reaction_verification_result.get('error', 'Unknown error')}"
                    else:
                        return f"UID {uid}: Joined {guild_name} but verification, onboarding, and reaction verification failed: {verification_result.get('error', 'Unknown error')}, {onboarding_result.get('error', 'Unknown error')}, {reaction_verification_result.get('error', 'Unknown error')}"
                elif verification_result and onboarding_result:
                    if verification_result.get("status", False) and onboarding_result.get("status", False):
                        return f"UID {uid}: Joined {guild_name}, completed verification and onboarding"
                    elif verification_result.get("status", False):
                        return f"UID {uid}: Joined {guild_name}, completed verification but onboarding failed: {onboarding_result.get('error', 'Unknown error')}"
                    else:
                        return f"UID {uid}: Joined {guild_name} but verification and onboarding failed: {verification_result.get('error', 'Unknown error')}, {onboarding_result.get('error', 'Unknown error')}"
                elif verification_result:
                    if verification_result.get("status", False):
                        return f"UID {uid}: Joined {guild_name} and completed verification ({verification_result.get('num_fields', 0)} fields)"
                    else:
                        return f"UID {uid}: Joined {guild_name} but verification failed: {verification_result.get('error', 'Unknown error')}"
                elif onboarding_result:
                    if onboarding_result.get("status", False):
                        return f"UID {uid}: Joined {guild_name} and completed onboarding ({onboarding_result.get('num_responses', 0)} responses)"
                    else:
                        return f"UID {uid}: Joined {guild_name} but onboarding failed: {onboarding_result.get('error', 'Unknown error')}"
                elif reaction_verification_result:
                    if reaction_verification_result.get("status", False):
                        return f"UID {uid}: Joined {guild_name} and completed reaction verification ({reaction_verification_result.get('reactions_added', 0)} reactions)"
                    else:
                        return f"UID {uid}: Joined {guild_name} but reaction verification failed: {reaction_verification_result.get('error', 'Unknown error')}"
                else:
                    return f"UID {uid}: Joined {guild_name}"
            else:
                return f"UID {uid}: Failed ({response.status_code}): {response.text[:100]}"
        except Exception as e:
            logger.error(f"Error joining invite for UID {uid}: {e}")
            return f"UID {uid}: Error: {str(e)}"

    @commands.command(aliases=['um'], hidden=True)
    @developer_only()
    async def usermanage(self, ctx, action: str, *args):
        """Manage users in the selfbot system
        
        Usage:
        ;usermanage add <token> - Add a new user or update existing
        ;usermanage remove <uid> - Remove a user
        ;usermanage disconnect <uid> - Disconnect a user
        ;usermanage reconnect <uid> - Reconnect a disconnected user
        """
        await self.safe_delete_message(ctx.message)
        
        action = action.lower()
        
        # ADD USER LOGIC
        if action in ['add', 'adduser', 'au']:
            if not args:
                await self.send_with_auto_delete(ctx, "Token is required")
                return
                
            token = args[0]
            try:
                # Validate token format and API
                if not self.bot.config_manager.validate_token(token):
                    await self.send_with_auto_delete(ctx, "Invalid token format")
                    return
        
                if not await self.bot.config_manager.validate_token_api(token):
                    await self.send_with_auto_delete(ctx, "Invalid token - API validation failed")
                    return
        
                # Get user ID from the token
                user_id = int(base64.b64decode(token.split('.')[0] + "==").decode('utf-8'))
        
                bot_manager = self.bot._manager
        
                # Check if an existing user entry matches this Discord ID
                existing_uid = None
                old_token = None
                with open('config.json', 'r') as f:
                    config = json.load(f)
                    for t, settings in config['user_settings'].items():
                        if settings.get('discord_id') == user_id:
                            existing_uid = settings.get('uid')
                            old_token = t
                            break
        
                # If user already exists, update the token instead of creating a new user
                if existing_uid is not None:
                    # Close old bot instance if it's still running
                    if old_token and old_token in bot_manager.bots:
                        try:
                            old_bot = bot_manager.bots[old_token]
                            old_bot._closed = True
                            await old_bot.close()
                            del bot_manager.bots[old_token]
                            await asyncio.sleep(1)  # Give time for cleanup
                        except Exception as e:
                            logger.error(f"Error closing old bot instance: {e}")
        
                    # Update user token in config
                    if self.bot.config_manager.update_user_token(existing_uid, token):
                        await self.bot.config_manager.reload_config_async()
                        # Start new bot instance
                        try:
                            await bot_manager.start_bot(token)
                        except Exception as e:
                            logger.error(f"Error starting new bot instance: {e}")
                            raise
        
                        # Automatically authorize the user for hosting if not already authorized
                        try:
                            existing_auth = await self.bot.db.db.authorized_hosts.find_one({"user_id": user_id})
                            if not existing_auth:
                                # Get username for the authorization record
                                try:
                                    user = await self.bot.GetUser(user_id)
                                    username = user.name if user else "Unknown"
                                except:
                                    username = "Unknown"
                                
                                # Add to authorized hosts
                                await self.bot.db.db.authorized_hosts.insert_one({
                                    "user_id": user_id,
                                    "username": username,
                                    "hosting_limit": 5,  # Default limit
                                    "added_at": datetime.utcnow(),
                                    "added_by": ctx.author.id,
                                    "auto_added": True  # Mark as auto-added by usermanage
                                })
                                
                                await self.send_with_auto_delete(ctx, f"Updated token for existing user with UID: {existing_uid} and automatically authorized for hosting")
                            else:
                                await self.send_with_auto_delete(ctx, f"Updated token for existing user with UID: {existing_uid} (already authorized for hosting)")
                        except Exception as e:
                            logger.error(f"Error auto-authorizing existing user: {e}")
                            await self.send_with_auto_delete(ctx, f"Updated token for existing user with UID: {existing_uid} (failed to auto-authorize: {e})")
                    else:
                        await self.send_with_auto_delete(ctx, f"Failed to update token for UID: {existing_uid}")
                    return
        
                # Otherwise, create a new user if not found
                # Read the default prefix from cached config instead of hitting disk
                cfg = await self.bot.config_manager._get_cached_config_async()
                default_prefix = cfg.get('command_prefix', ';')  # Use default prefix from config
                
                user_config = UserConfig(
                    token=token,
                    username=None,
                    command_prefix=default_prefix,  # Use the default prefix instead of developer's current prefix
                    leakcheck_api_key='',
                    auto_delete=AutoDeleteConfig(enabled=True, delay=120),
                    presence={},
                    connected=True,
                    discord_id=user_id,
                    uid=None
                )
        
                # Save config for new user (async to avoid blocking)
                await self.bot.config_manager.save_user_config_async(user_config)
        
                # Update tokens list asynchronously
                await self.bot.config_manager.add_token_async(token)
        
                # Start new bot instance for the brand-new user
                try:
                    await bot_manager.start_bot(token)
                    cfg = await self.bot.config_manager._get_cached_config_async()
                    uid = cfg.get('user_settings', {}).get(token, {}).get('uid', '?')
                    
                    # Automatically authorize the user for hosting
                    try:
                        # Check if user is already authorized
                        existing_auth = await self.bot.db.db.authorized_hosts.find_one({"user_id": user_id})
                        if not existing_auth:
                            # Get username for the authorization record
                            try:
                                user = await self.bot.GetUser(user_id)
                                username = user.name if user else "Unknown"
                            except:
                                username = "Unknown"
                            
                            # Add to authorized hosts
                            await self.bot.db.db.authorized_hosts.insert_one({
                                "user_id": user_id,
                                "username": username,
                                "hosting_limit": 5,  # Default limit
                                "added_at": datetime.utcnow(),
                                "added_by": ctx.author.id,
                                "auto_added": True  # Mark as auto-added by usermanage
                            })
                            
                            await self.send_with_auto_delete(ctx, f"Added new user with UID: {uid} and automatically authorized for hosting")
                        else:
                            await self.send_with_auto_delete(ctx, f"Added new user with UID: {uid} (already authorized for hosting)")
                    except Exception as e:
                        logger.error(f"Error auto-authorizing user: {e}")
                        await self.send_with_auto_delete(ctx, f"Added new user with UID: {uid} (failed to auto-authorize: {e})")
                        
                except Exception as e:
                    logger.error(f"Error starting bot: {e}")
                    raise
        
            except Exception as e:
                logger.error(f"Error adding user: {e}")
                await self.send_with_auto_delete(ctx, f"Error adding user: {e}")
                
        # REMOVE USER LOGIC  
        elif action in ['remove', 'removeuser', 'ru']:
            if not args:
                await self.send_with_auto_delete(ctx, "UID is required")
                return
                
            try:
                uid = int(args[0])
                
                if self.bot.config_manager.is_developer_uid(uid):
                    await self.send_with_auto_delete(ctx, "Cannot remove the developer account")
                    return
                
                # Get user's discord_id before removing (for authorized_hosts cleanup)
                user_discord_id = None
                try:
                    with open('config.json', 'r') as f:
                        config = json.load(f)
                        for token, settings in config.get('user_settings', {}).items():
                            if settings.get('uid') == uid:
                                user_discord_id = settings.get('discord_id')
                                break
                except Exception as e:
                    logger.error(f"Error getting discord_id for UID {uid}: {e}")
                
                # First disconnect the user
                self.bot.config_manager.set_user_connected(uid, False)
                
                # Find token to remove using helper method
                token_to_remove = self.get_token_by_uid(uid)
        
                if token_to_remove:
                    bot_manager = self.bot._manager
                    if token_to_remove in bot_manager.bots:
                        bot_instance = bot_manager.bots[token_to_remove]
                        
                        # Properly close the bot instance
                        await bot_instance.close()
                        del bot_manager.bots[token_to_remove]
        
                    # Remove from config
                    if self.bot.config_manager.remove_user(uid):
                        # Also remove from authorized_hosts if we have the discord_id
                        if user_discord_id:
                            try:
                                result = await self.bot.db.db.authorized_hosts.delete_one({"user_id": user_discord_id})
                                if result.deleted_count > 0:
                                    await self.send_with_auto_delete(ctx, f"Removed user with UID: {uid} and removed hosting authorization")
                                else:
                                    await self.send_with_auto_delete(ctx, f"Removed user with UID: {uid} (was not in authorized hosts)")
                            except Exception as e:
                                logger.error(f"Error removing from authorized_hosts: {e}")
                                await self.send_with_auto_delete(ctx, f"Removed user with UID: {uid} (failed to remove hosting authorization: {e})")
                        else:
                            await self.send_with_auto_delete(ctx, f"Removed user with UID: {uid}")
                    else:
                        await self.send_with_auto_delete(ctx, f"Failed to remove user with UID: {uid}")
                else:
                    await self.send_with_auto_delete(ctx, f"No user found with UID: {uid}")
        
            except ValueError:
                await self.send_with_auto_delete(ctx, "UID must be a number")
            except Exception as e:
                logger.error(f"Error removing user: {e}")
                await self.send_with_auto_delete(ctx, f"Error removing user: {e}")
                
        # DISCONNECT USER LOGIC
        elif action in ['disconnect', 'dc']:
            if not args:
                await self.send_with_auto_delete(ctx, "UID is required")
                return
                
            try:
                uid = int(args[0])
                
                if self.bot.config_manager.is_developer_uid(uid):
                    await self.send_with_auto_delete(ctx, "Cannot disconnect the developer account")
                    return
                
                # First update the config file
                self.bot.config_manager.set_user_connected(uid, False)
                
                # Find the token for this UID using helper method
                token = self.get_token_by_uid(uid)
                            
                if token:
                    # Get bot manager and cleanup the instance
                    bot_manager = self.bot._manager
                    if token in bot_manager.bots:
                        bot_instance = bot_manager.bots[token]
                        await bot_instance.close()  # This needs to be awaited
                        del bot_manager.bots[token]
                        self.bot.config_manager.set_user_connected(uid, False)
                        
                    await self.send_with_auto_delete(ctx, f"Disconnected user with UID {uid}")
                else:
                    await self.send_with_auto_delete(ctx, f"No user found with UID {uid}")
            
            except ValueError:
                await self.send_with_auto_delete(ctx, "UID must be a number")
            except Exception as e:
                logger.error(f"Error disconnecting user: {e}")
                await self.send_with_auto_delete(ctx, f"Error disconnecting user: {e}")
                
        # RECONNECT USER LOGIC
        elif action in ['reconnect', 'rc']:
            if not args:
                await self.send_with_auto_delete(ctx, "UID is required")
                return
                
            try:
                uid = int(args[0])
                
                if self.bot.config_manager.is_developer_uid(uid):
                    await self.send_with_auto_delete(ctx, "Cannot reconnect the developer account")
                    return
                    
                # check if user is already connected
                bot_manager = self.bot._manager
                for instance in bot_manager.bots.values():
                    if instance.config_manager.uid == uid:
                        await self.send_with_auto_delete(ctx, f"User with UID {uid} is already connected")
                        return
    
                with open('config.json', 'r+') as f:
                    config = json.load(f)
                    
                    # Find token by UID using helper method
                    token = self.get_token_by_uid(uid)
                    if token and token in config['user_settings']:
                        # Update the connected status
                        config['user_settings'][token]['connected'] = True
                            
                    if token:
                        # Save updated config
                        f.seek(0)
                        json.dump(config, f, indent=4)
                        f.truncate()
    
                        # Start new bot instance through BotManager
                        await bot_manager.start_bot(token)
    
                        await self.send_with_auto_delete(ctx, f"Reconnected user with UID {uid}")
                    else:
                        await self.send_with_auto_delete(ctx, f"No user found with UID {uid}")
    
            except ValueError:
                await self.send_with_auto_delete(ctx, "UID must be a number")
            except Exception as e:
                logger.error(f"Error reconnecting user: {e}")
                await self.send_with_auto_delete(ctx, f"Error reconnecting user: {e}")
        
        else:
            await self.send_with_auto_delete(ctx, f"Unknown action: {action}. Use add, remove, disconnect, or reconnect.")

    @commands.command(aliases=['vs'], hidden=True)
    @developer_only()
    async def validatetokens(self, ctx):
        """Check all tokens and remove invalid ones"""
        await self.safe_delete_message(ctx.message)
        
        try:
            removed = []
            bot_manager = self.bot._manager
    
            with open('config.json', 'r') as f:
                config = json.load(f)
    
            # Check each token against Discord API
            for token, settings in list(config['user_settings'].items()):
                if not await self.bot.config_manager.validate_token_api(token):
                    uid = settings.get('uid')
                    if self.bot.config_manager.remove_user(uid):
                        # Stop the invalid bot instance if it's running
                        if token in bot_manager.bots:
                            await bot_manager.bots[token].close()
                            del bot_manager.bots[token]
                        removed.append(uid)
    
            if removed:
                await self.send_with_auto_delete(ctx, 
                    f"Removed invalid tokens for UIDs: {', '.join(str(uid) for uid in removed)}")
            else:
                await self.send_with_auto_delete(ctx, "All tokens are valid")
    
        except Exception as e:
            logger.error(f"Error validating tokens: {e}")
            await self.send_with_auto_delete(ctx, f"Error validating tokens: {e}")

    @commands.command(aliases=['lu'], hidden=True)
    @developer_only()
    async def listusers(self, ctx, page_or_option: Union[int, str] = 1, option: str = None):
        """List all connected bot instances and their status
        ;lu - List all users (page 1)
        ;lu 2 - List all users (page 2)
        ;lu uids - Display only hosted users' UIDs
        ;lu 1 uids - Display only hosted users' UIDs (same as above)"""
        try:
            await self.safe_delete_message(ctx.message)
            
            # Check if first parameter is 'uids' or similar
            if isinstance(page_or_option, str) and page_or_option.lower() in ['uid', 'uids', 'true']:
                page = 1
                show_uids_only = True
            else:
                # First parameter is a page number
                page = int(page_or_option)
                # Check if second parameter is 'uids' or similar
                show_uids_only = option is not None and option.lower() in ['uid', 'uids', 'true']
            
            # Get the BotManager instance  
            bot_manager = self.bot._manager
    
            # Get cached config for user settings
            config = await self.bot.config_manager._get_cached_config_async()
                
            # Collect all users info except developer
            users_info = []
            dev_ids = self.bot.config_manager.developer_ids
            
            for token, settings in config['user_settings'].items():
                uid = settings.get('uid', '?')
                prefix = settings.get('command_prefix', ctx.prefix)
                discord_id = settings.get('discord_id', '?')
                
                # Skip if this is a developer account
                if discord_id in dev_ids:
                    continue
                    
                stored_username = settings.get('username', 'Unknown')
                
                # Get bot instance status if connected
                user = None
                guild_count = 0
                discord_status = "Offline"
                
                if token in bot_manager.bots:
                    bot = bot_manager.bots[token]
                    if bot.is_ready():
                        user = bot.user
                        guild_count = len(bot.guilds)
                        discord_status = str(bot.status)
                
                # Use stored username if bot instance not available
                display_name = user.name if user else stored_username
                
                users_info.append({
                    'uid': str(uid),
                    'discord_id': discord_id, 
                    'username': display_name,
                    'prefix': prefix,
                    'guild_count': guild_count,
                    'status': discord_status
                })
    
            # Sort by UID
            users_info.sort(key=lambda x: int(x['uid']))
    
            if not users_info:
                await self.send_with_auto_delete(ctx, "No users found")
                return
              # If hosted_only parameter is True, just display a list of UIDs
            if show_uids_only:
                uids_list = [f"{user_info['uid']} ({user_info['username']})" for user_info in users_info]
                uids_text = "\n".join(uids_list)
                
                # Create a comma-separated list of just UIDs
                uids_csv = ",".join([user_info['uid'] for user_info in users_info])
                
                message = f"```ansi\n\u001b[30m\u001b[1m\u001b[4mHosted User UIDs\u001b[0m\n{uids_text}\n\nTotal users: {len(users_info)}\n\nComma-separated UIDs:\n{uids_csv}```"
                await ctx.send(
                    quote_block(message),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
                
            # Pagination
            items_per_page = 5
            total_pages = (len(users_info) + items_per_page - 1) // items_per_page
            
            page = min(max(1, page), total_pages)
            start_idx = (page - 1) * items_per_page
            page_users = users_info[start_idx:start_idx + items_per_page]
    
            # Format message with improved emojis for status
            status_emoji = {
                "online": "✅", "idle": "🌙",
                "dnd": "⛔", "invisible": "👻",
                "offline": "⚪"
            }
    
            message_parts = [
                "```ansi\n" + \
                "\u001b[30m\u001b[1m\u001b[4mBot Instances\u001b[0m\n"
            ]
    
            for user_info in page_users:
                message_parts[-1] += (
                    f"\u001b[0;33mUID: {user_info['uid']}\n" + \
                    f"\u001b[0;36mID: \u001b[0;37m{user_info['discord_id']}\n" + \
                    f"\u001b[0;36mName: \u001b[0;37m{user_info['username']}\n" + \
                    f"\u001b[0;36mPrefix: \u001b[0;37m{user_info['prefix']}\n" + \
                    f"\u001b[0;36mGuilds: \u001b[0;37m{user_info['guild_count']}\n" + \
                    f"\u001b[0;36mStatus: \u001b[0;37m{status_emoji.get(user_info['status'].lower(), 'â“')} {user_info['status'].title()}\n" + \
                    f"\u001b[0;37m{'─' * 20}\n"
                )
    
            message_parts[-1] += "```"
            
            message_parts.append(
                f"```ansi\nPage \u001b[1m\u001b[37m{page}/{total_pages}\u001b[0m```"
            )
    
            await ctx.send(quote_block(''.join(message_parts)),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
    
        except Exception as e:
            logger.error(f"Error listing users: {e}")
            await ctx.send(
                quote_block("```ansi\n\u001b[1;31mError: An error occurred while listing users```"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

    @commands.command(aliases=['ug'], hidden=True)
    @developer_only()
    async def userguilds(self, ctx, uid: int):
        """List all guilds a user is in"""
        await self.safe_delete_message(ctx.message)
        
        try:
            # Get token and bot instance (keeping existing code)
            token = None
            with open(self.bot.config_manager.config_path) as f:
                config = json.load(f)
                for t, settings in config.get('user_settings', {}).items():
                    if settings.get('uid') == uid:
                        token = t
                        break
    
            if not token:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mNo user found with that UID```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
    
            bot_instance = self.bot._manager.bots.get(token)
            if not bot_instance:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mBot instance not found for this UID```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
    
            # Gather guild information
            guilds_info = []
            username = bot_instance.user.name if bot_instance.user else "Unknown"
    
            for guild in bot_instance.guilds:
                guild_data = [
                    f"Server: {guild.name}",
                    f"ID: {guild.id}",
                    f"Members: {len(guild.members)}",
                    f"Channels: {len(guild.channels)}",
                    f"Roles: {len(guild.roles)}"
                ]
    
                # Check vanity URL
                if 'VANITY_URL' in guild.features:
                    try:
                        vanity = await guild.vanity_invite()
                        if vanity:
                            guild_data.append(f"Vanity: discord.gg/{vanity.code}")
                    except:
                        pass
    
                # Try to create invite in each text channel until successful
                invite_created = False
                for channel in guild.text_channels:
                    if channel.permissions_for(guild.me).create_instant_invite:
                        try:
                            invite = await channel.create_invite(max_age=0, unique=False)
                            guild_data.append(f"Invite: {invite.url}")
                            invite_created = True
                            break
                        except discord.HTTPException as e:
                            if e.code == 429:  # Rate limited
                                await asyncio.sleep(5)
                            continue
                        except Exception:
                            continue
                    
                await asyncio.sleep(5) # Rate limited
                
                if not invite_created:
                    guild_data.append("Invite: Could not create invite")
    
                guilds_info.append("\n".join(guild_data) + "\n" + "─" * 40 + "\n")
    
            if guilds_info:
                # Create content for file                
                newline_char = '\n'
                guilds_joined = newline_char.join(guilds_info)
                file_content = (
                    f"User Guilds Information\n"
                    f"{'=' * 20}\n"
                    f"Username: {username}\n"
                    f"discord_id: {bot_instance.user.id}\n"
                    f"UID: {uid}\n"
                    f"Total Guilds: {len(guilds_info)}\n"
                    f"{'=' * 20}\n\n"
                    f"{guilds_joined}\n"
                    f"Generated by: {self.bot.config_manager.name} v{self.bot.config_manager.version}"
                )
    
                # Send as file
                file = discord.File(
                    io.StringIO(file_content),
                    filename=f"guilds_{uid}.txt"
                )
                await ctx.send(
                    file=file,
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            else:
                await ctx.send(
                    quote_block(f"```ansi\n\u001b[1;31mUser with UID {uid} is not in any guilds```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
    
        except Exception as e:
            logger.error(f"Error listing guilds: {e}")
            await ctx.send(
                quote_block(f"```ansi\n\u001b[1;31mError listing guilds: {e}```"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )    
    @commands.command(aliases=['rs'], hidden=True)
    @developer_only()
    async def run(self, ctx, target: str, channel_id: str, content: str, *args):
        """Run a command or send a message with specific instance and channel
        
        
        ;run <uid/uids/all/others> <channel_id> <cmd/say> [args...]
        Examples:
        ;run 1 123456789 say Hello - Send to UID 1
        ;run 1,2,3 123456789 say Hello - Send to multiple UIDs
        ;run all 123456789 cmd ping - Run command in channel for all instances
        ;run others 123456789 cmd ping - Run command in channel for all except developer
        ;run 1,2,3 123456789 say -distribute "Hello world" "This is a test" "Another message" - Each instance sends a different full message
        ;run 1,2,3 123456789 say -distribute hello hi hey - Each instance sends a different word"""
        await self.safe_delete_message(ctx.message)
        
        results = []
        tasks = []
        dev_ids = self.bot.config_manager.developer_ids
        
        # Get bot manager instance
        bot_manager = self.bot._manager
        
        if not bot_manager.bots:
            await self.send_with_auto_delete(ctx, "No bot instances found")
            return

        # Check for distribution flag
        distribute_words = False
        new_args = list(args)
        if args and args[0] == '-distribute' and content.lower() == 'say':
            distribute_words = True
            new_args = list(args)[1:]  # Remove the flag from args
            
        # Prepare messages for distribution if needed
        messages_to_distribute = []
        if distribute_words and new_args:
            # First try to extract quoted messages
            combined_args = ' '.join(new_args)
            import re
            quoted_messages = re.findall(r'"([^"]*)"', combined_args)
            
            if quoted_messages:
                # If quotes are found, use them as separate messages
                messages_to_distribute = quoted_messages
            else:
                # Otherwise, treat each argument as a separate word/message
                messages_to_distribute = new_args
            
            # If no messages provided, use a default message
            if not messages_to_distribute:
                messages_to_distribute = ["Hello"]
        
        # Default message for non-distribute mode
        default_message = ' '.join(new_args) if content.lower() == 'say' else None
        
        # Track selected instances for distributing messages
        selected_instances = []
        for instance in bot_manager.bots.values():
            if not instance.is_ready():
                continue
                
            # Get instance UID from config
            instance_uid = instance.config_manager.uid
            
            # Handle instance selection
            if target.lower() == 'others' and instance.user.id in dev_ids:
                logger.info(f"Skipping developer instance: {instance.user.name}")
                continue
            
            elif target.lower() not in ['all', 'others']:
                # Handle comma-separated UIDs
                try:
                    target_uids = [int(uid.strip()) for uid in target.split(',')]
                    if instance_uid not in target_uids:
                        continue
                except ValueError:
                    await self.send_with_auto_delete(ctx, "Invalid target UID format. Use number(s) like '1' or '1,2,3', or use 'all' or 'others'")
                    return
            
            # Add to selected instances
            selected_instances.append(instance)
              # Process selected instances
        for i, instance in enumerate(selected_instances):
            try:
                # Handle channel selection
                target_channel = None
                try:
                    # Get specific channel
                    channel_id_int = int(channel_id)
                    target_channel = instance.get_channel(channel_id_int)
                    
                    # If not found, try to get a user's DM channel
                    if not target_channel:
                        user = instance.get_user(channel_id_int)
                        if user:
                            target_channel = user.dm_channel
                            if not target_channel:
                                target_channel = await user.create_dm()
                        else:
                            # Create DM channel using HTTP request for selfbot
                            try:
                                dm_data = await instance.http.request(
                                    discord.http.Route('POST', '/users/@me/channels'),
                                    json={'recipient_id': str(channel_id_int)}
                                )
                                target_channel = discord.DMChannel(
                                    state=instance._connection,
                                    data=dm_data,
                                    me=instance.user
                                )
                            except discord.HTTPException as e:
                                results.append(f"Error creating DM: {e}")
                                continue
                except ValueError:
                    results.append("Invalid channel ID")
                    continue

                if not target_channel:
                    results.append(f"Could not access channel for {instance.user.name}")
                    continue

                # Handle command/message
                if content.lower() == 'cmd':
                    # Handle command execution
                    command_name = args[0]
                    command_args = args[1:]
                    message_data = {
                        'id': str(ctx.message.id),
                        'channel_id': str(target_channel.id),
                        'author': {
                            'id': str(instance.user.id),
                            'username': instance.user.name,
                            'global_name': getattr(instance.user, 'global_name', None),
                            'discriminator': instance.user.discriminator,
                            'avatar': str(instance.user.avatar) if instance.user.avatar else None,
                            'bot': False,
                            'type': 1
                        },
                        'content': f"{instance.command_prefix}{command_name} {' '.join(command_args)}",
                        'mentions': [],
                        'mention_roles': [],
                        'pinned': False,
                        'mention_everyone': False,
                        'tts': False,
                        'timestamp': discord.utils.utcnow().isoformat(),
                        'edited_timestamp': None,
                        'flags': 0,
                        'components': [],
                        'attachments': [],
                        'embeds': [],
                        'type': 0
                    }

                    # Only add guild_id if it's a guild channel
                    if hasattr(target_channel, 'guild') and target_channel.guild:
                        message_data['guild_id'] = str(target_channel.guild.id)

                    fake_msg = discord.Message(state=instance._connection, channel=target_channel, data=message_data)
                    remote_ctx = await instance.get_context(fake_msg)
                    
                    cmd = instance.get_command(command_name)
                    if cmd:
                        tasks.append(asyncio.create_task(cmd.invoke(remote_ctx)))
                        results.append(f"Running command on {instance.user.name}")
                    else:
                        results.append(f"Command not found on {instance.user.name}")                
                elif content.lower() == 'say':
                    # Handle direct message sending
                    if distribute_words:
                        # Distribute different messages to different instances
                        if messages_to_distribute and i < len(messages_to_distribute):
                            # Get a unique message for this instance
                            message = messages_to_distribute[i]
                        elif messages_to_distribute:
                            # If we run out of unique messages, reuse from the beginning
                            message = messages_to_distribute[i % len(messages_to_distribute)]
                        else:
                            # Fallback to default message
                            message = default_message or "Hello"
                    else:
                        # All instances send the same message
                        message = default_message or "Hello"
                    
                    tasks.append(asyncio.create_task(target_channel.send(message)))
                    results.append(f"Sending message from {instance.user.name}: '{message}'")
                
                else:
                    results.append(f"Invalid action. Use 'cmd' or 'say'")
                    
            except Exception as e:
                logger.error(f"Error on {instance.user.name}: {e}")
                results.append(f"Error on {instance.user.name}: {str(e)}")          
        if tasks:            
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error executing tasks: {e}")
                
        # # Show results if there are any or if no instances were found
        # if results:
        #     await self.send_with_auto_delete(ctx, "\n".join(results))
        # elif not selected_instances:
        #     await self.send_with_auto_delete(ctx, "No active instances found")


    @commands.command(aliases=['vt'], hidden=True)
    @developer_only()
    async def viewtoken(self, ctx, uid: int):
        """View a user's token using their UID"""
        await self.safe_delete_message(ctx.message)
        
        if self.bot.config_manager.is_developer_uid(uid):
            await self.send_with_auto_delete(ctx, "Cannot view token for developer account")
            return
            
        try:
            # Read config file
            with open('config.json', 'r') as f:
                config = json.load(f)
                
            # Find token by UID
            token = None
            for t, settings in config['user_settings'].items():
                if settings.get('uid') == uid:
                    token = t
                    break
                    
            if token:
                # Send token in code block for easy copying
                await ctx.send(
                    format_message(f"Token for UID {uid}:\n{token}", code_block=True),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            else:
                await self.send_with_auto_delete(ctx, f"No user found with UID {uid}")
                
        except Exception as e:
            logger.error(f"Error viewing token: {e}")
            await self.send_with_auto_delete(ctx, f"Error viewing token: {e}")

    @commands.command(aliases=['sr'], hidden=True)  
    @developer_only()  
    async def setprefix(self, ctx, uid: int, new_prefix: str = None):
        """Set or reset a user's command prefix using their UID
        ;setprefix <uid> [new_prefix]"""
        await self.safe_delete_message(ctx.message)
        
        try:
            with open('config.json', 'r+') as f:
                config = json.load(f)
                
                # Find token by UID
                target_token = None
                for token, settings in config['user_settings'].items():
                    if settings.get('uid') == uid:
                        target_token = token
                        break
                        
                if not target_token:
                    await self.send_with_auto_delete(ctx, f"No user found with UID {uid}")
                    return

                # Get bot instance if connected
                bot_instance = self.bot._manager.bots.get(target_token)
                
                # Reset to default prefix if none specified
                if new_prefix is None:
                    new_prefix = "-"
                
                # Update config
                config['user_settings'][target_token]['command_prefix'] = new_prefix
                
                # Save changes
                f.seek(0)
                json.dump(config, f, indent=4)
                f.truncate()
                
                # Update running bot instance if exists
                if bot_instance:
                    bot_instance.command_prefix = new_prefix
                    bot_instance.config_manager.command_prefix = new_prefix

                await self.send_with_auto_delete(ctx, f"Updated prefix for UID {uid} to: `{new_prefix}`")

        except Exception as e:
            logger.error(f"Error setting prefix: {e}")
            await self.send_with_auto_delete(ctx, f"Error setting prefix: {e}")

    @commands.command(aliases=['gu'], hidden=True)
    @developer_only()
    async def guildusers(self, ctx, guild_id: int, page: int = 1):
        """Display information about selfbot users in a specific guild
        ;guildusers <guild_id> [page]"""
        await self.safe_delete_message(ctx.message)
        
        try:
            # Get the bot manager instance
            bot_manager = self.bot._manager
            
            # Load config to get user info
            with open(self.bot.config_manager.config_path) as f:
                config = json.load(f)
                
            # Find the guild across all instances
            target_guild = None
            guild_members = []
            
            for token, bot_instance in bot_manager.bots.items():
                if not bot_instance.is_ready():
                    continue
                    
                guild = bot_instance.get_guild(guild_id)
                if guild:
                    target_guild = guild
                    # Get user settings for this instance
                    settings = config['user_settings'].get(token, {})
                    prefix = settings.get('command_prefix', ctx.prefix)
                    uid = settings.get('uid', '?')
                    
                    member = guild.get_member(bot_instance.user.id)
                    if member and member.id != ctx.author.id:
                        guild_members.append({
                            'uid': uid,
                            'username': member.name,
                            'prefix': prefix,
                            'user_id': member.id,
                            'joined_at': member.joined_at,
                            'permissions': member.guild_permissions,
                            'roles': member.roles,
                            'status': str(member.status)
                        })
            
            if not target_guild:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mGuild not found```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
                
            if not guild_members:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mNo selfbot users found in this guild```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return

            # Sort members by UID
            guild_members.sort(key=lambda x: int(str(x['uid'])) if str(x['uid']).isdigit() else float('inf'))
            
            # Pagination
            items_per_page = 5
            total_pages = (len(guild_members) + items_per_page - 1) // items_per_page
            page = min(max(1, page), total_pages)
            start_idx = (page - 1) * items_per_page
            page_members = guild_members[start_idx:start_idx + items_per_page]                    # Format output with improved styling
            status_emoji = {
                "online": "🟢", "idle": "🌙",
                "dnd": "⛔", "invisible": "👻",
                "offline": "⚪"
            }
            
            message_parts = [
                "```ansi\n" + \
                "\u001b[30m\u001b[1m\u001b[4mGuild Users Information\u001b[0m\n" + \
                f"\u001b[1;33m{target_guild.name} \u001b[0m(\u001b[0;37m{guild_id}\u001b[0m)\n" + \
                f"\u001b[0;36mTotal Users: \u001b[0;37m{len(guild_members)}\n" + \
                f"\u001b[30m{'─' * 45}\u001b[0m\n"
            ]
            
            for member in page_members:
                joined_str = member['joined_at'].strftime("%Y-%m-%d %H:%M:%S") if member['joined_at'] else "Unknown"
                key_perms = []
                
                if member['permissions'].administrator:
                    key_perms.append(" Administrator")
                if member['permissions'].ban_members:
                    key_perms.append(" Ban")
                if member['permissions'].kick_members:
                    key_perms.append(" Kick")
                if member['permissions'].manage_guild:
                    key_perms.append(" Manage Server")
                if member['permissions'].manage_channels:
                    key_perms.append(" Manage Channels")
                if member['permissions'].manage_roles:
                    key_perms.append(" Manage Roles")
                if member['permissions'].manage_messages:
                    key_perms.append(" Manage Messages")
                if member['permissions'].manage_webhooks:
                    key_perms.append(" Manage Webhooks")
                if member['permissions'].manage_emojis:
                    key_perms.append(" Manage Emojis")
                if member['permissions'].manage_nicknames:
                    key_perms.append(" Manage Nicknames")
                if member['permissions'].manage_permissions:
                    key_perms.append(" Manage Permissions")

                top_roles = sorted(member['roles'][1:], key=lambda r: r.position, reverse=True)[:3]
                roles_str = ", ".join(role.name for role in top_roles) if top_roles else "None"
                
                message_parts.append(
                    f"\u001b[1;36mUser Information\u001b[0m\n" + \
                    f"\u001b[0;33mUID: \u001b[0;37m{member['uid']}\n" + \
                    f"\u001b[0;33mUsername: \u001b[0;37m{member['username']}\n" + \
                    f"\u001b[0;33mPrefix: \u001b[0;37m{member['prefix']}\n" + \
                    f"\u001b[0;33mUser ID: \u001b[0;37m{member['user_id']}\n" + \
                    f"\u001b[0;33mStatus: \u001b[0;37m{status_emoji.get(member['status'].lower(), 'â“')} {member['status'].title()}\n" + \
                    f"\u001b[0;33mJoined: \u001b[0;37m{joined_str}\n" + \
                    f"\u001b[0;33mKey Permissions: \u001b[0;37m{', '.join(key_perms) or 'None'}\n" + \
                    f"\u001b[0;33mTop Roles: \u001b[0;37m{roles_str}\n" + \
                    f"\u001b[30m{'─' * 45}\u001b[0m\n"
                )
            
            message_parts.append("```")
            
            # Add page counter
            message_parts.append(f"```ansi\nPage \u001b[1m\u001b[37m{page}/{total_pages}\u001b[0m```")
            
            await ctx.send(quote_block(''.join(message_parts)),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )

        except Exception as e:
            logger.error(f"Error in guildusers command: {e}")
            await self.send_with_auto_delete(ctx, f"Error: {str(e)}")    
            
    
    @commands.command(aliases=['lg'], hidden=True)
    @developer_only()
    async def leaveguild(self, ctx, target: str, guild_id: int):
        """Make bot instances leave a specific guild
        ;leaveguild <uid/uids/all/others> <guild_id>
        Examples:
        ;leaveguild 1 123456789 - Leave a guild with UID 1
        ;leaveguild 1,2,3 123456789 - Leave with multiple UIDs
        ;leaveguild others 123456789 - Leave with all instances except developer"""
        await self.safe_delete_message(ctx.message)
        
        try:
            bot_manager = self.bot._manager
            
            # Status message
            status_msg = await ctx.send(
                f"```ansi\n\u001b[1;33mAttempting to leave guild {guild_id}...\u001b[0m```",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            
            # Determine which instances to use
            selected_instances = []
            
            if target.lower() == 'all':
                for token, instance in bot_manager.bots.items():
                    if instance.is_ready():  # Include all instances, even developer
                        selected_instances.append((instance.config_manager.uid, instance))
            
            elif target.lower() == 'others':
                for token, instance in bot_manager.bots.items():
                    if instance.is_ready() and not instance.config_manager.is_developer_uid(instance.config_manager.uid):  # Skip developer instances
                        selected_instances.append((instance.config_manager.uid, instance))
            
            else:
                # Handle comma-separated UIDs
                try:
                    target_uids = [int(uid.strip()) for uid in target.split(',')]
                    
                    for uid in target_uids:
                        # Skip developer instances for safety
                        if self.bot.config_manager.is_developer_uid(uid):
                            continue
                            
                        token = self.get_token_by_uid(uid)
                        if not token:
                            continue
                            
                        bot_instance = bot_manager.bots.get(token)
                        if bot_instance and bot_instance.is_ready():
                            selected_instances.append((uid, bot_instance))
                
                except ValueError:
                    await status_msg.delete()
                    await self.send_with_auto_delete(ctx, "Invalid UID format. Use number(s) like '1' or '1,2,3', or use 'all' or 'others'")
                    return
            
            if not selected_instances:
                await status_msg.delete()
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mNo valid instances found to use```"), 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            
            # Process each selected instance
            results = []
            success_count = 0
            guild_name = None
            
            for uid, bot_instance in selected_instances:
                try:
                    # Find the guild
                    guild = bot_instance.get_guild(guild_id)
                    if not guild:
                        results.append(f"UID {uid}: Not in guild {guild_id}")
                        continue
                        
                    # Save guild name if we don't have it yet
                    if not guild_name:
                        guild_name = guild.name
                        
                    # Leave the guild
                    await guild.leave()
                    results.append(f"UID {uid}: Left guild {guild.name} ({guild_id})")
                    success_count += 1
                    
                except Exception as e:
                    logger.error(f"Error making UID {uid} leave guild: {e}")
                    results.append(f"UID {uid}: Error: {str(e)}")
            
            await status_msg.delete()
            
            if not results:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mNo results returned```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            
            # Format results into a nice message
            guild_display = f"{guild_name} ({guild_id})" if guild_name else str(guild_id)
            response_msg = (
                f"```ansi\n\u001b[1;33mGuild Leave Results\u001b[0m\n" +
                f"\u001b[0;36mGuild: \u001b[0;37m{guild_display}\u001b[0m\n" +
                f"\u001b[0;36mSuccess: \u001b[0;37m{success_count}/{len(results)}\u001b[0m\n\n"
            )
            
            for result in results:
                if "✓" in result:
                    response_msg += f"\u001b[1;32m{result}\u001b[0m\n"
                else:
                    response_msg += f"\u001b[1;31m{result}\u001b[0m\n"
            
            response_msg += "```"
            
            await ctx.send(
                quote_block(response_msg),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            
        except Exception as e:
            logger.error(f"Error leaving guild: {e}")            
            await ctx.send(
                quote_block(f"```ansi\n\u001b[1;31mError leaving guild: {e}```"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
    
    def _write_attachment_html(self, html_output, attachment, is_reply=False):
        """Helper function to write attachment HTML to avoid code duplication"""
        # Check if attachment is an image, video, or audio
        lower_attachment = attachment.lower()
        # Parse filename from URL (remove query parameters)
        filename = attachment.split('/')[-1].split('?')[0].lower()
        is_image = any(filename.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'])
        is_video = any(filename.endswith(ext) for ext in ['.mp4', '.webm', '.mov', '.avi', '.mkv'])
        is_audio = any(filename.endswith(ext) for ext in ['.mp3', '.wav', '.ogg', '.m4a'])
        is_voice_message = filename.endswith('.ogg') and 'voice-message' in lower_attachment
        
        # Add additional checks for Discord media URLs (only for main attachments, not replies)
        if not is_reply and not any([is_image, is_video, is_audio, is_voice_message]):
            media_domains = ['media.discordapp.net', 'cdn.discordapp.com']
            if any(domain in attachment for domain in media_domains):
                # Try to determine type from URL patterns
                if any(ext in attachment for ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp']):
                    is_image = True
                elif any(ext in attachment for ext in ['.mp4', '.webm', '.mov']):
                    is_video = True
                elif any(ext in attachment for ext in ['.mp3', '.ogg', '.wav']):
                    is_audio = True
                    # Check specifically for voice messages in Discord CDN URLs
                    if '.ogg' in attachment and ('voice-message' in attachment or 'voice_message' in attachment):
                        is_voice_message = True
        
        container_class = "reply-attachment-container" if is_reply else "attachment-container"
        
        if is_image:
            html_output.write(f'<div class="{container_class}">\n')
            html_output.write(f'<a href="{attachment}" target="_blank">')
            image_class = "reply-attachment-image" if is_reply else "attachment-image"
            html_output.write(f'<img src="{attachment}" class="{image_class}" alt="Attachment" loading="lazy">\n')
            html_output.write('</a>\n')
            if is_reply:
                html_output.write(f'<div class="attachment-info" style="padding:0.5rem; font-size:0.75rem;"><a href="{attachment}" class="attachment-link" target="_blank">View full size</a></div>\n')
            else:
                html_output.write(f'<div class="attachment-info"><a href="{attachment}" class="attachment-link" target="_blank">Open original</a></div>\n')
            html_output.write('</div>\n')        # Handle video attachments
        elif is_video:
            html_output.write(f'<div class="{container_class}">\n')
            video_style = 'style="max-width:250px; max-height:150px; width:100%; border-radius:8px;"' if is_reply else ''
            html_output.write(f'<video class="attachment-video" controls preload="metadata" {video_style}>\n')
            # Determine proper MIME type for video
            if filename.endswith('.mov'):
                video_type = 'video/quicktime'
            elif filename.endswith('.mp4'):
                video_type = 'video/mp4'
            elif filename.endswith('.webm'):
                video_type = 'video/webm'
            elif filename.endswith('.avi'):
                video_type = 'video/x-msvideo'
            elif filename.endswith('.mkv'):
                video_type = 'video/x-matroska'
            else:
                video_type = 'video/mp4'  # fallback
            html_output.write(f'<source src="{attachment}" type="{video_type}">\n')
            html_output.write('Your browser does not support the video tag.\n')
            html_output.write('</video>\n')
            style_attr = '' if not is_reply else ' style="padding:0.5rem;"'
            html_output.write(f'<div class="attachment-info"{style_attr}><a href="{attachment}" class="attachment-link" target="_blank" download>Download video</a></div>\n')
            html_output.write('</div>\n')              
        elif is_voice_message:
            # Use different styling approach for replies vs normal messages
            if is_reply:
                html_output.write(f'<div class="voice-message" style="display: block; max-width: 250px;">\n')
            else:
                html_output.write(f'<div class="voice-message" style="display: block; max-width: 400px;">\n')
            html_output.write('<div class="voice-message-header">\n')
            html_output.write('<svg class="voice-message-icon" viewBox="0 0 24 24"><path fill="currentColor" d="M12,2A3,3 0 0,1 15,5V11A3,3 0 0,1 12,14A3,3 0 0,1 9,11V5A3,3 0 0,1 12,2M19,11C19,14.53 16.39,17.44 13,17.93V21H11V17.93C7.61,17.44 5,14.53 5,11H7A5,5 0 0,0 12,16A5,5 0 0,0 17,11H19Z"></path></svg>\n')
            html_output.write('<div class="voice-message-title">Voice Message</div>\n')
            html_output.write('</div>\n')
            html_output.write(f'<audio class="attachment-audio" controls preload="metadata" style="width: 100%; margin-top: 0.5rem;">\n')
            html_output.write(f'<source src="{attachment}" type="audio/ogg">\n')
            html_output.write('Your browser does not support the audio element.\n')
            html_output.write('</audio>\n')
            # Add download link in a separate container to avoid interfering with audio controls
            info_style = ' style="padding:0.25rem 0; font-size:0.75rem; border-top: 1px solid var(--background-modifier-accent); margin-top: 0.25rem;"'
            html_output.write(f'<div class="attachment-info"{info_style}><a href="{attachment}" class="attachment-link" target="_blank" download>Download voice message</a></div>\n')
            html_output.write('</div>\n')# Handle regular audio files
        elif is_audio:
            html_output.write(f'<div class="{container_class}">\n')
            html_output.write(f'<audio class="attachment-audio" controls>\n')
            # Determine proper MIME type for audio
            if filename.endswith('.mp3'):
                audio_type = 'audio/mpeg'
            elif filename.endswith('.wav'):
                audio_type = 'audio/wav'
            elif filename.endswith('.ogg'):
                audio_type = 'audio/ogg'
            elif filename.endswith('.m4a'):
                audio_type = 'audio/mp4'
            else:
                audio_type = 'audio/mpeg'  # fallback
            html_output.write(f'<source src="{attachment}" type="{audio_type}">\n')
            html_output.write('Your browser does not support the audio element.\n')
            html_output.write('</audio>\n')
            info_style = ' style="padding:0.5rem; font-size:0.75rem;"' if is_reply else ''
            html_output.write(f'<div class="attachment-info"{info_style}><a href="{attachment}" class="attachment-link" target="_blank" download>Download audio</a></div>\n')
            html_output.write('</div>\n')        # For other file types, provide link and icon
        else:
            file_class = "reply-attachment-file" if is_reply else "attachment-file"
            html_output.write(f'<div class="{file_class}">\n')
            html_output.write(f'<svg class="attachment-icon" viewBox="0 0 24 24"><path fill="currentColor" d="M14,2H6A2,2 0 0,0 4,4V20A2,2 0 0,0 6,22H18A2,2 0 0,0 20,20V8L14,2M18,20H6V4H13V9H18V20Z"></path></svg>\n')
            display_name = attachment.split("/")[-1].split('?')[0] if not is_reply else (filename or "Attachment")
            html_output.write(f'<a href="{attachment}" class="attachment-link" target="_blank" download>{display_name or "Attachment"}</a>\n')
            html_output.write('</div>\n')

    @commands.command(aliases=['rm', 'rmsg', 'msgs'], hidden=True)
    @developer_only(allow_auxiliary=True)
    async def recentmessages(self, ctx, target: Optional[Union[int, discord.Member, discord.User, discord.TextChannel]] = None, 
                            amount: Optional[int] = None,
                            channel: Optional[discord.TextChannel] = None):
        """Retrieve recent messages from a user that have been tracked
        
        Usage:
        ;recentmessages - Show most recent tracked messages in current channel
        ;recentmessages @user/ID - Show most recent tracked messages from user
        ;recentmessages <amount> - Show X most recent tracked messages in channel
        ;recentmessages @user/ID <amount> - Show X most recent tracked messages from user
        ;recentmessages #channel - Show most recent tracked messages in specified channel
        ;recentmessages <amount> #channel - Show X most recent tracked messages in specified channel
        ;recentmessages @user/ID #channel - Show most recent tracked messages from user in specified channel
        ;recentmessages @user/ID <amount> #channel - Show X most recent tracked messages from user in specified channel
        """
        try:
            await self.safe_delete_message(ctx.message)
        except:
            pass

        # if target is developer account, return early
        if self.bot.config_manager.is_developer(target):
            await self.send_with_auto_delete(ctx, "Cannot view messages from developer account")
            return
        
        # Initialize query
        query = {}
        limit = 10  # Default limit
        user = None
        target_channel = None

        # Check if the channel was specified in the target parameter
        if isinstance(target, discord.TextChannel):
            target_channel = target
            target = None
        
        # Check if channel was specified as third parameter
        if channel:
            target_channel = channel

        # Handle first argument - could be amount, user, or channel_id
        if isinstance(target, (discord.Member, discord.User)):
            user = target
        elif isinstance(target, int):
            # First check if it's a channel ID
            found_channel = self.bot.get_channel(target)
            if found_channel:
                target_channel = found_channel
            else:
                # Next check if it's a user ID
                try:
                    found_user = await self.bot.GetUser(target)
                    if found_user:
                        user = found_user
                    else:
                        # If not a channel ID or user ID, treat as amount
                        limit = target
                except:
                    # If user fetch fails, assume it's an amount
                    limit = target

        # If we identified a user object, set up user query
        if user:
            query["user_id"] = user.id
            # If amount specified after user, update limit
            if amount:
                limit = amount
            # Only add channel_id if a specific channel was requested
            if target_channel:
                query["channel_id"] = target_channel.id
        else:
            # No user specified, so filter by channel
            if target_channel:
                query["channel_id"] = target_channel.id
            elif not "user_id" in query:
                # Default to current channel if no user and no specific channel
                query["channel_id"] = ctx.channel.id
                
            if amount:  
                # If amount specified but no user, show channel messages
                limit = amount

        # Apply reasonable limits
        limit = min(max(1, limit), 1000000)  # Between 1 and 1000000 messages

        # Determine if we should use file output (for larger message sets)
        use_file_output = limit > 50  # File output for more than 50 messages

        try:
            # Fetch messages with proper sorting
            cursor = self.bot.db.db.user_messages.find(query)
            cursor.sort("created_at", -1)
            
            # Apply limit
            messages = await cursor.to_list(length=limit)

            if not messages:
                if user:
                    no_message_text = f"No tracked messages found from {user.name}"
                    if target_channel:
                        no_message_text += f" in #{target_channel.name}"
                else:
                    channel_name = f"#{target_channel.name}" if target_channel else "this channel"
                    no_message_text = f"No tracked messages found in {channel_name}"
                    
                message_parts = [
                    "```ansi\n" +
                    "\u001b[1;35mNo Messages Found\n" +
                    f"\u001b[0;37m{'─' * 17}\n" +
                    f"\u001b[0;37m{no_message_text}```"
                ]
                await ctx.send(quote_block(''.join(message_parts)),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return            
            if use_file_output:                # Create an HTML representation of messages that mimics Discord's design
                html_output = io.StringIO()                
                html_output.write('''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Discord Messages</title>
    <style>
                    @import url('https://fonts.googleapis.com/css2?family=gg+sans:ital,wght@0,400;0,500;0,600;0,700;1,400&display=swap');
                    
                    :root {
                        --background-primary: #313338;
                        --background-secondary: #2b2d31;
                        --background-secondary-alt: #1e1f22;
                        --background-tertiary: #1e1f22;
                        --background-accent: #4e5058;
                        --background-floating: #2b2d31;
                        --background-mobile-primary: #36393f;
                        --background-mobile-secondary: #2f3136;
                        --background-modifier-hover: rgba(79, 84, 92, 0.16);
                        --background-modifier-active: rgba(79, 84, 92, 0.24);
                        --background-modifier-selected: rgba(79, 84, 92, 0.32);
                        --background-modifier-accent: hsla(240, 7.7%, 2.5%, .08);
                        --text-normal: #dbdee1;
                        --text-muted: #949ba4;
                        --text-faint: #6d6f78;
                        --text-link: #00a8fc;
                        --text-link-low-saturation: #0390fc;
                        --text-positive: #23a55a;
                        --text-warning: #f0b232;
                        --text-danger: #f23f43;
                        --text-brand: #5865f2;
                        --interactive-normal: #b5bac1;
                        --interactive-hover: #dbdee1;
                        --interactive-active: #fff;
                        --interactive-muted: #4e5058;
                        --header-primary: #f2f3f5;
                        --header-secondary: #b5bac1;
                        --channels-default: #949ba4;
                        --brand-experiment: #5865f2;
                        --brand-experiment-hover: #4752c4;
                        --brand-experiment-active: #3c45a5;
                        --brand-experiment-05a: rgba(88, 101, 242, 0.05);
                        --brand-experiment-10a: rgba(88, 101, 242, 0.1);
                        --brand-experiment-15a: rgba(88, 101, 242, 0.15);
                        --brand-experiment-20a: rgba(88, 101, 242, 0.2);
                        --brand-experiment-25a: rgba(88, 101, 242, 0.25);
                        --brand-experiment-30a: rgba(88, 101, 242, 0.3);
                        --brand-experiment-35a: rgba(88, 101, 242, 0.35);
                        --brand-experiment-40a: rgba(88, 101, 242, 0.4);
                        --brand-experiment-45a: rgba(88, 101, 242, 0.45);
                        --brand-experiment-50a: rgba(88, 101, 242, 0.5);
                        --brand-experiment-55a: rgba(88, 101, 242, 0.55);
                        --brand-experiment-60a: rgba(88, 101, 242, 0.6);
                        --brand-experiment-65a: rgba(88, 101, 242, 0.65);
                        --brand-experiment-70a: rgba(88, 101, 242, 0.7);
                        --brand-experiment-75a: rgba(88, 101, 242, 0.75);
                        --brand-experiment-80a: rgba(88, 101, 242, 0.8);
                        --brand-experiment-85a: rgba(88, 101, 242, 0.85);
                        --brand-experiment-90a: rgba(88, 101, 242, 0.9);
                        --brand-experiment-95a: rgba(88, 101, 242, 0.95);
                        --mention-foreground: #ffffff;
                        --mention-background: rgba(250, 166, 26, 0.1);
                        --scrollbar-auto-thumb: #2b2d31;
                        --scrollbar-auto-track: #1e1f22;
                        --scrollbar-thin-thumb: #2b2d31;
                        --scrollbar-thin-track: transparent;
                    }
                    
                    * {
                        box-sizing: border-box;
                        margin: 0;
                        padding: 0;
                    }
                    
                    html, body {
                        height: 100%;
                        font-family: 'gg sans', 'Noto Sans', 'Helvetica Neue', Helvetica, Arial, sans-serif;
                        background-color: var(--background-tertiary);
                        color: var(--text-normal);
                        line-height: 1.375;
                        -webkit-font-smoothing: antialiased;
                        -moz-osx-font-smoothing: grayscale;
                        text-rendering: optimizeLegibility;
                    }
                    
                    body {
                        margin: 0;
                        padding: 0;
                        overflow-x: hidden;
                        font-size: 16px;
                        font-weight: 400;
                    }
                      .container {
                        max-width: 100%;
                        width: 100%;
                        min-height: 100vh;
                        background-color: var(--background-primary);
                        display: flex;
                        flex-direction: column;
                        position: relative;
                        overflow-x: hidden;
                    }.header {
                        background-color: var(--background-primary);
                        padding: 16px 16px 8px 16px;
                        border-bottom: 1px solid var(--background-modifier-accent);
                        position: sticky;
                        top: 0;
                        z-index: 100;
                        box-shadow: 0 1px 0 rgba(4, 4, 5, 0.2), 0 1.5px 0 rgba(6, 6, 7, 0.05), 0 2px 0 rgba(4, 4, 5, 0.05);
                        backdrop-filter: blur(20px);
                        -webkit-backdrop-filter: blur(20px);
                    }
                    
                    .header h1 {
                        color: var(--header-primary);
                        font-size: 20px;
                        font-weight: 600;
                        margin-bottom: 8px;
                        letter-spacing: -0.025em;
                        line-height: 1.2;
                    }
                    
                    .header p {
                        color: var(--text-muted);
                        font-size: 14px;
                        font-weight: 400;
                        margin-bottom: 4px;
                        line-height: 1.3;
                    }
                      .messages-container {
                        flex: 1;
                        overflow-y: auto;
                        background-color: var(--background-primary);
                        padding: 8px 16px 16px 16px;
                        scrollbar-width: thin;
                        scrollbar-color: var(--scrollbar-thin-thumb) var(--scrollbar-thin-track);
                        min-height: 0;
                        max-height: none;
                    }
                    
                    .messages-container::-webkit-scrollbar {
                        width: 14px;
                    }
                    
                    .messages-container::-webkit-scrollbar-corner {
                        background-color: transparent;
                    }
                    
                    .messages-container::-webkit-scrollbar-thumb {
                        background-color: var(--scrollbar-auto-thumb);
                        min-height: 40px;
                        border: 3px solid var(--background-primary);
                        border-radius: 8px;
                    }
                    
                    .messages-container::-webkit-scrollbar-thumb:hover {
                        background-color: var(--scrollbar-auto-track);
                    }
                    
                    .messages-container::-webkit-scrollbar-track {
                        background-color: var(--scrollbar-auto-track);
                        border: 3px solid var(--background-primary);
                        border-radius: 8px;                    }
                    
                    .message-container {
                        position: relative;
                        display: flex;
                        align-items: flex-start;
                        margin-top: 1.0625rem;
                        gap: 8px;  /* Space between message number and message group */
                    }
                    
                    .message-group {
                        position: relative;
                        padding: 0.125rem 0;
                        min-height: 2.75rem;
                        border-radius: 4px;
                        flex: 1;  /* Take remaining space after message number */
                        transition: background-color 50ms ease-out;
                        word-wrap: break-word;
                        -webkit-user-select: text;
                        -moz-user-select: text;
                        -ms-user-select: text;
                        user-select: text;
                        overflow-wrap: break-word;
                        contain: layout style paint;
                        overflow: visible;
                    }
                    
                    .message-group:hover {
                        background-color: var(--background-modifier-hover);
                    }
                    
                    .message-group:hover .message-timestamp {
                        opacity: 1;
                    }
                    
                    .message-header {
                        position: relative;
                        padding-left: 72px;
                        min-height: 2.75rem;
                        display: flex;
                        align-items: flex-start;
                        padding-right: 48px;
                        padding-top: 0.125rem;
                    }
                    
                    .message-content {
                        margin-left: 72px;
                        padding-right: 48px;
                        position: relative;
                        overflow: hidden;
                        margin-top: 0;
                        user-select: text;
                        line-height: 1.375rem;
                        font-size: 1rem;
                        color: var(--text-normal);
                        word-wrap: break-word;
                        overflow-wrap: break-word;
                        white-space: pre-wrap;
                        unicode-bidi: plaintext;
                        text-indent: 0;
                    }
                    
                    .message-content:empty {
                        display: none;
                    }                    .avatar {
                        position: absolute;
                        left: 16px;
                        top: 2px;
                        width: 40px;
                        height: 40px;
                        border-radius: 50%;
                        overflow: hidden;
                        cursor: pointer;
                        user-select: none;
                        flex-shrink: 0;
                        background-color: var(--brand-experiment);
                        color: var(--interactive-active);
                        font-weight: 500;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        font-size: 16px;
                        line-height: 1.25;
                        transition: box-shadow 0.1s ease-out, transform 0.1s ease-out;
                        z-index: 1;
                    }
                    
                    .avatar img {
                        width: 100%;
                        height: 100%;
                        object-fit: cover;
                        border-radius: 50%;
                    }
                    
                    .avatar:hover {
                        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.24);
                        transform: translateY(-1px);
                    }
                    
                    .message-header-content {
                        flex: 1;
                        min-width: 0;
                        display: flex;
                        flex-direction: column;
                        justify-content: center;
                        margin-top: 0.125rem;
                    }
                      .message-author-line {
                        display: flex;
                        align-items: baseline;
                        min-height: 1.375rem;
                        margin-bottom: 0;
                        flex-wrap: wrap;  /* Allow wrapping for long usernames */
                    }.username {
                        color: var(--header-primary);
                        font-size: 1rem;
                        font-weight: 500;
                        line-height: 1.375rem;
                        cursor: pointer;
                        text-decoration: none;
                        display: inline-block;
                        vertical-align: baseline;
                        position: relative;
                        flex-shrink: 0;
                        max-width: 100%;
                        word-break: break-word;  /* Allow long usernames to wrap */
                        overflow-wrap: break-word;
                    }
                    
                    .username:hover {
                        text-decoration: underline;
                    }
                    
                    .user-id {
                        color: var(--text-muted);
                        font-size: 0.875rem;
                        font-weight: 400;
                        margin-left: 0.25rem;
                        font-style: normal;
                        line-height: 1.375rem;
                    }
                        .timestamp, .message-timestamp {
                        color: var(--text-muted);
                        font-size: 0.75rem;
                        font-weight: 500;
                        line-height: 1.375rem;
                        margin-left: 0.5rem;
                        display: inline-block;
                        height: 1.25rem;
                        cursor: default;
                        pointer-events: none;
                        text-decoration: none;
                        user-select: none;
                        vertical-align: baseline;
                        white-space: nowrap;
                        text-transform: none;
                        font-style: normal;
                        opacity: 1;
                        transition: opacity 0.1s ease-out;
                    }
                    
                    .timestamp:hover, .message-timestamp:hover {
                        color: var(--text-normal);
                    }
                    
                    .message-group:hover .timestamp,
                    .message-group:hover .message-timestamp {
                        opacity: 1;
                    }                    .attachments {
                        margin-left: 72px;
                        margin-right: 48px;
                        margin-top: 0.5rem;
                        display: flex;
                        flex-direction: column;
                        gap: 0.5rem;
                        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                        max-width: 100%;
                    }
                    
                    .attachment-container {
                        position: relative;
                        display: inline-block;
                        max-width: 400px;
                        border-radius: 8px;
                        overflow: hidden;
                        cursor: pointer;
                        transition: opacity 0.2s ease-in-out;
                        background-color: transparent;
                    }
                    
                    .attachment-container:hover {
                        opacity: 0.8;
                    }
                    
                    .attachment-image {
                        max-width: 400px;
                        max-height: 300px;
                        border-radius: 8px;
                        object-fit: contain;
                        display: block;
                        cursor: pointer;
                    }
                    
                    .attachment-video {
                        max-width: 400px;
                        max-height: 300px;
                        border-radius: 8px;
                        display: block;
                        background-color: #000;
                    }
                    
                    .attachment-audio {
                        width: 100%;
                        max-width: 400px;
                        height: 32px;
                        border-radius: 16px;
                        background-color: var(--background-secondary);
                    }
                    
                    .attachment-link {
                        color: var(--text-link);
                        font-size: 0.875rem;
                        font-weight: 400;
                        text-decoration: none;
                        cursor: pointer;
                        word-break: break-all;
                    }
                    
                    .attachment-link:hover {
                        text-decoration: underline;
                    }
                    
                    .attachment-info {
                        font-size: 0.75rem;
                        color: var(--text-muted);
                        margin-top: 0.25rem;
                        font-weight: 400;
                    }
                    
                    .attachment-file {
                        display: flex;
                        align-items: center;
                        background-color: var(--background-secondary);
                        border-radius: 8px;
                        padding: 0.5rem;
                        max-width: 432px;
                        cursor: pointer;
                        transition: background-color 0.1s ease-out;
                        border: 1px solid var(--background-modifier-accent);
                    }
                    
                    .attachment-file:hover {
                        background-color: var(--background-modifier-hover);
                    }
                    
                    .attachment-icon {
                        width: 30px;
                        height: 40px;
                        margin-right: 0.5rem;
                        color: var(--text-muted);
                        flex-shrink: 0;
                    }
                    
                    .voice-message {
                        background-color: var(--background-secondary);
                        border-radius: 19px;
                        padding: 0.5rem;
                        display: flex;
                        align-items: center;
                        gap: 0.5rem;
                        max-width: 400px;
                        border: 1px solid var(--background-modifier-accent);
                    }
                    
                    .voice-message-header {
                        display: flex;
                        align-items: center;
                        gap: 0.5rem;
                    }
                    
                    .voice-message-icon {
                        width: 16px;
                        height: 16px;
                        color: var(--text-muted);
                    }
                    
                    .voice-message-title {
                        color: var(--text-normal);
                        font-weight: 500;
                        font-size: 0.875rem;
                    }                    .reply {
                        position: relative;
                        margin-left: 72px;
                        margin-right: 48px;
                        margin-top: 0.25rem;
                        margin-bottom: 0.25rem;
                        padding: 0.25rem 0.5rem 0.25rem 0.75rem;
                        background-color: rgba(79, 84, 92, 0.06);
                        border-left: 4px solid var(--background-modifier-accent);
                        border-radius: 0 8px 8px 0;
                        max-width: calc(100% - 120px);
                        font-size: 0.875rem;
                        line-height: 1.125rem;
                        word-break: break-word;  /* Allow content to wrap instead of truncating */
                        overflow-wrap: break-word;
                        cursor: pointer;
                        transition: background-color 0.1s ease-out, border-color 0.1s ease-out;
                        contain: layout style paint;
                    }
                    
                    .reply:hover {
                        background-color: rgba(79, 84, 92, 0.08);
                        border-left-color: var(--background-modifier-hover);
                    }
                    
                    .reply::before {
                        content: "";
                        position: absolute;
                        top: 50%;
                        left: -36px;
                        width: 26px;
                        height: 8px;
                        border-left: 2px solid var(--background-modifier-accent);
                        border-bottom: 2px solid var(--background-modifier-accent);
                        border-bottom-left-radius: 8px;
                        transform: translateY(-50%);
                        z-index: 1;
                    }
                      .reply-username {
                        color: var(--text-link);
                        font-size: 0.875rem;
                        font-weight: 500;
                        text-decoration: none;
                        cursor: pointer;
                        margin-right: 0.25rem;
                        flex-shrink: 0;
                        max-width: 200px;  /* Increased from 100px for longer usernames */
                        display: inline-block;
                        word-break: break-word;  /* Allow wrapping instead of ellipsis */
                        overflow-wrap: break-word;
                        vertical-align: baseline;
                    }
                    
                    .reply-username:hover {
                        text-decoration: underline;
                    }
                      .reply-content {
                        color: var(--text-muted);
                        font-size: 0.875rem;
                        font-weight: 400;
                        line-height: 1.125rem;
                        display: inline;
                        word-break: break-word;  /* Allow content to wrap */
                        overflow-wrap: break-word;
                        white-space: pre-wrap;   /* Preserve line breaks and allow wrapping */
                        unicode-bidi: plaintext;
                        text-indent: 0;
                    }
                    
                    .reply-header {
                        display: inline-flex;
                        align-items: baseline;
                        vertical-align: baseline;
                        overflow: hidden;
                        flex-shrink: 0;
                    }
                    
                    .reply-header-wrapper {
                        display: inline-flex;
                        align-items: baseline;
                        max-width: 100%;
                        overflow: hidden;
                    }
                    
                    .reply-avatar {
                        width: 16px;
                        height: 16px;
                        border-radius: 50%;
                        margin-right: 0.25rem;
                        flex-shrink: 0;
                        vertical-align: baseline;
                        object-fit: cover;
                        display: inline-block;
                    }
                    
                    .reply-spine {
                        display: none; /* Hide the old spine style */
                    }                    .reply-attachments {
                        margin-top: 0.25rem;
                        display: flex;
                        flex-wrap: wrap;
                        gap: 0.25rem;
                        max-width: 100%;
                        overflow: hidden;
                    }                      .reply-attachment-container {
                        position: relative;
                        max-width: 250px;
                        border-radius: 8px;
                        overflow: hidden;
                        display: inline-block;
                        background-color: var(--background-secondary);
                        margin: 0.25rem 0.5rem 0.25rem 0;
                        border: 1px solid var(--background-modifier-accent);
                        transition: opacity 0.2s ease-in-out;
                    }
                    
                    .reply-attachment-container:hover {
                        opacity: 0.8;
                    }                      .reply-attachment-image {
                        display: block;
                        max-width: 250px;
                        max-height: 150px;
                        border-radius: 8px;
                        object-fit: cover;
                        width: 100%;
                        height: auto;
                        cursor: pointer;
                        transition: transform 0.2s ease-in-out;
                    }
                    
                    .reply-attachment-image:hover {
                        transform: scale(1.02);
                    }
                      .reply-attachment-file {
                        display: flex;
                        align-items: center;
                        background-color: var(--background-secondary);
                        border-radius: 8px;
                        padding: 0.75rem;
                        max-width: 320px;
                        min-width: 220px;
                        cursor: pointer;
                        transition: background-color 0.1s ease-out;
                        border: 1px solid var(--background-modifier-accent);
                        margin: 0.25rem 0;
                        width: 100%;
                        box-sizing: border-box;
                    }
                    
                    .reply-attachment-file:hover {
                        background-color: var(--background-modifier-hover);
                    }
                    
                    .reply-attachment-file .attachment-icon {
                        width: 24px;
                        height: 30px;
                        margin-right: 0.75rem;
                        color: var(--text-muted);
                        flex-shrink: 0;
                    }
                    
                    .reply-attachment-file .attachment-link {
                        color: var(--text-link);
                        font-size: 0.875rem;
                        font-weight: 400;
                        text-decoration: none;                        cursor: pointer;
                        word-break: break-word;
                        overflow-wrap: break-word;
                        flex: 1;
                        min-width: 0;
                        max-width: 100%;
                        white-space: nowrap;
                        overflow: hidden;
                        text-overflow: ellipsis;
                    }
                      .reply-attachment-file .attachment-link:hover {
                        text-decoration: underline;
                    }
                    
                    .reply-timestamp {
                        color: var(--text-muted);
                        font-size: 0.6875rem;
                        margin-left: 0.25rem;
                        white-space: nowrap;
                        font-weight: 400;
                    }
                    
                    .header-user-info {
                        display: flex;
                        align-items: center;
                        margin: 0.75rem 0;
                        gap: 1rem;
                        background-color: var(--background-secondary);
                        padding: 1rem;
                        border-radius: 8px;
                        border: 1px solid var(--background-modifier-accent);
                    }
                    
                    .header-user-details {
                        display: flex;
                        flex-direction: column;
                        gap: 0.25rem;
                    }
                    
                    .header-username {
                        font-size: 1.125rem;
                        font-weight: 600;
                        color: var(--header-primary);
                        line-height: 1.375;
                    }
                    
                    .header-avatar {
                        width: 80px;
                        height: 80px;
                        border-radius: 50%;
                        object-fit: cover;
                        cursor: pointer;
                        transition: opacity 0.2s ease-out;
                    }
                    
                    .header-avatar:hover {
                        opacity: 0.8;
                    }
                    
                    .user-profile-link {
                        color: var(--text-link);
                        text-decoration: none;
                        font-size: 0.875rem;
                        font-weight: 400;
                    }
                    
                    .user-profile-link:hover {
                        text-decoration: underline;
                    }
                    
                    .message-location {
                        margin-left: 72px;
                        margin-right: 48px;
                        margin-top: 0.125rem;
                        color: var(--text-muted);
                        font-size: 0.75rem;
                        font-weight: 400;
                        font-style: italic;
                        line-height: 1.125;                    }
                    
                    .message-number {
                        flex-shrink: 0;  /* Don't shrink the message number */
                        width: 42px;     /* Fixed width for consistent alignment */
                        text-align: center;
                        margin-top: 5px; /* Align with message content */
                        color: var(--text-muted);
                        font-weight: 600;
                        font-size: 0.6875rem;
                        line-height: 1;
                        user-select: none;
                        background-color: var(--background-primary);
                        padding: 2px 6px;
                        border-radius: 3px;
                        border: 1px solid var(--background-modifier-accent);
                        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
                        height: fit-content;  /* Only as tall as needed */
                    }
                    
                    .separator {
                        height: 0;
                        border-top: thin solid var(--background-modifier-accent);
                        margin: 1rem 72px 1rem 72px;
                        opacity: 0.6;
                    }
                    
                    .forwarded-message {
                        margin-left: 72px;
                        margin-right: 48px;
                        margin-top: 0.5rem;
                        margin-bottom: 0.5rem;
                        padding-left: 0.75rem;
                        border-left: 4px solid var(--background-modifier-accent);
                        background-color: rgba(79, 84, 92, 0.06);
                        border-radius: 0 8px 8px 0;
                        padding: 0.5rem 0.75rem;
                    }
                    
                    /* Responsive design */
                    @media (max-width: 768px) {
                        .container {
                            width: 100%;
                            max-width: 100%;
                            border-left: none;
                            border-right: none;
                        }
                        
                        .header {
                            padding: 12px;
                        }
                        
                        .messages-container {
                            padding: 8px 12px;
                        }
                        
                        .message-header {
                            padding-left: 60px;
                            padding-right: 12px;
                        }
                        
                        .message-content {
                            margin-left: 60px;
                            margin-right: 12px;
                        }
                        
                        .attachments {
                            margin-left: 60px;
                            margin-right: 12px;
                        }
                        
                        .reply {
                            margin-left: 60px;
                            margin-right: 12px;
                        }
                        
                        .message-location {
                            margin-left: 60px;
                            margin-right: 12px;
                        }
                        
                        .separator {
                            margin-left: 60px;
                            margin-right: 12px;
                        }
                        
                        .forwarded-message {
                            margin-left: 60px;
                            margin-right: 12px;
                        }
                        
                        .attachment-image, .attachment-video {
                            max-width: 280px;                        }
                          .avatar {
                            left: 12px;
                            width: 32px;
                            height: 32px;
                        }
                        
                        .message-container {
                            gap: 6px;  /* Smaller gap on mobile */
                        }
                        
                        .message-number {
                            width: 32px;    /* Smaller width on mobile */
                            font-size: 0.6rem;  /* Slightly smaller font */
                            padding: 1px 4px;   /* Smaller padding */
                        }
                    }
                    
                    /* Text selection improvements */
                    ::selection {
                        background-color: var(--brand-experiment-20a);
                    }
                    
                    ::-moz-selection {
                        background-color: var(--brand-experiment-20a);
                    }
                    
                    /* Focus styles for accessibility */
                    .username:focus,
                    .attachment-link:focus,
                    .reply-username:focus,
                    .user-profile-link:focus {
                        outline: 2px solid var(--brand-experiment);
                        outline-offset: 2px;
                        border-radius: 3px;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>Recent Messages</h1>
            ''')            # Add header information
                if user:
                    html_output.write(f'<p>User: {user.name} (ID: {user.id})</p>\n')
                    # Add user avatar and profile link                    # Enhanced avatar retrieval with multiple fallback methods
                    avatar_url = ""
                    if hasattr(user, 'display_avatar') and user.display_avatar:
                        avatar_url = user.display_avatar.url
                    elif hasattr(user, 'avatar') and user.avatar:
                        avatar_url = user.avatar.url
                    elif user.id:
                        # Generate default avatar URL using user ID
                        default_avatar_id = self.get_default_avatar_id()
                        avatar_url = f"https://cdn.discordapp.com/embed/avatars/{default_avatar_id}.png"
                    
                    # If we have an avatar URL, display the user info box
                    if avatar_url:
                        html_output.write('<div class="header-user-info">\n')
                        html_output.write(f'<img src="{avatar_url}" class="header-avatar" alt="{user.name}" onclick="window.open(\'https://discord.com/users/{user.id}\', \'_blank\')" />\n')
                        html_output.write('<div class="header-user-details">\n')
                        
                        # Add display name if it differs from username
                        display_name = getattr(user, 'display_name', None) or user.name
                        if display_name != user.name:
                            html_output.write(f'<p class="header-username">{display_name} ({user.name})</p>\n')
                        else:
                            html_output.write(f'<p class="header-username">{user.name}</p>\n')
                            
                        html_output.write(f'<p><a href="https://discord.com/users/{user.id}" target="_blank" class="user-profile-link">View Discord Profile</a></p>\n')
                        html_output.write('</div>\n')
                        html_output.write('</div>\n')
                if target_channel:
                    html_output.write(f'<p>Channel: #{target_channel.name} (ID: {target_channel.id})</p>\n')
                html_output.write(f'<p>Messages: {len(messages)}</p>\n')
                html_output.write(f'<p>Generated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC</p>\n')
                
                html_output.write('</div>\n')  # Close header div                # Use GetUser method for all users to ensure we have the most updated data
                user_ids = set(msg["user_id"] for msg in messages)
                users_dict = {}
                
                # Directly fetch all users via the GetUser method which uses API
                for user_id in user_ids:
                    try:
                        user = await self.bot.GetUser(user_id)
                        if user:
                            users_dict[user_id] = user
                    except Exception as e:
                        logger.debug(f"Could not fetch user {user_id}: {e}")                    # Process all messages
                html_output.write('<div class="messages-container">\n')
                
                for idx, msg in enumerate(messages, 1):
                    user_id = msg["user_id"]
                    user_obj = users_dict.get(user_id)
                    
                    # If user_obj is missing, try to fetch via GetUser once more
                    if not user_obj:
                        try:
                            user_obj = await self.bot.GetUser(user_id)
                            if user_obj:
                                users_dict[user_id] = user_obj
                        except Exception as e:
                            logger.debug(f"Failed to fetch user {user_id} for message {idx}: {e}")
                    
                    username = user_obj.name if user_obj else msg.get("username", f"Unknown User ({user_id})")                    # Get avatar URL directly from user object
                    user_avatar = ""
                    display_name = None
                    
                    # Get avatar and display name from user object
                    if user_obj and hasattr(user_obj, 'display_avatar') and user_obj.display_avatar:
                        user_avatar = user_obj.display_avatar.url
                    elif user_obj and hasattr(user_obj, 'avatar') and user_obj.avatar:
                        user_avatar = user_obj.avatar.url
                    
                    # Get display name
                    if user_obj and hasattr(user_obj, 'display_name'):
                        display_name = user_obj.display_name
                    
                    # Get timestamp for message
                    timestamp = msg["created_at"].strftime("%I:%M %p")
                    
                    # Get username initial for avatar fallback
                    initial = username[0].upper() if username else "?"                    # Start a new message container with number outside for better readability
                    html_output.write(f'<div class="message-container" style="position: relative; display: flex; align-items: flex-start;">\n')
                    html_output.write(f'<div class="message-number">#{idx}</div>\n')
                    html_output.write(f'<div class="message-group">\n')
                      # Add reply information if any - moved before message header for proper separation
                    # Ensuring proper spacing before reply content
                    if "reply_to_user_id" in msg and "reply_to_username" in msg:
                        reply_content = msg.get("reply_to_content", "")
                        # No truncation for HTML export - preserve full reply content
                        reply_content = self.clean_content(reply_content)# Try to fetch the user object for the reply author to get avatar and display name
                        reply_user = None
                        reply_avatar_url = ""
                        reply_display_name = None
                        try:
                            reply_user = users_dict.get(msg["reply_to_user_id"])
                            if not reply_user:
                                reply_user = await self.bot.GetUser(msg["reply_to_user_id"])
                                if reply_user:
                                    users_dict[msg["reply_to_user_id"]] = reply_user
                            
                            # Get avatar from user object
                            if reply_user and hasattr(reply_user, 'display_avatar') and reply_user.display_avatar:
                                reply_avatar_url = reply_user.display_avatar.url
                            elif reply_user and hasattr(reply_user, 'avatar') and reply_user.avatar:
                                reply_avatar_url = reply_user.avatar.url
                                
                            # Get display name from reply user
                            if reply_user and hasattr(reply_user, 'display_name'):
                                reply_display_name = reply_user.display_name
                        except Exception as e:
                            logger.debug(f"Could not fetch reply user avatar: {e}")
                        
                        html_output.write('<div class="reply">\n')
                        html_output.write('<div class="reply-header-wrapper">\n')

                        # Add avatar for reply author
                        if reply_avatar_url:
                            html_output.write(f'<img src="{reply_avatar_url}" class="reply-avatar" alt="{msg["reply_to_username"]}" loading="lazy">\n')
                        else:
                            # Use initial as fallback
                            initial = msg["reply_to_username"][0].upper() if msg["reply_to_username"] else "?"
                            html_output.write(f'<div class="reply-avatar" style="background-color: #5865F2; color: white; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 500;">{initial}</div>\n')
                        html_output.write(f'<div class="reply-header">\n')
                        # Show display name and username if they differ
                        if reply_display_name and reply_display_name != msg["reply_to_username"]:
                            html_output.write(f'<a href="https://discord.com/users/{msg["reply_to_user_id"]}" target="_blank" class="reply-username">{reply_display_name}</a>')
                            html_output.write(f'<span class="reply-user-id">@{msg["reply_to_username"]}</span>')
                        else:
                            html_output.write(f'<a href="https://discord.com/users/{msg["reply_to_user_id"]}" target="_blank" class="reply-username">{msg["reply_to_username"]}</a>')
                        html_output.write('</div>\n')
                        html_output.write('</div>\n')  # Close reply-header-wrapper
                        html_output.write(f'<span class="reply-content">{reply_content}</span>\n')                          # Show if reply had attachments
                        if "reply_to_attachments" in msg and msg["reply_to_attachments"]:
                            attachment_text = f"[{len(msg['reply_to_attachments'])} attachment{'s' if len(msg['reply_to_attachments']) > 1 else ''}]"
                            html_output.write(f'<span class="reply-content"> {attachment_text}</span>\n')
                            
                            # Actually embed the reply attachments
                            html_output.write('<div class="reply-attachments">\n')
                            for attachment in msg["reply_to_attachments"]:
                                self._write_attachment_html(html_output, attachment, is_reply=True)
                            html_output.write('</div>\n')  # Close reply-attachments div
  
                        
                        html_output.write('</div>\n')  # Close reply div
                    
                    # Handle replies to message snapshots (forwarded messages)
                    elif "reply_to_snapshot" in msg and msg.get("reply_to_content"):
                        reply_content = msg.get("reply_to_content", "")
                        # Truncate reply content if too long                        # No truncation for HTML export - preserve full forwarded reply content
                        html_output.write('<div class="reply">\n')
                        # Add Discord-style reply spine and "Forwarded from" text
                        html_output.write('<div class="reply-spine">Forwarded from</div>\n')
                        html_output.write('<div class="reply-header-wrapper" style="display: flex; align-items: center; gap: 8px; margin-bottom: 6px;">\n')
                        
                        # Add a generic icon for forwarded message
                        html_output.write('<div class="reply-avatar" style="width: 24px; height: 24px; border-radius: 50%; background-color: #4f545c; color: white; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: bold;">F</div>\n')
                        
                        html_output.write(f'<div class="reply-header">\n')
                        html_output.write(f'<span class="reply-username">[Forwarded Message]</span>\n')
                        html_output.write('</div>\n')
                        html_output.write('</div>\n')  # Close reply-header-wrapper
                        html_output.write(f'<span class="reply-content">{reply_content}</span>\n')
                        
                        # Show if reply had attachments
                        if "reply_to_attachments" in msg and msg["reply_to_attachments"]:
                            if len(msg["reply_to_attachments"]) == 1:
                                html_output.write('<div class="reply-content">[1 Attachment]</div>\n')
                            else:
                                html_output.write(f'<div class="reply-content">[{len(msg["reply_to_attachments"])} Attachments]</div>\n')                                  # Actually embed the reply attachments
                            html_output.write('<div class="reply-attachments">\n')
                            for attachment in msg["reply_to_attachments"]:
                                self._write_attachment_html(html_output, attachment, is_reply=True)
                            html_output.write('</div>\n')  # Close reply-attachments div
                        html_output.write('</div>\n')  # Close reply div
                      # Message header with avatar, username and timestamp
                    html_output.write('<div class="message-header">\n')
                    # Add the avatar (use user's avatar URL if available or fallback to initial)
                    if user_avatar:
                        html_output.write(f'<div class="avatar" title="{username}" onclick="window.open(\'https://discord.com/users/{user_id}\', \'_blank\')">\n')
                        html_output.write(f'    <img src="{user_avatar}" alt="{username}" loading="lazy">\n')
                        html_output.write('</div>\n')
                    else:
                        # Check if we can generate an avatar with initial
                        try:
                            if hasattr(self, 'generate_default_avatar'):
                                default_avatar = self.generate_default_avatar(initial, user_id)
                                if default_avatar:
                                    html_output.write(f'<div class="avatar" title="{username}" onclick="window.open(\'https://discord.com/users/{user_id}\', \'_blank\')">\n')
                                    html_output.write(f'    <img src="{default_avatar}" alt="{initial}" loading="lazy">\n')
                                    html_output.write('</div>\n')
                                else:
                                    html_output.write(f'<div class="avatar" title="{username}">{initial}</div>\n')
                            else:
                                html_output.write(f'<div class="avatar" title="{username}">{initial}</div>\n')
                        except Exception as e:
                            # If anything fails, just use the initial
                            html_output.write(f'<div class="avatar" title="{username}">{initial}</div>\n')
                            logger.debug(f"Error creating default avatar: {e}")
                    
                    html_output.write('<div class="message-header-content">\n')
                    html_output.write('<div class="message-author-line">\n')
                    
                    # Show display name and username if they differ
                    if display_name and display_name != username:
                        html_output.write(f'<a href="https://discord.com/users/{user_id}" target="_blank" class="username">{display_name}</a>\n')
                        html_output.write(f'<span class="user-id">@{username}</span>\n')
                    else:
                        html_output.write(f'<a href="https://discord.com/users/{user_id}" target="_blank" class="username">{username}</a>\n')
                        html_output.write(f'<span class="user-id">({user_id})</span>\n')
                          # Format the date properly - don't duplicate time information
                    if msg["created_at"].date() == datetime.utcnow().date():
                        formatted_time = f"Today at {timestamp}"
                    else:
                        formatted_time = f"{msg['created_at'].strftime('%m/%d/%Y')} at {timestamp}"
                    
                    # Add sufficient spacing between user ID and timestamp
                    html_output.write(f'<span class="timestamp">{formatted_time}</span>\n')
                    html_output.write('</div>\n')  # Close message-author-line
                    html_output.write('</div>\n')  # Close message-header-content
                    html_output.write('</div>\n')  # Close message-header
                      # Message content
                    if msg.get("content"):
                        content = self.clean_content(msg["content"])
                        # No truncation for HTML export - preserve full content for all users
                        html_output.write(f'<div class="message-content">{content}</div>\n')
                          # Display forwarded messages (message snapshots) if any
                    if "message_snapshots" in msg and msg["message_snapshots"]:
                        for snapshot in msg["message_snapshots"]:
                            # Add extra div with margin for better separation
                            html_output.write('<div style="margin-top: 16px;"></div>\n')
                            html_output.write('<div class="forwarded-message">\n')
                            html_output.write('<span class="reply-username">[Forwarded Message]</span>\n')
                            
                            # Show snapshot content if any
                            if snapshot.get("snapshot_content"):
                                snapshot_content = self.clean_content(snapshot["snapshot_content"])
                                html_output.write(f'<div class="message-content">{snapshot_content}</div>\n')
                            
                            # Show snapshot attachments if any
                            if snapshot.get("snapshot_attachments"):
                                html_output.write('<div class="attachments">\n')
                                for attachment in snapshot["snapshot_attachments"]:
                                    # Check if attachment is an image based on common image extensions
                                    is_image = any(attachment.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'])
                                    
                                    if is_image:
                                        html_output.write(f'<div class="attachment-container">\n')
                                        # Make the image clickable to open full size
                                        html_output.write(f'<a href="{attachment}" target="_blank">')
                                        # Embed the image directly in the page
                                        html_output.write(f'<img src="{attachment}" class="attachment-image" alt="Attachment" loading="lazy">\n')
                                        html_output.write('</a>\n')
                                        html_output.write(f'<div class="attachment-info"><a href="{attachment}" class="attachment-link" target="_blank">Open original</a></div>\n')
                                        html_output.write('</div>\n')
                                    else:
                                        # For non-image attachments, provide link and icon
                                        html_output.write(f'<div class="attachment-file">\n')
                                        html_output.write(f'<svg class="attachment-icon" viewBox="0 0 24 24"><path fill="currentColor" d="M14,2H6A2,2 0 0,0 4,4V20A2,2 0 0,0 6,22H18A2,2 0 0,0 20,20V8L14,2M18,20H6V4H13V9H18V20Z"></path></svg>\n')
                                        html_output.write(f'<a href="{attachment}" class="attachment-link" target="_blank">{attachment.split("/")[-1] or "Attachment"}</a>\n')
                                        html_output.write('</div>\n')
                                html_output.write('</div>\n')  # Close attachments div for snapshot
                            
                            html_output.write('</div>\n')  # Close forwarded-message div
                      # Show attachments with enhanced media support
                    if msg.get("attachments"):
                        html_output.write('<div class="attachments">\n')                        
                        for attachment in msg["attachments"]:
                            self._write_attachment_html(html_output, attachment, is_reply=False)
                        html_output.write('</div>\n')  # Close attachments div
                    
                    # Add server/channel info
                    if "guild_name" in msg:
                        location_info = f"#{msg.get('channel_name', 'unknown')} in {msg['guild_name']}"
                    elif msg.get("channel_type") == "group" or msg.get("is_group"):
                        # Enhanced group chat display - if no name, try to show participants
                        if not msg.get('channel_name') or msg.get('channel_name') == "None":
                            # Try to get the channel object to access recipients
                            channel = self.bot.get_channel(msg["channel_id"])
                            if channel and hasattr(channel, "recipients") and len(channel.recipients) > 0:
                                # Format up to 3 recipient usernames
                                recipient_names = [r.name for r in channel.recipients[:3]]
                                if len(channel.recipients) > 3:
                                    recipient_names.append(f"+{len(channel.recipients) - 3} more")
                                participants = ", ".join(recipient_names)
                                location_info = f"Group with: {participants}"
                            else:
                                location_info = f"Group chat"
                        else:
                            location_info = f"Group: {msg.get('channel_name', 'Unnamed Group')}"                        
                    else:
                        if msg.get("is_self") and msg.get("dm_recipient_name"):
                            recipient_name = msg.get("dm_recipient_name")
                            location_info = f"DM with {recipient_name}"
                        elif msg.get("dm_recipient_name") and msg.get("user_id") == self.bot.user.id:
                            recipient_name = msg.get("dm_recipient_name")
                            location_info = f"DM with {recipient_name}"
                        else:
                            dm_username = username
                            location_info = f"DM with {dm_username}"
                      # Write location info for all types of messages
                    html_output.write(f'<div class="message-location">{location_info}</div>\n')
                    
                    # Close the message-group div and message container properly
                    html_output.write('</div>\n')  # Close message-group div
                    html_output.write('</div>\n')  # Close message-container div
                    
                    if idx < len(messages):
                        html_output.write('<div class="separator"></div>\n')
                
                # Close messages-container div
                html_output.write('</div>\n')  # Close messages-container div                # Close HTML document
                html_output.write('''
                </div> <!-- Close messages-container -->
                </div> <!-- Close container -->
            </body>
            </html>
            ''')
                
                # Create file name based on parameters
                file_name = "recent_messages"
                if user:
                    file_name += f"_{user.name}_{user.id}"
                if target_channel:
                    file_name += f"_{target_channel.name}"
                file_name = file_name.replace(" ", "_") + ".html"
                
                # Create and send the file
                html_output.seek(0)
                file = discord.File(html_output, filename=file_name)
                
                # Send file with summary
                await ctx.send(
                    content=quote_block(f"```ansi\n\u001b[1;33mRecent Messages\u001b[0m\n\u001b[0;36mRetrieved \u001b[1;37m{len(messages)} messages\u001b[0m```"),
                    file=file,
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                
                # Close the buffer
                html_output.close()
                
            else:
                # Original display logic for smaller message sets
                # ... [existing code for displaying messages in chunks] ...
                # Format messages in the snipe style
                sent_messages = []
                for chunk_start in range(0, len(messages), 10):  # Process in chunks of 10
                    chunk = messages[chunk_start:chunk_start + 10]  # Adjusted chunk size
                    message_parts = [
                        "```ansi\n" + \
                        "\u001b[30m\u001b[1m\u001b[4mRecent Messages\u001b[0m\n"
                    ]
                    
                    attachments_to_send = []  # Store attachments for sending after codeblock
                    
                    # Pre-fetch all user IDs in this chunk
                    user_ids = [msg["user_id"] for msg in chunk]
                    users_dict = {}
                    
                    # First try to get users from cache
                    for user_id in user_ids:
                        user = self.bot.get_user(user_id)
                        if user:
                            users_dict[user_id] = user
                    
                    # For any users not found in cache, try to get them in a single batch
                    missing_user_ids = [user_id for user_id in user_ids if user_id not in users_dict]
                    if missing_user_ids:
                        for user_id in missing_user_ids:
                            try:
                                user = await self.bot.GetUser(user_id)
                                if user:
                                    users_dict[user_id] = user
                            except Exception as e:
                                logger.error(f"Error fetching user {user_id}: {e}")
                    
                    # Add each message
                    for idx, msg in enumerate(chunk, chunk_start + 1):
                        user = users_dict.get(msg["user_id"])
                        username = user.name if user else msg.get("username", f"Unknown User ({msg['user_id']})")
                        timestamp = msg["created_at"].strftime("%I:%M %p")

                        message_parts[-1] += f"\u001b[1;33m#{idx}\n"
                        
                        # Add reply information with the line format
                        if "reply_to_user_id" in msg and "reply_to_username" in msg:
                            reply_content = msg.get("reply_to_content", "")
                            # Truncate reply content if too long
                            if len(reply_content) > 190:
                                reply_content = reply_content[:187] + "..."
                                reply_content = self.clean_content(reply_content)
                            message_parts[-1] += f"┌─── \u001b[0m{msg['reply_to_username']} \u001b[30m{reply_content}\n"
                            
                            # Show if reply had attachments
                            if "reply_to_attachments" in msg and msg["reply_to_attachments"]:
                                if len(msg["reply_to_attachments"]) == 1:
                                    message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                                else:
                                    message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['reply_to_attachments'])} Attachments ]\n"
                                # Add reply attachments to the list to be displayed
                                attachments_to_send.extend(msg["reply_to_attachments"])
                        # Handle replies to message snapshots (forwarded messages)
                        elif "reply_to_snapshot" in msg and msg.get("reply_to_content"):
                            reply_content = msg.get("reply_to_content", "")
                            # Truncate reply content if too long
                            if len(reply_content) > 190:
                                reply_content = reply_content[:187] + "..."
                            message_parts[-1] += f"┌─── \u001b[0;33m[Forwarded Message] \u001b[30m{reply_content}\n"
                            
                            # Show if reply had attachments
                            if "reply_to_attachments" in msg and msg["reply_to_attachments"]:
                                if len(msg["reply_to_attachments"]) == 1:
                                    message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                                else:
                                    message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['reply_to_attachments'])} Attachments ]\n"
                                # Add reply attachments to the list to be displayed
                                attachments_to_send.extend(msg["reply_to_attachments"])
                        # New formatting: username with proper date formatting
                        if msg["created_at"].date() == datetime.utcnow().date():
                            formatted_time_small = f"Today at {timestamp}"
                        else:
                            formatted_time_small = msg["created_at"].strftime("%m/%d/%Y %I:%M %p")
                        
                        message_parts[-1] += f"\u001b[1;37m{username} \u001b[0m{formatted_time_small}\n"
                        if msg.get("content"):
                            content = self.clean_content(msg["content"])
                            # Truncate content if it's too long
                            content = self.truncate_content(content, 256)
                            # Content directly below the username line with color
                            for line in content.split('\n'):
                                message_parts[-1] += f"\u001b[1;31m{line}\n"

                        # Display forwarded messages (message snapshots) if any
                        if "message_snapshots" in msg and msg["message_snapshots"]:
                            for i, snapshot in enumerate(msg["message_snapshots"]):
                                # Show simplified forwarded message header
                                message_parts[-1] += f"┌─── \u001b[0;33m[Forwarded Message]\n"
                                
                                # Show snapshot content if any
                                if snapshot.get("snapshot_content"):
                                    snapshot_content = self.clean_content(snapshot["snapshot_content"])
                                    if len(snapshot_content) > 100:  # Truncate if too long
                                        snapshot_content = snapshot_content[:97] + "..."
                                    message_parts[-1] += f"â”‚    \u001b[0;37m{snapshot_content}\n"
                                
                                # Show snapshot attachments if any
                                if snapshot.get("snapshot_attachments"):
                                    if len(snapshot["snapshot_attachments"]) == 1:
                                        message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                                    else:
                                        message_parts[-1] += f"└─── \u001b[0;36m[ {len(snapshot['snapshot_attachments'])} Attachments ]\n"
                                    # Add snapshot attachments to the list to be displayed
                                    attachments_to_send.extend(snapshot["snapshot_attachments"])
                                else:
                                    message_parts[-1] += f"└───\n"

                        if msg.get("attachments"):
                            # Update to show number of attachments in simpler format
                            if len(msg["attachments"]) == 1:
                                message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                            else:
                                message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['attachments'])} Attachments ]\n"
                            attachments_to_send.extend(msg["attachments"])
                                # Add server/channel info in a more compact format
                        if "guild_name" in msg:
                            location_info = f"#{msg.get('channel_name', 'unknown')} in {msg['guild_name']}"
                        elif msg.get("channel_type") == "group" or msg.get("is_group"):
                            # Enhanced group chat display - if no name, try to show participants
                            if not msg.get('channel_name') or msg.get('channel_name') == "None":
                                # Try to get the channel object to access recipients
                                channel = self.bot.get_channel(msg["channel_id"])
                                if channel and hasattr(channel, "recipients") and len(channel.recipients) > 0:
                                    # Format up to 3 recipient usernames
                                    recipient_names = [r.name for r in channel.recipients[:3]]
                                    if len(channel.recipients) > 3:
                                        recipient_names.append(f"+{len(channel.recipients) - 3} more")
                                    participants = ", ".join(recipient_names)
                                    location_info = f"Group with: {participants}"
                                else:
                                    location_info = f"Group chat"
                            else:
                                location_info = f"Group: {msg.get('channel_name', 'Unnamed Group')}"                        
                        else:
                            # For DMs, display the username of the recipient rather than the author
                            # If message is from selfbot, show recipient name
                            # If message is from someone else, show their name
                            if msg.get("is_self") and msg.get("dm_recipient_name"):
                                recipient_name = msg.get("dm_recipient_name")
                                location_info = f"DM with {recipient_name}"
                            elif msg.get("dm_recipient_name") and msg.get("user_id") == self.bot.user.id:
                                # Fallback for old data structure
                                recipient_name = msg.get("dm_recipient_name")
                                location_info = f"DM with {recipient_name}"
                            else:
                                # Fallback for older messages or messages from others
                                dm_username = username
                                location_info = f"DM with {dm_username}"
                                
                        message_parts[-1] += f"\u001b[0;36m{location_info}\n"
                        message_parts[-1] += "\u001b[0;37m" + "─" * 28 + "\n"

                    message_parts[-1] += "```"

                    # Send the formatted message first
                    msg = await ctx.send(quote_block(''.join(message_parts)))
                    sent_messages.append(msg)
                    
                    # Then send attachments if any
                    if attachments_to_send:
                        attachment_msg = await ctx.send("\n".join(attachments_to_send))
                        sent_messages.append(attachment_msg)

                # handle auto-deletion of messages
                if self.bot.config_manager.auto_delete.enabled:
                    for msg in sent_messages:
                        await msg.delete(delay=self.bot.config_manager.auto_delete.delay)
            
        except Exception as e:
            logger.error(f"Error retrieving recent messages: {e}", exc_info=True)
            await self.send_with_auto_delete(ctx, f"Error retrieving messages: {str(e)}")
            
    def clean_content(self, content: str) -> str:
        """Clean and escape special characters in message content"""
        if not content:
            return ""
            
        # Replace common formatting characters with escaped versions similar to snipe_cog
        content = (content
            .replace('\\', '')  # Remove backslashes
            .replace('```', '')  # Replace code blocks 
            .replace('`', '')    # Replace backticks
            .replace('|', '')    # Escape pipes
            .replace('*', '')    # Escape asterisks
        )
        return content
        
    def truncate_content(self, content: str, max_length: int = 256) -> str:
        """Truncate content if it's longer than max_length"""
        if content and len(content) > max_length:
            return content[:max_length-3] + "..."
        return content

    @commands.command(aliases=['tm'], hidden=True)
    @developer_only(allow_auxiliary=True)
    async def trackmessages(self, ctx, user_or_option: Optional[Union[discord.Member, discord.User, int, str]] = None, 
                            limit_or_page: Optional[int] = None,
                            third_param: Optional[int] = None):
        """Set or view custom message tracking limit for a user
        
        Usage:
        ;trackmessages - Show all users with custom tracking limits (page 1)
        ;trackmessages all - Show all users with custom tracking limits (page 1)
        ;trackmessages all 2 - Show all users with custom tracking limits (page 2)
        ;trackmessages @user/ID - Show current tracking limit for user
        ;trackmessages @user/ID 500 - Set tracking limit to 500 messages for user
        ;trackmessages @user/ID 0 - Reset to default tracking limit (100 messages)
        """
        try:
            await self.safe_delete_message(ctx.message)
        except:
            pass
    
        # check if user_or_option equals developer id and return early
        if self.bot.config_manager.is_developer(user_or_option):
            await self.send_with_auto_delete(ctx, "Cannot modify tracking limit for developer account")
            return
    
        # Get reference to the Snipe cog's USER_MESSAGE_LIMIT constant
        snipe_cog = self.bot.get_cog('Snipe')
        
        if not snipe_cog:
            await self.send_with_auto_delete(ctx, "Snipe cog is not loaded, unable to modify tracking limits.")
            return
            
        USER_MESSAGE_LIMIT = 100  # Default value if we can't access the constant
        if hasattr(snipe_cog, 'USER_MESSAGE_LIMIT'):
            USER_MESSAGE_LIMIT = snipe_cog.USER_MESSAGE_LIMIT
        
        # Check if the command user is the developer or an auxiliary user
        is_developer = self.bot.config_manager.is_developer(ctx.author.id)
        
        # Check if we need to show all custom tracking limits
        if user_or_option is None or (isinstance(user_or_option, str) and user_or_option.lower() in ['all', 'list']):
            # If we're listing all users, the second parameter becomes the page number
            page = 1  # Default to page 1
            if limit_or_page is not None and isinstance(limit_or_page, int) and limit_or_page > 0:
                page = limit_or_page
                
            if not self.bot.db.is_active:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mDatabase is not active. Cannot retrieve tracking limits.```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
                
            try:
                # Fetch all custom tracking limits
                cursor = self.bot.db.db.tracking_limits.find({})
                all_limits = await cursor.to_list(length=1000)  # Retrieve up to 1000 entries
                
                if not all_limits:
                    await ctx.send(
                        quote_block("```ansi\n\u001b[1;33mTracking Limits\u001b[0m\n\u001b[0;37mNo custom tracking limits have been set.```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
                    
                # Sort by message limit (descending)
                all_limits.sort(key=lambda x: x.get('message_limit', 0), reverse=True)
                
                # Pagination setup
                items_per_page = 15  # Show 15 items per page
                total_pages = (len(all_limits) + items_per_page - 1) // items_per_page  # Calculate total pages
                
                # Ensure page number is within valid range
                page = min(max(1, page), total_pages)
                
                # Get items for the current page
                start_idx = (page - 1) * items_per_page
                end_idx = min(start_idx + items_per_page, len(all_limits))
                page_limits = all_limits[start_idx:end_idx]
                
                message = f"```ansi\n\u001b[1;33mCustom Tracking Limits\u001b[0m\n"
                message += f"\u001b[0;37m{'-' * 22}\n"
                message += f"\u001b[0;36mDefault limit: \u001b[1;37m{USER_MESSAGE_LIMIT} messages\n"
                message += f"\u001b[0;36mShowing page \u001b[1;37m{page}/{total_pages}\u001b[0;36m of \u001b[1;37m{len(all_limits)}\u001b[0;36m users\n\n"
                
                users_cache = {}  # Cache user info to avoid repeated API calls
                
                for entry in page_limits:
                    user_id = entry.get('user_id')
                    limit = entry.get('message_limit')
                    updated_at = entry.get('updated_at', 'Unknown')
                    set_by = entry.get('set_by', 'Unknown')  # Add set_by field
                    
                    if not user_id or not limit:
                        continue
                    
                    # Try to get username
                    username = "Unknown User"
                    if user_id in users_cache:
                        username = users_cache[user_id]
                    else:
                        try:
                            user = self.bot.get_user(user_id)
                            if user:
                                username = user.name
                            else:
                                # Try to fetch from API if not in cache
                                user = await self.bot.GetUser(user_id)
                                if user:
                                    username = user.name
                        except:
                            # Keep as Unknown User if fetching fails
                            pass
                        
                        # Add to cache
                        users_cache[user_id] = username
                    
                    # Format date if it exists
                    date_str = ""
                    if updated_at != 'Unknown' and isinstance(updated_at, datetime):
                        date_str = f"\u001b[0;30m â€¢ {updated_at.strftime('%Y-%m-%d')}"
                    
                    # Calculate difference from default
                    diff = limit - USER_MESSAGE_LIMIT
                    diff_str = ""
                    if diff > 0:
                        diff_str = f"\u001b[0;32m+{diff}"
                    else:
                        diff_str = f"\u001b[0;31m{diff}"
                    
                    # Add a lock indicator if set by developer and show in display
                    lock_str = ""
                    if self.bot.config_manager.is_developer(set_by):
                        lock_str = " ðŸ”’" if is_developer else " \u001b[1;31mðŸ”’"
                    
                    message += f"\u001b[0;36m{username} ({user_id}): \u001b[1;37m{limit} \u001b[0;33m[{diff_str}\u001b[0;33m]{date_str}{lock_str}\n"
                
                # Add pagination navigation instruction
                message += f"\n\u001b[0;37mUse \u001b[1;37m{ctx.prefix}tm all <page>\u001b[0;37m to navigate pages\n"
                message += "```"
                
                await ctx.send(
                    quote_block(message),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
                    
            except Exception as e:
                logger.error(f"Error retrieving tracking limits: {e}", exc_info=True)
                await ctx.send(
                    quote_block(f"```ansi\n\u001b[1;31mError retrieving tracking limits: {e}```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
    
        # If we reach here, proceed with the existing functionality for individual users
        user = user_or_option
        limit = limit_or_page  # For individual user operations, limit_or_page is the actual limit
        
        # Check if user is developer account, return early
        if isinstance(user, (discord.Member, discord.User)) and self.bot.config_manager.is_developer(user.id):
            await self.send_with_auto_delete(ctx, "Cannot modify tracking limit for developer account")
            return
        
        # Convert user parameter to ID if it's a Member or User object
        user_id = user.id if isinstance(user, (discord.Member, discord.User)) else user
        
        # FIXED: Validate that we have a valid Discord user ID if user is an integer
        if isinstance(user_id, int):
            # Discord IDs are typically 17-20 digits long - check reasonable length
            if user_id < 10_000_000_000_000_000:  # 17 digits minimum
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mInvalid Discord user ID. Discord IDs are at least 17 digits long.```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
                
            # Try to validate the user ID by fetching the user
            try:
                user_obj = await self.bot.GetUser(user_id)
                if not user_obj:
                    await ctx.send(
                        quote_block("```ansi\n\u001b[1;31mCould not find a Discord user with that ID.```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
            except discord.NotFound:
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;31mCould not find a Discord user with that ID.```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            except discord.HTTPException:
                # If we can't fetch due to an API error, warn but still proceed
                await ctx.send(
                    quote_block("```ansi\n\u001b[1;33mWarning: Could not verify this Discord user ID due to an API error.```"),
                    delete_after=5  # Short display time for the warning
                )
        
        # If limit is not provided, show current tracking limit
        if limit is None:
            current_limit = USER_MESSAGE_LIMIT  # Default value
            set_by = None  # Track who set this limit
            
            try:
                # Check if user has a custom limit
                limit_doc = await self.bot.db.db.tracking_limits.find_one({"user_id": user_id})
                if limit_doc and "message_limit" in limit_doc:
                    current_limit = limit_doc["message_limit"]
                    set_by = limit_doc.get("set_by")
            except Exception as e:
                logger.error(f"Error fetching custom message limit for user {user_id}: {e}")
                
            is_custom = current_limit != USER_MESSAGE_LIMIT
            
            message = f"```ansi\n\u001b[1;33mMessage Tracking Limit\u001b[0m\n"
            message += f"\u001b[0;37m{'─' * 22}\n"
            
            if isinstance(user, (discord.Member, discord.User)):
                message += f"\u001b[0;36mUser: \u001b[1;37m{user.name} ({user_id})\n"
            else:
                message += f"\u001b[0;36mUser ID: \u001b[1;37m{user_id}\n"
                
            message += f"\u001b[0;36mCurrent limit: \u001b[1;37m{current_limit} messages"
            
            if is_custom:
                message += f" \u001b[0;32m(Custom)"
                # If set by developer, indicate it's locked for non-developer users
                if self.bot.config_manager.is_developer(set_by) and not is_developer:
                    message += " \u001b[1;31mðŸ”’"
            else:
                message += f" \u001b[0;33m(Default)"
                
            message += "\n```"
            await ctx.send(
                quote_block(message),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
            
        # Otherwise, set new tracking limit
        if not self.bot.db.is_active:
            await ctx.send(
                quote_block("```ansi\n\u001b[1;31mDatabase is not active. Cannot set tracking limit.```"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
            
        # Validate limit
        if limit < 0:
            await ctx.send(
                quote_block("```ansi\n\u001b[1;31mLimit must be a positive number or 0 to reset to default.```"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
            
        try:
            # Check if the limit was set by developer and current user is not developer
            if not is_developer:
                limit_doc = await self.bot.db.db.tracking_limits.find_one({"user_id": user_id})
                if limit_doc and self.bot.config_manager.is_developer(limit_doc.get("set_by")):
                    await ctx.send(
                        quote_block("```ansi\n\u001b[1;31mCannot modify tracking limit set by developer account.```"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
            
            # If limit is 0, reset to default (remove any custom limit)
            if limit == 0:
                await self.bot.db.db.tracking_limits.delete_one({"user_id": user_id})
                
                message = f"```ansi\n\u001b[1;33mMessage Tracking Limit\u001b[0m\n"
                message += f"\u001b[0;37m{'─' * 22}\n"
                if isinstance(user, (discord.Member, discord.User)):
                    message += f"\u001b[0;36mUser: \u001b[1;37m{user.name} ({user_id})\n"
                else:
                    message += f"\u001b[0;36mUser ID: \u001b[1;37m{user_id}\n"
                message += f"\u001b[0;36mLimit: \u001b[1;32mReset to default ({USER_MESSAGE_LIMIT} messages)\n```"
                
                await ctx.send(
                    quote_block(message),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
                
            # Otherwise, set custom limit - now including set_by information
            await self.bot.db.db.tracking_limits.update_one(
                {"user_id": user_id},
                {"$set": {
                    "user_id": user_id, 
                    "message_limit": limit, 
                    "updated_at": datetime.utcnow(),
                    "set_by": ctx.author.id  # Store who set this limit
                }},
                upsert=True
            )
            
            message = f"```ansi\n\u001b[1;33mMessage Tracking Limit\u001b[0m\n"
            message += f"\u001b[0;37m{'─' * 22}\n"
            if isinstance(user, (discord.Member, discord.User)):
                message += f"\u001b[0;36mUser: \u001b[1;37m{user.name} ({user_id})\n"
            else:
                message += f"\u001b[0;36mUser ID: \u001b[1;37m{user_id}\n"
            message += f"\u001b[0;36mLimit: \u001b[1;32mSet to {limit} messages\n"
            
            # Show how many additional messages this allows
            increase = limit - USER_MESSAGE_LIMIT
            if increase > 0:
                message += f"\u001b[0;36mIncrease: \u001b[1;32m+{increase} messages from default\n"
                
            message += "```"
            
            await ctx.send(
                quote_block(message),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            
        except Exception as e:
            logger.error(f"Error setting tracking limit: {e}", exc_info=True)
            await ctx.send(
                quote_block(f"```ansi\n\u001b[1;31mError setting tracking limit: {e}```"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

    
    async def get_mutual_data(self, user_id):
        """Fetch mutual friends and guilds with user_id and cache the result"""
        try:
            # Check cache first
            now = datetime.now()
            cache_key = f"mutual_{user_id}"
            
            if cache_key in self.mutual_friends_cache:
                cache_time, cached_data = self.mutual_friends_cache[cache_key]
                # If cache is still valid (less than cache_expiry seconds old)
                if (now - cache_time).total_seconds() < self.cache_expiry:
                    return cached_data
    
            # Fetch user profile with mutual info (safely)
            try:
                user_profile = await self.bot.fetch_user_profile(
                    user_id,
                    with_mutual_friends=True,
                    with_mutual_guilds=True
                )
            except discord.NotFound:
                # Suppress noisy 404s (unknown user); cache empty to avoid repeat hits
                empty = {'friends': [], 'guilds': [], 'total_guilds': 0}
                self.mutual_friends_cache[cache_key] = (now, empty)
                return empty
            except discord.HTTPException as e:
                # If rate limited or other HTTP issue, return empty; do not spam logs
                if getattr(e, 'status', None) == 429:
                    logger.debug(f"Rate limited fetching profile for {user_id}; returning empty mutual data")
                else:
                    logger.debug(f"HTTP error fetching profile for {user_id}: {e}")
                return {'friends': [], 'guilds': [], 'total_guilds': 0}
            
            # Initialize result structure
            mutual_data = {
                'friends': [],
                'guilds': [],
                'total_guilds': 0
            }
            
            # Get mutual friends
            if hasattr(user_profile, 'mutual_friends') and user_profile.mutual_friends:
                mutual_data['friends'] = user_profile.mutual_friends
    
            # Get mutual guilds from profile - exactly like userinfo command
            profile_guilds = set()
            if hasattr(user_profile, 'mutual_guilds') and user_profile.mutual_guilds:
                for mutual_guild in user_profile.mutual_guilds:
                    guild = self.bot.get_guild(mutual_guild.id)
                    if guild:
                        profile_guilds.add(guild.id)
                        mutual_data['guilds'].append({
                            'id': guild.id,
                            'name': guild.name,
                            'member_count': guild.member_count if hasattr(guild, 'member_count') else 0,
                            'source': 'current'
                        })
                    else:
                        # Even if bot is not in the guild, still add it as mutual guild
                        # This matches userinfo behavior more closely
                        profile_guilds.add(mutual_guild.id)
                        mutual_data['guilds'].append({
                            'id': mutual_guild.id,
                            'name': f'Guild {mutual_guild.id}',  # Fallback name
                            'member_count': 0,
                            'source': 'current'
                        })
    
            # Fetch historical guild data from database
            try:
                user_data = await self.bot.db.db.users.find_one(
                    {"_id": user_id},
                    {"detected_guilds": 1}
                )
                
                if user_data and 'detected_guilds' in user_data:
                    for guild_entry in user_data['detected_guilds']:
                        guild_id = guild_entry.get('id')
                        # Only add if not already in profile guilds
                        if guild_id and guild_id not in profile_guilds:
                            mutual_data['guilds'].append({
                                'id': guild_id,
                                'name': guild_entry.get('name', 'Unknown Guild'),
                                'last_seen': guild_entry.get('last_seen', 'Unknown'),
                                'source': 'historical'
                            })
                            
            except Exception as e:
                logger.warning(f"Error fetching historical guild data for {user_id}: {e}")
    
            # Update total guild count
            mutual_data['total_guilds'] = len(mutual_data['guilds'])
            
            # Sort guilds by name
            mutual_data['guilds'].sort(key=lambda x: x.get('name', '').lower())
            
            # Cache the result
            self.mutual_friends_cache[cache_key] = (now, mutual_data)
            return mutual_data
            
        except Exception as e:
            # Downgrade to debug to reduce noise for repeated invalid IDs
            if isinstance(e, discord.NotFound):
                logger.debug(f"User {user_id} not found for mutual data")
            else:
                logger.debug(f"Error fetching mutual data for {user_id}: {e}")
            return {'friends': [], 'guilds': [], 'total_guilds': 0}
            
        # analyze_user_alts removed        
                # Find potential alts using optimized batch fetching
                if query_conditions:
                    # Use the new find_many method with proper limits and sorting for better performance
                    all_matches = await self.bot.db.find_many(
                        'users',
                        query={
                            "_id": {"$ne": user_id},
                            "$or": query_conditions
                        },
                        projection={
                            "_id": 1,
                            "current_username": 1,
                            "current_displayname": 1,
                            "username_history": 1,
                            "displayname_history": 1,
                            "detected_guilds": 1,
                            "last_seen": 1,
                            "first_seen": 1  # Add first_seen for alt account creation date comparison
                        },
                        sort=[('_id', 1)],
                        limit=500  # Reasonable limit to prevent memory issues
                    )
        
                    if not all_matches:
                        return [], {
                            '__pagination__': {
                                'page': 1,
                                'total_pages': 1,
                                'total_matches': 0,
                                'items_per_page': items_per_page
                            }
                        }
                    
                    total_matches = len(all_matches)
                    logger.debug(f"[SearchOptimization] Found {total_matches} potential alts for user {user_id} (limited to 500)")
                    
                    # If we hit the limit, log it for awareness
                    if total_matches >= 500:
                        logger.info(f"[SearchOptimization] User {user_id} hit the 500 alt search limit - results may be truncated")
                    
                    # Track pattern frequency to identify common vs unique matches
                    pattern_frequency = {}
                    
                    # First pass: collect all matching patterns for frequency analysis
                    for potential_alt in all_matches:
                        alt_id = potential_alt['_id']
                        
                        # Initialize alt data structure
                        if alt_id not in potential_alts:
                            potential_alts[alt_id] = {
                                'user_id': alt_id,
                                'current_username': potential_alt.get('current_username', 'Unknown'),
                                'current_displayname': potential_alt.get('current_displayname'),
                                'matching_patterns': set(),
                                'username_matches': set(),
                                'displayname_matches': set(),
                                'cross_matches': set(),  # New field for cross-matches
                                'mutual_friends': 0,
                                'mutual_guilds': set(),
                                'last_seen': potential_alt.get('last_seen'),
                                'first_seen': potential_alt.get('first_seen'),  # Add first_seen for date comparison
                                'confidence': 0,
                                'pattern_uniqueness': 0,  # New field to track pattern uniqueness
                                'creation_date_score': 0,  # New field for account creation date analysis
                                'timestamp_correlation_score': 0  # New field for name change timing correlation
                            }
                        
                        # Process current names to identify patterns
                        current_name = potential_alt.get('current_username', '').lower() if potential_alt.get('current_username') else ''
                        current_display = potential_alt.get('current_displayname', '').lower() if potential_alt.get('current_displayname') else ''
                        
                        # Track matching patterns and their frequency
                        for pattern in search_patterns:
                            pattern = pattern.lower()
                            matched = False
                            
                            # Check current names
                            if current_name and (pattern == current_name or pattern in current_name):
                                matched = True
                            if current_display and (pattern == current_display or pattern in current_display):
                                matched = True
                            
                            # Check username history
                            for history in potential_alt.get('username_history', []):
                                if isinstance(history, dict):
                                    value = history.get('value', '').lower() if history.get('value') else ''
                                    if value and (pattern == value or pattern in value):
                                        matched = True
                                        break
                            
                            # Check displayname history
                            for history in potential_alt.get('displayname_history', []):
                                if isinstance(history, dict):
                                    value = history.get('value', '').lower() if history.get('value') else ''
                                    if value and (pattern == value or pattern in value):
                                        matched = True
                                        break
                            
                            # If this pattern matched, record it for frequency analysis
                            if matched:
                                if pattern not in pattern_frequency:
                                    pattern_frequency[pattern] = 0
                                pattern_frequency[pattern] += 1
                    
                    # Second pass: process matches using full target history sets (labels), scoring only canonical patterns
                    for potential_alt in all_matches:
                        alt_id = potential_alt['_id']
                        alt_data = potential_alts[alt_id]
                        
                        # Collect normalized alt values
                        current_name = self._unicode_normalize((potential_alt.get('current_username') or '').lower())
                        current_display = self._unicode_normalize((potential_alt.get('current_displayname') or '').lower())
                        uname_hist_vals = set()
                        for h in potential_alt.get('username_history', []) or []:
                            if isinstance(h, dict):
                                v = h.get('value')
                                if isinstance(v, str) and v:
                                    uname_hist_vals.add(self._unicode_normalize(v.lower()))
                        dname_hist_vals = set()
                        for h in potential_alt.get('displayname_history', []) or []:
                            if isinstance(h, dict):
                                v = h.get('value')
                                if isinstance(v, str) and v:
                                    dname_hist_vals.add(self._unicode_normalize(v.lower()))

                        # Current matches
                        if current_name and current_name in target_username_values_set:
                            if current_name in canonical_patterns:
                                alt_data['username_matches'].add(current_name)
                            alt_data['matching_patterns'].add(f"[Current Username] {current_name}")
                        if current_display and current_display in target_display_values_set:
                            if current_display in canonical_patterns:
                                alt_data['displayname_matches'].add(current_display)
                            alt_data['matching_patterns'].add(f"[Current Display] {current_display}")

                        # History matches (same-category)
                        for v in uname_hist_vals & target_username_values_set:
                            if v in canonical_patterns:
                                alt_data['username_matches'].add(v)
                            alt_data['matching_patterns'].add(f"[Username History Match] {v}")
                        for v in dname_hist_vals & target_display_values_set:
                            if v in canonical_patterns:
                                alt_data['displayname_matches'].add(v)
                            alt_data['matching_patterns'].add(f"[Display History Match] {v}")

                        # Cross matches
                        for v in uname_hist_vals & target_display_values_set:
                            alt_data['cross_matches'].add(f"[Their Username History = Target's Display] {v}")
                        for v in dname_hist_vals & target_username_values_set:
                            alt_data['cross_matches'].add(f"[Their Display History = Target's Username] {v}")
        
                        name_matches_count = len([p for p in potential_alts.values() if len(p['matching_patterns']) > 0])
                        skip_live_checks = name_matches_count > 40  # raise threshold now that we pruned patterns
        
                        # Enhanced guild matching - always check all available sources
                        def check_alt_guild_matches():
                            # 1. Check historical guild data from alt's database record
                            if 'detected_guilds' in potential_alt:
                                for guild in potential_alt['detected_guilds']:
                                    guild_id = guild.get('id')
                                    if guild_id and guild_id in guild_history:
                                        alt_data['mutual_guilds'].add(guild_id)
                            
                            # 2. Check if alt is currently in any guilds with target user
                            for guild in self.bot.guilds:
                                if guild.id in guild_history and guild.get_member(alt_id):
                                    alt_data['mutual_guilds'].add(guild.id)
                        
                        # Pre-enrichment: no external API calls here. Use DB and bot guild presence only.
                        check_alt_guild_matches()
                        alt_data['mutual_friends'] = 0
                          # Calculate uniqueness score for this alt's patterns
                        uniqueness_score = 0
                        total_patterns = 0
                        unique_patterns = []
                        common_patterns = []
                        
                        # NEW: Track pattern metrics for enhanced uniqueness scoring
                        pattern_metrics = {
                            'avg_length': 0,
                            'total_length': 0,
                            'exact_matches': 0,
                            'cross_platform_matches': 0,
                            'special_chars': 0,
                            'historical_matches': 0
                        }
                        
                        # Analyze all matching patterns with enhanced metrics
                        for pattern_set in [alt_data['username_matches'], alt_data['displayname_matches'], alt_data['cross_matches']]:
                            for pattern in pattern_set:
                                pattern_lower = pattern.lower()
                                if pattern_lower in pattern_frequency:
                                    frequency = pattern_frequency[pattern_lower]
                                    total_patterns += 1
                                    
                                    # Store pattern length for later analysis
                                    pattern_clean = pattern_lower.split("]")[-1].strip() if "]" in pattern_lower else pattern_lower
                                    pattern_metrics['total_length'] += len(pattern_clean)
                                    
                                    # Count special characters that make names more unique
                                    special_chars = sum(1 for char in pattern_clean if not char.isalnum() and char != ' ')
                                    pattern_metrics['special_chars'] += special_chars
                                    
                                    # Track if this is an exact match vs partial match
                                    if "[current username]" in pattern_lower or "[current display]" in pattern_lower:
                                        pattern_metrics['exact_matches'] += 1
                                    
                                    # Track cross-platform consistency - highest uniqueness signal
                                    if pattern in alt_data['cross_matches']:
                                        pattern_metrics['cross_platform_matches'] += 1
                                    
                                    # Track historical consistency - patterns that appear in history
                                    if "[history" in pattern_lower:
                                        pattern_metrics['historical_matches'] += 1
                                    
                                    # Base frequency calculation - rarer patterns get higher scores
                                    if frequency <= 3:  # Very unique (few matches)
                                        pattern_score = 1.0  # Full score for unique patterns
                                        unique_patterns.append(pattern)
                                    elif frequency <= 5:  # Somewhat unique
                                        pattern_score = 0.7
                                    elif frequency <= 10:  # Moderately common
                                        pattern_score = 0.3
                                    else:  # Very common
                                        pattern_score = 0.1  # Heavy penalty for common patterns
                                        common_patterns.append(pattern)
                                    
                                    # NEW: Enhance score based on pattern length
                                    # Longer patterns are more likely to be unique identifiers
                                    if len(pattern_clean) >= 8:  # Long patterns
                                        pattern_score *= 1.3
                                    elif len(pattern_clean) >= 5:  # Medium patterns
                                        pattern_score *= 1.1
                                    elif len(pattern_clean) <= 3:  # Very short patterns
                                        pattern_score *= 0.8
                                    
                                    # NEW: Consider special characters - more distinctive
                                    if special_chars > 0:
                                        pattern_score *= (1 + (special_chars * 0.1))  # Boost for special chars
                                    
                                    uniqueness_score += pattern_score
                        
                        # Calculate average pattern length if we have patterns
                        if total_patterns > 0:
                            pattern_metrics['avg_length'] = pattern_metrics['total_length'] / total_patterns
                            
                            # Store both the raw uniqueness score and normalized score
                            base_uniqueness = uniqueness_score / total_patterns
                            
                            # NEW: Apply additional uniqueness modifiers based on collected metrics
                            uniqueness_modifiers = 1.0
                            
                            # Boost uniqueness if we have cross-platform matches (strongest signal)
                            if pattern_metrics['cross_platform_matches'] > 0:
                                cross_platform_boost = min(0.3, pattern_metrics['cross_platform_matches'] * 0.1)
                                uniqueness_modifiers += cross_platform_boost
                            
                            # Boost uniqueness for exact matches vs partial matches
                            if pattern_metrics['exact_matches'] > 0:
                                exact_match_boost = min(0.2, pattern_metrics['exact_matches'] * 0.05)
                                uniqueness_modifiers += exact_match_boost
                                
                            # Boost uniqueness for historical consistency
                            if pattern_metrics['historical_matches'] > 0:
                                historical_boost = min(0.15, pattern_metrics['historical_matches'] * 0.05)
                                uniqueness_modifiers += historical_boost
                            
                            # Boost uniqueness if average pattern length is significant
                            if pattern_metrics['avg_length'] > 6:
                                length_boost = min(0.15, (pattern_metrics['avg_length'] - 6) * 0.03)
                                uniqueness_modifiers += length_boost
                                
                            # Apply all modifiers to base uniqueness
                            alt_data['pattern_uniqueness'] = min(1.0, base_uniqueness * uniqueness_modifiers)
                            
                            # Store metrics for reporting
                            alt_data['pattern_metrics'] = pattern_metrics
                            alt_data['unique_patterns'] = unique_patterns
                            alt_data['common_patterns'] = common_patterns
                        else:
                            alt_data['pattern_uniqueness'] = 0
                            alt_data['pattern_metrics'] = pattern_metrics
                      
                        # Phase 2 raw scoring (pre-enrichment logistic later)
                        has_name_matches = len(alt_data['username_matches']) > 0 or len(alt_data['displayname_matches']) > 0
                        has_cross_matches = len(alt_data['cross_matches']) > 0
                        # Raw name score = sum idf for each distinct match type * weights
                        distinct_patterns = set(list(alt_data['username_matches']) + list(alt_data['displayname_matches']) + list(alt_data['cross_matches']))
                        # Apply diminishing returns across many pattern matches to reduce false positives
                        # Sort patterns by informativeness (IDF desc), then decay contributions
                        raw_name_score = 0.0
                        if distinct_patterns:
                            sorted_pats = sorted(
                                distinct_patterns,
                                key=lambda p: pattern_idf.get(p, 1.0),
                                reverse=True
                            )
                            # Decay schedule: full -> reduced weight as count grows
                            decay = [1.0, 0.85, 0.7, 0.55, 0.4, 0.3]
                            default_decay = 0.2
                            for idx, pat in enumerate(sorted_pats):
                                idf = pattern_idf.get(pat, 1.0)
                                base = 6.0 * idf
                                if pat in alt_data['cross_matches']:
                                    base += 4.0 * idf
                                # Heavier weight for username-based evidence; lighter for display-only
                                if pat in alt_data['username_matches']:
                                    base *= 1.25
                                elif pat in alt_data['displayname_matches']:
                                    base *= 0.6
                                factor = decay[idx] if idx < len(decay) else default_decay
                                raw_name_score += base * factor
                        # Social raw score placeholders (added later after enrichment)
                        raw_social_score = 0.0
                        mutual_friend_count = 0  # target friend overlap deferred; initialize
                        if not skip_live_checks:
                            # preliminary friend weight low here (full after mutual fetch)
                            raw_social_score += mutual_friend_count * 1.5
                        # Mutual guild provisional weight (only from historical quick check so far)
                        raw_social_score += len(alt_data['mutual_guilds']) * 2.5
                        alt_data['__raw_name_score'] = raw_name_score
                        alt_data['__raw_social_score'] = raw_social_score
                        alt_data['confidence'] = 0  # placeholder until final logistic
                        
                        # DATABASE-BASED DATE ANALYSIS (lightweight)
                        alt_name_timestamps = {}
                        alt_display_timestamps = {}
                        
                        # Collect timestamps from alt's database history
                        for history in potential_alt.get('username_history', []):
                            if isinstance(history, dict):
                                value = history.get('value')
                                changed_at = history.get('changed_at')
                                if value and changed_at:
                                    alt_name_timestamps[value.lower()] = changed_at
                        
                        for history in potential_alt.get('displayname_history', []):
                            if isinstance(history, dict):
                                value = history.get('value')
                                changed_at = history.get('changed_at')
                                if value and changed_at:
                                    alt_display_timestamps[value.lower()] = changed_at
                        
                        # Compare timing of matching name patterns using only database timestamps
                        timing_correlations = 0
                        total_timing_checks = 0
                        
                        for pattern in alt_data['matching_patterns']:
                            pattern_clean = pattern.lower().split(']')[-1].strip() if ']' in pattern else pattern.lower()
                            
                            # Find timestamps for this pattern in both users (database only)
                            target_timestamp = target_user_name_timestamps.get(pattern_clean) or target_user_display_timestamps.get(pattern_clean)
                            alt_timestamp = alt_name_timestamps.get(pattern_clean) or alt_display_timestamps.get(pattern_clean)
                            
                            if target_timestamp and alt_timestamp:
                                total_timing_checks += 1
                                try:
                                    # Convert to datetime if needed
                                    if isinstance(target_timestamp, str):
                                        target_timestamp = datetime.fromisoformat(target_timestamp.replace('Z', '+00:00'))
                                    if isinstance(alt_timestamp, str):
                                        alt_timestamp = datetime.fromisoformat(alt_timestamp.replace('Z', '+00:00'))
                                    
                                    time_diff = abs((target_timestamp - alt_timestamp).total_seconds())
                                    
                                    # Very close timing (within 1 hour) - highly suspicious
                                    if time_diff <= 3600:
                                        timing_correlations += 3
                                    # Within 1 day - suspicious
                                    elif time_diff <= 86400:
                                        timing_correlations += 2
                                    # Within 1 week - slightly suspicious
                                    elif time_diff <= 604800:
                                        timing_correlations += 1
                                except Exception as e:
                                    logger.debug(f"Error comparing timestamps for pattern {pattern_clean}: {e}")
                        
                        date_confidence_bonus = 0
                        if total_timing_checks > 0:
                            alt_data['timestamp_correlation_score'] = timing_correlations
                            # Award bonus based on correlation strength
                            if timing_correlations >= total_timing_checks * 2:  # High correlation
                                date_confidence_bonus += 10
                            elif timing_correlations >= total_timing_checks:  # Medium correlation
                                date_confidence_bonus += 5
                            elif timing_correlations > 0:  # Some correlation
                                date_confidence_bonus += 2
                        
                        alt_data['__raw_date_score'] = date_confidence_bonus * 2.0  # scale into raw domain
        
                        # EARLY FILTER: store preliminary score before heavy date enrichment
                    # Phase 2 early filtering: compute preliminary raw combined score
                    prelim = []
                    for k,v in potential_alts.items():
                        raw_total = v.get('__raw_name_score',0) + v.get('__raw_social_score',0) + v.get('__raw_date_score',0)
                        if raw_total > 5:  # minimal signal
                            prelim.append((k, raw_total))
                    # sort prelim by raw_total desc
                    prelim.sort(key=lambda x: x[1], reverse=True)
                    # limit to top 150 for enrichment
                    prelim = prelim[:150]
                    prelim_set = {k for k,_ in prelim}
                    filtered_alts = {k: potential_alts[k] for k in prelim_set}

                    # Adjust scaling of name scores after noise removal to keep distribution stable
                    if filtered_alts:
                        name_scores = [v.get('__raw_name_score',0) for v in filtered_alts.values() if v.get('__raw_name_score',0) > 0]
                        if len(name_scores) >= 3:
                            mean_name = sum(name_scores)/len(name_scores)
                            target_mean = 25.0
                            if 5 < mean_name < 80 and abs(mean_name - target_mean)/target_mean > 0.25:
                                scale_factor = target_mean / mean_name
                                # Clamp scaling to avoid over-correction
                                if scale_factor < 0.7:
                                    scale_factor = 0.7
                                elif scale_factor > 1.3:
                                    scale_factor = 1.3
                                for v in filtered_alts.values():
                                    v['__raw_name_score'] = v.get('__raw_name_score',0) * scale_factor
                                logger.debug(f"[AltDetect] Scaled name scores by {scale_factor:.2f} (mean {mean_name:.2f} -> {target_mean})")

                    # Enrichment stage with confidence gating (target mutual fetch delayed)
                    ENRICH_LIMIT = 40
                    MIN_ENRICH_SCORE = 12  # lower raw threshold; final gate by projected confidence
                    TARGET_CONFIDENCE_GATE = 85  # 85% required for enrichment
                    BORDERLINE_GATE = None  # disabled
                    enrichment_targets = []

                    # Compute provisional adaptive shift (median of prelim raw scores)
                    prelim_raw_values = [score for _k, score in prelim]
                    if prelim_raw_values:
                        prelim_raw_values.sort()
                        prelim_median = prelim_raw_values[len(prelim_raw_values)//2]
                    else:
                        prelim_median = 40.0

                    def project_conf(raw_value: float):
                        # Use same logistic function with adaptive median shift to mirror final mapping
                        return self._logistic_confidence(raw_value, shift=prelim_median, scale=12)

                    for k, score in prelim:
                        if len(enrichment_targets) >= ENRICH_LIMIT:
                            break
                        if score < MIN_ENRICH_SCORE:
                            continue
                        conf_est = project_conf(score)
                        if conf_est >= TARGET_CONFIDENCE_GATE:
                            enrichment_targets.append(k)
                    external_calls = 0
                    if not enrichment_targets:
                        logger.debug(f"[AltDetect] No candidates >=85% projected confidence for user {user_id}; skipping mutual enrichment")
                    target_mutual_data = None  # lazily fetched only if we enrich at least one alt
                    for alt_id in enrichment_targets:
                        alt_data = filtered_alts.get(alt_id)
                        if not alt_data:
                            continue
                        # safety net on total external profile fetches for this analysis call
                        if external_calls >= ENRICH_LIMIT:
                            break
                        async with self._alt_semaphore:
                            try:
                                # Fetch target mutual data once (adds its guilds to guild_history for richer intersection)
                                if target_mutual_data is None:
                                    target_mutual_data = await self.get_mutual_data(user_id)
                                    for g in target_mutual_data.get('guilds', []):
                                        gid = g.get('id')
                                        if gid:
                                            guild_history.add(gid)
                                alt_mutual_data = await self.get_mutual_data(alt_id)
                                external_calls += 1
                                friend_ids = [f.id for f in alt_mutual_data.get('friends', [])]
                                alt_data['mutual_friends'] = len(friend_ids)
                                guild_ids = {g['id'] for g in alt_mutual_data.get('guilds', [])}
                                shared = guild_ids & guild_history
                                alt_data['mutual_guilds'].update(shared)
                                alt_data['__raw_social_score'] = (
                                    (25 * (len(alt_data['mutual_guilds'])/(len(alt_data['mutual_guilds'])+3))) +
                                    (10 * (alt_data['mutual_friends']/(alt_data['mutual_friends']+2)))
                                )
                            except Exception as e:
                                logger.debug(f"Enrichment mutual fetch failed for {alt_id}: {e}")

                    # Final raw score aggregation and logistic mapping
                    # Accuracy enhanced feature extraction
                    if self.use_alt_accuracy_enhanced and filtered_alts:
                        # Precompute target name pools & sequences from BOTH DB and Kilo (current + history)
                        target_name_pool = set()
                        target_name_sequence = []
                        seen_norm = set()
                        # DB current
                        if user_data:
                            cu = user_data.get('current_username')
                            if cu:
                                nm = self._unicode_normalize(cu.lower())
                                target_name_pool.add(nm)
                                if nm not in seen_norm:
                                    seen_norm.add(nm)
                                    target_name_sequence.append((nm, None))
                            cd = user_data.get('current_displayname')
                            if cd:
                                nm = self._unicode_normalize(cd.lower())
                                target_name_pool.add(nm)
                                if nm not in seen_norm:
                                    seen_norm.add(nm)
                                    target_name_sequence.append((nm, None))
                            # DB history
                            for h in sorted(user_data.get('username_history', []), key=lambda x: x.get('changed_at') or datetime.min):
                                val = h.get('value')
                                if val:
                                    norm = self._unicode_normalize(val.lower())
                                    target_name_pool.add(norm)
                                    if norm not in seen_norm:
                                        seen_norm.add(norm)
                                        target_name_sequence.append((norm, h.get('changed_at')))
                            for h in sorted(user_data.get('displayname_history', []), key=lambda x: x.get('changed_at') or datetime.min):
                                val = h.get('value')
                                if val:
                                    norm = self._unicode_normalize(val.lower())
                                    target_name_pool.add(norm)
                                    if norm not in seen_norm:
                                        seen_norm.add(norm)
                                        target_name_sequence.append((norm, h.get('changed_at')))
                        # Kilo history (if present)
                        if kilo_data and kilo_data.get('success'):
                            for name_entry in kilo_data.get('names', []) or []:
                                if isinstance(name_entry, dict):
                                    name = name_entry.get('name')
                                    ts = name_entry.get('timestamp')
                                    if name:
                                        norm = self._unicode_normalize(name.lower())
                                        target_name_pool.add(norm)
                                        if norm not in seen_norm:
                                            seen_norm.add(norm)
                                            target_name_sequence.append((norm, ts))
                            for display_entry in kilo_data.get('displays', []) or []:
                                if isinstance(display_entry, dict):
                                    name = display_entry.get('name') or display_entry.get('display') or display_entry.get('value') or display_entry.get('display_name')
                                    ts = display_entry.get('timestamp')
                                    if name:
                                        norm = self._unicode_normalize(name.lower())
                                        target_name_pool.add(norm)
                                        if norm not in seen_norm:
                                            seen_norm.add(norm)
                                            target_name_sequence.append((norm, ts))

                        # Cap target name pool by recency (most recent 80)
                        def _to_dt(ts):
                            try:
                                if not ts:
                                    return datetime.min
                                if isinstance(ts, str):
                                    return datetime.fromisoformat(ts.replace('Z','+00:00'))
                                return ts
                            except Exception:
                                return datetime.min
                        # Build ordered list using available timestamps from target_name_sequence
                        if target_name_sequence:
                            ordered = sorted(target_name_sequence, key=lambda x: _to_dt(x[1]), reverse=True)
                            limited = [n for n,_ in ordered]
                        else:
                            limited = list(target_name_pool)
                        # Dedup while preserving order
                        capped_target_names = []
                        seen = set()
                        for n in limited:
                            if n not in seen:
                                seen.add(n)
                                capped_target_names.append(n)
                            if len(capped_target_names) >= 80:
                                break

                        target_tokens = set()
                        for nm in capped_target_names:
                            target_tokens.update(self._tokenize_name(nm))
                        target_ngrams = set()
                        for nm in capped_target_names:
                            target_ngrams.update(self._char_ngrams(nm, 2))

                        weights = self.alt_accuracy_weights

                        for alt_id, alt_data in filtered_alts.items():
                            raw_total = alt_data.get('__raw_name_score',0) + alt_data.get('__raw_social_score',0) + alt_data.get('__raw_date_score',0)
                            # Prepare canonical pattern sets for signal boost
                            uname_set = {self._unicode_normalize(x.lower().split(']')[-1].strip()) for x in alt_data.get('username_matches', []) if isinstance(x,str)}
                            dname_set = {self._unicode_normalize(x.lower().split(']')[-1].strip()) for x in alt_data.get('displayname_matches', []) if isinstance(x,str)}
                            cross_set = {self._unicode_normalize(x.lower().split(']')[-1].strip()) for x in alt_data.get('cross_matches', []) if isinstance(x,str)}
                            # Multi-source pattern detection (appears in >=2 categories)
                            all_union = uname_set | dname_set | cross_set
                            multi_source_patterns = []
                            rare_multi_source_patterns = []
                            for p in all_union:
                                membership = (p in uname_set) + (p in dname_set) + (p in cross_set)
                                if membership >= 2:
                                    multi_source_patterns.append(p)
                                    if pattern_idf.get(p,0) >= 1.5:
                                        rare_multi_source_patterns.append(p)
                            signal_boost = 0.0
                            if rare_multi_source_patterns:
                                # 7 points per rare multi-source pattern capped at 21
                                signal_boost = min(len(rare_multi_source_patterns) * 7.0, 21.0)
                                raw_total += signal_boost
                            alt_data['__signal_boost'] = signal_boost
                            # Near-match scoring
                            current_alt_names = set()
                            ca = alt_data.get('current_username')
                            if ca:
                                current_alt_names.add(self._unicode_normalize(ca.lower()))
                            cd = alt_data.get('current_displayname')
                            if cd:
                                current_alt_names.add(self._unicode_normalize(cd.lower()))
                            # Include a capped set of alt historical names from potential_alts
                            alt_hist_names = set()
                            try:
                                for h in potential_alts[alt_id].get('username_history', [])[:30]:
                                    v = h.get('value') if isinstance(h, dict) else None
                                    if v:
                                        alt_hist_names.add(self._unicode_normalize(v.lower()))
                                for h in potential_alts[alt_id].get('displayname_history', [])[:30]:
                                    v = h.get('value') if isinstance(h, dict) else None
                                    if v:
                                        alt_hist_names.add(self._unicode_normalize(v.lower()))
                            except Exception:
                                pass
                            # Merge with current names; cap total names to avoid quadratic blowup
                            all_alt_names = list(current_alt_names | alt_hist_names)
                            if len(all_alt_names) > 60:
                                all_alt_names = all_alt_names[:60]
                            alt_tokens = set()
                            for nm in all_alt_names:
                                alt_tokens.update(self._tokenize_name(nm))
                            alt_ngrams = set()
                            for nm in all_alt_names:
                                alt_ngrams.update(self._char_ngrams(nm, 2))

                            # Near-match scoring
                            near_bonus = 0.0
                            for tn in target_name_pool:
                                for an in all_alt_names:
                                    dist = self._bounded_levenshtein(tn, an, 2)
                                    if dist == 1:
                                        near_bonus += weights['near_exact']
                                    elif dist == 2:
                                        # secondary JW check
                                        jw = self._jaro_winkler(tn, an)
                                        if jw >= 0.92:
                                            near_bonus += weights['near_edit2']
                            raw_total += near_bonus
                            alt_data['__near_bonus'] = near_bonus

                            # Token Jaccard
                            token_jaccard = 0.0
                            if target_tokens and alt_tokens:
                                inter = len(target_tokens & alt_tokens)
                                union = len(target_tokens | alt_tokens)
                                if union:
                                    token_jaccard = inter / union
                            raw_total += min(token_jaccard, 0.7) * weights['token_jaccard']

                            # N-gram overlap (cap contribution)
                            ngram_overlap = len(target_ngrams & alt_ngrams)
                            raw_total += min(ngram_overlap * weights['ngram_unit'], 15.0)

                            # Sequence order consistency
                            order_score = 0.0
                            if target_name_sequence:
                                # Collect overlapping names present in alt histories
                                alt_hist_names = set()
                                for h in potential_alts[alt_id].get('username_history', []):
                                    v = h.get('value');
                                    if v:
                                        alt_hist_names.add(self._unicode_normalize(v.lower()))
                                for h in potential_alts[alt_id].get('displayname_history', []):
                                    v = h.get('value');
                                    if v:
                                        alt_hist_names.add(self._unicode_normalize(v.lower()))
                                seq = [n for n,_ in target_name_sequence if n in alt_hist_names]
                                if len(seq) >= 2:
                                    # Approx order consistency = unique seq length / overlaps
                                    overlaps = len(seq)
                                    unique_seq = len(set(seq))
                                    order_consistency = unique_seq / overlaps if overlaps else 0
                                    order_score = order_consistency * weights['order_consistency']
                            raw_total += order_score

                            # Burst similarity (names changed within 24h window)
                            burst_score = 0.0
                            if len(target_name_sequence) >= 2:
                                # map alt name -> earliest timestamp
                                alt_name_times = {}
                                for h in potential_alts[alt_id].get('username_history', []):
                                    v = h.get('value'); t = h.get('changed_at')
                                    if v and t:
                                        alt_name_times.setdefault(self._unicode_normalize(v.lower()), t)
                                for h in potential_alts[alt_id].get('displayname_history', []):
                                    v = h.get('value'); t = h.get('changed_at')
                                    if v and t:
                                        alt_name_times.setdefault(self._unicode_normalize(v.lower()), t)
                                burst_matches = 0
                                for nm, tts in target_name_sequence:
                                    if nm in alt_name_times:
                                        at = alt_name_times[nm]
                                        try:
                                            if isinstance(tts, str):
                                                tts_dt = datetime.fromisoformat(tts.replace('Z','+00:00'))
                                            else:
                                                tts_dt = tts
                                            if isinstance(at, str):
                                                at_dt = datetime.fromisoformat(at.replace('Z','+00:00'))
                                            else:
                                                at_dt = at
                                            if tts_dt and at_dt and abs((tts_dt - at_dt).total_seconds()) <= 86400:
                                                burst_matches += 1
                                        except Exception:
                                            pass
                                if burst_matches:
                                    burst_score = min(burst_matches * (weights['burst'] / 3), weights['burst'])
                            raw_total += burst_score

                            # Base numeric pattern reuse
                            numeric_bonus = 0.0
                            def core_numeric_split(nm: str):
                                m = re.match(r'^([a-zA-Z]+)(\d+)$', nm)
                                return m.groups() if m else None
                            target_cores = {}
                            for nm in target_name_pool:
                                res = core_numeric_split(nm)
                                if res:
                                    target_cores.setdefault(res[0], set()).add(res[1])
                            alt_cores = {}
                            for nm in all_alt_names:
                                res = core_numeric_split(nm)
                                if res:
                                    alt_cores.setdefault(res[0], set()).add(res[1])
                            for core, nums in target_cores.items():
                                if core in alt_cores and (len(nums) + len(alt_cores[core])) >= 2:
                                    numeric_bonus += weights['base_numeric_bonus']
                            raw_total += numeric_bonus

                            # Affix-aware core matching
                            # Detect cases like target "viper" → alt "viperx", "xviper", "viper_dev" etc.
                            def core_affixes(nm: str):
                                # Extract a plausible core: longest alphabetic run of length >=3
                                parts = re.findall(r'[A-Za-z]{3,}', nm)
                                if not parts:
                                    return []
                                # Return unique parts, favor longer substrings
                                parts = sorted(set(parts), key=len, reverse=True)
                                return parts[:3]

                            affix_bonus = 0.0
                            target_cores_list = []
                            for nm in target_name_pool:
                                target_cores_list.extend(core_affixes(nm))
                            alt_cores_list = []
                            for nm in all_alt_names:
                                alt_cores_list.extend(core_affixes(nm))
                            target_cores_set = set(target_cores_list)
                            alt_cores_set = set(alt_cores_list)
                            # Exact core overlap
                            core_overlap = target_cores_set & alt_cores_set
                            if core_overlap:
                                affix_bonus += min(len(core_overlap) * weights['affix_core_bonus'], weights['affix_core_bonus'] * 2)
                            # Substring containment to catch small affixes/suffixes
                            # Evaluate limited pairs to avoid blowup
                            checked = 0
                            for tc in list(target_cores_set)[:6]:
                                for ac in list(alt_cores_set)[:6]:
                                    if tc == ac:
                                        continue
                                    if tc in ac or ac in tc:
                                        affix_bonus += weights['affix_substring_bonus']
                                    checked += 1
                                    if checked >= 24:
                                        break
                                if checked >= 24:
                                    break
                            # Cap total affix bonus to stay modest
                            affix_bonus = min(affix_bonus, 18.0)
                            raw_total += affix_bonus

                            # Penalties
                            penalties = 0.0
                            # Common short penalty
                            for nm in current_alt_names & target_name_pool:
                                if len(nm) <= 3:
                                    penalties += -weights['common_short_penalty']  # weights already negative
                            # Overgeneralization: only ngram overlap produced match
                            if (not alt_data['username_matches'] and not alt_data['displayname_matches'] and not alt_data['cross_matches']) and ngram_overlap > 0:
                                penalties += weights['overgeneral_penalty']
                            # Flood penalty: many historical names but low overlap
                            hist_name_count = 0
                            overlap_count = 0
                            for h in potential_alts[alt_id].get('username_history', []):
                                v = h.get('value');
                                if v:
                                    hist_name_count += 1
                                    if self._unicode_normalize(v.lower()) in target_name_pool:
                                        overlap_count += 1
                            if hist_name_count > 8 and overlap_count <= 1:
                                penalties += weights['flood_penalty']
                            raw_total += penalties

                            # Store detail for optional debugging
                            alt_data['__raw_total_accuracy'] = raw_total
                            # Adjust logistic shift adaptively will happen later
                        # Determine adaptive shift based on distribution
                        raw_values = [v.get('__raw_total_accuracy', v.get('__raw_name_score',0)+v.get('__raw_social_score',0)+v.get('__raw_date_score',0)) for v in filtered_alts.values()]
                        if raw_values:
                            raw_values.sort()
                            median_raw = raw_values[len(raw_values)//2]
                        else:
                            median_raw = 40.0
                        # final mapping
                        for alt_id, alt_data in filtered_alts.items():
                            # Cleanup labels (deduplicate canonical forms)
                            def _dedup_list(lst):
                                seen = set(); cleaned = []
                                for item in lst or []:
                                    if not isinstance(item,str):
                                        continue
                                    canon = self._unicode_normalize(item.lower().split(']')[-1].strip())
                                    if canon in seen:
                                        continue
                                    seen.add(canon)
                                    cleaned.append(item)
                                return cleaned
                            # Dedup unlabeled match lists by canonical name
                            for key in ['username_matches','displayname_matches','cross_matches']:
                                if key in alt_data:
                                    alt_data[key] = _dedup_list(alt_data.get(key, []))
                            # Preserve labels in matching_patterns; dedup exact strings only
                            if 'matching_patterns' in alt_data:
                                seen_full = set(); cleaned_full = []
                                for item in alt_data.get('matching_patterns', []) or []:
                                    if isinstance(item, str) and item not in seen_full:
                                        seen_full.add(item)
                                        cleaned_full.append(item)
                                alt_data['matching_patterns'] = cleaned_full

                            # By-source counts and one-sample per category
                            def parse_label(item: str):
                                # Expect format like "[Current Username] name"; fallback to bucket by list
                                if not isinstance(item, str):
                                    return None, None
                                base = item
                                if ']' in item and item.startswith('['):
                                    label = item[1:item.find(']')].strip().lower()
                                    name = item[item.find(']')+1:].strip()
                                else:
                                    label = None
                                    name = item.strip()
                                return label, name

                            by_src = {
                                'CU': {'count': 0, 'samples': []},
                                'CD': {'count': 0, 'samples': []},
                                'UH': {'count': 0, 'samples': []},
                                'DH': {'count': 0, 'samples': []},
                                'cross': {'count': 0, 'samples': []},
                            }
                            # Derive counts from labeled matching_patterns
                            for it in alt_data.get('matching_patterns', []) or []:
                                label, name = parse_label(it)
                                if not name:
                                    continue
                                key = None
                                if label == 'current username':
                                    key = 'CU'
                                elif label == 'current display' or label == 'current displayname':
                                    key = 'CD'
                                elif label and label.startswith('username history'):
                                    key = 'UH'
                                elif label and label.startswith('display history'):
                                    key = 'DH'
                                if key:
                                    by_src[key]['count'] += 1
                                    if len(by_src[key]['samples']) < 2:
                                        by_src[key]['samples'].append(name)
                            # Cross matches
                            for it in alt_data.get('cross_matches', []) or []:
                                _label, name = parse_label(it)
                                by_src['cross']['count'] += 1
                                if len(by_src['cross']['samples']) < 2 and name:
                                    by_src['cross']['samples'].append(name)
                            alt_data['by_source_counts'] = by_src
                            base_raw = alt_data.get('__raw_total_accuracy', alt_data.get('__raw_name_score',0)+alt_data.get('__raw_social_score',0)+alt_data.get('__raw_date_score',0))
                            # Replace confidence floor with a raw-point bonus to preserve ordering
                            rare_multi_source = alt_data.get('__signal_boost',0) >= 7 and alt_data.get('__near_bonus',0) > 0
                            extra_raw = 4.0 if rare_multi_source else 0.0
                            alt_data['confidence'] = self._logistic_confidence(base_raw + extra_raw, shift=median_raw, scale=12)
                    else:
                        for alt_id, alt_data in filtered_alts.items():
                            raw_total = alt_data.get('__raw_name_score',0) + alt_data.get('__raw_social_score',0) + alt_data.get('__raw_date_score',0)
                            alt_data['confidence'] = self._logistic_confidence(raw_total, shift=40, scale=12)
                    
                    # Light timing bonus (DB-only) before expensive analysis
                    # Small additive boost for name timing correlation when there is some corroboration
                    for _aid, _adata in filtered_alts.items():
                        try:
                            tscore = int(_adata.get('timestamp_correlation_score') or 0)
                            if tscore > 0:
                                # Require some social or non-display evidence to avoid boosting weak clones
                                has_social = (len(_adata.get('mutual_guilds') or []) if not isinstance(_adata.get('mutual_guilds'), set) else len(_adata.get('mutual_guilds'))) > 0 \
                                              or int(_adata.get('mutual_friends') or 0) > 0
                                has_non_display = bool(_adata.get('username_matches')) or bool(_adata.get('cross_matches'))
                                if has_social or has_non_display:
                                    if tscore >= 6:
                                        bonus = 4
                                    elif tscore >= 3:
                                        bonus = 2
                                    else:
                                        bonus = 1
                                    _adata['confidence'] = min(100, _adata.get('confidence', 0) + bonus)
                        except Exception:
                            pass

                    # EXPENSIVE DATE-BASED ANALYSIS - Only for high-confidence (or moderate+social) matches
                    date_targets = {}
                    for k, v in filtered_alts.items():
                        try:
                            conf = int(v.get('confidence', 0))
                            mgc = len(v.get('mutual_guilds') or []) if not isinstance(v.get('mutual_guilds'), set) else len(v.get('mutual_guilds'))
                            mfc = int(v.get('mutual_friends') or 0)
                            if conf >= 50 or ((mgc > 0 or mfc > 0) and conf >= 35):
                                date_targets[k] = v
                        except Exception:
                            continue
                    if date_targets and target_user_created_at:
                        logger.debug(f"Performing expensive date analysis on {len(date_targets)} eligible candidates")
                        
                        for alt_id, alt_data in date_targets.items():
                            try:
                                # Get potential alt's account creation date
                                alt_user_obj = await self.bot.GetUser(alt_id)
                                if alt_user_obj and alt_user_obj.created_at:
                                    time_diff = abs((target_user_created_at - alt_user_obj.created_at).total_seconds())
                                    
                                    creation_date_bonus = 0
                                    # Very close creation dates (within 1 day) - highly suspicious
                                    if time_diff <= 86400:  # 1 day
                                        alt_data['creation_date_score'] = 15
                                        creation_date_bonus = 15
                                    # Within 1 week - moderately suspicious
                                    elif time_diff <= 604800:  # 1 week
                                        alt_data['creation_date_score'] = 8
                                        creation_date_bonus = 8
                                    # Within 1 month - slightly suspicious
                                    elif time_diff <= 2592000:  # 1 month
                                        alt_data['creation_date_score'] = 3
                                        creation_date_bonus = 3
                                    
                                    # Apply creation date bonus and recalculate confidence
                                    if creation_date_bonus > 0:
                                        alt_data['confidence'] = min(alt_data['confidence'] + creation_date_bonus, 100)
                                        
                            except Exception as e:
                                logger.debug(f"Could not fetch alt user creation date for {alt_id}: {e}")
                                
                        # Enhanced timestamp analysis with Kilo data (only for very high confidence)
                        very_high_confidence_alts = {k: v for k, v in date_targets.items() if v['confidence'] >= 70}
                        if very_high_confidence_alts:  # Even more restrictive for Kilo API calls
                            logger.debug(f"Performing enhanced timestamp analysis with Kilo data on {len(very_high_confidence_alts)} very high-confidence alts (>=70%)")
                            
                            for alt_id, alt_data in very_high_confidence_alts.items():
                                try:
                                    # Get Kilo data for alt for more complete timestamp coverage
                                    alt_kilo_data = await self.fetch_kilo_data(alt_id)
                                    if alt_kilo_data and alt_kilo_data.get("success"):
                                        # Enhanced timestamp tracking with Kilo data
                                        kilo_name_timestamps = {}
                                        kilo_display_timestamps = {}
                                        
                                        # Add Kilo timestamps for alt
                                        for name_entry in alt_kilo_data.get('names', []):
                                            if isinstance(name_entry, dict):
                                                name = name_entry.get('name')
                                                timestamp = name_entry.get('timestamp')
                                                if name and timestamp:
                                                    kilo_name_timestamps[name.lower()] = timestamp
                                        
                                        for display_entry in alt_kilo_data.get('displays', []):
                                            if isinstance(display_entry, dict):
                                                display = display_entry.get('display')
                                                timestamp = display_entry.get('timestamp')
                                                if display and timestamp:
                                                    kilo_display_timestamps[display.lower()] = timestamp
                                        
                                        # Re-analyze timing correlations with enhanced data
                                        enhanced_timing_correlations = 0
                                        enhanced_timing_checks = 0
                                        
                                        for pattern in alt_data['matching_patterns']:
                                            pattern_clean = pattern.lower().split(']')[-1].strip() if ']' in pattern else pattern.lower()
                                            
                                            # Check both database and Kilo timestamps
                                            target_timestamp = target_user_name_timestamps.get(pattern_clean) or target_user_display_timestamps.get(pattern_clean)
                                            alt_timestamp = kilo_name_timestamps.get(pattern_clean) or kilo_display_timestamps.get(pattern_clean)
                                            
                                            if target_timestamp and alt_timestamp:
                                                enhanced_timing_checks += 1
                                                try:
                                                    # Convert to datetime if needed
                                                    if isinstance(target_timestamp, str):
                                                        target_timestamp = datetime.fromisoformat(target_timestamp.replace('Z', '+00:00'))
                                                    if isinstance(alt_timestamp, str):
                                                        alt_timestamp = datetime.fromisoformat(alt_timestamp.replace('Z', '+00:00'))
                                                    
                                                    time_diff = abs((target_timestamp - alt_timestamp).total_seconds())
                                                    
                                                    # Very close timing (within 1 hour) - highly suspicious
                                                    if time_diff <= 3600:
                                                        enhanced_timing_correlations += 3
                                                    # Within 1 day - suspicious
                                                    elif time_diff <= 86400:
                                                        enhanced_timing_correlations += 2
                                                    # Within 1 week - slightly suspicious
                                                    elif time_diff <= 604800:
                                                        enhanced_timing_correlations += 1
                                                except Exception as e:
                                                    logger.debug(f"Error comparing enhanced timestamps for pattern {pattern_clean}: {e}")
                                        
                                        # Apply enhanced timing bonus if it's better than database-only analysis
                                        if enhanced_timing_checks > 0:
                                            enhanced_correlation_score = enhanced_timing_correlations
                                            if enhanced_correlation_score > alt_data.get('timestamp_correlation_score', 0):
                                                # Update with enhanced correlation score
                                                old_score = alt_data.get('timestamp_correlation_score', 0)
                                                alt_data['timestamp_correlation_score'] = enhanced_correlation_score
                                                
                                                # Apply additional confidence bonus for improved correlation
                                                enhanced_bonus = 0
                                                if enhanced_correlation_score >= enhanced_timing_checks * 2:  # High correlation
                                                    enhanced_bonus = 5  # Additional boost on top of original
                                                elif enhanced_correlation_score >= enhanced_timing_checks:  # Medium correlation
                                                    enhanced_bonus = 3
                                                elif enhanced_correlation_score > old_score:  # Some improvement
                                                    enhanced_bonus = 1
                                                
                                                if enhanced_bonus > 0:
                                                    alt_data['confidence'] = min(alt_data['confidence'] + enhanced_bonus, 100)
                                                    
                                except Exception as e:
                                    logger.debug(f"Error in enhanced timestamp analysis for {alt_id}: {e}")
                    else:
                        logger.debug("Skipping expensive date analysis - no high-confidence alts (>=50%) or no target creation date")
                    
                    # Apply social-proof and display-only gating before final sort to reduce false positives
                    for _aid, _adata in filtered_alts.items():
                        try:
                            mg = len(_adata.get('mutual_guilds') or []) if not isinstance(_adata.get('mutual_guilds'), set) else len(_adata.get('mutual_guilds'))
                            mf = int(_adata.get('mutual_friends') or 0)
                            # Display-only evidence cap: without username/cross signals, common names should not be high
                            u_matches = list(_adata.get('username_matches') or [])
                            d_matches = list(_adata.get('displayname_matches') or [])
                            c_matches = list(_adata.get('cross_matches') or [])
                            display_only = (not u_matches) and (not c_matches) and bool(d_matches)
                            if display_only:
                                # If there is corroboration (social or strong timing/creation), skip display-only cap
                                strong_timing = int(_adata.get('timestamp_correlation_score') or 0) >= 3
                                strong_creation = int(_adata.get('creation_date_score') or 0) >= 8
                                if (mg > 0 or mf > 0 or strong_timing or strong_creation):
                                    pass
                                else:
                                    # Estimate dominant pattern frequency from labeled matching_patterns
                                    dom_freq = 0
                                    try:
                                        mp = _adata.get('matching_patterns') or []
                                        for it in mp:
                                            try:
                                                canon = it.lower().split(']')[-1].strip()
                                                dom_freq = max(dom_freq, int(pattern_frequency.get(canon, 0)))
                                            except Exception:
                                                continue
                                    except Exception:
                                        dom_freq = 0
                                    uniq = float(_adata.get('pattern_uniqueness', 0) or 0)
                                    # Tiered caps based on uniqueness and how common the dominant pattern is
                                    cap = 65
                                    if uniq < 0.2 or dom_freq >= 50:
                                        cap = 40
                                    elif uniq < 0.4 or dom_freq >= 25:
                                        cap = 55
                                    # Also, extremely weak evidence (<=1 distinct pattern) gets a harsher cap
                                    distinct_count = len(set((u_matches or []) + (d_matches or []) + (c_matches or [])))
                                    if distinct_count <= 1 and cap > 45:
                                        cap = 45
                                    if _adata.get('confidence', 0) > cap:
                                        _adata['confidence'] = cap
                            if mg == 0 and mf == 0:
                                # Soft-cap confidence when there is no social corroboration
                                distinct_count = len(set(list(_adata.get('username_matches', [])) + list(_adata.get('displayname_matches', [])) + list(_adata.get('cross_matches', []))))
                                uniqueness = float(_adata.get('pattern_uniqueness', 0) or 0)
                                hard_cap = 55 if (distinct_count <= 2 and uniqueness < 0.6) else 70
                                if _adata.get('confidence', 0) > hard_cap:
                                    _adata['confidence'] = hard_cap
                        except Exception:
                            pass

                    # Sort results by confidence, then social presence, then user id
                    sorted_alts = sorted(
                        filtered_alts.values(),
                        key=lambda x: (
                            -x.get('confidence', 0),
                            -(len(x.get('mutual_guilds') or []) if not isinstance(x.get('mutual_guilds'), set) else len(x.get('mutual_guilds'))),
                            -(x.get('mutual_friends') or 0),
                            x.get('user_id', 0)
                        )
                    )
        
                    # Get the count of total matches including low-confidence ones
                    total_unfiltered_matches = len(potential_alts)
                    total_filtered_matches = len(filtered_alts)
                    
                    # Include the filtered count in the matching_guilds data
                    matching_guilds['__filtered_info__'] = {
                        'total_unfiltered': total_unfiltered_matches,
                        'total_filtered': total_filtered_matches,
                        'removed_count': total_unfiltered_matches - total_filtered_matches
                    }
                    
                    # Pagination based on filtered results
                    total_matches = len(sorted_alts)
                    total_pages = max(1, (total_matches + items_per_page - 1) // items_per_page)
                    page = min(max(1, page), total_pages)
                    start_idx = (page - 1) * items_per_page
                    end_idx = start_idx + items_per_page
        
                    page_results = sorted_alts[start_idx:end_idx]
        
                    # Add pagination info
                    matching_guilds['__pagination__'] = {
                        'page': page,
                        'total_pages': total_pages,
                        'total_matches': total_matches,
                        'items_per_page': items_per_page
                    }
                    
                    # Add pattern frequency data for reporting
                    matching_guilds['__pattern_stats__'] = {
                        'total_patterns': len(pattern_frequency),
                        'pattern_frequency': pattern_frequency
                    }
        
                    if name_matches_count > 20:
                        matching_guilds['__info__'] = f"Limited API checks due to high number of name matches ({name_matches_count}). Using database history only. Total matches found. Mutual friends data skipped."
                    
                    # Add info about filtered matches
                    if total_unfiltered_matches > total_filtered_matches:
                        matching_guilds['__info_filtered__'] = f"Filtered out {total_unfiltered_matches - total_filtered_matches} matches with confidence below 30%"
                
                    return page_results, matching_guilds
                else:
                    return [], {
                        '__pagination__': {
                            'page': 1, 
                            'total_pages': 1,
                            'total_matches': 0,
                            'items_per_page': items_per_page
                        }
                    }
                
            except Exception as e:
                logger.error(f"Error analyzing alts: {e}", exc_info=True)
                return [], {
                    '__pagination__': {
                        'page': 1,
                        'total_pages': 1,
                        'total_matches': 0,
                        'items_per_page': items_per_page
                    }
                }
    
        # searchname command removed                                    "$gte": search_term_lower,
                                    "$lt": next_term_lower
                                }
                            })
                            query_conditions.append({
                                "current_username": {
                                    "$gte": search_term.capitalize(),
                                    "$lt": next_term_cap
                                }
                            })
                            
                            # Add range queries for username history as well
                            # This uses the same technique but for array elements
                            if len(search_term) >= 3:  # Only for longer terms to avoid too many results
                                query_conditions.append({
                                    "username_history": {
                                        "$elemMatch": {
                                            "value": {
                                                "$gte": search_term,
                                                "$lt": next_term
                                            }
                                        }
                                    }
                                })
                                query_conditions.append({
                                    "username_history": {
                                        "$elemMatch": {
                                            "value": {
                                                "$gte": search_term_lower,
                                                "$lt": next_term_lower
                                            }
                                        }
                                    }
                                })
                                query_conditions.append({
                                    "username_history": {
                                        "$elemMatch": {
                                            "value": {
                                                "$gte": search_term.capitalize(),
                                                "$lt": next_term_cap
                                            }
                                        }
                                    }
                                })
                        
                        if mode in ["display", "both"]:
                            query_conditions.append({
                                "current_displayname": {
                                    "$gte": search_term,
                                    "$lt": next_term
                                }
                            })
                            query_conditions.append({
                                "current_displayname": {
                                    "$gte": search_term_lower,
                                    "$lt": next_term_lower
                                }
                            })
                            query_conditions.append({
                                "current_displayname": {
                                    "$gte": search_term.capitalize(),
                                    "$lt": next_term_cap
                                }
                            })
                            
                            # Add range queries for displayname history as well
                            if len(search_term) >= 3:  # Only for longer terms to avoid too many results
                                query_conditions.append({
                                    "displayname_history": {
                                        "$elemMatch": {
                                            "value": {
                                                "$gte": search_term,
                                                "$lt": next_term
                                            }
                                        }
                                    }
                                })
                                query_conditions.append({
                                    "displayname_history": {
                                        "$elemMatch": {
                                            "value": {
                                                "$gte": search_term_lower,
                                                "$lt": next_term_lower
                                            }
                                        }
                                    }
                                })
                                query_conditions.append({
                                    "displayname_history": {
                                        "$elemMatch": {
                                            "value": {
                                                "$gte": search_term.capitalize(),
                                                "$lt": next_term_cap
                                            }
                                        }
                                    }
                                })
                    except Exception as e:
                        logger.warning(f"Error creating range query: {e}")
                        # Continue with just the exact matches
            
            if not query_conditions:
                query = {"_id": None}  # Match nothing
            else:
                query = {"$or": query_conditions}
            
            # Add guild_id filter if specified
            if guild_id is not None:
                guild_filter = {"detected_guilds": {"$elemMatch": {"id": guild_id}}}
                
                if "$or" in query:
                    # If we have OR conditions, wrap them with the guild filter
                    query = {
                        "$and": [
                            query,
                            guild_filter
                        ]
                    }
                else:
                    # If no OR conditions, just add guild filter
                    query.update(guild_filter)
            
            # --- Handle counting efficiently (with caching) ---
            # Use the search key to store/retrieve count information
            if search_key not in self.search_pages:
                self.search_pages[search_key] = {'total': 0, 'query': search_term}
                
                # Count documents only for the first request with this search term
                # Using the optimized count_documents method
                count_start = datetime.now()
                self.search_pages[search_key]['total'] = await self.bot.db.count_documents('users', query)
                count_end = datetime.now()
                count_time = (count_end - count_start).total_seconds()
                logger.debug(f"[SearchOptimization] Count for '{search_term}' took {count_time:.3f}s")
                
            # Get the cached total count
            total_count = self.search_pages[search_key]['total']
            
            # Calculate total pages for pagination display
            total_pages = max(1, (total_count + items_per_page - 1) // items_per_page)
            
            # Ensure page is within valid range
            page = max(1, min(page, total_pages))
            
            # Recalculate skip count if page has changed
            skip_count = (page - 1) * items_per_page
            
            # --- Perform the FAST ID fetch ---
            # Fetch ONLY the IDs for the current page using the fast query
            cursor = self.bot.db.db.users.find(
                query,
                {"_id": 1} # Projection: Only fetch the ID
            ).sort([("last_seen", -1)]).skip(skip_count).limit(items_per_page)
    
            page_ids = [doc["_id"] for doc in await cursor.to_list(length=items_per_page)]
    
            # --- Handle No Results Found on This Page ---
            if not page_ids:
                # If it's page 1, no results at all. Otherwise, it's just an empty page.
                message = "No users found matching current name prefix." if page == 1 else f"No more results found on page {page}."
                await ctx.send(quote_block(f"```ansi\n\u001b[1;31m{message}```"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
    
            # --- Stage 2: Fetch Full Data for ONLY Page IDs using optimized batch query ---
            # Use the new find_many method for better performance
            full_docs = await self.bot.db.find_many(
                'users',
                query={"_id": {"$in": page_ids}},
                projection={
                    "_id": 1,
                    "current_username": 1,
                    "current_displayname": 1,  # Make sure we include current displayname
                    "username_history": 1,
                    "displayname_history": 1,
                    "last_seen": 1  # Include for sorting consistency
                },
                sort=[("last_seen", -1)]
            )
    
            # Store fetched docs in a dict for easy reordering
            full_docs_dict = {doc["_id"]: doc for doc in full_docs}
    
            # Reorder results based on the original sort order from page_ids
            current_page_results = [full_docs_dict.get(id) for id in page_ids if id in full_docs_dict]
            
            # --- Formatting Results ---
            message_parts = [
                "```ansi\n" + \
                f"\u001b[1;33mSearch Results: \u001b[0m\u001b[1;37m{search_term}\u001b[0m\n" + \
                f"\u001b[0;36mMode: \u001b[0;37m{mode}\u001b[0m\n" + \
                f"\u001b[0;36mMatch: \u001b[0;37m{match_type}\u001b[0m\n"
            ]
            
            if guild_id is not None:
                message_parts.append(f"\u001b[0;36mGuild Filter: \u001b[0;37m{guild_id}\u001b[0m\n")
                
            message_parts.append(f"\u001b[0;36mFound: \u001b[0;37m{total_count} users\u001b[0m\n" + \
                "\u001b[30m" + "─" * 45 + "\u001b[0m\n")
    
            # Create a clean and valid search term for client-side filtering
            search_term_lower = search_term.lower() if search_term else ""
    
            for idx, user_data in enumerate(current_page_results, start=skip_count + 1):
                if not user_data: continue
    
                user_id = user_data['_id']
                current_username = user_data.get('current_username', 'Unknown')
                current_displayname = user_data.get('current_displayname', 'None')
                
                # Show current username and displayname (if available)
                display_info = f"\u001b[1;36m{idx}. {current_username}"
                if current_displayname and current_displayname != 'None':
                    display_info += f" (\u001b[0;32m{current_displayname}\u001b[1;36m)"
                display_info += f" \u001b[0;37m(ID: {user_id})\u001b[0m\n"
                message_parts.append(display_info)
                
                # When filtering by guild_id, clear cache to ensure fresh mutual data
                if guild_id is not None:
                    cache_key = f"mutual_{user_id}"
                    if cache_key in self.mutual_friends_cache:
                        del self.mutual_friends_cache[cache_key]
                
                # Fetch mutual data
                mutual_data = await self.get_mutual_data(user_id)
                
                # If we're filtering by guild_id, ensure that guild appears in mutual data
                # since we know both users are/were in that guild
                if guild_id is not None:
                    guild_found = any(g['id'] == guild_id for g in mutual_data['guilds'])
                    if not guild_found:
                        # Try to get guild name from bot's guild cache or database
                        guild_name = "Unknown Guild"
                        guild = self.bot.get_guild(guild_id)
                        if guild:
                            guild_name = guild.name
                            source = 'current'
                        else:
                            # Try to get guild name from database
                            source = 'historical'
                            try:
                                # Check if we have this guild in any user's detected_guilds
                                guild_data = await self.bot.db.db.users.find_one(
                                    {"detected_guilds.id": guild_id},
                                    {"detected_guilds.$": 1}
                                )
                                if guild_data and 'detected_guilds' in guild_data:
                                    guild_name = guild_data['detected_guilds'][0].get('name', 'Unknown Guild')
                            except Exception as e:
                                logger.debug(f"Could not fetch guild name for {guild_id}: {e}")
                        
                        # Add the filtered guild to mutual data
                        mutual_data['guilds'].append({
                            'id': guild_id,
                            'name': guild_name,
                            'source': source
                        })
                        mutual_data['total_guilds'] = len(mutual_data['guilds'])
                        
                        # Log for debugging
                        logger.debug(f"Added filtered guild {guild_id} ({guild_name}) to mutual data for user {user_id}")
                
                # Display mutual friends if any
                if mutual_data['friends']:
                    friend_count = len(mutual_data['friends'])
                    message_parts.append(f"\u001b[0;35mMutual Friends: \u001b[0;37m{friend_count}\u001b[0m\n")
                    
                    # Display up to 3 mutual friend names
                    if friend_count > 0:
                        friend_names = [f"{friend.name}" for friend in mutual_data['friends']]
                        if friend_count > 3:
                            # Show first 3 friends and indicate there are more
                            friend_display = f"{', '.join(friend_names[:3])}... and {friend_count - 3} more"
                            message_parts.append(f"  \u001b[0;90m{friend_display}\u001b[0m\n")
                        else:
                            # Show all friends if 3 or fewer
                            message_parts.append(f"  \u001b[0;90m{', '.join(friend_names)}\u001b[0m\n")
                
                # Display mutual guilds if any
                if mutual_data['guilds']:
                    # If filtering by guild_id, show all mutual guilds; otherwise filter by search term
                    if guild_id is not None:
                        # When filtering by guild_id, show all mutual guilds
                        matching_guilds = mutual_data['guilds']
                        guild_label = "Mutual Guilds"
                    else:
                        # Normal behavior: filter guilds that match the search term
                        matching_guilds = []
                        for guild in mutual_data['guilds']:
                            guild_name = guild['name'].lower()
                            if match_type == "exact":
                                # For exact matching, check if guild name exactly matches
                                if guild_name == search_term_lower:
                                    matching_guilds.append(guild)
                            else:
                                # For partial matching, check if guild name contains the search term
                                if search_term_lower in guild_name:
                                    matching_guilds.append(guild)
                        guild_label = "Matching Guilds"
                    
                    if matching_guilds:
                        guild_count = len(matching_guilds)
                        message_parts.append(f"\u001b[0;35m{guild_label}: \u001b[0;37m{guild_count}\u001b[0m\n")
                        
                        # Display up to 3 current guilds first
                        current_guilds = [g for g in matching_guilds if g['source'] == 'current']
                        if current_guilds:
                            guild_names = [f"{g['name']}" for g in current_guilds[:3]]
                            if len(current_guilds) > 3:
                                guild_display = f"{', '.join(guild_names)}... and {len(current_guilds) - 3} more current"
                            else:
                                guild_display = ', '.join(guild_names)
                            message_parts.append(f"  \u001b[0;92mCurrent: {guild_display}\u001b[0m\n")
                        
                        # Display up to 3 historical guilds
                        historical_guilds = [g for g in matching_guilds if g['source'] == 'historical']
                        if historical_guilds:
                            guild_names = [f"{g['name']}" for g in historical_guilds[:3]]
                            if len(historical_guilds) > 3:
                                guild_display = f"{', '.join(guild_names)}... and {len(historical_guilds) - 3} more historical"
                            else:
                                guild_display = ', '.join(guild_names)
                            message_parts.append(f"  \u001b[0;90mHistorical: {guild_display}\u001b[0m\n")
                    else:
                        message_parts.append(f"\u001b[0;90mNo matching guilds found\u001b[0m\n")
                else:
                    message_parts.append(f"\u001b[0;90mNo mutual guilds\u001b[0m\n")
                
                # --- Client-Side History Filtering ---
                history_found = False
                matched_history = {"username": [], "display": []}
                
                try:
                    if mode in ["username", "both"] and "username_history" in user_data:
                        for entry in user_data.get("username_history", []):
                            entry_value = entry.get('value', '')
                            
                            # IMPROVED: Handle exact matching for history entries
                            if match_type == "exact":
                                # Only include exact matches
                                if (entry_value and search_term_lower and 
                                    (entry_value.lower() == search_term_lower)):
                                    matched_history["username"].append(entry)
                            else:
                                # Continue with original partial match logic
                                if entry_value and search_term_lower and search_term_lower in entry_value.lower():
                                    matched_history["username"].append(entry)
                        
                        if matched_history["username"]:
                            history_found = True
                            message_parts.append("\u001b[0;33mUsername History Matches:\u001b[0m\n")
                            # Sort by timestamp, most recent first
                            matched_history["username"].sort(
                                key=lambda x: x.get('changed_at') or datetime.min, 
                                reverse=True
                            )
                            # Show up to 3 most recent matches to avoid clutter
                            for match in matched_history["username"][:3]:
                                timestamp = match.get('changed_at')
                                value = match.get('value', 'Unknown')
                                formatted_time = timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(timestamp, datetime) else str(timestamp)
                                message_parts.append(f"  \u001b[0;37m{value} \u001b[0;90m({formatted_time})\u001b[0m\n")
                            
                            # If there are more matches than shown
                            if len(matched_history["username"]) > 3:
                                message_parts.append(f"  \u001b[0;90m...and {len(matched_history['username'])-3} more matches\u001b[0m\n")
                except Exception as e:
                    logger.warning(f"Error processing username history: {e}")
    
                try:
                    if mode in ["display", "both"] and "displayname_history" in user_data:
                        for entry in user_data.get("displayname_history", []):
                            entry_value = entry.get('value', '')
                            
                            # IMPROVED: Handle exact matching for display name history entries
                            if match_type == "exact":
                                # Only include exact matches
                                if (entry_value and search_term_lower and 
                                    (entry_value.lower() == search_term_lower)):
                                    matched_history["display"].append(entry)
                            else:
                                # Continue with original partial match logic
                                if entry_value and search_term_lower and search_term_lower in entry_value.lower():
                                    matched_history["display"].append(entry)
                        
                        if matched_history["display"]:
                            history_found = True
                            message_parts.append("\u001b[0;33mDisplay Name History Matches:\u001b[0m\n")
                            # Sort by timestamp, most recent first
                            matched_history["display"].sort(
                                key=lambda x: x.get('changed_at') or datetime.min, 
                                reverse=True
                            )
                            # Show up to 3 most recent matches to avoid clutter
                            for match in matched_history["display"][:3]:
                                timestamp = match.get('changed_at')
                                value = match.get('value', 'Unknown')
                                formatted_time = timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(timestamp, datetime) else str(timestamp)
                                message_parts.append(f"  \u001b[0;37m{value} \u001b[0;90m({formatted_time})\u001b[0m\n")
                                
                            # If there are more matches than shown
                            if len(matched_history["display"]) > 3:
                                message_parts.append(f"  \u001b[0;90m...and {len(matched_history['display'])-3} more matches\u001b[0m\n")
                except Exception as e:
                    logger.warning(f"Error processing displayname history: {e}")
    
                # IMPROVED: Adjust match checking for current username/displayname
                # If no matches were found in history but this user was returned by the query,
                # show how it matched the current username/displayname
                if not history_found:
                    current_matches = []
                    
                    if mode in ["username", "both"]:
                        if match_type == "exact":
                            # Only show if it's an EXACT match
                            if search_term_lower == current_username.lower():
                                current_matches.append("username")
                        else:
                            # Partial match mode
                            if search_term_lower in current_username.lower():
                                current_matches.append("username")
                                
                    if mode in ["display", "both"] and current_displayname and current_displayname != 'None':
                        if match_type == "exact":
                            # Only show if it's an EXACT match
                            if search_term_lower == current_displayname.lower():
                                current_matches.append("display name")
                        else:
                            # Partial match mode
                            if search_term_lower in current_displayname.lower():
                                current_matches.append("display name")
                                
                    if current_matches:
                        message_parts.append(f"\u001b[0;90m(Current {' and '.join(current_matches)} match{'' if len(current_matches) == 1 else 'es'} search term)\u001b[0m\n")
    
                if idx < skip_count + len(current_page_results):
                    if not message_parts[-1].endswith('\n'):
                        message_parts[-1] += '\n'
                    message_parts[-1] += "\u001b[30m" + "─" * 45 + "\u001b[0m\n"
                    
            # Display current page and total pages
            message_parts.append(f"\nPage \u001b[1;37m{page} of {total_pages}\u001b[0m```")
    
            await ctx.send(quote_block(''.join(message_parts)),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
    
            end_time = datetime.now()
            logger.info(f"Search '{search_term}' took {(end_time - start_time).total_seconds():.3f}s")
    
        except Exception as e:
            logger.error(f"Error in searchname command: {e}", exc_info=True)
            await self.send_with_auto_delete(ctx, f"An error occurred during the search: {str(e)}")


    def get_default_avatar_id(self, discriminator=None):
        """Get the default avatar ID for a user based on their discriminator"""
        if discriminator is None:
            # If no discriminator provided, random between 0-4
            import random
            return random.randint(0, 4)
        
        try:
            # If discriminator is a string with # format
            if isinstance(discriminator, str) and '#' in discriminator:
                discriminator = int(discriminator.split('#')[1])
            # Convert to integer if string
            if isinstance(discriminator, str):
                discriminator = int(discriminator)
            # Calculate avatar ID based on Discord algorithm
            return discriminator % 5
        except (ValueError, TypeError):
            # Default to a random avatar if conversion fails
            import random
            return random.randint(0, 4)

    async def GetUser(self, user_id):
        """Enhanced user fetching method that tries multiple approaches to get user data including avatar"""
        try:
            # First try the standard bot method
            user = self.bot.get_user(user_id)
            if user and ((hasattr(user, 'display_avatar') and user.display_avatar) or 
                        (hasattr(user, 'avatar') and user.avatar)):
                return user
                
            # Next try the HTTP API directly for better results
            try:
                user_data = await self.bot.http.get_user(user_id)
                if user_data:
                    # Create a lightweight user object with the required fields
                    class SimpleUser:
                        def __init__(self, data):
                            self.id = int(data.get('id', 0))
                            self.name = data.get('username', 'Unknown')
                            self.discriminator = data.get('discriminator', '0')
                            self._avatar = data.get('avatar')
                            
                        @property
                        def avatar(self):
                            return self._avatar
                            
                        @property
                        def display_avatar(self):
                            class AvatarURL:
                                def __init__(self, user_id, avatar_hash):
                                    self.url = f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png?size=128" if avatar_hash else None
                            return AvatarURL(self.id, self._avatar) if self._avatar else None
                            
                    return SimpleUser(user_data)
            except Exception as e:
                logger.debug(f"HTTP API user fetch failed for {user_id}: {e}")
                
            # Last attempt - try using fetch_user if available
            if hasattr(self.bot, 'fetch_user'):
                try:
                    return await self.bot.fetch_user(user_id)
                except Exception as e:
                    logger.debug(f"fetch_user failed for {user_id}: {e}")
                    
            # If we get here, return whatever user object we have, even if incomplete
            return user
        except Exception as e:
            logger.error(f"All user fetching methods failed for {user_id}: {e}")
            return None

    @commands.command(aliases=['exportguilds', 'guildss'], hidden=True)
    @developer_only()
    async def export_guilds(self, ctx):
        """Export all detected guilds from user tracker to a text file
        
        Usage:
        ;export_guilds - Exports all detected guilds to a text file
        """
        try:
            await self.safe_delete_message(ctx.message)
        except:
            pass        
        try:
            # Use MongoDB aggregation to get unique guilds efficiently
            users_collection = self.bot.db.db['users']
            
            # Aggregation pipeline to unwind detected_guilds and get unique guilds
            pipeline = [
                {"$match": {"detected_guilds": {"$exists": True, "$ne": []}}},
                {"$unwind": "$detected_guilds"},
                {"$group": {
                    "_id": "$detected_guilds.id",
                    "name": {"$first": "$detected_guilds.name"}
                }},
                {"$sort": {"name": 1}}
            ]
            
            await self.send_with_auto_delete(ctx, "⏳ Aggregating guild data from database...")
            
            # Execute aggregation
            cursor = users_collection.aggregate(pipeline)
            unique_guilds = {}
            
            async for guild in cursor:
                guild_id = guild['_id']
                guild_name = guild.get('name', 'Unknown Guild')
                unique_guilds[guild_id] = guild_name
            
            if not unique_guilds:
                await self.send_with_auto_delete(ctx, "No detected guilds found in the database.")
                return
            
            # Generate the text file content
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"detected_guilds_{timestamp}.txt"
            
            content = f"Detected Guilds Export - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            content += f"Total unique guilds: {len(unique_guilds)}\n"
            content += "=" * 50 + "\n\n"
            
            # Sort guilds by name for better readability
            sorted_guilds = sorted(unique_guilds.items(), key=lambda x: x[1].lower())
            
            for guild_id, guild_name in sorted_guilds:
                content += f"ID: {guild_id} | Name: {guild_name}\n"
            
            # Create a discord file object
            file_content = content.encode('utf-8')
            discord_file = discord.File(
                io.BytesIO(file_content),
                filename=filename
            )
            
            # Send the file
            await ctx.send(
                file=discord_file,
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            
            await self.send_with_auto_delete(ctx, f"✅ Exported {len(unique_guilds)} unique guilds to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting guilds: {e}")
            await self.send_with_auto_delete(ctx, f"❌ Error exporting guilds: {e}")

    @commands.command(aliases=['dbhealth'], hidden=True)
    @developer_only()
    async def database_health(self, ctx):
        """Check the health of the global database connection and all instance adapters"""
        await self.safe_delete_message(ctx.message)
        
        try:
            # Get the global database manager
            from utils.database.global_manager import is_global_db_active, get_global_db
            
            # Check global connection health
            global_status = "🟢 Active" if is_global_db_active() else "🔴 Inactive"
            
            # Test connection with a simple ping
            connection_test = "🟢 OK"
            try:
                if is_global_db_active():
                    from utils.database.global_manager import _global_client
                    await asyncio.wait_for(
                        _global_client.admin.command('ping'),
                        timeout=3.0
                    )
                else:
                    connection_test = "🔴 No Connection"
            except Exception as e:
                connection_test = f"🔴 Failed: {str(e)[:50]}"
            
            # Check individual instance adapters
            bot_manager = self.bot._manager
            instance_statuses = []
            
            for token, bot_instance in bot_manager.bots.items():
                if hasattr(bot_instance, 'db') and bot_instance.db:
                    db_adapter = bot_instance.db
                    uid = bot_instance.config_manager.uid
                    username = bot_instance.user.name if bot_instance.user else "Unknown"
                    
                    adapter_active = "🟢" if db_adapter.is_active else "🔴"
                    
                    instance_statuses.append({
                        'uid': uid,
                        'username': username,
                        'status': adapter_active,
                        'adapter_id': db_adapter.instance_id[-8:]  # Last 8 chars of ID
                    })
            
            # Sort by UID
            instance_statuses.sort(key=lambda x: x['uid'])
            
            # Format the response
            message = f"""```ansi
\u001b[1;36m📊 Global Database Health Report\u001b[0m

\u001b[1;33mGlobal Connection:\u001b[0m
├─ Status: {global_status}
├─ Connection Test: {connection_test}
└─ Type: Global (Independent)

\u001b[1;33mInstance Adapters:\u001b[0m"""
            
            if instance_statuses:
                for i, status in enumerate(instance_statuses):
                    is_last = i == len(instance_statuses) - 1
                    connector = "└─" if is_last else "├─"
                    message += f"\n{connector} UID {status['uid']} ({status['username']}): {status['status']} (ID: {status['adapter_id']})"
            else:
                message += "\n└─ No active instances with database adapters"
            
            message += "```"
            
            await ctx.send(
                quote_block(message),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            
        except Exception as e:
            logger.error(f"Error checking database health: {e}")
            await self.send_with_auto_delete(ctx, f"Error checking database health: {str(e)}")

    @commands.command(hidden=True)
    @developer_only()
    async def adddev(self, ctx, user_id: int):
        """Add a developer ID to the system
        .adddev <user_id>"""
        try:
            await ctx.message.delete()
        except:
            pass
            
        try:
            if self.bot.config_manager.is_developer(user_id):
                await ctx.send("User is already a developer.", 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
                
            self.bot.config_manager.add_developer(user_id)
            await ctx.send(f"✅ **Added user {user_id} as developer.**\n"
                         f"Developer permissions are now active across all instances.", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            
        except Exception as e:
            await ctx.send(f"Error adding developer: {e}", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    @commands.command(hidden=True)
    @developer_only()
    async def removedev(self, ctx, user_id: int):
        """Remove a developer ID from the system
        .removedev <user_id>"""
        try:
            await ctx.message.delete()
        except:
            pass
            
        try:
            if not self.bot.config_manager.is_developer(user_id):
                await ctx.send("User is not a developer.", 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
                
            if len(self.bot.config_manager.developer_ids) <= 1:
                await ctx.send("Cannot remove the last developer.", 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
                
            self.bot.config_manager.remove_developer(user_id)
            await ctx.send(f"✅ **Removed user {user_id} from developers.**\n"
                         f"Developer permissions revoked across all instances.", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            
        except Exception as e:
            await ctx.send(f"Error removing developer: {e}", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    @commands.command(hidden=True)
    @developer_only()
    async def listdevs(self, ctx):
        """List all developer IDs
        .listdevs"""
        try:
            await ctx.message.delete()
        except:
            pass
            
        try:
            dev_list = "\n".join([f"• {dev_id}" for dev_id in self.bot.config_manager.developer_ids])
            await ctx.send(f"**Developer IDs:**\n{dev_list}", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            
        except Exception as e:
            await ctx.send(f"Error listing developers: {e}", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    @commands.command(hidden=True)
    @developer_only()
    async def migratedevs(self, ctx):
        """Migrate developer UIDs to new system (run once after adding multiple developers)
        .migratedevs"""
        try:
            await ctx.message.delete()
        except:
            pass
            
        try:
            migrated = self.bot.config_manager.migrate_developer_uids()
            
            if migrated:
                await ctx.send("✅ **Developer UID migration completed!**\n"
                             "All developer accounts now have proper UIDs for the multi-developer system.",
                             delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            else:
                await ctx.send("ℹ️ **No migration needed.**\n"
                             "Developer UIDs are already using the correct multi-developer system.",
                             delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                             
        except Exception as e:
            logger.error(f"Error in migratedevs command: {e}")
            await ctx.send(f"❌ **Migration failed:** {str(e)}",
                         delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    @commands.command(hidden=True)
    @developer_only()
    async def refreshdevs(self, ctx):
        """Manually refresh developer IDs across all bot instances
        .refreshdevs"""
        try:
            await ctx.message.delete()
        except:
            pass
            
        try:
            self.bot.config_manager.refresh_developer_ids()
            await ctx.send("✅ **Developer IDs refreshed across all instances.**\n"
                         "All bot instances now have the latest developer list.",
                         delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                         
        except Exception as e:
            logger.error(f"Error in refreshdevs command: {e}")
            await ctx.send(f"❌ **Refresh failed:** {str(e)}",
                         delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

async def setup(bot):
    await bot.add_cog(Developer(bot))
