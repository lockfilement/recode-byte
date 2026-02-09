import discord
from discord.ext import commands
import logging
import base64
import re
import asyncio
import json
from datetime import datetime, timedelta
from utils.general import format_message, quote_block
from utils.config_manager import UserConfig, AutoDeleteConfig

logger = logging.getLogger(__name__)

class Host(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.rate_limits = {}  # Dict to track rate limits per user
        
    def is_rate_limited(self, user_id, command="host", cooldown_seconds=30):
        """Check if user is rate limited for a command"""
        now = datetime.utcnow()
        key = f"{user_id}_{command}"
        
        if key in self.rate_limits:
            last_used = self.rate_limits[key]
            if now - last_used < timedelta(seconds=cooldown_seconds):
                return True, int((last_used + timedelta(seconds=cooldown_seconds) - now).total_seconds())
        
        self.rate_limits[key] = now
        return False, 0
    
    def cleanup_old_rate_limits(self):
        """Clean up old rate limit entries to prevent memory leaks"""
        now = datetime.utcnow()
        cutoff = now - timedelta(hours=1)  # Remove entries older than 1 hour
        
        keys_to_remove = []
        for key, timestamp in self.rate_limits.items():
            if timestamp < cutoff:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.rate_limits[key]
        
    async def validate_token(self, token):
        """Validate a Discord token using the API"""
        try:
            return await self.bot.config_manager.validate_token_api(token)
        except Exception as e:
            logger.error(f"Error validating token: {e}")
            return False

    def decode_user_id_from_token(self, token):
        """Extract the user ID from the first part of a Discord token"""
        try:
            # Extract the first part of the token
            token_part = token.split('.')[0]
            
            # Add padding if needed
            padding_needed = len(token_part) % 4
            if padding_needed:
                token_part += '=' * (4 - padding_needed)
            
            # Decode the base64 to get the user ID
            user_id = int(base64.b64decode(token_part).decode('utf-8'))
            return user_id
        except Exception as e:
            logger.error(f"Error decoding user ID from token: {e}")
            return None
            
    async def is_authorized_host(self, user_id):
        """Check if a user is in the authorized_hosts collection"""
        try:
            # Query the authorized_hosts collection
            host = await self.bot.db.db.authorized_hosts.find_one({"user_id": user_id})
            return host is not None
        except Exception as e:
            logger.error(f"Error checking authorized host: {e}")
            return False
            
    async def is_blacklisted_user(self, user_id):
        """Check if a user ID is blacklisted from hosting"""
        try:
            # Query the blacklisted_users collection
            blacklisted = await self.bot.db.db.blacklisted_users.find_one({"user_id": user_id})
            return blacklisted is not None
        except Exception as e:
            logger.error(f"Error checking blacklisted user: {e}")
            return False
            
    async def get_hosting_limit(self, user_id):
        """Get the hosting limit for a user (default 5 if not specified)"""
        try:
            # Query the authorized_hosts collection for the user's limit
            host = await self.bot.db.db.authorized_hosts.find_one({"user_id": user_id})
            if host:
                return host.get("hosting_limit", 5)
            return 5  # Default limit
        except Exception as e:
            logger.error(f"Error getting hosting limit: {e}")
            return 5  # Default limit on error
            
    async def get_hosted_count(self, user_id):
        """Get the number of tokens a user has hosted"""
        try:
            count = await self.bot.db.db.hosted_tokens.count_documents({"host_user_id": user_id})
            return count
        except Exception as e:
            logger.error(f"Error getting hosted count: {e}")
            return 0
            
    async def add_hosted_token(self, host_user_id, token_owner_id, token):
        """Add a hosted token record to MongoDB"""
        try:
            # First check if this exact combination already exists
            existing = await self.bot.db.db.hosted_tokens.find_one({
                "host_user_id": host_user_id,
                "token_owner_id": token_owner_id
            })
            
            if existing:
                # Update the existing record with new token
                await self.bot.db.db.hosted_tokens.update_one(
                    {"host_user_id": host_user_id, "token_owner_id": token_owner_id},
                    {"$set": {"token": token, "updated_at": datetime.utcnow()}}
                )
            else:
                # Insert new record
                await self.bot.db.db.hosted_tokens.insert_one({
                    "host_user_id": host_user_id,
                    "token_owner_id": token_owner_id,
                    "token": token,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                })
        except Exception as e:
            logger.error(f"Error adding hosted token: {e}")
            
    async def remove_hosted_token(self, host_user_id, token_owner_id):
        """Remove a hosted token record from MongoDB"""
        try:
            result = await self.bot.db.db.hosted_tokens.delete_one({
                "host_user_id": host_user_id,
                "token_owner_id": token_owner_id
            })
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error removing hosted token: {e}")
            return False
            
    async def get_hosted_tokens(self, user_id):
        """Get all tokens hosted by a user"""
        try:
            cursor = self.bot.db.db.hosted_tokens.find({"host_user_id": user_id})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error getting hosted tokens: {e}")
            return []
    
    async def _handle_host_command(self, message, prefix):
        """Handle the host command logic"""
        # Clean up old rate limit entries periodically
        self.cleanup_old_rate_limits()
        
        # Check rate limiting (30 second cooldown)
        is_limited, time_remaining = self.is_rate_limited(message.author.id, "host", 30)
        if is_limited:
            try:
                await message.channel.send(format_message(f"Rate limited! Please wait {time_remaining} seconds before using the host command again."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
            return
        
        # Extract token from message
        token = message.content[len(f"{prefix}host "):].strip()
        
        # Validate token structure
        if not self.bot.config_manager.validate_token(token):
            try:
                await message.channel.send(format_message("Invalid token format."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
            return
            
        # Validate token with Discord API
        is_valid = await self.validate_token(token)
        if not is_valid:
            try:
                await message.channel.send(format_message("Invalid token - API validation failed."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
            return
            
        # Extract user ID from token
        user_id = self.decode_user_id_from_token(token)
        if not user_id:
            try:
                await message.channel.send(format_message("Could not extract user ID from token."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
            return
              # Check if the user ID from the token is blacklisted
        is_blacklisted = await self.is_blacklisted_user(user_id)
        if is_blacklisted:
            try:
                await message.channel.send(format_message("This user ID is blacklisted and cannot be hosted."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
            return
            
        # Check if the message author is authorized to host
        is_authorized = await self.is_authorized_host(message.author.id)
        if not is_authorized:
            try:
                await message.channel.send(format_message("You are not authorized to host accounts on this selfbot."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
            return
            
        # Check if hosting different account (not their own)
        if user_id != message.author.id:
            # Check the hosting limit for this user
            user_limit = await self.get_hosting_limit(message.author.id)
            hosted_count = await self.get_hosted_count(message.author.id)
            if hosted_count >= user_limit:
                try:
                    await message.channel.send(f"You have reached the maximum limit of {user_limit} hosted accounts. Use `unhost <discord_id>` to remove an account first.",
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
  
                
        # Get the bot manager reference for user management
        bot_manager = self.bot._manager
        
        try:
            # Check if the user already exists in config - match usermanage logic
            existing_uid = None
            old_token = None
            cfg = await self.bot.config_manager._get_cached_config_async()
            for t, settings in cfg.get('user_settings', {}).items():
                if settings.get('discord_id') == user_id:
                    existing_uid = settings.get('uid')
                    old_token = t
                    break
                        
            # If user exists, update the token
            if existing_uid is not None and old_token is not None:
                logger.info(f"Updating token for existing user. UID: {existing_uid}, Discord ID: {user_id}")
                if self.bot.config_manager.update_user_token(existing_uid, token):
                    # Reload config to ensure we have the latest settings
                    await self.bot.config_manager.reload_config_async()
                    logger.info(f"Config reloaded after token update for UID {existing_uid}")
                    
                    # Now close old bot instance if it's still running
                    if old_token in bot_manager.bots:
                        try:
                            logger.info(f"Closing existing bot instance for UID {existing_uid}")
                            old_bot = bot_manager.bots[old_token]
                            old_bot._closed = True  # Ensure bot is marked as closed
                            await old_bot.close()
                            del bot_manager.bots[old_token]
                            await asyncio.sleep(1)  # Give time for cleanup
                            logger.info(f"Successfully closed old bot instance for UID {existing_uid}")
                        except Exception as e:
                            logger.error(f"Error closing old bot instance for UID {existing_uid}: {e}")
                    else:
                        logger.info(f"No existing bot instance found for token {old_token[:10]}...")
                      # Start new bot instance with updated token
                    try:
                        logger.info(f"Starting new bot instance for UID {existing_uid}")
                        await bot_manager.start_bot(token)
                        logger.info(f"Successfully started new bot instance for UID {existing_uid}")
                    except Exception as e:
                        logger.error(f"Error starting new bot instance for UID {existing_uid}: {e}")
                        raise
                        
                    # Track in MongoDB if hosting different account
                    if user_id != message.author.id:
                        await self.add_hosted_token(message.author.id, user_id, token)
                        
                    await message.channel.send(format_message(f"Successfully updated your token. Your user ID is {existing_uid}"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                else:
                    await message.channel.send(format_message("Failed to update token."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            else:
                # Create a new user
                # Read the default prefix from config.json (like in usermanage command)
                cfg = await self.bot.config_manager._get_cached_config_async()
                default_prefix = cfg.get('command_prefix', ';')
                  # Create new user config with proper settings
                user_config = UserConfig(
                    token=token,
                    username=message.author.name,
                    command_prefix=default_prefix,
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
                
                # Start new bot instance for the user
                await bot_manager.start_bot(token)
                
                # Get the assigned UID from cached config
                cfg = await self.bot.config_manager._get_cached_config_async()
                uid = cfg.get('user_settings', {}).get(token, {}).get('uid', '?')
                await message.channel.send(format_message(f"Successfully added you to the selfbot with UID: {uid}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                  # Track in MongoDB if hosting different account
                if user_id != message.author.id:
                    await self.add_hosted_token(message.author.id, user_id, token)
                
        except Exception as e:
            logger.error(f"Error processing host command: {e}")
            try:
                await message.channel.send(format_message(f"Error processing host command: {e}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
    
    async def _handle_listhosted_command(self, message):
        """Handle the listhosted command logic"""
        try:
            # Check rate limiting (10 second cooldown for listhosted)
            is_limited, time_remaining = self.is_rate_limited(message.author.id, "listhosted", 10)
            if is_limited:
                try:
                    await message.channel.send(format_message(f"Rate limited! Please wait {time_remaining} seconds before using this command again."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Only allow authorized users to use this command
            is_authorized = await self.is_authorized_host(message.author.id)
            if not is_authorized:
                try:
                    await message.channel.send(format_message("You are not authorized to use this command."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Parse page and option parameters
            parts = message.content.split()
            page = 1
            show_uids_only = False
            
            if len(parts) > 1:
                # Check if second parameter is 'uids' or page number
                if parts[1].lower() in ['uid', 'uids', 'true']:
                    show_uids_only = True
                else:
                    try:
                        page = int(parts[1])
                    except ValueError:
                        page = 1
                
                # Check if third parameter is 'uids'
                if len(parts) > 2 and parts[2].lower() in ['uid', 'uids', 'true']:
                    show_uids_only = True
            
            # Get hosted tokens and user's limit
            hosted_tokens = await self.get_hosted_tokens(message.author.id)
            user_limit = await self.get_hosting_limit(message.author.id)
            
            if not hosted_tokens:
                try:
                    await message.channel.send(format_message("You have not hosted any accounts."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Get cached config for user settings to find UIDs
            config = await self.bot.config_manager._get_cached_config_async()
            
            # Collect hosted users info
            hosted_users_info = []
            bot_manager = self.bot._manager
            
            for hosted_token in hosted_tokens:
                token_owner_id = hosted_token['token_owner_id']
                token = hosted_token['token']
                created_at = hosted_token['created_at'].strftime('%Y-%m-%d %H:%M')
                
                # Find UID from config
                uid = '?'
                prefix_val = ';'
                stored_username = 'Unknown'
                config_token = None
                
                for t, settings in config['user_settings'].items():
                    if settings.get('discord_id') == token_owner_id:
                        uid = str(settings.get('uid', '?'))
                        prefix_val = settings.get('command_prefix', ';')
                        stored_username = settings.get('username', 'Unknown')
                        config_token = t  # Store the config token
                        break
                
                # Get bot instance status if connected
                user = None
                guild_count = 0
                discord_status = "Offline"
                
                if config_token and config_token in bot_manager.bots:
                    bot = bot_manager.bots[config_token]
                    if bot.is_ready():
                        user = bot.user
                        guild_count = len(bot.guilds)
                        discord_status = str(bot.status)
                
                # Use stored username if bot instance not available
                display_name = user.name if user else stored_username
                
                hosted_users_info.append({
                    'uid': uid,
                    'discord_id': token_owner_id,
                    'username': display_name,
                    'prefix': prefix_val,
                    'guild_count': guild_count,
                    'status': discord_status,
                    'hosted_date': created_at
                })
            
            # Sort by UID
            hosted_users_info.sort(key=lambda x: int(x['uid']) if x['uid'].isdigit() else 999)
            
            # If show_uids_only parameter is True, just display a list of UIDs
            if show_uids_only:
                uids_list = [f"{user_info['uid']} ({user_info['username']})" for user_info in hosted_users_info]
                uids_text = "\n".join(uids_list)
                
                # Create a comma-separated list of just UIDs
                uids_csv = ",".join([user_info['uid'] for user_info in hosted_users_info])
                
                message = f"```ansi\n\u001b[30m\u001b[1m\u001b[4mHosted User UIDs ({len(hosted_users_info)}/{user_limit})\u001b[0m\n{uids_text}\n\nTotal hosted: {len(hosted_users_info)}\n\nComma-separated UIDs:\n{uids_csv}```"
                try:
                    await message.channel.send(quote_block(message),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Pagination
            items_per_page = 5
            total_pages = (len(hosted_users_info) + items_per_page - 1) // items_per_page
            
            page = min(max(1, page), total_pages)
            start_idx = (page - 1) * items_per_page
            page_users = hosted_users_info[start_idx:start_idx + items_per_page]
            
            # Format message with improved emojis for status
            status_emoji = {
                "online": "✅", "idle": "🌙",
                "dnd": "⛔", "invisible": "👻",
                "offline": "⚪"
            }
            
            message_parts = [
                "```ansi\n" + \
                f"\u001b[30m\u001b[1m\u001b[4mHosted Accounts ({len(hosted_users_info)}/{user_limit})\u001b[0m\n"
            ]
            
            for user_info in page_users:
                message_parts[-1] += (
                    f"\u001b[0;33mUID: {user_info['uid']}\n" + \
                    f"\u001b[0;36mID: \u001b[0;37m{user_info['discord_id']}\n" + \
                    f"\u001b[0;36mName: \u001b[0;37m{user_info['username']}\n" + \
                    f"\u001b[0;36mPrefix: \u001b[0;37m{user_info['prefix']}\n" + \
                    f"\u001b[0;36mGuilds: \u001b[0;37m{user_info['guild_count']}\n" + \
                    f"\u001b[0;36mStatus: \u001b[0;37m{status_emoji.get(user_info['status'].lower(), '❓')} {user_info['status'].title()}\n" + \
                    f"\u001b[0;36mHosted: \u001b[0;37m{user_info['hosted_date']}\n" + \
                    f"\u001b[0;37m{'─' * 20}\n"
                )
            
            message_parts[-1] += "```"
            
            message_parts.append(
                f"```ansi\nPage \u001b[1m\u001b[37m{page}/{total_pages}\u001b[0m```"
            )
            
            try:
                await message.channel.send(quote_block(''.join(message_parts)),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
                
        except Exception as e:
            logger.error(f"Error in listhosted command: {e}")
            try:
                await message.channel.send(format_message(f"Error listing hosted accounts: {e}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
    
    async def _handle_unhost_command(self, message):
        """Handle the unhost command logic"""
        try:
            # Check rate limiting (15 second cooldown for unhost)
            is_limited, time_remaining = self.is_rate_limited(message.author.id, "unhost", 15)
            if is_limited:
                try:
                    await message.channel.send(format_message(f"Rate limited! Please wait {time_remaining} seconds before using this command again."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Extract identifier from message
            parts = message.content.split()
            if len(parts) != 2:
                try:
                    await message.channel.send(format_message("Usage: `unhost <discord_id|uid>`"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return

            identifier = parts[1]
            # Only allow authorized users to use this command
            is_authorized = await self.is_authorized_host(message.author.id)
            if not is_authorized:
                try:
                    await message.channel.send(format_message("You are not authorized to use this command."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return

            # Check if the user actually hosted this account
            hosted_tokens = await self.get_hosted_tokens(message.author.id)
            hosted_account = None
            discord_id = None

            # Load cached config for UID lookup
            config = await self.bot.config_manager._get_cached_config_async()
            uid_to_discord = {}
            for t, settings in config.get('user_settings', {}).items():
                uid = str(settings.get('uid', ''))
                did = settings.get('discord_id')
                if uid and did:
                    uid_to_discord[uid] = did

            # Try to interpret identifier as UID first
            if identifier in uid_to_discord:
                discord_id = uid_to_discord[identifier]
            else:
                try:
                    discord_id = int(identifier)
                except ValueError:
                    try:
                        await message.channel.send(format_message("Invalid Discord ID or UID. Please provide a valid number or UID."),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                    except:
                        pass
                    return

            # Find the hosted account by discord_id
            for token_record in hosted_tokens:
                if token_record['token_owner_id'] == discord_id:
                    hosted_account = token_record
                    break

            if not hosted_account:
                try:
                    await message.channel.send(format_message(f"You have not hosted an account with Discord ID or UID {identifier}."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return

            # Get the token for removal (similar to usermanage remove logic)
            token_to_remove = hosted_account['token']
            
            # Remove from selfbot instances (like usermanage remove)
            bot_manager = self.bot._manager
            if token_to_remove in bot_manager.bots:
                try:
                    logger.info(f"Closing bot instance for Discord ID {discord_id}")
                    bot_instance = bot_manager.bots[token_to_remove]
                    bot_instance._closed = True
                    await bot_instance.close()
                    del bot_manager.bots[token_to_remove]
                    logger.info(f"Successfully closed bot instance for Discord ID {discord_id}")
                except Exception as e:
                    logger.error(f"Error closing bot instance for Discord ID {discord_id}: {e}")
            
            # Remove from config.json (like usermanage remove)
            try:
                with open('config.json', 'r+') as f:
                    config = json.load(f)
                    
                    # Remove from tokens list
                    if token_to_remove in config.get('tokens', []):
                        config['tokens'].remove(token_to_remove)
                    
                    # Remove from user_settings
                    if token_to_remove in config.get('user_settings', {}):
                        del config['user_settings'][token_to_remove]
                    
                    f.seek(0)
                    json.dump(config, f, indent=4)
                    f.truncate()
                    
                logger.info(f"Removed Discord ID {discord_id} from config.json")
            except Exception as e:
                logger.error(f"Error removing from config.json: {e}")
            
            # Remove from MongoDB
            removed = await self.remove_hosted_token(message.author.id, discord_id)
            
            if removed:
                try:
                    await message.channel.send(format_message(f"Successfully removed hosted account with Discord ID {discord_id}."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                logger.info(f"User {message.author.id} removed hosted account {discord_id}")
            else:
                try:
                    await message.channel.send(format_message(f"Failed to remove hosted account from database."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                
        except Exception as e:
            logger.error(f"Error in unhost command: {e}")
            try:
                await message.channel.send(format_message(f"Error removing hosted account: {e}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)                
            except:
                pass
    
    async def _handle_viewtoken_command(self, message):
        """Handle the viewtoken command logic"""
        try:
            # Check rate limiting (20 second cooldown for viewtoken)
            is_limited, time_remaining = self.is_rate_limited(message.author.id, "viewtoken", 20)
            if is_limited:
                try:
                    await message.channel.send(format_message(f"Rate limited! Please wait {time_remaining} seconds before using this command again."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Extract UID from message
            parts = message.content.split()
            if len(parts) != 2:
                try:
                    await message.channel.send(format_message("Usage: `viewtoken <uid>` or `vt <uid>`"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            try:
                uid = int(parts[1])
            except ValueError:
                try:
                    await message.channel.send(format_message("Invalid UID. Please provide a valid number."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Only allow authorized users to use this command
            is_authorized = await self.is_authorized_host(message.author.id)
            if not is_authorized:
                try:
                    await message.channel.send(format_message("You are not authorized to use this command."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Developer account check - prevent viewing tokens for any developer account
            try:
                # Load config to get discord_id for this UID
                with open('config.json', 'r') as f:
                    config = json.load(f)
                    uid_to_discord = {}
                    for t, settings in config.get('user_settings', {}).items():
                        u = settings.get('uid')
                        did = settings.get('discord_id')
                        if u is not None and did:
                            uid_to_discord[u] = did
                
                # Check if this UID belongs to a developer account
                if uid in uid_to_discord:
                    discord_id = uid_to_discord[uid]
                    if self.bot.config_manager.is_developer(discord_id):
                        try:
                            await message.channel.send(format_message("Cannot view token for developer account."),
                                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                        except:
                            pass
                        return
            except Exception as e:
                logger.error(f"Error checking developer status for UID {uid}: {e}")
            
            # Get hosted tokens to verify the user can access this UID
            hosted_tokens = await self.get_hosted_tokens(message.author.id)
            
            # Read config to find the token by UID
            with open('config.json', 'r') as f:
                config = json.load(f)
            
            # Find the token and discord_id for the given UID
            target_token = None
            target_discord_id = None
            target_username = "Unknown"
            
            for token, settings in config['user_settings'].items():
                if settings.get('uid') == uid:
                    target_token = token
                    target_discord_id = settings.get('discord_id')
                    target_username = settings.get('username', 'Unknown')
                    break
            
            if not target_token:
                try:
                    await message.channel.send(format_message(f"No user found with UID {uid}."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Check if the user has hosted this account
            is_hosted_by_user = False
            for hosted_token in hosted_tokens:
                if hosted_token['token_owner_id'] == target_discord_id:
                    is_hosted_by_user = True
                    break
            
            if not is_hosted_by_user:
                try:
                    await message.channel.send(format_message(f"You have not hosted the account with UID {uid}."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Send token in code block for easy copying
            try:
                await message.channel.send(
                    format_message(f"Token for UID {uid} ({target_username}):\n{target_token}", code_block=True),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            except:
                pass
            
        except Exception as e:
            logger.error(f"Error in viewtoken command: {e}")
            try:
                await message.channel.send(format_message(f"Error viewing token: {e}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
    
    async def _handle_validate_command(self, message):
        """Handle the validate command - remove invalid hosted tokens"""
        try:
            # Check rate limiting (60 second cooldown for validate)
            is_limited, time_remaining = self.is_rate_limited(message.author.id, "validate", 60)
            if is_limited:
                try:
                    await message.channel.send(format_message(f"Rate limited! Please wait {time_remaining} seconds before using this command again."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Only allow authorized users to use this command
            is_authorized = await self.is_authorized_host(message.author.id)
            if not is_authorized:
                try:
                    await message.channel.send(format_message("You are not authorized to use this command."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            # Get hosted tokens for this user
            hosted_tokens = await self.get_hosted_tokens(message.author.id)
            
            if not hosted_tokens:
                try:
                    await message.channel.send(format_message("You have not hosted any accounts."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                return
            
            removed = []
            bot_manager = self.bot._manager
            
            try:
                await message.channel.send(format_message(f"Validating {len(hosted_tokens)} hosted tokens..."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
            
            # Check each hosted token
            for hosted_token in hosted_tokens:
                token = hosted_token['token']
                token_owner_id = hosted_token['token_owner_id']
                
                # Validate token with Discord API
                is_valid = await self.validate_token(token)
                
                if not is_valid:
                    # Get UID from config and fetch actual user info from Discord
                    uid = '?'
                    username = 'Unknown'
                    with open('config.json', 'r') as f:
                        config = json.load(f)
                        for t, settings in config['user_settings'].items():
                            if settings.get('discord_id') == token_owner_id:
                                uid = str(settings.get('uid', '?'))
                                break
                    
                    # Fetch actual user info from Discord
                    try:
                        fetched_user = await self.bot.GetUser(token_owner_id)
                        if fetched_user:
                            username = fetched_user.name
                        else:
                            username = "Unknown User"
                    except Exception as e:
                        logger.debug(f"Could not fetch user info for ID {token_owner_id}: {e}")
                        username = "Unknown User"
                    
                    # Remove from bot manager if running
                    if token in bot_manager.bots:
                        try:
                            logger.info(f"Closing invalid bot instance for UID {uid}")
                            bot_instance = bot_manager.bots[token]
                            bot_instance._closed = True
                            await bot_instance.close()
                            del bot_manager.bots[token]
                        except Exception as e:
                            logger.error(f"Error closing invalid bot instance for UID {uid}: {e}")
                    
                    # Remove from config.json
                    try:
                        with open('config.json', 'r+') as f:
                            config = json.load(f)
                            
                            # Remove from tokens list
                            if token in config.get('tokens', []):
                                config['tokens'].remove(token)
                            
                            # Remove from user_settings
                            if token in config.get('user_settings', {}):
                                del config['user_settings'][token]
                            
                            f.seek(0)
                            json.dump(config, f, indent=4)
                            f.truncate()
                    except Exception as e:
                        logger.error(f"Error removing invalid token from config: {e}")
                    
                    # Remove from MongoDB hosted_tokens
                    await self.remove_hosted_token(message.author.id, token_owner_id)
                    
                    removed.append(f"UID {uid} | {username} ({token_owner_id})")
                    logger.info(f"Removed invalid hosted token for UID {uid} | {username} ({token_owner_id})")
            
            # Send results
            if removed:
                removed_list = "\n".join(removed)
                try:
                    await message.channel.send(format_message(f"Removed {len(removed)} invalid hosted accounts:\n{removed_list}"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
            else:
                try:
                    await message.channel.send(format_message("All hosted tokens are valid."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Error in validate command: {e}")
            try:
                await message.channel.send(format_message(f"Error validating hosted tokens: {e}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass

    async def _handle_help_command(self, message):
        """Handle the help command - show available host commands and usage"""
        try:
            prefix = self.bot.config_manager.command_prefix
            
            # Build help content
            help_content = f"""```ansi
\u001b[30m\u001b[1m\u001b[4mHost Commands Help\u001b[0m

\u001b[0;36m{prefix}host <token>\u001b[0m
  \u001b[0;37mHost a Discord account on the selfbot
  \u001b[0;33mExample: {prefix}host MTA1ODc2NzQ0ODQyODc4OTgxNA.GzDqEp.xyz123
  \u001b[0;35m• Validates token format and API
  \u001b[0;35m• Checks hosting limits and blacklist
  \u001b[0;35m• Updates existing users or creates new ones\u001b[0m

\u001b[0;36m{prefix}listhosted [page] [uids]\u001b[0m \u001b[0;90m(alias: {prefix}lh)\u001b[0m
  \u001b[0;37mList all accounts you have hosted
  \u001b[0;33mExample: {prefix}listhosted
  \u001b[0;33mExample: {prefix}lh 2
  \u001b[0;33mExample: {prefix}lh uids
  \u001b[0;35m• Shows UID, username, status, guild count
  \u001b[0;35m• Paginated (5 per page)
  \u001b[0;35m• Use 'uids' parameter for UID-only list\u001b[0m

\u001b[0;36m{prefix}unhost <discord_id|uid>\u001b[0m
  \u001b[0;37mRemove a hosted account
  \u001b[0;33mExample: {prefix}unhost 123456789012345678
  \u001b[0;33mExample: {prefix}unhost 42
  \u001b[0;35m• Accepts Discord ID or UID
  \u001b[0;35m• Closes bot instance and removes from config
  \u001b[0;35m• Only works on accounts you hosted\u001b[0m

\u001b[0;36m{prefix}viewtoken <uid>\u001b[0m \u001b[0;90m(alias: {prefix}vt)\u001b[0m
  \u001b[0;37mView token for a hosted account
  \u001b[0;33mExample: {prefix}viewtoken 42
  \u001b[0;33mExample: {prefix}vt 42
  \u001b[0;35m• Only shows tokens for accounts you hosted
  \u001b[0;35m• Cannot view developer account tokens
  \u001b[0;35m• Token displayed in copy-friendly format\u001b[0m

\u001b[0;36m{prefix}validate\u001b[0m
  \u001b[0;37mValidate and remove invalid hosted accounts
  \u001b[0;33mExample: {prefix}validate
  \u001b[0;35m• Checks all hosted tokens against Discord API
  \u001b[0;35m• Automatically removes invalid accounts
  \u001b[0;35m• Frees up hosting slots\u001b[0m

\u001b[0;36m{prefix}hosthelp\u001b[0m
  \u001b[0;37mShow this help menu\u001b[0m

\u001b[30m\u001b[1m\u001b[4mRequirements\u001b[0m
\u001b[0;35m• Must be authorized to host accounts
\u001b[0;35m• Commands only work in DMs
\u001b[0;35m• Must DM the developer instance
\u001b[0;35m• Hosting limits apply (default: 5 accounts)\u001b[0m

\u001b[30m\u001b[1m\u001b[4mNotes\u001b[0m
\u001b[0;37m• Tokens are validated with Discord API
\u001b[0;37m• Blacklisted users cannot be hosted
\u001b[0;37m• Existing users get token updates
\u001b[0;37m• Messages auto-delete based on settings
\u001b[0;37m• Rate limiting prevents command spam\u001b[0m

\u001b[30m\u001b[1m\u001b[4mRate Limits\u001b[0m
\u001b[0;35m• host: 30 seconds
\u001b[0;35m• listhosted: 10 seconds
\u001b[0;35m• unhost: 15 seconds
\u001b[0;35m• viewtoken: 20 seconds
\u001b[0;35m• validate: 60 seconds\u001b[0m
```"""
            
            try:
                await message.channel.send(
                    help_content,
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            except:
                pass
                
        except Exception as e:
            logger.error(f"Error in help command: {e}")
            try:
                await message.channel.send(format_message(f"Error displaying help: {e}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            except:
                pass
            
    async def _handle_message(self, message):
        """Handler for direct messages with host command"""
        # Skip if message is from the selfbot itself
        if message.author.id == self.bot.user.id:
            return
            
        # Only process DM messages
        if not isinstance(message.channel, discord.DMChannel):
            return
            
        # Only process if this is a developer instance
        if not self.bot.config_manager.is_developer_uid(self.bot.config_manager.uid):
            return
            
        # Get the developer instance prefix
        prefix = self.bot.config_manager.command_prefix
        
        # Process host command
        if message.content.startswith(f"{prefix}host "):
            await self._handle_host_command(message, prefix)
        
        # Process listhosted command
        elif message.content.startswith(f"{prefix}listhosted") or message.content.startswith(f"{prefix}lh"):
            await self._handle_listhosted_command(message)
        
        # Process unhost command
        elif message.content.startswith(f"{prefix}unhost "):
            await self._handle_unhost_command(message)
        
        # Process viewtoken command
        elif message.content.startswith(f"{prefix}viewtoken ") or message.content.startswith(f"{prefix}vt "):
            await self._handle_viewtoken_command(message)
        
        # Process validate command
        elif message.content == f"{prefix}validate":
            await self._handle_validate_command(message)
        
        # Process help command
        elif message.content == f"{prefix}hosthelp":
            await self._handle_help_command(message)

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)
        
    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)

async def setup(bot):
    await bot.add_cog(Host(bot))