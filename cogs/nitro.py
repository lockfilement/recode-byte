import discord
from discord.ext import commands, tasks
import re
import logging
import json
import os
import random
from utils.general import format_message, quote_block

logger = logging.getLogger(__name__)

class NitroSniper(commands.Cog):
    # Use a single compiled regex pattern with non-capturing groups (?:)
    GIFT_PATTERN = re.compile(r'(?:discord\.gift\/|discord\.com\/gifts\/|discordapp\.com\/gifts\/)([a-zA-Z0-9]{16,24})', re.IGNORECASE)
    # Pattern to match Discord invites
    INVITE_PATTERN = re.compile(r'(?:discord\.gg\/|discord\.com\/invite\/|discordapp\.com\/invite\/)([a-zA-Z0-9]{2,10})', re.IGNORECASE)
    
    def __init__(self, bot):
        self.bot = bot
        self.enabled = False
        
        # Initialize empty statistics
        self.stats = {
            'total_seen': 0,
            'invalid_length': 0,
            'already_seen': 0,
            'already_redeemed': 0,
            'failed_redeem': 0,
            'successful_redeem': 0,
            'rate_limited': 0
        }
        
        self.seen_codes = set()
        self.redeemed_codes = set()
        self.failed_codes = {}
        # self.snipe_cooldown = commands.CooldownMapping.from_cooldown(1, 5, commands.BucketType.user)

        # File path for saved invites
        self.invites_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'saved_invites.txt')
        
        # Set for caching loaded invites        
        self.loaded_invites = set()
        self.invites_loaded = False

        # Load settings from config during initialization
        nitro_settings = self.bot.config_manager.nitro_sniper
        self.enabled = nitro_settings.get('enabled', False)
        
    def get_padding(self, labels: list) -> int:
        """Calculate padding for consistent alignment based on longest label"""
        return max(len(label) - 1 for label in labels) + 2 if labels else 2

    @tasks.loop(hours=24)
    async def auto_reset_stats(self):
        """Reset statistics every 24 hours"""
        self.reset_stats()
        logger.info("Auto-reset Nitro sniper statistics")

    def reset_stats(self):
        """Reset all statistics to zero"""
        self.stats = {key: 0 for key in self.stats}
        self.seen_codes.clear()
        self.redeemed_codes.clear()
        self.failed_codes.clear()
        logger.info("Reset Nitro sniper statistics")

    def update_stats(self, stat_type: str):
        """Update statistics counter for given type"""
        if stat_type in self.stats:
            self.stats[stat_type] += 1
            if stat_type == 'total_seen':
                # Save milestone stats
                if self.stats['total_seen'] % 100 == 0:
                    logger.info(f"Milestone: {self.stats['total_seen']} gifts seen")
            logger.debug(f"Updated {stat_type} stat: {self.stats[stat_type]}")

    def _load_invites(self):
        """Load all invites into memory once"""
        if self.invites_loaded:
            return
            
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.invites_file), exist_ok=True)
        
        if os.path.exists(self.invites_file):
            try:
                with open(self.invites_file, 'r') as f:
                    self.loaded_invites = set(line.strip() for line in f if line.strip())
            except Exception as e:
                logger.error(f"Failed to load invites: {e}")
        
        self.invites_loaded = True

    def save_invite(self, invite_code):
        """Save a Discord invite code to the file efficiently"""
        if not invite_code:
            return False
            
        try:
            # Load existing invites if not already loaded
            self._load_invites()
            
            # Skip if already saved
            if invite_code in self.loaded_invites:
                return False
                
            # Add to set and append to file (no need to read the whole file again)
            self.loaded_invites.add(invite_code)
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(self.invites_file), exist_ok=True)
            
            # Append to file
            with open(self.invites_file, 'a') as f:
                f.write(f"{invite_code}\n")
                
            return True
        except Exception as e:
            logger.error(f"Failed to save invite {invite_code}: {e}")
            return False

    def get_random_invites(self, count=10):
        """Get random invites from the saved invites file"""
        # Load invites if not already loaded
        self._load_invites()
        
        if not self.loaded_invites:
            return []
            
        try:
            # Return random invites, up to the requested count
            return random.sample(list(self.loaded_invites), min(count, len(self.loaded_invites)))
        except Exception as e:
            logger.error(f"Error getting random invites: {e}")
            return []

    @commands.command(aliases=['nt'])
    async def nitro(self, ctx, setting: str = None, amount: int = 10):
        """Configure nitro sniper"""
        try:await ctx.message.delete()
        except:pass
        
        if setting and setting.lower() in ('on', 'off'):
            # Update the enabled state
            self.enabled = setting.lower() == 'on'
            
            # Clear caches if turning off
            if not self.enabled:
                self.reset_stats()  # This clears seen_codes, redeemed_codes, and failed_codes
            
            # Get current config and update it
            with open('config.json', 'r+') as f:
                config = json.load(f)
                if 'user_settings' not in config:
                    config['user_settings'] = {}
                if self.bot.config_manager.token not in config['user_settings']:
                    config['user_settings'][self.bot.config_manager.token] = {}
                    
                if 'nitro_sniper' not in config['user_settings'][self.bot.config_manager.token]:
                    config['user_settings'][self.bot.config_manager.token]['nitro_sniper'] = {
                        'enabled': self.enabled,
                    }
                else:
                    config['user_settings'][self.bot.config_manager.token]['nitro_sniper']['enabled'] = self.enabled
                
                # Save config
                f.seek(0)
                json.dump(config, f, indent=4)
                f.truncate()
    
            await ctx.send(
                format_message(f"Nitro sniping {'enabled' if self.enabled else 'disabled'}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
        elif setting and setting.lower() == 'invites':
            # Validate amount
            if amount < 1:
                amount = 10
            elif amount > 50:  # Set a reasonable upper limit
                amount = 50
                
            # Get random invites with the specified amount
            invites = self.get_random_invites(amount)
            
            if not invites:
                await ctx.send(
                    format_message("No saved invites found."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
                
            # Format the invites as clickable links
            formatted_invites = "\n".join([f"discord.gg/{invite}" for invite in invites])
            
            await ctx.send(
                f"**{len(invites)} Random server invites:**\n```\n{formatted_invites}\n```",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )        
        else:            # Load invites to ensure the count is available
            self._load_invites()
            
            # Build a message in the style of the info command
            status_color = "\u001b[0;32m" if self.enabled else "\u001b[0;31m"
            status_text = "Enabled" if self.enabled else "Disabled"
            
            message_parts = [
                "```ansi\n"
            ]
            
            # Status section with proper padding like in info command
            message_parts.append("\u001b[30m\u001b[1m\u001b[4mStatus\u001b[0m\n")
            
            # Calculate padding for labels
            status_labels = ["Status", "Total", "Invs"]
            status_padding = self.get_padding(status_labels)
            
            message_parts.append(f"\u001b[0;37mStatus{' ' * (status_padding - len('Status'))}\u001b[30m| {status_color}{status_text}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mTotal{' ' * (status_padding - len('Total'))}\u001b[30m| \u001b[0;34m{self.stats['total_seen']:,}\u001b[0m\n")
            if self.invites_loaded:
                message_parts.append(f"\u001b[0;37mInvs{' ' * (status_padding - len('Invs'))}\u001b[30m| \u001b[0;34m{len(self.loaded_invites):,}\u001b[0m\n")
            
            # Statistics section with proper padding
            message_parts.append("\n\u001b[30m\u001b[1m\u001b[4mStats\u001b[0m\n")
            
            # Calculate padding for statistics labels
            stats_labels = ["Bad", "Dupes", "Tried", "Failed", "Rated", "Claims"]
            stats_padding = self.get_padding(stats_labels)
            
            message_parts.append(f"\u001b[0;37mBad{' ' * (stats_padding - len('Bad'))}\u001b[30m| \u001b[0;34m{self.stats['invalid_length']:,}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mDupes{' ' * (stats_padding - len('Dupes'))}\u001b[30m| \u001b[0;34m{self.stats['already_seen']:,}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mTried{' ' * (stats_padding - len('Tried'))}\u001b[30m| \u001b[0;34m{self.stats['already_redeemed']:,}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mFailed{' ' * (stats_padding - len('Failed'))}\u001b[30m| \u001b[0;34m{self.stats['failed_redeem']:,}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mRated{' ' * (stats_padding - len('Rated'))}\u001b[30m| \u001b[0;34m{self.stats['rate_limited']:,}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mClaims{' ' * (stats_padding - len('Claims'))}\u001b[30m| \u001b[0;32m{self.stats['successful_redeem']:,}\u001b[0m\n")
            
            # Help section
            message_parts.append("\n\u001b[30m\u001b[1m\u001b[4mCommands\u001b[0m\n")
            commands_labels = [".nitro on/off", ".nitro invites"]
            commands_padding = self.get_padding(commands_labels)
            
            message_parts.append(f"\u001b[0;37m.nitro on/off{' ' * (commands_padding - len('.nitro on/off'))}\u001b[30m| \u001b[0;34mToggle\u001b[0m\n")
            message_parts.append(f"\u001b[0;37m.nitro invites{' ' * (commands_padding - len('.nitro invites'))}\u001b[30m| \u001b[0;34minvites\u001b[0m\n")
            
            # Close the code block
            message_parts.append("```")
    
            await ctx.send(quote_block(''.join(message_parts)),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

    async def _handle_message(self, message):
        """Handler for message events"""
        if not self.enabled or message.author.bot:
            return

        # Check for Nitro gift codes
        codes = []
        
        # Check message content for gift links
        codes.extend([match.group(1) for match in self.GIFT_PATTERN.finditer(message.content) 
         if match.group(1)])
         
        # Check message embeds
        for embed in message.embeds:
            if embed.type == "gift" and embed.url:
                # Extract code from embed URL
                if match := self.GIFT_PATTERN.search(embed.url):
                    codes.append(match.group(1))
                    
        # Check for direct gift codes in message
        if hasattr(message, 'giftCodes') and message.giftCodes:
            codes.extend(code for code in message.giftCodes if code and len(code) >= 16)
        
        # Process gift codes
        for code in codes:
            if not code:
                continue
            self.update_stats("total_seen")

            if len(code) < 16:
                self.update_stats("invalid_length")
                continue
                
            if code in self.seen_codes:
                self.update_stats("already_seen")
                continue

            self.seen_codes.add(code)
                
            try:
                # Fetch gift info
                gift = await self.bot.fetch_gift(code)
                
                if gift.redeemed:
                    logger.info(f"Gift code {code} already redeemed")
                    self.update_stats("already_redeemed")
                    continue

                # Attempt to redeem
                result = await gift.redeem(channel=message.channel)
                
                self.redeemed_codes.add(code)
                self.update_stats("successful_redeem")
                
                channel_name = message.channel.name if isinstance(message.channel, discord.TextChannel) else "DM"
                logger.info(
                    f"Successfully claimed Nitro gift:\n"
                    f"Code: {code}\n"
                    f"Channel: {channel_name} ({message.channel.id})\n"
                    f"Server: {message.guild.name if message.guild else 'DM'}\n"
                    f"Result: {result}"
                )
                continue # Don't catch exceptions if successful

            except discord.NotFound:
                logger.debug(f"Invalid gift code: {code}")
                self.failed_codes[code] = "Invalid code"
                self.update_stats("failed_redeem")

            except discord.HTTPException as e:
                if e.status == 429:
                    logger.warning(f"Rate limited while claiming {code}")
                    self.update_stats("rate_limited")
                else:
                    logger.error(f"Failed to claim gift {code}: {e}")
                    self.failed_codes[code] = str(e)
                    self.update_stats("failed_redeem")            
            except Exception as e:
                # Handle the 'outbound_title' error specifically
                error_msg = str(e)
                if "'outbound_title'" in error_msg:
                    logger.warning(f"Discord API change detected when claiming gift {code}")
                    # The API response structure might have changed, but we still want to track the attempt
                    self.failed_codes[code] = "Discord API response format changed"
                else:
                    logger.error(f"Unexpected error claiming gift {code}: {e}")
                    self.failed_codes[code] = error_msg
                self.update_stats("failed_redeem")

        # Check for server invites (no redundant enabled check as we already checked above)
        invites = []
        
        # Check message content for invite links
        invites.extend([match.group(1) for match in self.INVITE_PATTERN.finditer(message.content) 
            if match.group(1)])
            
        # Check message embeds
        for embed in message.embeds:
            if embed.url:
                if match := self.INVITE_PATTERN.search(embed.url):
                    invites.append(match.group(1))
        
        # Save unique invites to file
        for invite in invites:
            if invite:
                self.save_invite(invite)

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)
            
        # Start auto-reset task
        self.auto_reset_stats.start()

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)
            
        self.auto_reset_stats.cancel()

async def setup(bot):
    await bot.add_cog(NitroSniper(bot))
