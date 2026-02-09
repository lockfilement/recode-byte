import discord
from discord.ext import commands
import logging
import time
from utils.general import format_message, quote_block

logger = logging.getLogger(__name__)

class AutoResponder(commands.Cog):
    """Handle AFK status and auto-responses"""
    def __init__(self, bot):
        self.bot = bot
        self.afk = False
        self.afk_message = "I'm currently AFK."
        self.last_afk_message = {}  # Track when we last sent AFK message per channel
        self.cooldown = 60  # Cooldown in seconds between AFK messages in same channel
        self.afk_since = None
        
    def reset_afk(self):
        """Reset AFK status and clear tracking"""
        self.afk = False
        self.afk_message = "I'm currently AFK."
        self.afk_since = None
        self.last_afk_message.clear()

    @commands.command(aliases=['away'])
    async def afk(self, ctx, *, message: str = None):
        """Set your AFK status
        
        .afk [message] - Enable AFK with optional message"""
        
        try:
            try:await ctx.message.delete()
            except:pass

            self.afk = True
            self.afk_since = time.time()
            if message:
                self.afk_message = message
                
            await ctx.send(
                format_message(f"AFK enabled: {self.afk_message}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

        except Exception as e:
            logger.error(f"Error in afk command: {e}")
            await ctx.send(
                format_message("❌ An error occurred"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

    def get_time_message(self):
        """Calculate and format the time elapsed since going AFK"""
        if not self.afk_since:
            return ""
            
        elapsed = int(time.time() - self.afk_since)
        days = elapsed // 86400
        hours = (elapsed % 86400) // 3600
        minutes = (elapsed % 3600) // 60
        seconds = elapsed % 60

        time_msg = f"`AFK since: "
        if days > 0:
            time_msg += f"{days} days "
        if hours > 0:
            time_msg += f"{hours} hours "
        if minutes > 0:
            time_msg += f"{minutes} mins "
        if seconds > 0:
            time_msg += f"{seconds} secs"
        time_msg += "`"
        
        return time_msg

    async def _handle_message(self, message):
        """Handle AFK responses"""
        if not message.author.bot and message.author.id == self.bot.user.id:
            # Check if we should disable AFK mode
            if (self.afk and 
                len(message.content) > 0 and 
                not message.content.startswith(f"{self.bot.command_prefix}afk") and
                not message.content.startswith("AFK enabled:") and
                self.afk_message not in message.content):
                
                self.reset_afk()
                return
    
        if not self.afk:
            return
            
        if message.author.bot or message.author.id == self.bot.user.id:
            return
    
        should_respond = False
        
        # Check for mentions
        if self.bot.user in message.mentions:
            should_respond = True
            
        # Check for replies
        if message.reference and isinstance(message.reference.resolved, discord.Message):
            if message.reference.resolved.author.id == self.bot.user.id:
                should_respond = True
    
        # Handle DMs
        if isinstance(message.channel, discord.DMChannel):
            should_respond = True
    
        if should_respond:
            # Check cooldown for this channel
            current_time = time.time()
            last_time = self.last_afk_message.get(message.channel.id, 0)
            
            if current_time - last_time < self.cooldown:
                return

            try:
                # Get time message
                time_msg = self.get_time_message()
                # Send AFK response
                await message.reply(f"{self.afk_message} {time_msg}")
                self.last_afk_message[message.channel.id] = current_time
            except discord.HTTPException as e:
                logger.error(f"Failed to send AFK response: {e}")

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
        self.reset_afk()

async def setup(bot):
    await bot.add_cog(AutoResponder(bot))
