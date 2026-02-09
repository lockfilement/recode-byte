import discord
from discord.ext import commands
from utils.rate_limiter import rate_limiter
from utils.general import get_max_message_length, format_message, quote_block
from typing import Optional
import logging
import random

logger = logging.getLogger(__name__)

class RizzMock(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.target = None
        self.sent_messages = {}  # Track {original_msg_id: our_msg}
        self.used_rizz_lines = set()
        self.use_hashtag = False
        self.random_hashtag = False

    @commands.command(aliases=['rzm'])
    async def rizzmock(self, ctx, *args):
        """Rizz-mock a user
        
        rizzmock [#/rh] <user> - Mock with rizz options
        """
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

        # Parse arguments
        target = None
        self.use_hashtag = False
        self.random_hashtag = False
        
        args = list(args)
        while args:
            arg = args[0]
            if arg == '#':
                self.use_hashtag = True
                args.pop(0)
            elif arg.lower() == 'rh':
                self.random_hashtag = True
                args.pop(0)
            else:
                # Last argument should be the user
                try:
                    target = await commands.UserConverter().convert(ctx, arg)
                except:
                    try:
                        target = await commands.MemberConverter().convert(ctx, arg)
                    except:
                        await ctx.send(
                        format_message("Invalid user specified"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
                break

        if not target:
            await ctx.send(
                format_message("You need to specify a user"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
        
        # Check for self-mocking or bot-mocking
        elif target and (target.id == ctx.author.id or target.bot):
            await ctx.send(
                format_message("You can't rizz-mock yourself or a bot"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return

        if self.target == target.id:
            self.target = None
            return

        self.target = target.id
        self.used_rizz_lines.clear()
        

    @commands.command(aliases=['srzm'])
    async def stoprizzmock(self, ctx):
        """Stop rizz-mocking"""
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

        self.used_rizz_lines.clear()
        self.sent_messages.clear()
        self.use_hashtag = False
        self.random_hashtag = False

        if self.target is not None:
            self.target = None

    @rate_limiter(command_only=True)
    async def send_rizz_mock(self, message: discord.Message, rizz_line: str) -> Optional[discord.Message]:
        """Send a rizz-mock reply with rate limiting"""
        try:
            max_length = get_max_message_length(self.bot)
            # Make one random decision for hashtags per message
            use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
            content = f"# {rizz_line}" if use_hashtag else rizz_line

            if len(content) > max_length:
                content = content[:max_length-3] + "..."

            sent_msg = await message.reply(content.strip())
            self.sent_messages[message.id] = sent_msg
            return sent_msg

        except discord.Forbidden as e:
            logger.error(f"Failed to send rizz-mock: {e}")
            # Stop rizz-mocking if failed
            self.target = None
            self.used_rizz_lines.clear()
            self.sent_messages.clear()
            return None

    async def _handle_message(self, message):
        """Handler for message events"""
        if not self.target or message.author.id != self.target:
            return

        if message.author.bot:
            return

        try:
            # Get available rizz lines
            available_lines = list(set(self.bot._manager.shared_rizz_lines) - self.used_rizz_lines)
            if not available_lines:
                self.used_rizz_lines.clear()
                available_lines = self.bot._manager.shared_rizz_lines.copy()

            # Pick a random rizz line
            rizz_line = random.choice(available_lines)
            self.used_rizz_lines.add(rizz_line)

            await self.send_rizz_mock(message, rizz_line)

        except Exception as e:
            logger.error(f"Error in rizz-mock: {e}")

    async def _handle_message_delete(self, message):
        """Handler for message delete events"""
        if message.id in self.sent_messages:
            try:
                our_msg = self.sent_messages[message.id]
                await our_msg.delete()
                del self.sent_messages[message.id]
            except (discord.NotFound, discord.HTTPException):
                pass

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)
            event_manager.register_handler('on_message_delete', self.__class__.__name__, self._handle_message_delete)

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)
            
        self.target = None
        self.used_rizz_lines.clear()
        self.sent_messages.clear()

async def setup(bot):
    await bot.add_cog(RizzMock(bot))
