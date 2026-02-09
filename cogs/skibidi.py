import discord
from discord.ext import commands
import asyncio
from utils.rate_limiter import rate_limiter
from utils.general import format_message, quote_block
import logging
import random

logger = logging.getLogger(__name__)

class Skibidi(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.current_target = None
        self.skibidi_task = None
        self.sent_skibidi_lines = set()
        self.use_hashtag = False
        self.random_hashtag = False

    async def cog_unload(self) -> None:
        if self.skibidi_task:
            self.skibidi_task.cancel()

    async def send_temp_message(self, ctx: commands.Context, content: str) -> None:
        await ctx.send(
            format_message(content),
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )

    @rate_limiter(global_only=True)
    async def send_skibidi_line(self, ctx: commands.Context, target: discord.User, skibidi_line: str) -> None:
        try:
            # Determine whether to use hashtag for the entire message
            use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
            message = f"# {target.mention} {skibidi_line}" if use_hashtag else f"{target.mention} {skibidi_line}"
            await ctx.send(message)
            self.sent_skibidi_lines.add(skibidi_line)

        except (discord.HTTPException, discord.Forbidden) as e:
            logger.error(f"Failed to send skibidi line: {e}")
            # Stop sending skibidi lines if failed
            self.current_target = None
            if self.skibidi_task and not self.skibidi_task.done():
                self.skibidi_task.cancel()
                self.skibidi_task = None
                # Clear sent skibidi lines
                self.sent_skibidi_lines.clear()

    async def continuous_skibidi(self, ctx: commands.Context, target: discord.User) -> None:
        """Continuously send skibidi lines to the target without repeats."""
        try:
            while self.current_target == target.id:
                skibidi_lines = self.bot._manager.shared_skibidi_lines  # Use shared lines
                if skibidi_lines:
                    available_lines = list(set(skibidi_lines) - self.sent_skibidi_lines)
                    if not available_lines:
                        self.sent_skibidi_lines.clear()
                        available_lines = skibidi_lines.copy()
                    skibidi_line = random.choice(available_lines)
                    await self.send_skibidi_line(ctx, target, skibidi_line)
                else:
                    await self.send_temp_message(ctx, "No skibidi lines available")
                    break
        except asyncio.CancelledError:
            pass

    @commands.command(aliases=['sk'])
    async def skibidi(self, ctx: commands.Context, *args) -> None:
        """Send skibidi lines to a user
        .skibidi [#/rh] <user>
        # - Add hashtags
        rh - Random hashtags"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        # Initialize flags and target
        target = None
        self.use_hashtag = False
        self.random_hashtag = False

        # Parse arguments
        args = list(args)
        while args:
            arg = args.pop(0)
            if arg == '#':
                self.use_hashtag = True
            elif arg.lower() == 'rh':
                self.random_hashtag = True
            else:
                # Try to convert to user
                try:
                    target = await commands.UserConverter().convert(ctx, arg)
                except:
                    await self.send_temp_message(ctx, "Invalid user specified.")
                    return
                break

        if target is None:
            await self.send_temp_message(ctx, "You need to specify a user.")
            return

        if isinstance(ctx.channel, discord.DMChannel) and target is None:
            await self.send_temp_message(ctx, "You need to specify a user ID in DMs.")
            return

        if target.bot:
            await self.send_temp_message(ctx, "You cannot use this command on bots.")
            return

        if self.skibidi_task and not self.skibidi_task.done():
            self.skibidi_task.cancel()
            await asyncio.sleep(0.1)

        self.current_target = target.id
        self.skibidi_task = asyncio.create_task(self.continuous_skibidi(ctx, target))

    @commands.command(aliases=['ssk'])
    async def sskibidi(self, ctx: commands.Context):
        """Stop sending skibidi lines"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        if self.skibidi_task and not self.skibidi_task.done():
            self.skibidi_task.cancel()
            self.skibidi_task = None
            # Clear sent skibidi lines
            self.sent_skibidi_lines.clear()
            self.current_target = None

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Skibidi(bot))
