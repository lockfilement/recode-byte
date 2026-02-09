import discord
from discord.ext import commands
import asyncio
from utils.rate_limiter import rate_limiter
import logging
import random
from utils.general import format_message, quote_block

logger = logging.getLogger(__name__)

class Rizz(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.current_target = None
        self.rizz_task = None
        self.sent_rizz_lines = set()  # Add this line
        self.use_hashtag = False  # Add this flag
        self.random_hashtag = False  # Add random hashtag flag

    async def cog_unload(self) -> None:
        if self.rizz_task:
            self.rizz_task.cancel()

    async def send_temp_message(self, ctx: commands.Context, content: str) -> None:
        await ctx.send(
            format_message(content),
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )

    @rate_limiter(global_only=True)
    async def send_rizz_line(self, ctx: commands.Context, target: discord.User, rizz_line: str) -> None:
        try:
            # Determine whether to use hashtag for the entire message
            use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
            message = f"# {target.mention} {rizz_line}" if use_hashtag else f"{target.mention} {rizz_line}"
            await ctx.send(message)
            self.sent_rizz_lines.add(rizz_line)

        except (discord.HTTPException, discord.Forbidden) as e:
            logger.error(f"Failed to send rizz line: {e}")
            # Stop sending rizz lines if failed
            self.current_target = None
            if self.rizz_task and not self.rizz_task.done():
                self.rizz_task.cancel()
                self.rizz_task = None
                # Clear sent rizz lines
                self.sent_rizz_lines.clear()

    async def continuous_rizz(self, ctx: commands.Context, target: discord.User) -> None:
        """Continuously send rizz lines to the target without repeats."""
        try:
            while self.current_target == target.id:
                rizz_lines = self.bot._manager.shared_rizz_lines  # Use shared lines
                if rizz_lines:
                    available_lines = list(set(rizz_lines) - self.sent_rizz_lines)
                    if not available_lines:
                        self.sent_rizz_lines.clear()
                        available_lines = rizz_lines.copy()
                    rizz_line = random.choice(available_lines)
                    await self.send_rizz_line(ctx, target, rizz_line)
                else:
                    await self.send_temp_message(ctx, "No rizz lines available")
                    break
        except asyncio.CancelledError:
            pass

    @commands.command(aliases=['rz'])
    async def rizz(self, ctx: commands.Context, *args) -> None:
        """Send rizz lines to a user
        .rizz [#/rh] <user>
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

        if self.rizz_task and not self.rizz_task.done():
            self.rizz_task.cancel()
            await asyncio.sleep(0.1)

        self.current_target = target.id
        self.rizz_task = asyncio.create_task(self.continuous_rizz(ctx, target))

    @commands.command(aliases=['srz'])
    async def srizz(self, ctx: commands.Context):
        """Stop sending rizz lines"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        if self.rizz_task and not self.rizz_task.done():
            self.rizz_task.cancel()
            self.rizz_task = None
            # Clear sent rizz lines
            self.sent_rizz_lines.clear()
            self.current_target = None

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Rizz(bot))
