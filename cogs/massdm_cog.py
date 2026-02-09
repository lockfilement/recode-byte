import discord
from discord.ext import commands
import asyncio
from utils.rate_limiter import rate_limiter
import logging
from utils.general import format_message, quote_block

logger = logging.getLogger(__name__)

class MassDM(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.is_dming = False

    @commands.command(aliases=['mdm'])
    async def massdm(self, ctx, *, message: str):
        """DM all friends
        massdm hello - Send message to all friends"""
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

        if self.is_dming:
            await ctx.send(
                format_message("Already sending mass DMs. Use .stopmdm to stop."), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return

        self.is_dming = True
        sent_count = 0
        failed_count = 0

        status_msg = await ctx.send(
            format_message("Starting mass DM...")
        )

        for friend in self.bot.friends:
            if not self.is_dming:
                break

            try:
                await self.send_dm(friend, message)
                sent_count += 1

                if sent_count % 5 == 0:  # Update status every 5 messages
                    await status_msg.edit(
                        content=format_message(f"Sent {sent_count} DMs...")
                    )

            except Exception as e:
                logger.error(f"Failed to DM {friend}: {e}")
                failed_count += 1
                continue

        self.is_dming = False
        await status_msg.edit(
            content=format_message(f"Mass DM completed. Sent: {sent_count}, Failed: {failed_count}")
        )

    @commands.command(aliases=['smdm'])
    async def stopmdm(self, ctx):
        """Stop ongoing mass DM"""
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

        if self.is_dming:
            self.is_dming = False
            await ctx.send(
                format_message("Stopping mass DM..."),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
        else:
            await ctx.send(
                format_message("No mass DM in progress"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

    @rate_limiter(global_only=True)
    async def send_dm(self, user: discord.User, content: str):
        """Send DM with rate limiting"""
        try:
            # Get the actual User object from the relationship
            user_id = user.user.id if hasattr(user, 'user') else user.id
            user_obj = self.bot.get_user(user_id)
            
            if user_obj:
                dm_channel = await user_obj.create_dm()
                await dm_channel.send(content)
                await asyncio.sleep(0.5)  # Small delay between messages
            else:
                raise Exception("Could not get user object")
                
        except discord.HTTPException as e:
            if e.code == 50007:  # Cannot send messages to this user
                raise Exception("Cannot send DMs to this user")
            elif e.status == 429:  # Rate limited
                await asyncio.sleep(5)
                return await self.send_dm(user, content)
            else:
                raise e

async def setup(bot):
    await bot.add_cog(MassDM(bot))