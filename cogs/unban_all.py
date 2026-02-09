# cogs/unban_all.py
import discord
from discord.ext import commands
from utils.rate_limiter import rate_limiter
from utils.general import format_message, quote_block

class UnbanAll(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command()
    @commands.has_permissions(administrator=True)
    @rate_limiter(command_only=True)
    async def unban_all(self, ctx):
        """Unban all users in the server
        
        unban_all - Unban all users (requires admin)
        """
        try:await ctx.message.delete()
        except:pass
        
        bans = [ban async for ban in ctx.guild.bans()]
        if not bans:
            await ctx.send(
                format_message("There are no banned users in this server."), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return

        await ctx.send(
            format_message(f"Starting to unban {len(bans)} users..."), 
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )
        
        unbanned_count = 0
        for ban_entry in bans:
            user = ban_entry.user
            try:
                await ctx.guild.unban(user)
                unbanned_count += 1
            except discord.HTTPException as e:
                await ctx.send(
                    format_message(f"Failed to unban {user.name}: {e}"), 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )

        await ctx.send(
            format_message(f"Unbanned {unbanned_count} users."), 
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )

async def setup(bot):
    await bot.add_cog(UnbanAll(bot))