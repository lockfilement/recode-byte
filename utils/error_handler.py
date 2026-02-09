import discord
from discord.ext import commands
import traceback
import logging

logger = logging.getLogger(__name__)

class ErrorHandler(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        """Handle command errors globally"""
        if isinstance(error, commands.CommandNotFound):
            return  # Ignore command not found errors
            
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You don't have permission to use this command!")
            return
            
        if isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument: {str(error)}")
            return
            
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing required argument: {error.param.name}")
            return
            
        # Log unexpected errors
        logger.error(f"Unexpected error in command {ctx.command}:")
        logger.error(''.join(traceback.format_exception(type(error), error, error.__traceback__)))
        
        # Notify user of unexpected error
        await ctx.send("An unexpected error occurred. Please try again later.")

async def setup(bot):
    await bot.add_cog(ErrorHandler(bot))
