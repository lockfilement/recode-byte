import discord
from discord.ext import commands
import logging
from utils.general import format_message, quote_block
from utils.rate_limiter import rate_limiter

logger = logging.getLogger(__name__)

class Poll(commands.Cog):
    """A cog to vote on Discord polls"""
    
    def __init__(self, bot):
        self.bot = bot
    
    @commands.command(aliases=['v'])
    @rate_limiter(command_only=True)
    async def vote(self, ctx, answer_id: str, channel_id: str, message_id: str):
        """Vote on a poll
        
        .vote <answer_id> <channel_id> <message_id>
        
        answer_id: The ID of the poll option to vote for (or 0 to unvote)
        channel_id: The ID of the channel containing the poll
        message_id: The ID of the poll message
        
        Example:
        .vote 1 1234567890 1234567891 - Vote for option 1
        .vote 0 1234567890 1234567891 - Remove your vote
        """
        try:
            # Delete the command message to avoid detection
            await ctx.message.delete()
        except:
            pass
        
        try:
            # Build payload - empty string for answer_id will remove the vote
            if answer_id == "0":
                # Unvote payload uses empty array
                payload = {"answer_ids": []}
            else:
                # Vote payload includes the answer ID
                payload = {"answer_ids": [answer_id]}
            
            # Use the bot's HTTP handler to make the PUT request
            url = f'/channels/{channel_id}/polls/{message_id}/answers/@me'
            
            # Make the PUT request
            try:
                await self.bot.http.request(
                    discord.http.Route('PUT', url), 
                    json=payload
                )
                
                # # Send success message with auto-delete
                # if answer_id == "0":
                #     await ctx.send(
                #         format_message(f"Successfully removed vote from poll in channel {channel_id}, message {message_id}"),
                #         delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                #     )
                # else:
                #     await ctx.send(
                #         format_message(f"Successfully voted for option {answer_id} in poll in channel {channel_id}, message {message_id}"),
                #         delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                #     )
            
            except discord.HTTPException as e:
                # Handle errors - send error message to user
                error_message = f"Failed to vote: {e.status} {e.text}"
                await ctx.send(
                    format_message(error_message),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                logger.error(f"Poll vote error: {e}")
        
        except Exception as e:
            # Handle any other exceptions
            await ctx.send(
                format_message(f"Error processing poll vote command: {str(e)}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            logger.error(f"Unexpected error in poll vote command: {e}")

async def setup(bot):
    await bot.add_cog(Poll(bot))
