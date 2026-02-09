import discord
import asyncio
import logging
from typing import List

logger = logging.getLogger('LeakCheck.MessageManager')

class MessageManager:
    """Handles message management and cleanup"""
    def __init__(self, bot: discord.Client):
        self.bot = bot

    async def schedule_deletion(self, messages: List[discord.Message], delay: float):
        """Schedule messages for deletion after delay"""
        if not messages:
            return

        await asyncio.sleep(delay)
        for message in messages:
            try:
                await message.delete()
            except discord.errors.NotFound:
                pass  # Message already deleted
            except discord.errors.Forbidden:
                logger.warning(f"Cannot delete message {message.id}: Missing permissions")
            except Exception as e:
                logger.error(f"Error deleting message {message.id}: {e}")
