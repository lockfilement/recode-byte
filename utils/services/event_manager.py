import discord
from discord.ext import commands
from typing import Dict, List, Callable, Any
import logging
import asyncio
from collections import defaultdict

logger = logging.getLogger(__name__)

class EventManager(commands.Cog):
    """Centralized event management system"""
    
    def __init__(self, bot):
        self.bot = bot
        # Dictionary to store event handlers for each event type
        # Format: {event_name: {cog_name: handler_function}}
        self.event_handlers = defaultdict(dict)

    def register_handler(self, event_name: str, cog_name: str, handler: Callable):
        """Register an event handler for a specific event"""
        self.event_handlers[event_name][cog_name] = handler
        logger.info(f"Registered {event_name} handler for {cog_name}")

    def unregister_handler(self, event_name: str, cog_name: str):
        """Unregister an event handler"""
        try:
            del self.event_handlers[event_name][cog_name]
            logger.info(f"Unregistered {event_name} handler for {cog_name}")
        except KeyError:
            pass

    def unregister_cog(self, cog_name: str):
        """Unregister all handlers for a cog"""
        for event_handlers in self.event_handlers.values():
            event_handlers.pop(cog_name, None)
        logger.info(f"Unregistered all handlers for {cog_name}")

    async def dispatch_event(self, event_name: str, *args, **kwargs):
        """Dispatch an event to all registered handlers"""
        if event_name not in self.event_handlers:
            return

        for cog_name, handler in self.event_handlers[event_name].items():
            try:
                await handler(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {cog_name} handling {event_name}: {e}")

    @commands.Cog.listener()
    async def on_ready(self):
        """Central ready event handler"""
        logger.info("Bot is ready! Dispatching on_ready event to registered handlers")
        await self.dispatch_event('on_ready')

    @commands.Cog.listener()
    async def on_message(self, message):
        """Central message event handler"""
        await self.dispatch_event('on_message', message)

    @commands.Cog.listener()
    async def on_message_delete(self, message):
        """Central message delete event handler"""
        await self.dispatch_event('on_message_delete', message)

    @commands.Cog.listener()
    async def on_message_edit(self, before, after):
        """Central message edit event handler"""
        await self.dispatch_event('on_message_edit', before, after)

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction, user):
        """Central reaction add event handler"""
        await self.dispatch_event('on_reaction_add', reaction, user)

    @commands.Cog.listener()
    async def on_member_update(self, before, after):
        """Central member update event handler"""
        await self.dispatch_event('on_member_update', before, after)

    @commands.Cog.listener()
    async def on_user_update(self, before, after):
        """Central user update event handler"""
        await self.dispatch_event('on_user_update', before, after)

    @commands.Cog.listener()
    async def on_presence_update(self, before, after):
        """Central presence update event handler"""
        await self.dispatch_event('on_presence_update', before, after)

    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        """Central voice state update event handler"""
        await self.dispatch_event('on_voice_state_update', member, before, after)

    @commands.Cog.listener()
    async def on_member_join(self, member):
        """Central member join event handler"""
        await self.dispatch_event('on_member_join', member)

    @commands.Cog.listener()
    async def on_relationship_update(self, before, after):
        """Central relationship update event handler"""
        await self.dispatch_event('on_relationship_update', before, after)

    @commands.Cog.listener()
    async def on_relationship_add(self, relationship):
        """Central relationship add event handler"""
        await self.dispatch_event('on_relationship_add', relationship)

    @commands.Cog.listener()
    async def on_relationship_remove(self, relationship):
        """Central relationship remove event handler"""
        await self.dispatch_event('on_relationship_remove', relationship)

async def setup(bot):
    await bot.add_cog(EventManager(bot))
