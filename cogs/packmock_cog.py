import random
import discord
from discord.ext import commands
from utils.rate_limiter import rate_limiter
from utils.general import get_max_message_length, format_message, quote_block
from typing import Optional
import logging
import asyncio
import time

logger = logging.getLogger(__name__)

class PackMock(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.target = None
        self.sent_messages = {}  # Track {original_msg_id: our_msg}
        self.used_insults = set()
        self.use_hashtag = False  # Add this flag
        self.use_ladder = False  # Add ladder mode flag
        self.random_ladder = False  # Add new flag for random ladder mode
        self.random_hashtag = False  # Add new flag for random hashtag mode
        self.manual_mode = False  # Add manual mode flag
        self.sentences_sent = 0  # Counter for manual mode sentence tracking

    async def get_recent_user_messages(self, channel, target_user, limit=10):
        """Get recent messages from target user in the channel"""
        try:
            messages = []
            async for message in channel.history(limit=20):
                if message.author.id == target_user.id and len(messages) < limit:
                    messages.append(message)
                elif len(messages) >= limit:
                    break
            return messages
        except:
            return []


    async def should_reply_to_different_message(self, channel, target_user, current_message):
        """Determine if we should reply to the current message instead of a different one"""
        if not self.manual_mode:
            return None, False
            
        if self.sentences_sent >= random.randint(5, 7):
            self.sentences_sent = 0  # Reset counter
            return current_message, True  # Always reply to the current message
        
        return None, False

    async def should_ping_randomly(self):
        """Determine if we should ping randomly instead of never pinging"""
        if not self.manual_mode:
            return False  # Never ping in normal modes (packmock just replies)
        # Ping rarely in manual mode - 10% chance
        return random.choices([True, False], weights=[1, 9])[0]

    async def get_manual_mode_action(self):
        """Determine what action to take in manual mode: reply or send"""
        if not self.manual_mode:
            return "reply"  # Always reply in normal modes
        
        # Manual mode distribution:
        # 15% - Reply to the message
        # 85% - Send as regular message 
        choices = ["reply", "send"]
        weights = [1.5, 8.5]
        return random.choices(choices, weights=weights)[0]
        
    @commands.command(aliases=['pmc'])
    async def packmock(self, ctx, *args):
        """Pack-mock a user
        
        packmock [#/l/r/rh/mn] <user> - Mock with options
        # - Add hashtags
        l - Ladder mode
        r - Random ladder mode
        rh - Random hashtag mode
        mn - Manual mode (random replies, reactive to target messages)
        """
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass
    
        # Parse arguments
        target = None
        self.use_hashtag = False
        self.use_ladder = False
        self.random_ladder = False
        self.random_hashtag = False
        self.manual_mode = False
        self.sentences_sent = 0  # Reset sentence counter
        
        args = list(args)
        while args:
            arg = args[0]
            if arg == '#':
                self.use_hashtag = True
                args.pop(0)
            elif arg.lower() == 'l':
                self.use_ladder = True
                args.pop(0)
            elif arg.lower() == 'r':
                self.random_ladder = True
                args.pop(0)
            elif arg.lower() == 'rh':
                self.random_hashtag = True
                args.pop(0)
            elif arg.lower() == 'mn':
                self.manual_mode = True
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
        
        # add self mocking check and bot mocking check
        if target.id == ctx.author.id or target.bot:
            await ctx.send(
                format_message("You can't pack-mock yourself or a bot"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
    
        if self.target == target.id:
            self.target = None
            return
    
        self.target = target.id
        self.used_insults.clear()
        

    @commands.command(aliases=['spmc'])
    async def stoppackmock(self, ctx):
        """Stop pack-mocking"""
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

        self.used_insults.clear()
        self.sent_messages.clear()
        self.use_hashtag = False
        self.use_ladder = False
        self.random_ladder = False
        self.random_hashtag = False
        self.manual_mode = False
        self.sentences_sent = 0
        if self.target is not None:
            self.target = None

    @rate_limiter(command_only=True)
    async def send_pack_mock(self, message: discord.Message, insult: str) -> Optional[discord.Message]:
        """Send a pack-mock reply with rate limiting"""
        try:
            # Determine action in manual mode (reply/send)
            action = await self.get_manual_mode_action()

            # Manual mode features
            if self.manual_mode:
                # Check if we should reply to a different message instead (only for replies)
                if action == "reply":
                    different_message, should_reply_different = await self.should_reply_to_different_message(
                        message.channel, message.author, message
                    )
                else:
                    different_message, should_reply_different = None, False
                
                # Only ping when sending regular messages, never when replying
                should_ping = (action == "send") and await self.should_ping_randomly()
            else:
                # Default behavior for all other modes
                different_message, should_reply_different = None, False
                should_ping = False

            # Determine which message to reply to
            reply_to_message = different_message if should_reply_different else message

            # Get max message length for the user
            max_length = get_max_message_length(self.bot)
            content = ""
    
            # Determine if we should use ladder for this message
            use_ladder_this_time = self.use_ladder or (self.random_ladder and random.choice([True, False]))
    
            if use_ladder_this_time:
                # Split insult into words for ladder mode
                use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
                words = insult.split()
                for word in words:
                    formatted_word = f"# {word}" if use_hashtag else word
                    if len(content) + len(formatted_word) + 1 <= max_length:
                        content += f"{formatted_word}\n"
                    else:
                        break
                
                # Add ping in manual mode if should_ping is True
                if should_ping:
                    ping_text = f"{message.author.mention}"
                    if len(content) + len(ping_text) <= max_length:
                        content += ping_text
            else:
                # Regular mode
                use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
                formatted_insult = f"# {insult}" if use_hashtag else insult
                
                # Add ping in manual mode if should_ping is True
                if should_ping:
                    ping_text = f" {message.author.mention}"
                    if len(formatted_insult) + len(ping_text) <= max_length:
                        formatted_insult += ping_text
                    elif len(formatted_insult) >= max_length:
                        # Truncate to make room for ping
                        formatted_insult = formatted_insult[:max_length-3-len(ping_text)] + "..." + ping_text
                elif len(formatted_insult) >= max_length:
                    formatted_insult = formatted_insult[:max_length-3] + "..."
                    
                content = formatted_insult
    
            if action == "reply":
                sent_msg = await reply_to_message.reply(content.strip())
            else:  # action == "send"
                sent_msg = await message.channel.send(content.strip())
            
            self.sent_messages[message.id] = sent_msg
            
            # Update sentence counter for manual mode
            if self.manual_mode:
                self.sentences_sent += 1
                
            return sent_msg
    
        except discord.Forbidden as e:
            logger.error(f"Failed to send pack-mock: {e}")
            # Stop pack-mocking if failed
            self.target = None
            self.used_insults.clear()
            self.sent_messages.clear()
            self.manual_mode = False
            self.sentences_sent = 0
            return None

    async def _handle_message(self, message):
        """Handler for message events"""
        if not self.target or message.author.id != self.target:
            return
    
        if message.author.bot:
            return
    
        try:
            # Get available insults
            available_insults = list(set(self.bot._manager.shared_insults) - self.used_insults)
            if not available_insults:
                self.used_insults.clear()
                available_insults = self.bot._manager.shared_insults.copy()
    
            # Pick a random insult
            insult = random.choice(available_insults)
            self.used_insults.add(insult)
    
            await self.send_pack_mock(message, insult)
    
        except Exception as e:
            logger.error(f"Error in pack-mock: {e}")

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
        self.used_insults.clear()
        self.sent_messages.clear()
        self.manual_mode = False
        self.sentences_sent = 0

async def setup(bot):
    await bot.add_cog(PackMock(bot))
