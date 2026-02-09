# filepath: c:\Users\brian\Desktop\discord.py-self bot\cogs\ladder_cog.py
import discord
from discord.ext import commands
import asyncio
from utils.rate_limiter import rate_limiter
import random
import time
from utils.general import format_message, quote_block, get_max_message_length

class Ladder(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.use_hashtag = False
        self.random_hashtag = False
        self.listen_mode = False
        self.author_id = None
        self.currently_processing = False  # Flag to indicate we're currently processing a message
        self.feedback_messages = []  # Track feedback message content to ignore them
        self.last_feedback_time = 0  # Timestamp of the last feedback message

    # @rate_limiter(command_only=True)
    async def send_ladder_step(self, ctx, message):
        # Make new random hashtag decision for each step
        use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
        
        # Format the message using the formatting utility, without code block
        # If hashtag is used, prepend it before formatting
        if use_hashtag:
            final_message = f"# {message}"
        else:
            final_message = format_message(message, code_block=False, escape_backticks=False)
        
        await ctx.send(final_message)

    @commands.command(aliases=['ld'])
    async def ladder(self, ctx, *, message: str = None):
        """Ladderize your message
        
        .ladder <message> - Normal ladder
        .ladder # <message> - Ladder with hashtags
        .ladder rh <message> - Ladder with random hashtags
        .ladder on - Turn on listening mode (ladderizes all your messages)
        .ladder # on - Turn on listening mode with hashtags
        .ladder rh on - Turn on listening mode with random hashtags
        .ladder off - Turn off listening mode
        Use quotes for phrases: "hello there" world"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        # Parse message for flags first
        if message:
            message = message.strip()
            self.use_hashtag = False
            self.random_hashtag = False
            
            # Check for flags at start
            parts = message.split()
            while parts:
                if parts[0] == '#':
                    self.use_hashtag = True
                    parts.pop(0)
                elif parts[0].lower() == 'rh':
                    self.random_hashtag = True
                    parts.pop(0)
                else:
                    break
            message = ' '.join(parts)
        
        # Handle the on/off mode (after parsing flags)
        if message and message.lower().strip() == "on":
            self.listen_mode = True
            self.author_id = ctx.author.id
            mode_description = ""
            if self.use_hashtag:
                mode_description = " with hashtags"
            elif self.random_hashtag:
                mode_description = " with random hashtags"
            
            # Create and store feedback message
            feedback_message = f"Ladder mode enabled{mode_description}. Your messages will be ladderized."
            # Store both raw and formatted versions of the feedback message
            self.feedback_messages.append(feedback_message)
            self.feedback_messages.append(quote_block(feedback_message))
            formatted_feedback = quote_block(feedback_message)
            # Update timestamp of last feedback message
            self.last_feedback_time = time.time()
            await ctx.send(formatted_feedback, delete_after=3)
            return
        elif message and message.lower().strip() == "off":
            self.listen_mode = False
            self.author_id = None
            
            # Create and store feedback message
            feedback_message = "Ladder mode disabled."
            # Store both raw and formatted versions of the feedback message
            self.feedback_messages.append(feedback_message)
            self.feedback_messages.append(quote_block(feedback_message))
            formatted_feedback = quote_block(feedback_message)
            # Update timestamp of last feedback message
            self.last_feedback_time = time.time()
            await ctx.send(formatted_feedback, delete_after=3)
            return
        elif not message:
            # Create and store feedback message
            feedback_message = "Please provide a message or use 'on'/'off' to toggle ladder mode."
            # Store both raw and formatted versions of the feedback message
            self.feedback_messages.append(feedback_message)
            self.feedback_messages.append(quote_block(feedback_message))
            formatted_feedback = quote_block(feedback_message)
            # Update timestamp of last feedback message
            self.last_feedback_time = time.time()
            await ctx.send(formatted_feedback, delete_after=3)
            return

        # Process regular ladder message
        await self.process_ladder_message(ctx, message)

    async def process_ladder_message(self, ctx, message):
        # Split by spaces but preserve quoted phrases
        parts = []
        current = []
        in_quotes = False
        
        for char in message:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ' ' and not in_quotes:
                if current:
                    parts.append(''.join(current))
                    current = []
            else:
                current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        # Clean up any empty quotes
        parts = [p.strip('"') for p in parts if p.strip('"')]

        # Get the max message length based on user's Nitro status
        max_length = get_max_message_length(self.bot)
        
        for part in parts:
            # Ensure part doesn't exceed max message length
            if len(part) > max_length:
                part = part[:max_length - 3] + "..."
                
            try:
                await self.send_ladder_step(ctx, part)
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited
                    await asyncio.sleep(5)
                    continue
                break

    async def _handle_message(self, message):
        """Handler for message events for ladder mode"""
        # Ignore if we're already processing another message or not in listen mode
        if self.currently_processing or not self.listen_mode:
            return
        
        # Ignore messages not from the author or if it's from a bot
        if message.author.id != self.author_id or message.author.bot:
            return
            
        # Ignore if it's within 2 seconds of sending a feedback message
        # This helps avoid processing the confirmation messages
        if time.time() - self.last_feedback_time < 2:
            return
            
        # Special check for feedback messages about ladder mode being turned on/off
        if "Ladder mode enabled" in message.content or "Ladder mode disabled" in message.content:
            return
            
        # Check if the message matches or contains any of our feedback messages
        for feedback in self.feedback_messages:
            if feedback in message.content or message.content in feedback:
                return
            
        # Keep the feedback messages list from growing too large
        if len(self.feedback_messages) > 10:
            self.feedback_messages = self.feedback_messages[-6:]  # Keep only the 6 most recent (3 raw + 3 formatted)
        
        # Ignore commands
        ctx = await self.bot.get_context(message)
        if ctx.valid or message.content.startswith(self.bot.command_prefix):
            return
            
        # Set the processing flag to prevent re-entry
        self.currently_processing = True
        
        try:
            # Create a mock context for processing
            ctx = await self.bot.get_context(message)
            
            # Only ladderize if more than one word
            words = message.content.split()
            if len(words) > 1:
                # Delete the original message
                await message.delete()
                await self.process_ladder_message(ctx, message.content)

        except Exception as e:
            print(f"Error in ladder listener: {e}")
        finally:
            # Reset the flag when we're done
            self.currently_processing = False

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        # Unregister from event manager
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)

async def setup(bot):
    await bot.add_cog(Ladder(bot))