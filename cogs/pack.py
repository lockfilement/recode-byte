import discord
from discord.ext import commands
import asyncio
import random
from typing import List
from utils.rate_limiter import rate_limiter
from utils.general import get_max_message_length, format_message, quote_block
import logging
import time

logger = logging.getLogger(__name__)

class Pack(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.current_target = None
        self.pack_task = None
        self.insults: List[str] = []
        self.sent_insults: set = set()
        self.use_hashtag = False
        self.single_line = False
        self.use_ladder = False  # Add ladder mode flag
        self.random_ladder = False  # Add new flag for random ladder mode
        self.random_hashtag = False  # Add new flag for random hashtag mode
        self.manual_mode = False  # Add manual mode flag
        self.sentences_sent = 0  # Counter for manual mode sentence tracking

    async def cog_unload(self):
        if self.pack_task:
            self.pack_task.cancel()

    async def send_temp_message(self, ctx: commands.Context, content: str) -> None:
        await ctx.send(
            format_message(content),
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )

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


    async def should_reply_to_message(self, ctx, target_user):
        """Determine if we should reply to the latest message after 3-4 sentences"""
        if not self.manual_mode:
            return None, False
            
        if self.sentences_sent >= random.randint(5, 7):
            recent_messages = await self.get_recent_user_messages(ctx.channel, target_user, 1)
            if recent_messages:
                self.sentences_sent = 0  # Reset counter
                return recent_messages[0], True  # Always get the latest message
        
        return None, False

    async def should_ping_randomly(self):
        """Determine if we should ping randomly instead of always pinging"""
        if not self.manual_mode:
            return True  # Always ping in normal modes
        # Ping rarely - 10% chance
        return random.choices([True, False], weights=[1, 9])[0]

    @rate_limiter(global_only=True)
    async def send_pack(self, ctx: commands.Context, target: discord.User, pack_lines: List[str]) -> None:
        try:
            max_length = get_max_message_length(self.bot)
            content = ""  # Remove the mention from the beginning
            
            # Manual mode features
            if self.manual_mode:
                # Check if we should reply to a message (manual mode only)
                reply_message, should_reply = await self.should_reply_to_message(ctx, target)
                # Check if we should ping randomly (manual mode only)
                should_ping = await self.should_ping_randomly()
            else:
                # Default behavior for all other modes
                reply_message, should_reply = None, False
                should_ping = True
            
            # Make one random decision for hashtags per message
            use_hashtag = self.use_hashtag or (self.random_hashtag and random.choice([True, False]))
            
            if self.single_line:
                line = pack_lines[0]
                # For random ladder mode, randomly decide whether to ladder this message
                use_ladder_this_time = self.use_ladder or (self.random_ladder and random.choice([True, False]))
                
                if use_ladder_this_time:
                    # Split into words and prepare laddered format
                    words = line.split()
                    laddered_content = ""
                    
                    # Add hashtag to each word if enabled
                    for word in words:
                        formatted_word = f"# {word}" if use_hashtag else word
                        if len(laddered_content) + len(formatted_word) + 1 <= max_length:
                            laddered_content += f"{formatted_word}\n"
                        else:
                            break
                    
                    # Add mention at the end of the ladder if should ping
                    if should_ping and len(laddered_content) + len(target.mention) + 1 <= max_length:
                        laddered_content += f"{target.mention}"
                            
                    if should_reply and reply_message:
                        await reply_message.reply(laddered_content)
                    else:
                        await ctx.send(laddered_content)
                else:
                    # Normal single line mode
                    formatted_line = f"# {line}" if use_hashtag else line
                    content += formatted_line
                    # Add mention at the end of the line if should ping
                    if should_ping:
                        content += f" {target.mention}"
                    
                    if should_reply and reply_message:
                        await reply_message.reply(content)
                    else:
                        await ctx.send(content)
            else:
                # Multiple lines mode
                for line in pack_lines:
                    formatted_line = f"# {line}\n" if use_hashtag else f"{line}\n"
                    if len(content) + len(formatted_line) >= max_length:
                        break
                    content += formatted_line
                
                if len(content) > max_length:
                    mention_length = len(target.mention) if should_ping else 0
                    content = content[:max_length - 3 - mention_length] + "..."
                
                # Add mention at the end of the pack if should ping
                if should_ping:
                    content += f" {target.mention}"
                    
                if should_reply and reply_message:
                    await reply_message.reply(content)
                else:
                    await ctx.send(content)
            
            # Update sentence counter for manual mode
            if self.manual_mode:
                if self.single_line:
                    self.sentences_sent += 1
                else:
                    self.sentences_sent += len(pack_lines)
            
        except (discord.HTTPException, discord.Forbidden, discord.NotFound) as e:
            # Handle 403 Forbidden errors (missing access)
            logger.error(f"{e}")
            # Cancel the packing task
            if self.pack_task and not self.pack_task.done():
                self.pack_task.cancel()
            self.current_target = None
            return
                
        except Exception as e:
            logger.error(f"Error in send_pack: {e}")

    async def continuous_pack(self, ctx: commands.Context, target: discord.User) -> None:
        """Continuously send packs of insults to the target without repeats"""
        try:
            while self.current_target == target.id:
                # Get available insults
                available_insults = list(set(self.bot._manager.shared_insults) - self.sent_insults)
                if len(available_insults) < (2 if self.single_line else 12):
                    self.sent_insults.clear()
                    available_insults = self.bot._manager.shared_insults.copy()

                # Generate pack size based on mode
                if self.single_line:
                    pack_size = 1
                else:
                    pack_size = random.randint(4, 12)
                    
                pack_lines = random.sample(available_insults, pack_size)
                
                # Add used insults to sent set
                self.sent_insults.update(pack_lines)
                
                await self.send_pack(ctx, target, pack_lines)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in continuous pack: {e}")

    @commands.command(aliases=['pc'])
    async def pack(self, ctx: commands.Context, *args) -> None:
        """Start packing a user with insults
        .pack [#|s|l|r|rh|mn] <user>
        # - Add hashtags
        s - Single line mode
        l - Ladder mode
        r - Random ladder mode
        rh - Random hashtag mode
        mn - Manual mode (random replies, random pinging)"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        # Initialize flags
        self.use_hashtag = False
        self.single_line = False
        self.use_ladder = False
        self.random_ladder = False
        self.random_hashtag = False
        self.manual_mode = False
        self.sentences_sent = 0  # Reset sentence counter
        target = None

        # Parse arguments
        args = list(args)
        while args:
            arg = args.pop(0)
            if arg == '#':
                self.use_hashtag = True
            elif arg.lower() == 's':
                self.single_line = True
            elif arg.lower() == 'l':
                self.single_line = True
                self.use_ladder = True
            elif arg.lower() == 'r':
                self.single_line = True
                self.random_ladder = True
            elif arg.lower() == 'rh':
                self.random_hashtag = True
            elif arg.lower() == 'mn':
                self.manual_mode = True
            else:
                # Assume the first non-flag argument is the user
                target = await commands.UserConverter().convert(ctx, arg)
                break

        if target is None:
            await self.send_temp_message(ctx, "You need to specify a user.")
            return
            
        if target.id == ctx.author.id or target.bot:
            await self.send_temp_message(ctx, "You cannot use this command on yourself or bots.")
            return

        if self.pack_task and not self.pack_task.done():
            self.pack_task.cancel()
            await asyncio.sleep(0.1)

        self.current_target = target.id
        self.pack_task = asyncio.create_task(self.continuous_pack(ctx, target))

    @commands.command(aliases=['spc'])
    async def spack(self, ctx: commands.Context):
        """Stop packing the current target"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        if self.pack_task and not self.pack_task.done():
            self.pack_task.cancel()
            # Clear sent insults and reset sentence counter
            self.sent_insults.clear()
            self.sentences_sent = 0

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Pack(bot))
