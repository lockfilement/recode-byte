import discord
from discord.ext import commands
import asyncio
import random
import shlex
from utils.rate_limiter import rate_limiter
from utils.general import get_max_message_length, format_message

class Spam(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.is_spamming = False 

    @rate_limiter(command_only=True)
    async def send_message(self, ctx, content, use_delete, channel=None):
        target_channel = channel if channel else ctx.channel
        msg = await target_channel.send(content)
        if use_delete:
            try:
                await msg.delete()
            except discord.HTTPException:
                pass

    @commands.command(aliases=['s'])
    async def spam(self, ctx, amount: int, *, content: str):
        """Spam messages
        spam 5 hello - Basic spam
        spam 10 test -max -delete - Max length with delete
        spam 5 -multi "hi" "hey" - Multiple messages"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        if amount < 1:
            await ctx.send(format_message("Amount must be at least 1"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            return

        # Initialize flags and parameters
        use_max = False
        use_delete = False
        use_multi = False
        use_random = False
        delay = 0
        channel_id = None
        channel = None
        messages = []
        
        try:
            # Parse the content with proper quoting support
            try:
                # Try to split keeping quotes intact
                args = shlex.split(content)
            except ValueError:
                # Fallback if quotes are mismatched
                args = content.split()
            
            # Process flags and extract message content
            i = 0
            while i < len(args):
                arg = args[i].lower()
                
                # Process flags
                if arg == '-max':
                    use_max = True
                    args.pop(i)
                    continue
                    
                elif arg == '-delete':
                    use_delete = True
                    args.pop(i)
                    continue
                    
                elif arg == '-multi':
                    use_multi = True
                    args.pop(i)
                    continue
                    
                elif arg == '-r':
                    use_random = True
                    args.pop(i)
                    continue
                    
                elif arg == '-d' and i + 1 < len(args):
                    try:
                        delay = float(args[i + 1])
                        args.pop(i)  # Remove -d
                        args.pop(i)  # Remove value
                        continue
                    except ValueError:
                        # Not a valid number, treat as regular text
                        i += 1
                        continue
                        
                elif arg == '-c' and i + 1 < len(args):
                    try:
                        channel_id = int(args[i + 1])
                        args.pop(i)  # Remove -c
                        args.pop(i)  # Remove value
                        continue
                    except ValueError:
                        # Not a valid number, treat as regular text
                        i += 1
                        continue
                
                # If we reach here, it's part of the message content
                i += 1
            
            # Get channel if channel_id is provided
            if channel_id:
                try:
                    channel = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
                    if not channel:
                        await ctx.send(format_message(f"Could not find channel with ID {channel_id}"), 
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                        return
                except discord.HTTPException:
                    await ctx.send(format_message(f"Error accessing channel with ID {channel_id}"), 
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                    return
            
            # Handle message content based on mode
            if use_multi:
                # In multi-message mode, each remaining arg is a separate message
                messages = args
                if not messages:
                    await ctx.send(format_message("No messages provided for multi-message mode"), 
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                    return
            else:
                # In single message mode, join remaining args as the message
                message = ' '.join(args)
                
                if use_max:
                    # Get max message length for the user
                    max_length = get_max_message_length(self.bot)
                    
                    # Calculate how many times we can repeat the content with spaces
                    base_content = message + " "  # Add space between repetitions
                    repeats = (max_length - 1) // len(base_content) if len(base_content) > 0 else 0  # -1 to be safe
                    
                    # Create maximized message
                    message = base_content * max(1, repeats)  # Ensure at least one repetition
                    message = message[:max_length].strip()  # Ensure we don't exceed limit
                
                if not message:
                    await ctx.send(format_message("Message content cannot be empty"), 
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                    return
            
            self.is_spamming = True
            sent_count = 0
            
            try:
                # Apply initial delay if specified
                if delay > 0 and self.is_spamming:
                    await asyncio.sleep(delay)
                
                if use_multi and messages:
                    # Multi-message mode
                    msg_index = 0  # For rotate mode
                    for i in range(amount):
                        if not self.is_spamming:
                            break
                            
                        try:
                            # Select message based on mode
                            if use_random:
                                msg = random.choice(messages)
                            else:
                                # Default multi-mode rotates through messages
                                msg = messages[msg_index]
                                msg_index = (msg_index + 1) % len(messages)  # Move to next message or wrap around
                            
                            # Send the message
                            await self.send_message(ctx, msg, use_delete, channel)
                            sent_count += 1
                            
                            # Apply delay if specified and not the last message
                            if delay > 0 and i < amount - 1 and self.is_spamming:
                                await asyncio.sleep(delay)

                        except discord.HTTPException as e:
                            if e.status == 429: # Rate limited
                                await asyncio.sleep(5)
                                continue
                            else:
                                break
                else:
                    # Standard single message mode
                    for i in range(amount):
                        if not self.is_spamming:
                            break
                        
                        try:
                            await self.send_message(ctx, message, use_delete, channel)
                            sent_count += 1
                            
                            # Apply delay if specified and not the last message
                            if delay > 0 and i < amount - 1 and self.is_spamming:
                                await asyncio.sleep(delay)

                        except discord.HTTPException as e:
                            if e.status == 429: # Rate limited
                                await asyncio.sleep(5)
                                continue
                            else:
                                break

            except Exception as e:
                print(f"Error in spam command: {e}")
            finally:
                self.is_spamming = False
                
        except Exception as e:
            # Catch any errors in the parsing/setup phase
            await ctx.send(format_message(f"Error processing spam command: {e}"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            self.is_spamming = False

    @commands.command(aliases=['ss']) 
    async def sspam(self, ctx):
        """Stop ongoing spam"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        if self.is_spamming:
            self.is_spamming = False
            await ctx.send(format_message("Stopped spamming"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        else:
            await ctx.send(format_message("No active spam to stop"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

async def setup(bot):
    await bot.add_cog(Spam(bot))
