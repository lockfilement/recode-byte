import discord
from discord.ext import commands
import asyncio
from utils.rate_limiter import rate_limiter
from utils.general import format_message, quote_block

class Purge(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.is_purging = False

    @rate_limiter(command_only=True)
    async def _delete_message(self, msg):
        """Helper method to delete messages with rate limiting"""
        await msg.delete()

    @commands.command(aliases=['p', 'clear'])
    async def purge(self, ctx, amount: str = "all", *args):
        """Delete messages
        purge 10 - Delete 10 recent messages
        purge all -e - Delete everyone's messages
        purge 50 -r - Delete oldest first"""
        try:await ctx.message.delete()
        except:pass
        self.is_purging = True
        deleted_count = 0
        messages_to_delete = []
        
        # Parse arguments
        channel_id = None
        silent = False
        reverse = False
        everyone = False
        
        for arg in args:
            # Handle flags with dash prefix
            if arg.startswith('-'):
                flag = arg.lower().strip('-')
                if flag in ['s', 'silent']:
                    silent = True
                elif flag in ['r', 'reverse']:
                    reverse = True
                elif flag in ['e', 'everyone']:
                    everyone = True
            else:
                # Try to interpret non-flag arg as channel_id
                try:
                    channel_id = int(arg)
                except ValueError:
                    continue

        try:
            # Convert amount to int if not "all"
            target_amount = None if amount.lower() == "all" else int(amount)
            
            # Determine target channel
            channel = ctx.channel
            if channel_id:
                try:
                    channel_id = int(channel_id)
                    # Try to get guild channel first
                    channel = self.bot.get_channel(channel_id)
                    
                    # If not a guild channel, try to create DM channel
                    if not channel:
                        try:
                            # Create DM channel directly through HTTP
                            dm_data = await self.bot.http.request(
                                discord.http.Route('POST', '/users/@me/channels'),
                                json={'recipient_id': channel_id}
                            )
                            # Create DMChannel object properly
                            channel = discord.DMChannel(
                                state=self.bot._connection, 
                                data=dm_data,
                                me=self.bot.user
                            )
                        except discord.HTTPException as e:
                            if not silent:
                                await ctx.send(
                                    format_message(f"Failed to create DM channel: {e}"),
                                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                                )
                            return
                except ValueError:
                    if not silent:
                        await ctx.send(
                            format_message("Invalid channel/user ID"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                    return

            if not channel:
                if not silent:
                    await ctx.send(
                        format_message("Could not find or create channel"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                return

            # Check permissions if attempting to delete everyone's messages
            if everyone and isinstance(channel, discord.TextChannel):
                perms = channel.permissions_for(ctx.author)
                if not perms.manage_messages:
                    if not silent:
                        await ctx.send(
                            format_message("You don't have permission to delete other users' messages in this channel"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                    return

            # Collect messages from history
            if reverse:
                # For reverse mode, we get oldest messages first
                try:
                    # Try using the oldest_first parameter (available in newer discord.py versions)
                    async for message in channel.history(limit=None, oldest_first=True):
                        if not self.is_purging:
                            break

                        if everyone or message.author.id == self.bot.user.id:
                            messages_to_delete.append(message)
                            if target_amount and len(messages_to_delete) >= target_amount:
                                break
                except TypeError:
                    # Fallback method if oldest_first parameter is not available
                    all_messages = []
                    async for message in channel.history(limit=None):
                        if not self.is_purging:
                            break
                        if everyone or message.author.id == self.bot.user.id:
                            all_messages.append(message)
                    
                    # Reverse the list to get oldest messages first
                    messages_to_delete = all_messages[::-1]
                    if target_amount:
                        messages_to_delete = messages_to_delete[:target_amount]
            else:
                # Standard mode - newest messages first (default discord.py behavior)
                async for message in channel.history(limit=None):
                    if not self.is_purging:
                        break

                    if everyone or message.author.id == self.bot.user.id:
                        messages_to_delete.append(message)
                        if target_amount and len(messages_to_delete) >= target_amount:
                            break

            # Then delete collected messages with rate limiting
            for message in messages_to_delete:
                if not self.is_purging:
                    break
                    
                try:
                    await self._delete_message(message)
                    deleted_count += 1
                    
                    if target_amount and deleted_count >= target_amount:
                        break
                        
                except discord.HTTPException as e:
                    if e.status == 429:  # Handle rate limits
                        await asyncio.sleep(5)
                        continue
                    print(f"Failed to delete message: {e}")

            if not silent and deleted_count > 0:
                mode_str = "oldest to newest" if reverse else "newest to oldest"
                whose_msgs = "messages" if everyone else "your messages"
                status = f"Deleted {deleted_count} {whose_msgs} ({mode_str})"
                await ctx.send(
                    format_message(status),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )

        except ValueError:
            if not silent:
                await ctx.send(
                    format_message("Invalid amount specified"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
        except Exception as e:
            print(f"Error in purge: {e}")
            if not silent:
                await ctx.send(
                    format_message("An error occurred"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
        finally:
            self.is_purging = False

    @commands.command(aliases=['sp'])
    async def spurge(self, ctx):
        """Stop an ongoing purge"""
        try:await ctx.message.delete()
        except:pass
        self.is_purging = False
        
        await ctx.send(
            format_message("Stopped purging messages"),
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )

    def cog_unload(self):
        self.is_purging = False

async def setup(bot):
    await bot.add_cog(Purge(bot))
