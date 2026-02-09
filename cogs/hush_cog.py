import discord
from discord.ext import commands
from utils.rate_limiter import rate_limiter
from utils.general import format_message, quote_block
import logging
from typing import Union, Optional

logger = logging.getLogger(__name__)

class Hush(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.hushed_users = {}  # Track hushed users {user_id: {silent: bool}}
        self.is_hushing = False
        self.deleted_message_counts = {}  # Track {user_id: count} for feedback
        self.channel_counts = set()  # Track channels where messages were deleted

    @rate_limiter(command_only=True)
    async def _delete_message(self, msg):
        """Helper method to delete messages with rate limiting"""
        await msg.delete()

    async def _handle_message(self, message):
        """Handler for message events - delete messages from hushed users"""
        # Skip if not in a guild (server) channel
        if not message.guild or not message.author:
            return
        
        user_id = message.author.id
        
        # Check if this user is being hushed
        if user_id in self.hushed_users:
            try:
                # Check if we have the permissions to delete in this channel
                permissions = message.channel.permissions_for(message.guild.get_member(self.bot.user.id))
                    
                if permissions and permissions.manage_messages:
                    await self._delete_message(message)
                    
                    # Update stats
                    self.deleted_message_counts[user_id] = self.deleted_message_counts.get(user_id, 0) + 1
                    self.channel_counts.add(message.channel.id)
                    
                    logger.debug(f"Deleted message from hushed user {user_id} in channel {message.channel.id}")
            except Exception as e:
                logger.error(f"Error deleting message from hushed user {user_id}: {e}")
    
    @commands.command(aliases=['hs'])
    async def hush(self, ctx, target: Optional[Union[discord.Member, discord.User, int, str]] = None):
        """Delete messages from a specific user silently
        
        Usage:
        .hush @user/ID  - Delete messages from the specified user
        
        Examples:
        .hush @user  - Delete messages from @user
        .hush 123456789  - Delete messages from user with ID 123456789
        """
        # Check if the command is used in a server
        if not ctx.guild:
            return  # Silently ignore if not in a server
            
        try:
            await ctx.message.delete()
        except Exception as e:
            logger.debug(f"Error deleting command message: {e}")
        
        # Handle target user
        user_id = None
        user_name = None
        
        if target is None:
            # Now provide feedback if no target specified
            await ctx.send(
                format_message("You need to specify a user to hush"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
        
        # If target is already a Member or User object
        if isinstance(target, (discord.Member, discord.User)):
            user_id = target.id
            user_name = target.name
        else:
            # Try to convert to user ID
            try:
                user_id = int(str(target))
                # Try to fetch user info to get name
                user = await self.bot.GetUser(user_id)
                if user:
                    user_name = user.name
                else:
                    user_name = f"User {user_id}"
            except ValueError:
                # If not a valid integer ID, treat as a username
                user_name = str(target)
                # Try to find user by name in visible users in this guild
                found_user = discord.utils.find(
                    lambda u: u.name.lower() == user_name.lower(),
                    ctx.guild.members
                )
                if found_user:
                    user_id = found_user.id
                    user_name = found_user.name
                else:
                    # Provide feedback if user not found
                    await ctx.send(
                        format_message("User not found"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
        
        # Check if the user is trying to hush themselves
        if user_id == self.bot.user.id:
            await ctx.send(
                format_message("You can't hush yourself"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
        
        # Reset tracking stats
        self.deleted_message_counts[user_id] = 0
        self.channel_counts.clear()
        
        try:
            # Add user to hushed users dict - rely on event handler for message deletion
            self.hushed_users[user_id] = True
            
            # No feedback message - operate silently
            
        except Exception as e:
            logger.error(f"Error in hush command: {e}")
            # No feedback message on error - remain silent
    
    @commands.command(aliases=['sh'])
    async def stophush(self, ctx, target: Optional[Union[discord.Member, discord.User, int, str]] = None):
        """Stop hushing a user's messages
        
        Usage:
        .stophush - Stop hushing all users
        .stophush @user/ID - Stop hushing the specified user
        """
        # Check if the command is used in a server
        if not ctx.guild:
            return  # Silently ignore if not in a server
            
        try:
            await ctx.message.delete()
        except Exception as e:
            logger.debug(f"Error deleting command message: {e}")
        
        # If a specific target was given, only stop hushing that user
        if target:
            user_id = None
            
            # Determine target user ID
            if isinstance(target, (discord.Member, discord.User)):
                user_id = target.id
            else:
                try:
                    # Try to convert to user ID
                    user_id = int(str(target))
                except ValueError:
                    # Try to find by name in guild members
                    user_name = str(target)
                    found_user = discord.utils.find(
                        lambda u: u.name.lower() == user_name.lower(),
                        ctx.guild.members
                    )
                    if found_user:
                        user_id = found_user.id
                    else:
                        # Provide feedback if user not found
                        await ctx.send(
                            format_message("User not found"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                        return
            
            # Remove specific user from hushed users
            if user_id in self.hushed_users:
                del self.hushed_users[user_id]
                # Provide feedback message
                await ctx.send(
                    format_message(f"Stopped hushing user"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            else:
                # Provide feedback if user wasn't being hushed
                await ctx.send(
                    format_message("That user wasn't being hushed"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
        else:
            # Stop hushing all users if no specific target
            self.hushed_users.clear()
            # Provide feedback message
            await ctx.send(
                format_message("Stopped hushing all users"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
        

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)
            logger.info("HushCog registered with EventManager")
        else:
            logger.warning("EventManager not found, HushCog will have limited functionality")

    async def cog_unload(self):
        """Clean up resources when cog is unloaded"""
        # Unregister from event manager
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)
            
        # Clean up data
        self.is_hushing = False
        self.hushed_users.clear()
        self.deleted_message_counts.clear()
        self.channel_counts.clear()


async def setup(bot):
    await bot.add_cog(Hush(bot))