import discord
from discord.ext import commands
import logging
from utils.general import format_message, quote_block

logger = logging.getLogger(__name__)

class Whitelist(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # In-memory storage for whitelisted user IDs for this bot instance
        self.whitelisted_users = set()
        # List of commands that only the selfbot owner can use
        self.whitelist_commands = ['whitelist', 'wl', 'unwhitelist', 'unwl', 'list_whitelist', 'listwl']
        # List of commands that whitelisted users cannot use
        self.blacklisted_commands = ['check', 'massdm', 'mdm', 'lc', 'smdm', 's', 'spam', 'ar', 'react', 'afk', 'away', 'mk', 'mock', 'hy', 'history', 'uh', 'username_history', 'dh', 'displayname_history', 'ah', 'avatar_history', 'bh', 'banner_history']
        # New: Keep track of developer category commands
        self.developer_commands = []
        
    async def _check_owner(self, ctx):
        """Check if command user is the selfbot owner"""
        if ctx.author.id != self.bot.user.id:
            await ctx.send(format_message("Only the selfbot owner can manage the whitelist."), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            return False
        return True

    @commands.command(aliases=["wl"])
    async def whitelist(self, ctx, user: discord.User):
        """Add user to whitelist
        .whitelist @user"""
        try:await ctx.message.delete()
        except:pass
        
        # Check if the command user is the selfbot owner
        if not await self._check_owner(ctx):
            return
            
        try:
            self.whitelisted_users.add(user.id)
            await ctx.send(format_message(f"Successfully whitelisted {user.name}"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except Exception as e:
            logger.error(f"Error in whitelist command: {e}")
            await ctx.send(format_message("Error adding user to whitelist"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    @commands.command(aliases=["unwl"])
    async def unwhitelist(self, ctx, user: discord.User):
        """Remove user from whitelist
        .unwhitelist @user"""
        try:await ctx.message.delete()
        except:pass
        
        # Check if the command user is the selfbot owner
        if not await self._check_owner(ctx):
            return
            
        try:
            # Prevent unwhitelisting of developers
            if self.bot.config_manager.is_developer(user.id):
                await ctx.send(format_message("Cannot unwhitelist the developer."), 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
                
            self.whitelisted_users.discard(user.id)
            await ctx.send(format_message(f"Successfully unwhitelisted {user.name}"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except Exception as e:
            logger.error(f"Error in unwhitelist command: {e}")
            await ctx.send(format_message("Error removing user from whitelist"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    @commands.command(aliases=["listwl"])
    async def list_whitelist(self, ctx):
        """List whitelisted users"""
        try:await ctx.message.delete()
        except:pass
        
        # Check if the command user is the selfbot owner
        if not await self._check_owner(ctx):
            return
            
        try:
            if not self.whitelisted_users:
                await ctx.send("```ansi\n\u001b[1;31mNo whitelisted users.```", 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return

            user_list = []
            for user_id in self.whitelisted_users:
                # Using discord.py-self's GetUser method for retrieving user objects
                user_obj = await self.bot.GetUser(user_id)
                if user_obj:
                    user_list.append(f"\u001b[0;36m• {user_obj.name} \u001b[0;37m({user_obj.id})")
                else:
                    user_list.append(f"\u001b[0;36m• Unknown User \u001b[0;37m({user_id})")
            
            message_parts = [
                "```ansi\n" + \
                "\u001b[30m\u001b[1m\u001b[4mWhitelisted Users\u001b[0m\n" + \
                "\n".join(user_list) + "```"
            ]
            
            await ctx.send(quote_block(''.join(message_parts)), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except Exception as e:
            logger.error(f"Error in list_whitelist command: {e}")
            await ctx.send(format_message("Error retrieving whitelist."), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    # Function to check if a command belongs to the developer category
    def _is_developer_command(self, command_name):
        """Check if a command belongs to the developer category"""
        # Check our cached list first
        if command_name in self.developer_commands:
            return True
            
        # Check if the command exists and has the developer_only check
        cmd = self.bot.get_command(command_name)
        if cmd:
            for check in cmd.checks:
                # Check if the function name contains 'developer_only'
                if check.__qualname__.endswith('developer_only.<locals>.predicate'):
                    # Add to our cache for future checks
                    self.developer_commands.append(command_name)
                    return True
        return False

    async def _handle_message(self, message):
        """Handler for message events"""
        # Skip if message is from the selfbot itself
        if message.author.id == self.bot.user.id:
            return
            
        # Only process messages from whitelisted users
        if message.author.id not in self.whitelisted_users:
            return
            
        # Get this bot instance's specific prefix
        prefix = self.bot.config_manager.command_prefix
        
        # Only process commands that start with this specific bot instance's prefix
        if not message.content.startswith(prefix):
            return
        
        # Get the command name from the message
        command_name = message.content[len(prefix):].split()[0].lower()
        
        # Check if the command actually exists
        command = self.bot.get_command(command_name)
        if not command:
            # Not a valid command
            return
        
        # Check if the message is trying to use whitelist management commands
        if command_name in self.whitelist_commands:
            logger.warning(f"Whitelisted user {message.author.id} attempted to use restricted command: {message.content}")
            return
        
        # Get the developer ID
        developer_ids = self.bot.config_manager.developer_ids
        
        # Check if the command is in the blacklisted commands list
        # Allow the developer to bypass this restriction
        if command_name in self.blacklisted_commands and not self.bot.config_manager.is_developer(message.author.id):
            logger.warning(f"Whitelisted user {message.author.id} attempted to use blacklisted command: {message.content}")
            try:
                # Send a message to the user that they can't use blacklisted commands
                await message.channel.send(
                    format_message("You don't have permission to use this command."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            except Exception as e:
                logger.error(f"Error sending blacklist restriction message: {e}")
            return
            
        # Check if the command belongs to the developer category
        if self._is_developer_command(command_name) and not self.bot.config_manager.is_developer(message.author.id):
            logger.warning(f"Whitelisted user {message.author.id} attempted to use developer command: {message.content}")
            try:
                # Optional: Send a message to the user that they can't use developer commands
                await message.channel.send(
                    format_message("You don't have permission to use developer commands."),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
            except Exception as e:
                logger.error(f"Error sending developer restriction message: {e}")
            return
            
        try:
            logger.info(f"Processing whitelisted command: {message.content}")
            message.author = self.bot.user
            await self.bot.process_commands(message)
        except Exception as e:
            logger.error(f"Error executing whitelisted command: {e}")

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        # Always whitelist the developers
        try:
            # Add all developer IDs to whitelist
            for dev_id in self.bot.config_manager.developer_ids:
                self.whitelisted_users.add(dev_id)
                logger.info(f"Developer ID {dev_id} automatically added to whitelist")
            
            # Hardcoded list of non-developer user IDs to whitelist on startup
            # Note: Developers are automatically whitelisted above
            auto_whitelist_ids = [
                # Add your non-developer user IDs here
                # Developers are automatically handled above, no need to add them here
                # Add more user IDs as needed
            ]
            
            # Add all hardcoded IDs to whitelist
            for user_id in auto_whitelist_ids:
                self.whitelisted_users.add(user_id)
                logger.info(f"User ID {user_id} automatically added to whitelist")
                
            # NEW: Pre-populate developer commands list for faster lookup
            # Look for commands with the developer_only decorator
            for command in self.bot.commands:
                for check in command.checks:
                    if check.__qualname__.endswith('developer_only.<locals>.predicate'):
                        self.developer_commands.append(command.name)
                        # Add aliases too
                        self.developer_commands.extend(command.aliases)
            
            logger.info(f"Identified {len(self.developer_commands)} developer commands to restrict")
        except Exception as e:
            logger.error(f"Error adding users to whitelist: {e}")
            
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)

async def setup(bot):
    await bot.add_cog(Whitelist(bot))
