from discord.ext import commands
import time
import logging
import discord
from discord.ext import tasks

logger = logging.getLogger(__name__)

class Info(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.start_time = time.time()
        self.cached_stats = {}
        self.update_stats.start()
        # Remove stats dict since we'll use MongoDB

    def cog_unload(self):
        self.update_stats.cancel()    
        
    @tasks.loop(minutes=5)
    async def update_stats(self):
        """Update cached statistics in background"""
        try:
            user_messages = self.bot.db.db.user_messages
            self.cached_stats = {
                "sniped_messages": await self.bot.db.db.deleted_messages.estimated_document_count(),
                "edited_messages": await self.bot.db.db.edited_messages.estimated_document_count(),
                "tracked_messages": await user_messages.estimated_document_count()
            }
        except Exception as e:
            logger.error(f"Error updating cached stats: {e}")

    def get_uptime(self) -> str:
        """Get formatted uptime string"""
        uptime = int(time.time() - self.start_time)
        days, remainder = divmod(uptime, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{days}d {hours}h {minutes}m {seconds}s"
    def quote_block(self, text: str) -> str:
        """Add > prefix to each line while preserving the content"""
        return '\n'.join(f'> {line}' for line in text.split('\n'))
    
    def get_padding(self, labels: list) -> int:
        """Calculate padding for consistent alignment based on longest label"""
        return max(len(label) for label in labels) + 2 if labels else 2
    
    async def get_stats(self, ctx) -> dict:
        """Get comprehensive bot statistics"""
        # Get user settings for UID without blocking
        uid = self.bot.config_manager.uid or 0

        # Use async cached config for totals without blocking disk
        try:
            config = await self.bot.config_manager._get_cached_config_async()
        except Exception:
            config = {"user_settings": {}}

        # Count connected users
        connected_users = 0
        total_users = 0
        for token, settings in config.get('user_settings', {}).items():
            if settings.get('uid') is not None:
                total_users += 1
                if (settings.get('connected', False) and 
                    token in self.bot._manager.bots and 
                    self.bot._manager.bots[token].is_ready()):
                    connected_users += 1

        # Get friends count from current instance
        friends = self.bot.friends
        friend_count = len(friends) if friends else 0
        
        # Track all unique friends across instances
        all_friends = set()
        if friends:
            all_friends.update(friend.id for friend in friends)
        
        # Get guilds count - already using a set for unique guild IDs
        all_guilds = {g.id for g in self.bot.guilds}
        
        # Track all unique users across instances
        all_users = {user.id for user in self.bot.users}

        # Add stats from other instances
        for other_bot in self.bot._manager.bots.values():
            if other_bot.is_ready() and other_bot != self.bot:  # Skip current bot
                # Add friend counts from other instances
                if other_bot.friends:
                    all_friends.update(friend.id for friend in other_bot.friends)
                    
                # Add guild and user counts (using sets for unique IDs)
                all_guilds.update(g.id for g in other_bot.guilds)
                all_users.update(user.id for user in other_bot.users)

        # Calculate total unique counts
        total_friends_count = len(all_friends)
        global_indexed = len(all_users)

        # Get command count
        command_count = len(set(cmd.name for cmd in self.bot.commands))

        return {
            "uid": uid,
            "indexed_users": len(self.bot.users),
            "global_indexed": global_indexed,
            "connected_users": connected_users,
            "total_users": total_users,
            "total_guilds": len(all_guilds),
            "user_guilds": sum(1 for g in self.bot.guilds if ctx.author in g.members),
            "friend_count": friend_count,
            "total_friends_count": total_friends_count,
            "command_count": command_count
        }    
    
    @commands.command(aliases=['i'])
    async def info(self, ctx):
        """Display information"""
        try:
            try:await ctx.message.delete()
            except:pass
            stats = await self.get_stats(ctx)
            
            # Make sure we have default values for all stats to prevent KeyError
            default_stats = {
                "sniped_messages": 0,
                "edited_messages": 0,
                "tracked_messages": 0
            }
            # Merge default stats with cached stats (cached_stats takes priority)
            message_stats = {**default_stats, **(self.cached_stats or {})}

            # UID already computed in stats above
            uid = stats.get('uid', 0)

            # Format the response with sleek design matching help_cog style
            message_parts = [
                "```ansi\n"
            ]
            
            # User Information section with clean styling
            message_parts.append("\u001b[30m\u001b[1m\u001b[4mUser Information\u001b[0m\n")
            
            # Calculate padding for labels based on longest label
            user_labels = ["ID", "UID", "Friends", "Servers", "Sent Messages", "Commands"]
            user_padding = self.get_padding(user_labels)
            
            # Add user information with consistent padding
            message_parts.append(f"\u001b[0;37mUsername{' ' * (user_padding - len('Username'))}\u001b[30m| \u001b[0;34m{self.bot.user.name}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mID{' ' * (user_padding - len('ID'))}\u001b[30m| \u001b[0;34m{ctx.author.id}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mUID{' ' * (user_padding - len('UID'))}\u001b[30m| \u001b[0;34m{uid}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mFriends{' ' * (user_padding - len('Friends'))}\u001b[30m| \u001b[0;34m{stats['friend_count']}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mServers{' ' * (user_padding - len('Servers'))}\u001b[30m| \u001b[0;34m{stats['user_guilds']}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mCommands{' ' * (user_padding - len('Commands'))}\u001b[30m| \u001b[0;34m{stats['command_count']}\u001b[0m\n")
            
            # Global Information section
            message_parts.append("\u001b[30m\u001b[1m\u001b[4mGlobal Statistic\u001b[0m\n")
            
            # Calculate padding for global stats labels
            global_labels = ["Uptime", "Users", "Servers", "Total Friends", "Version"]
            global_padding = self.get_padding(global_labels)
            
            # Add global stats with consistent padding
            message_parts.append(f"\u001b[0;37mUptime{' ' * (global_padding - len('Uptime'))}\u001b[30m| \u001b[0;34m{self.get_uptime()}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mUsers{' ' * (global_padding - len('Users'))}\u001b[30m| \u001b[0;34m{stats['connected_users']}/{stats['total_users']}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mServers{' ' * (global_padding - len('Servers'))}\u001b[30m| \u001b[0;34m{stats['total_guilds']}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mTotal Friends{' ' * (global_padding - len('Total Friends'))}\u001b[30m| \u001b[0;34m{stats['total_friends_count']}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mVersion{' ' * (global_padding - len('Version'))}\u001b[30m| \u001b[0;34m{self.bot.config_manager.version}\u001b[0m\n")
            
            # Tracking Statistics section
            message_parts.append("\u001b[30m\u001b[1m\u001b[4mTrack Statistics\u001b[0m\n")
            
            # Calculate padding for tracking stats labels
            tracking_labels = ["Indexed Users", "User Messages", "Messages", "Del Messages", "Red Messages"]
            tracking_padding = self.get_padding(tracking_labels)
            
            # Add tracking stats with consistent padding
            message_parts.append(f"\u001b[0;37mIndexed Users{' ' * (tracking_padding - len('Indexed Users'))}\u001b[30m| \u001b[0;34m{stats['indexed_users']:,} / {stats['global_indexed']:,}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mMessages{' ' * (tracking_padding - len('Messages'))}\u001b[30m| \u001b[0;34m{message_stats.get('tracked_messages', 0):,}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mDel Messages{' ' * (tracking_padding - len('Del Messages'))}\u001b[30m| \u001b[0;34m{message_stats['sniped_messages']:,}\u001b[0m\n")
            message_parts.append(f"\u001b[0;37mRed Messages{' ' * (tracking_padding - len('Red Messages'))}\u001b[30m| \u001b[0;34m{message_stats['edited_messages']:,}\u001b[0m\n")
            
            # Close code block
            message_parts.append("```")
            
            # Use the quote_block function to format the entire output
            await ctx.send(self.quote_block(''.join(message_parts)),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
    
        except Exception as e:
            logger.error(f"Error in info command: {e}")
            await ctx.send(
                "```ansi\n\u001b[1;31mError: An error occurred while fetching information```",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )    
            
    @commands.command(aliases=['si'])
    async def serverinfo(self, ctx, guild: discord.Guild = None):
        """Display server info
        .serverinfo"""
        try:
            try:await ctx.message.delete()
            except:pass
            

            if guild:
                # Fetch guild if not provided
                guild = self.bot.get_guild(guild.id)
                if not guild:
                    await ctx.send("Could not find server with that ID",
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                    return
            else:
                guild = ctx.guild
                if not guild:
                    await ctx.send("Please provide a server ID or use this command in a server",
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                    return
    
            # Format creation timestamp
            created_at = guild.created_at.strftime("%a, %b %d, %Y %I:%M %p")
              # Build the formatted message
            message_parts = [
                "```ansi\n" + \
                f"\u001b[33m{guild.name} \u001b[30m| \u001b[33m{guild.id}\n\n"
            ]            # Guild Description
            if guild.description:
                message_parts.append(
                    f"\u001b[30m\u001b[0;37mDescription \u001b[30m[\u001b[0;34m{guild.description}\u001b[30m]\n\n"
                )
    
            # Fetch guild owner properly
            try:
                owner = await guild.fetch_member(guild.owner_id) if guild.owner_id else None
            except discord.HTTPException:
                owner = None            
            if owner:
                message_parts.append(
                    f"\u001b[30m\u001b[0;37mOwner \u001b[30m[\u001b[0;34m{owner.name} \u001b[30m|\u001b[0;34m {owner.id}\u001b[30m]\n"
                )
            else:
                message_parts.append(
                    f"\u001b[30m\u001b[0;37mOwner \u001b[30m[\u001b[0;34mUnknown\u001b[30m]\n"
                )            # Creation date
            message_parts.append(
                f"\u001b[30m\u001b[0;37mCreated \u001b[30m[\u001b[0;34m{created_at}\u001b[30m]\n"
            )
            
            # Vanity URL if available
            if guild.vanity_url_code:
                message_parts.append(
                    f"\u001b[30m\u001b[0;37mVanity \u001b[30m[\u001b[0;34mdiscord.gg/{guild.vanity_url_code}\u001b[30m]\n"
                )
            
            message_parts.append("\n")            # Roles and channels
            message_parts.append(
                f"\u001b[30m\u001b[0;37mRoles \u001b[30m[\u001b[0;34m{len(guild.roles)}\u001b[30m]\n" + \
                f"\u001b[30m\u001b[0;37mChannels \u001b[30m[\u001b[0;34m{len(guild.channels)}\u001b[30m]\n\n"
            )            # Member count and boost count
            message_parts.append(
                f"\u001b[30m\u001b[0;37mMembers \u001b[30m[\u001b[0;34m{guild.member_count}\u001b[30m]\n" + \
                f"\u001b[30m\u001b[0;37mBoosts \u001b[30m[\u001b[0;34m{guild.premium_subscription_count}\u001b[30m]\n" + \
                f"\u001b[30m\u001b[0;37mLevel \u001b[30m[\u001b[0;34m{guild.premium_tier}\u001b[30m]\n"
                f"\u001b[0m```"
            )
    
            # Send the formatted message
            await ctx.send(
                self.quote_block(''.join(message_parts)),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
    
        except Exception as e:
            logger.error(f"Error in serverinfo command: {e}")
            await ctx.send(
                "```ansi\n\u001b[1;31mError: Could not fetch server information```",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

async def setup(bot):
    await bot.add_cog(Info(bot))