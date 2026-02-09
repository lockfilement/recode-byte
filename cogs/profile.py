import discord
from discord.ext import commands
from typing import Optional, Union
import logging
import datetime
from utils.general import format_message, quote_block, get_max_message_length

logger = logging.getLogger(__name__)

class Profile(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
    async def _send_asset(self, ctx, asset, description, delete_after=None):
        """Helper method to send Discord assets (avatars, banners, icons)
        Falls back to URL if the file is too large or user doesn't have Nitro"""
        try:
            # Check if user has Nitro based on message length limit
            has_nitro = get_max_message_length(self.bot) > 2000
            
            # Get file size (bytes) using the size parameter in the URL
            # Discord's CDN includes size in the URL querystring
            url = str(asset.url)
            
            # Try to send as file for Nitro users, otherwise use URL
            if has_nitro:
                try:
                    await ctx.send(quote_block(description), delete_after=delete_after)
                    await ctx.send(file=await asset.to_file(), delete_after=delete_after)
                except discord.HTTPException as e:
                    # If file is too large, fall back to URL
                    if e.status == 413:  # 413 = Payload Too Large
                        logger.debug(f"Asset too large, sending URL instead: {e}")
                        await ctx.send(quote_block(description), delete_after=delete_after)
                        await ctx.send(f"{url}", delete_after=delete_after)                    
                    else:
                        raise  # Re-raise for other HTTP exceptions
            else:
                # Non-Nitro users get URLs directly
                await ctx.send(quote_block(description), delete_after=delete_after)
                await ctx.send(f"{url}", delete_after=delete_after)
        except Exception as e:
            logger.error(f"Error sending asset: {e}")
            await ctx.send(format_message(f"Error sending asset: {e}"), delete_after=delete_after)
            
    @commands.command(aliases=['av'])
    async def pfp(self, ctx, user_input: Optional[Union[discord.Member, discord.User, str]] = None):
        """Get user's pfp
        .pfp [user/user_id]"""
        try:await ctx.message.delete()
        except:pass

        try:
            # Handle the user input
            if user_input is None:
                target_id = ctx.author.id
            else:
                # If it's already a Member or User object
                if isinstance(user_input, (discord.Member, discord.User)):
                    target_id = user_input.id
                else:
                    # Try to convert string to ID
                    try:
                        target_id = int(str(user_input))
                    except ValueError:
                        await ctx.send(format_message("Invalid user ID format"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                        return

            # Fetch user to ensure we have proper data
            target = await self.bot.GetUser(target_id)
            
            if not target:
                await ctx.send(format_message("Could not fetch user information"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
            
            delete_after = self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            
            # Global avatar
            if target.avatar:
                await self._send_asset(
                    ctx, 
                    target.avatar, 
                    f"```ansi\n\u001b[30m\u001b[1m\u001b[4m{target.name}'s Global Avatar\u001b[0m```",
                    delete_after=delete_after
                )
            else:
                await ctx.send(quote_block(f"```ansi\n\u001b[30m\u001b[1m\u001b[4m{target.name}'s Global Avatar\u001b[0m```\nNo global avatar"),
                    delete_after=delete_after)

            # Guild avatars
            if isinstance(target, discord.Member):
                guild_avatar = target.guild_avatar
                if guild_avatar:
                    await self._send_asset(
                        ctx, 
                        guild_avatar, 
                        f"```ansi\n\u001b[30m\u001b[1m\u001b[4m{target.name}'s Server Avatar ({ctx.guild.name})\u001b[0m```",
                        delete_after=delete_after
                    )

            # Check other mutual guilds for guild-specific avatars
            for guild in self.bot.guilds:
                if guild == ctx.guild:
                    continue
                member = guild.get_member(target.id)
                if member and member.guild_avatar:
                    await self._send_asset(
                        ctx, 
                        member.guild_avatar, 
                        f"```ansi\n\u001b[30m\u001b[1m\u001b[4m{target.name}'s Server Avatar ({guild.name})\u001b[0m```",
                        delete_after=delete_after
                    )        
        except discord.NotFound:
            await ctx.send(format_message("User not found"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except discord.HTTPException as e:
            await ctx.send(format_message(f"Error fetching user avatar: {e}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except Exception as e:
            logger.error(f"Error in pfp command: {e}")
            await ctx.send(format_message(f"An error occurred while fetching user avatar: {e}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                
    @commands.command(aliases=['bn'])
    async def banner(self, ctx, user_input: Optional[Union[discord.Member, discord.User, str]] = None):
        """Get user's banner
        .banner [user/user_id]"""
        try:await ctx.message.delete()
        except:pass

        try:
            # Handle the user input
            if user_input is None:
                target_id = ctx.author.id
            else:
                # If it's already a Member or User object
                if isinstance(user_input, (discord.Member, discord.User)):
                    target_id = user_input.id
                else:
                    # Try to convert string to ID
                    try:
                        target_id = int(str(user_input))
                    except ValueError:
                        await ctx.send(format_message("Invalid user ID format"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                        return
        
            delete_after = self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            
            # Fetch user to ensure we have banner data
            fetched_user = await self.bot.GetUser(target_id)
            
            if not fetched_user:
                await ctx.send(format_message("Could not fetch user information"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
            
            # Send banner directly as file if it exists
            if fetched_user.banner:
                await self._send_asset(
                    ctx, 
                    fetched_user.banner, 
                    f"```ansi\n\u001b[30m\u001b[1m\u001b[4m{fetched_user.name}'s Banner\u001b[0m```",
                    delete_after=delete_after
                )
            else:
                await ctx.send(quote_block(f"```ansi\n\u001b[30m\u001b[1m\u001b[4m{fetched_user.name}'s Banner\u001b[0m```\nNo banner"),
                    delete_after=delete_after)

        except discord.NotFound:
            await ctx.send(format_message("User not found"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except discord.HTTPException as e:
            await ctx.send(format_message(f"Error fetching banner: {e}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except Exception as e:
            logger.error(f"Error in banner command: {e}")
            await ctx.send(format_message(f"An error occurred while fetching user banner: {e}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
            
    @commands.command(aliases=['ui'])
    async def userinfo(self, ctx, user_input: Optional[Union[discord.Member, discord.User, str]] = None):
        """Get user information 
        .userinfo [user/user_id]"""
        try:await ctx.message.delete()
        except:pass
        try:
            # Handle the user input
            if user_input is None:
                target_id = ctx.author.id
            else:
                # If it's already a Member or User object
                if isinstance(user_input, (discord.Member, discord.User)):
                    target_id = user_input.id
                else:
                    # Try to convert string to ID
                    try:
                        target_id = int(str(user_input))
                    except ValueError:
                        await ctx.send(format_message("Invalid user ID format"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                        return
    
            # Fetch user profile data directly using the ID
            fetched_user = await self.bot.GetUser(target_id)
    
            if not fetched_user:
                await ctx.send(format_message("Could not fetch user information"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return

            # Fetch user profile with mutual info
            try:
                user_profile = await self.bot.fetch_user_profile(
                    target_id,
                    with_mutual_guilds=True,
                    with_mutual_friends=True
                )
            except Exception as e:
                logger.debug(f"Could not fetch user profile: {e}")
                user_profile = None
    
            # Get account age
            years_ago = int((datetime.datetime.now(datetime.timezone.utc) - fetched_user.created_at).days / 365)
            days_ago = (datetime.datetime.now(datetime.timezone.utc) - fetched_user.created_at).days
            account_age = f"{years_ago} {'year' if years_ago == 1 else 'years'}"
            if days_ago < 365:
                account_age = f"{days_ago} {'day' if days_ago == 1 else 'days'}"
              # Generate clean, elegant info block inspired by serverinfo command
            info_block = [
                "```ansi\n" + \
                f"\u001b[33m{fetched_user.name} \u001b[30m| \u001b[33m{fetched_user.id}\n\n"
            ]
            
            # User information section
            created_at = fetched_user.created_at.strftime("%a, %b %d, %Y %I:%M %p")
            
            # Display name if different from username
            if fetched_user.display_name != fetched_user.name:
                info_block.append(f"\u001b[30m\u001b[0;37mDisplay Name \u001b[30m[\u001b[0;34m{fetched_user.display_name}\u001b[30m]\n")
            
            # Account creation date
            info_block.append(f"\u001b[30m\u001b[0;37mCreated \u001b[30m[\u001b[0;34m{created_at}\u001b[30m]\n")
            info_block.append(f"\u001b[30m\u001b[0;37mAccount Age \u001b[30m[\u001b[0;34m{account_age}\u001b[30m]\n")
            
            # Status and Activity section with cleaner formatting
            try:
                relationship = next((r for r in self.bot.relationships if r.user.id == fetched_user.id), None)
                if relationship:
                    status = str(relationship.status).title()
                    status_emoji = ""
                    
                    if status == "Online":
                        status_emoji = "●"  # More elegant dots instead of colored circles
                    elif status == "Idle":
                        status_emoji = "○"
                    elif status == "Dnd":
                        status_emoji = "⊗"
                        status = "Do Not Disturb"
                    elif status == "Offline":
                        status_emoji = "⦾"
                    
                    info_block.append(f"\u001b[30m\u001b[0;37mStatus \u001b[30m[\u001b[0;34m{status_emoji} {status}\u001b[30m]\n")
                    
                    if relationship.user.activity:
                        activity_type = relationship.user.activity.type.name if hasattr(relationship.user.activity, 'type') else "Playing"
                        activity_name = relationship.user.activity.name
                        info_block.append(f"\u001b[30m\u001b[0;37mActivity \u001b[30m[\u001b[0;34m{activity_type} {activity_name}\u001b[30m]\n")
            except Exception as e:
                logger.debug(f"Could not fetch relationship status/activity: {e}")
            
            info_block.append("\n")

            # Mutual information section with cleaner formatting
            if user_profile:                # Mutual Friends
                if hasattr(user_profile, 'mutual_friends') and user_profile.mutual_friends:
                    friend_count = len(user_profile.mutual_friends)
                    info_block.append(f"\u001b[30m\u001b[0;37mMutual Friends \u001b[30m[\u001b[0;34m{friend_count}\u001b[30m]\n")
                    
                    # List friends with truncation
                    friend_names = [f"{friend.name}" for friend in user_profile.mutual_friends]
                    if friend_names:
                        if len(friend_names) > 5:
                            # Show first 5 friends and indicate there are more
                            friend_display = f"{', '.join(friend_names[:5])}... and {len(friend_names) - 5} more"
                            info_block.append(f"\u001b[30m\u001b[0;37m └─ \u001b[30m[\u001b[0;34m{friend_display}\u001b[30m]\n")
                        else:
                            # Show all friends if 5 or fewer
                            info_block.append(f"\u001b[30m\u001b[0;37m └─ \u001b[30m[\u001b[0;34m{', '.join(friend_names)}\u001b[30m]\n")
                  # Mutual Guilds/Servers
                if hasattr(user_profile, 'mutual_guilds') and user_profile.mutual_guilds:
                    # Get guild names
                    guild_names = []
                    for mutual_guild in user_profile.mutual_guilds:
                        guild = self.bot.get_guild(mutual_guild.id)
                        if guild:
                            guild_names.append(guild.name)
                    
                    if guild_names:
                        server_count = len(guild_names)
                        info_block.append(f"\u001b[30m\u001b[0;37mMutual Servers \u001b[30m[\u001b[0;34m{server_count}\u001b[30m]\n")
                        
                        # List servers with truncation
                        if len(guild_names) > 5:
                            # Show first 5 servers and indicate there are more
                            guild_display = f"{', '.join(guild_names[:5])}... and {len(guild_names) - 5} more"
                            info_block.append(f"\u001b[30m\u001b[0;37m └─ \u001b[30m[\u001b[0;34m{guild_display}\u001b[30m]\n")
                        else:
                            # Show all servers if 5 or fewer
                            info_block.append(f"\u001b[30m\u001b[0;37m └─ \u001b[30m[\u001b[0;34m{', '.join(guild_names)}\u001b[30m]\n")
                
                # Add separator for cleaner organization
                info_block.append("\n")
    
            # Server-specific information
            if ctx.guild:
                try:         
                    member = ctx.guild.get_member(target_id)
                    if not member:
                        # If get_member fails, try fetching the member
                        try:
                            member = await ctx.guild.fetch_member(target_id)
                        except discord.NotFound:
                            member = None
                        except Exception as e:
                            logger.debug(f"Error fetching member: {e}")
                            member = None

                    if member:
                        # Calculate server membership duration
                        years_since_join = int((datetime.datetime.now(datetime.timezone.utc) - member.joined_at).days / 365)
                        days_since_join = (datetime.datetime.now(datetime.timezone.utc) - member.joined_at).days
                        join_time = f"{years_since_join} {'year' if years_since_join == 1 else 'years'}"
                        if days_since_join < 365:
                            join_time = f"{days_since_join} {'day' if days_since_join == 1 else 'days'}"
                        
                        # Server info header
                        info_block.append(f"\u001b[30m\u001b[0;37mServer \u001b[30m[\u001b[0;34m{ctx.guild.name}\u001b[30m]\n")
                        
                        # Join date 
                        join_date = member.joined_at.strftime("%a, %b %d, %Y %I:%M %p")
                        info_block.append(f"\u001b[30m\u001b[0;37mJoined \u001b[30m[\u001b[0;34m{join_date}\u001b[30m]\n")
                        info_block.append(f"\u001b[30m\u001b[0;37mMember For \u001b[30m[\u001b[0;34m{join_time}\u001b[30m]\n")
                        
                        # Display boosting status if applicable
                        if member.premium_since:
                            boost_since = member.premium_since.strftime("%b %d, %Y")
                            boost_days = (datetime.datetime.now(datetime.timezone.utc) - member.premium_since).days
                            info_block.append(f"\u001b[30m\u001b[0;37mBoosting \u001b[30m[\u001b[0;34mSince {boost_since} • {boost_days} days\u001b[30m]\n")
                        
                        # Display roles (excluding @everyone)
                        if len(member.roles) > 1:
                            role_count = len(member.roles) - 1  # Exclude @everyone
                            info_block.append(f"\u001b[30m\u001b[0;37mRoles \u001b[30m[\u001b[0;34m{role_count}\u001b[30m]\n")
                            
                            # Show top roles if not too many
                            if role_count <= 6:
                                role_names = [role.name for role in reversed(member.roles[1:])]
                                info_block.append(f"\u001b[30m\u001b[0;37m └─ \u001b[30m[\u001b[0;34m{', '.join(role_names)}\u001b[30m]\n")
                        
                        # Show key permissions in compact format
                        if member.guild_permissions.administrator:
                            info_block.append(f"\u001b[30m\u001b[0;37mPermissions \u001b[30m[\u001b[0;34mAdministrator\u001b[30m]\n")
                        else:
                            perms = []
                            if member.guild_permissions.manage_guild:
                                perms.append("Manage Server")
                            if member.guild_permissions.ban_members or member.guild_permissions.kick_members:
                                perms.append("Moderate Members")
                            if member.guild_permissions.manage_channels:
                                perms.append("Manage Channels")
                            if member.guild_permissions.manage_roles:
                                perms.append("Manage Roles")
                            if member.guild_permissions.manage_messages:
                                perms.append("Manage Messages")
                            
                            if perms:
                                info_block.append(f"\u001b[30m\u001b[0;37mPermissions \u001b[30m[\u001b[0;34m{', '.join(perms)}\u001b[30m]\n")
                    else:
                        info_block.append(f"\u001b[30m\u001b[0;37mServer Member \u001b[30m[\u001b[0;34mNo\u001b[30m]\n")
                except Exception as e:
                    logger.error(f"Error getting member info: {e}")
            
            # Clean closing
            info_block.append("\u001b[0m```")
    
            # Combine blocks and apply quote formatting for Discord display
            message = quote_block("".join(info_block))
    
            await ctx.send(message,
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
    
        except discord.NotFound:
            await ctx.send(format_message("User not found"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except discord.HTTPException as e:
            await ctx.send(format_message(f"Error fetching user info: {e}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        except Exception as e:
            logger.error(f"Error in userinfo command: {e}")
            await ctx.send(format_message(f"An error occurred while fetching user info: {e}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                
    @commands.command(aliases=['gicon', 'servericon'])
    async def guildicon(self, ctx, guild_id: str = None):
        """Get a guild's icon
        .guildicon [guild_id]"""
        try:await ctx.message.delete()
        except:pass
        
        delete_after = self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        
        try:
            # If no guild ID specified, use current guild
            if guild_id is None:
                if ctx.guild:
                    guild = ctx.guild
                else:
                    await ctx.send(format_message("Please provide a guild ID or use this command in a server"),
                        delete_after=delete_after)
                    return
            else:
                # Try to convert string to ID
                try:
                    guild_id_int = int(guild_id)
                    guild = self.bot.get_guild(guild_id_int)
                    
                    # If we couldn't find the guild, try fetching it
                    if guild is None:
                        try:
                            guild = await self.bot.fetch_guild(guild_id_int)
                        except discord.HTTPException:
                            await ctx.send(format_message("Could not find guild with that ID"),
                                delete_after=delete_after)
                            return
                except ValueError:
                    await ctx.send(format_message("Invalid guild ID format"),
                        delete_after=delete_after)
                    return
            
            # Send guild icon directly as file if it exists
            if guild.icon:
                await self._send_asset(
                    ctx, 
                    guild.icon, 
                    f"```ansi\n\u001b[30m\u001b[1m\u001b[4m{guild.name}'s Guild Icon\u001b[0m```",
                    delete_after=delete_after
                )
            else:
                await ctx.send(quote_block(f"```ansi\n\u001b[30m\u001b[1m\u001b[4m{guild.name}\u001b[0m```\nNo guild icon"),
                    delete_after=delete_after)
                    
        except discord.Forbidden:
            await ctx.send(format_message("I don't have permission to access that guild"),
                delete_after=delete_after)
        except discord.HTTPException as e:
            await ctx.send(format_message(f"Error fetching guild icon: {e}"),
                delete_after=delete_after)
        except Exception as e:
            logger.error(f"Error in guildicon command: {e}")
            await ctx.send(format_message(f"An error occurred while fetching guild icon: {e}"),
                delete_after=delete_after)

async def setup(bot):
    await bot.add_cog(Profile(bot))
