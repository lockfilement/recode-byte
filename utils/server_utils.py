import discord
from typing import Dict, List
from utils.rate_limiter import rate_limiter

@rate_limiter(command_only=True)
async def delete_all_roles(guild: discord.Guild):
    failed_roles = []
    for role in reversed(guild.roles[1:]):
        try:
            await role.delete()
        except discord.HTTPException as e:
            failed_roles.append((role.name, str(e)))
            continue
    return failed_roles

@rate_limiter(command_only=True)
async def delete_all_channels(guild: discord.Guild):
    failed_channels = []
    for channel in guild.channels:
        try:
            await channel.delete()
        except discord.HTTPException as e:
            failed_channels.append((channel.name, str(e)))
            continue
    return failed_channels

@rate_limiter(command_only=True)
async def copy_roles(source_guild: discord.Guild, target_guild: discord.Guild) -> tuple[Dict[int, discord.Role], list]:
    role_map = {}
    failed_roles = []
    for role in reversed(source_guild.roles[1:]):
        try:
            new_role = await target_guild.create_role(
                name=role.name,
                permissions=role.permissions, 
                colour=role.colour,
                hoist=role.hoist,
                mentionable=role.mentionable
            )
            role_map[role.id] = new_role
        except discord.HTTPException as e:
            failed_roles.append((role.name, str(e)))
            continue
    return role_map, failed_roles

@rate_limiter(command_only=True)
async def copy_channels(source_guild: discord.Guild, target_guild: discord.Guild, role_map: Dict[int, discord.Role]):
    for category in source_guild.categories:
        try:
            new_category = await target_guild.create_category(
                name=category.name,
                overwrites=await update_overwrites(category.overwrites, role_map)
            )
        except discord.HTTPException as e:
            print(f"Failed to copy category: {e}")
            continue
            
        for channel in category.channels:
            await copy_channel(channel, target_guild, new_category, role_map)

    # Copy channels not in any category
    for channel in source_guild.channels:
        if not channel.category:
            await copy_channel(channel, target_guild, None, role_map)

# Remove delay parameter and use global rate limiter
@rate_limiter(command_only=True)
async def copy_channel(channel, target_guild, category, role_map):
    try:
        overwrites = await update_overwrites(channel.overwrites, role_map)
        if isinstance(channel, discord.TextChannel):
            await target_guild.create_text_channel(
                name=channel.name,
                topic=channel.topic,
                position=channel.position,
                slowmode_delay=channel.slowmode_delay,
                nsfw=channel.nsfw,
                category=category,
                overwrites=overwrites
            )
        elif isinstance(channel, discord.VoiceChannel):
            bitrate = min(channel.bitrate, 256000)
            await target_guild.create_voice_channel(
                name=channel.name,
                bitrate=bitrate,
                user_limit=channel.user_limit,
                position=channel.position,
                category=category,
                overwrites=overwrites
            )
    except discord.HTTPException as e:
        # return e
        print(f"Failed to copy channel: {e}")
        return
    
@rate_limiter(command_only=True)
async def update_overwrites(overwrites: Dict[discord.Role, discord.PermissionOverwrite], role_map: Dict[int, discord.Role]) -> Dict[discord.Role, discord.PermissionOverwrite]:
    new_overwrites = {}
    for role, overwrite in overwrites.items():
        if isinstance(role, discord.Role):
            new_role = role_map.get(role.id)
            if new_role:
                new_overwrites[new_role] = overwrite
        else:
            # For default role (@everyone)
            new_overwrites[role] = overwrite
    return new_overwrites

async def reassign_roles(guild: discord.Guild, member_roles: Dict[int, List[int]], role_map: Dict[int, discord.Role]):
    for member_id, old_role_ids in member_roles.items():
        member = guild.get_member(member_id)
        if member:
            new_roles = [role_map[role_id] for role_id in old_role_ids if role_id in role_map]
            await member.add_roles(*new_roles)
            print(f"Reassigned roles to member: {member.name}")

async def kick_all_members(guild: discord.Guild):
    """Kick all kickable members from the guild
    Returns a list of (member_name, error) tuples for members that couldn't be kicked"""
    failed_kicks = []
    
    for member in guild.members:
        # Skip bots and the guild owner
        if member.bot or member.id == guild.owner_id:
            continue
            
        # Skip ourselves
        if member.id == guild.me.id:
            continue
            
        # Skip members with higher roles
        if member.top_role >= guild.me.top_role:
            failed_kicks.append((member.name, "Higher role"))
            continue
            
        try:
            await member.kick(reason="Server nuke")
        except discord.Forbidden:
            failed_kicks.append((member.name, "Missing permissions"))
        except discord.HTTPException as e:
            failed_kicks.append((member.name, f"HTTP Error: {e.status}"))
        except Exception as e:
            failed_kicks.append((member.name, str(e)))
            
    return failed_kicks