import re
import discord
from discord.ext import commands
# from aiocache import caches, cached
from typing import Dict
import logging

# setup logging
logger = logging.getLogger(__name__)

# Global variable to store max message length
MAX_MESSAGE_LENGTH: Dict[int, int] = {}

from discord import PremiumType

async def detect_message_limit(bot: commands.Bot) -> int:
    """
    Detect the max message length for the bot user using Discord's PremiumType enum.
    Returns 4000 for Nitro users, 2000 for non-Nitro users.
    """
    try:
        if bot.user.id not in MAX_MESSAGE_LENGTH:
            # Check premium type using the proper enum
            has_nitro = bot.user.premium_type in (
                PremiumType.nitro,
                PremiumType.nitro_classic,
                PremiumType.nitro_basic
            )
            
            limit = 4000 if has_nitro else 2000
            MAX_MESSAGE_LENGTH[bot.user.id] = limit
            
            logger.info(f"Message limit for {bot.user.name}: {limit} (Nitro: {has_nitro})")
            
        return MAX_MESSAGE_LENGTH[bot.user.id]
        
    except Exception as e:
        logger.error(f"Error in message limit detection: {e}")
        return 2000  # Default fallback

def get_max_message_length(bot: commands.Bot) -> int:
    """
    Get the cached max message length or detect it if not cached
    """
    return MAX_MESSAGE_LENGTH.get(bot.user.id, 2000)

def format_as_yaml_code_block(results: list) -> str:
    formatted_rows = []

    for result in results:
        formatted_result = result.split('\n')
        source_line = formatted_result[0]
        data_lines = formatted_result[1:]
        formatted_rows.append(f"{source_line}\n" + "\n".join(data_lines))

    yaml_formatted = "\n\n".join(formatted_rows)
    return f"```yaml\n{yaml_formatted}\n```"

def calculate_chunk_size(total_results: int) -> int:
    if total_results <= 9:
        return total_results
    elif total_results <= 18:
        return (total_results + 1) // 2  # Round up to ensure all results are included
    else:
        return min(20, max(9, (total_results + 2) // 3))  # Ensure at least 9 results per page
    
def is_valid_emoji(emoji: str) -> bool:
    """Check if a string is a valid emoji using discord.py's emoji parsing"""
    try:
        # Reject emoji strings wrapped with colons (like :sk:) that aren't actual Discord emojis
        if emoji.startswith(':') and emoji.endswith(':') and len(emoji) > 2:
            emoji_name = emoji[1:-1]
            # If it's just a short text between colons, it's likely not a valid emoji
            if len(emoji_name) <= 2 or emoji_name.isalpha():
                return False
        
        # Use Discord's built-in method to parse the emoji
        partial_emoji = discord.PartialEmoji.from_str(emoji)
        
        # Check if it's a valid custom emoji (has ID)
        if partial_emoji.is_custom_emoji() and partial_emoji.id is not None:
            return True
        
        # For Unicode emojis, apply more strict validation
        if partial_emoji.is_unicode_emoji():
            # Check if unicode emoji consists purely of alphabetic characters
            # This filters out cases where words like "NIGGA" or "UR" are falsely 
            # identified as emoji
            if emoji.isalpha() and len(emoji) > 1:
                return False
                
            # Stricter check using the string representation
            partial_str = str(partial_emoji)
            # If Discord returned a different string than we gave it, it confirms it's an emoji
            if partial_str != emoji:
                return False
                
            # Reject emoji-like strings with colons if they're not actual emojis
            if emoji.startswith(':') and emoji.endswith(':'):
                emoji_name = emoji[1:-1]
                # For short text with colons like :sk:, it's probably not valid
                if len(emoji_name) <= 3:
                    return False
                
            return True
            
        return False
    except Exception:
        return False

def filter_valid_emojis(emojis):
    return [emoji for emoji in emojis if is_valid_emoji(emoji)]

def format_message(content, code_block=True, escape_backticks=True):
    """
    Format a message with proper styling
    - Optionally wrap in code block (default: True)
    - Optionally escape backticks in the content (default: True)
    
    Use this for consistent message formatting across commands
    """
    if escape_backticks and code_block:
        # Replace backticks with escaped version if we're using code blocks
        content = content.replace('`', '\\`') if isinstance(content, str) else str(content)
    
    if code_block:
        # If the content contains backticks and we're not escaping them, 
        # don't use code block formatting
        if '`' in content and not escape_backticks:
            return content
        return f"`{content}`"
    
    return content

def quote_block(text):
    """Add > prefix to each line while preserving the content"""
    
    return '\n'.join(f'> {line}' for line in text.split('\n'))

# # Configure the cache
# caches.set_config({
#     'default': {
#         'cache': 'aiocache.SimpleMemoryCache',
#         'serializer': {
#             'class': 'aiocache.serializers.PickleSerializer'
#         },
#         'plugins': [
#             {'class': 'aiocache.plugins.HitMissRatioPlugin'},
#             {'class': 'aiocache.plugins.TimingPlugin'}
#         ],
#         'ttl': 3600  # Time-to-live for cache items (in seconds)
#     }
# })

# @cached()
# async def query_guild_members(guild, query):
#     print(f"Querying members for guild: {guild.name} with query: {query}")
#     try:
#         members = await guild.query_members(query=query, limit=100, presences=False)
#         print(f"Found {len(members)} members matching query '{query}' in guild {guild.name}")
#         return members
#     except Exception as e:
#         print(f"Error while querying guild members: {e}")
#         return []

# async def fetch_members(guild, usernames):
#     try:
#         members = []
#         for username in usernames:
#             # Check the cache first
#             cached_member_id = await caches.get('default').get(username)
#             if cached_member_id:
#                 cached_member = guild.get_member(cached_member_id)
#                 if cached_member:
#                     members.append(cached_member)
#                     print(f"Found member in cache: {cached_member.name} (ID: {cached_member.id})")
#                     continue

#             # If not in cache, query the guild members
#             queried_members = await query_guild_members(guild, username)
#             matching_member = discord.utils.find(
#                 lambda m: m.name.lower() == username.lower() or m.display_name.lower() == username.lower(),
#                 queried_members
#             )
#             if matching_member:
#                 members.append(matching_member)
#                 await caches.get('default').set(username, matching_member.id)  # Cache only the member ID
#                 print(f"Found member: {matching_member.name} (ID: {matching_member.id})")
#             else:
#                 print(f"Member not found: {username}")
#         return members
#     except Exception as e:
#         print(f"Error while fetching guild members: {e}")
#         return []

async def parse_users_and_emojis(bot, ctx, args):
    users = []
    emojis = []

    for arg in args:
        if is_valid_emoji(arg):
            emojis.append(arg)
        elif arg.startswith('<@') and arg.endswith('>'):
            user_id = int(arg[2:-1].replace('!', ''))
            user = discord.utils.get(bot.users, id=user_id)
            if user:
                users.append(user)
            else:
                print(f"Failed to fetch user for mention: {arg}")
        else:
            # If it's not an emoji or mention, treat it as a username or user ID
            try:
                user_id = int(arg)
                user = discord.utils.get(bot.users, id=user_id)
                if user:
                    users.append(user)
                else:
                    print(f"Failed to fetch user for ID: {user_id}")
            except ValueError:
                # If it's not a valid integer, treat it as a username
                user = discord.utils.get(bot.users, name=arg)
                if user:
                    users.append(user)
                else:
                    print(f"Failed to fetch user for name: {arg}")
                    # Create a placeholder user object
                    user = discord.Object(id=0)
                    user.name = arg
                    users.append(user)

    if not users and ctx.guild is None:
        # If no users were found and we're in a DM, add the author
        users.append(ctx.author)

    return users, emojis
