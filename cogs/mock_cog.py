from unidecode import unidecode
import discord
from discord.ext import commands
import re
import aiohttp
import logging
from utils.rate_limiter import rate_limiter
import asyncio
import datetime
from typing import Optional
from utils.general import is_valid_emoji, format_message, quote_block
from typing import Union

logger = logging.getLogger(__name__)

class Mock(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.prefix = self.bot.config_manager.command_prefix
        self.mimic_target = None
        self.is_mimicking = False
        self.session = aiohttp.ClientSession()
        self.sent_messages = {}  # Track {original_msg_id: our_msg}
        self.custom_response = None  # Add this line
        
        # Updated report types and reasons with correct values
        self.REPORT_TYPES = {
            'MESSAGE': 0,
            'DM': 1
        }
        
        self.REPORT_REASONS = {
            'ILLEGAL': 0,  # Illegal content
            'HARASSMENT': 1,  # Harassment
            'SPAM': 2,  # Spam
            'SELF_HARM': 3,  # Self-harm content
            'NSFW': 4  # NSFW content in SFW channel
        }
        
        # Enhanced age detection patterns
        self.age_patterns = [
            r"(?i)i(?:\'|')?m?\s*(?:am)?\s*(?:under|only)?\s*\d{1,2}",
            r"(?i)(?:under|below)\s*(?:age|13)",
            r"(?i)i(?:\'|')?m?\s*(?:am)?\s*(?:a)?\s*(?:kid|child|minor)\s*(?:under)?\s*(?:\d{1,2})?",
            r"(?i)(?:my)?\s*age\s*(?:is)?\s*\d{1,2}",
        ]
        
        self.blocked_content = [
            "selfbot", "self bot", "self-bot",
            "cp", "child porn", "rape", "nigger", "bot"
            # Add other blocked terms as needed
        ]

        # Command prefix pattern (matches common prefixes)
        self.cmd_pattern = r'^[\.!$%^&*#@~/?,-;.]'

    async def report_underage(self, message, matched_text):
        """Report underage user via API using selfbot token"""
        try:
            headers = {
                'Authorization': self.bot.http.token,
                'Content-Type': 'application/json'
            }
            
            # Base payload - only include the essential fields
            payload = {
                'channel_id': str(message.channel.id),
                'message_id': str(message.id),  # We'll report the specific message
                'reason': self.REPORT_REASONS['ILLEGAL'],
                'report_type': self.REPORT_TYPES['MESSAGE']
            }

            # Add guild_id only if it's not a DM
            if message.guild:
                payload['guild_id'] = str(message.guild.id)
            else:
                # For DMs, switch to reporting the user instead of message
                payload = {
                    'channel_id': str(message.channel.id),
                    'user_id': str(message.author.id),  # Report the user in DMs
                    'reason': self.REPORT_REASONS['ILLEGAL'],
                    'report_type': self.REPORT_TYPES['DM']
                }

            async with self.session.post(
                'https://discord.com/api/v9/report', 
                headers=headers, 
                json=payload
            ) as resp:
                if resp.status in (201, 200):
                    logger.info(f"Successfully reported underage user {message.author.id}")
                else:
                    error_data = await resp.json()
                    logger.error(f"Failed to report user: Status {resp.status}, Response: {error_data}")
                    
                if resp.status == 429:
                    retry_after = (await resp.json()).get('retry_after', 5)
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return await self.report_underage(message, matched_text)
                    
        except Exception as e:
            logger.error(f"Error reporting underage user: {e}")

    def clean_mentions(self, content: str, message: discord.Message) -> str:
        """Clean mentions to prevent self-mentions"""
        # Replace any mention of the bot with a mention of the original author
        if self.bot.user.mentioned_in(message):
            content = content.replace(f'<@{self.bot.user.id}>', f'<@{message.author.id}>')
            content = content.replace(f'<@!{self.bot.user.id}>', f'<@{message.author.id}>')
        return content

    def is_safe_content(self, message: discord.Message) -> bool:
        """Enhanced safety check with improved underage user detection"""
        content = message.content.lower()
        
        # Patterns that indicate the message author is referring to themselves
        self_reference_patterns = [
            r'\bi(?:\s+am|\s*\'m)\b',  # "i am" or "i'm"
            r'\biam\b',                 # "iam"
            r'\bme\b',                  # "me"
            r'\bmyself\b',              # "myself"
            r'\bmy\b'                   # "my"
        ]
        
        # Patterns that indicate discussion of age
        age_related_patterns = [
            r'(?:age|years?\s*old|y/?o)',
            r'(?:kid|child|minor|young|youth|underage)',
            r'(?:under|below|beneath)',
            r'(?:born\s+in|since)',
        ]
        
        # First check if message contains both self-reference and age-related content
        has_self_reference = any(re.search(pattern, content) for pattern in self_reference_patterns)
        has_age_content = any(re.search(pattern, content) for pattern in age_related_patterns)
        
        if has_self_reference and has_age_content:
            # Look for numbers in the message
            number_matches = re.findall(r'\b(\d+)\b', content)
            for num_str in number_matches:
                try:
                    age = int(num_str)
                    # Report if age is suspiciously young (under 16)
                    if age < 16:
                        asyncio.create_task(self.report_underage(message, content))
                        return False
                except ValueError:
                    continue
        
        # Check for explicit age statements
        explicit_age_patterns = [
            r'(?:i(?:\s+am|\s*\'?m)?|me|my|myself)\s*(?:age\s+(?:is|being|=))?\s*(\d{1,2})',
            r'(?:i\s+turn(?:ed)?|turned)\s*(\d{1,2})',
            r'(?:i(?:\s+am|\s*\'?m)?)\s*(?:a)?\s*(\d{1,2})\s*(?:y/?o|year|years?\s*old)',
            r'(?:born\s+in)\s*(?:19|20)(\d{2})',
        ]
        
        current_year = datetime.datetime.now().year
        
        for pattern in explicit_age_patterns:
            matches = re.finditer(pattern, content)
            for match in matches:
                try:
                    if 'born in' in match.group(0).lower():
                        birth_year = int('19' + match.group(1)) if match.group(1).startswith('9') else int('20' + match.group(1))
                        age = current_year - birth_year
                    else:
                        age = int(match.group(1))
                    
                    if age < 16:
                        asyncio.create_task(self.report_underage(message, match.group(0)))
                        return False
                except (ValueError, IndexError):
                    continue
        
        return True

    def clean_message(self, content: str, is_custom: bool = False) -> str:
        """Clean message content and normalize special fonts"""
        # Normalize special fonts/characters to regular text, preserving emojis
        temp = ""
        skip_until = 0
        
        # Preserve default emojis and custom emojis
        for i, char in enumerate(content):
            if i < skip_until:
                continue
            
            # Check for custom emoji <:name:id> or <a:name:id>
            if char == '<' and i + 1 < len(content):
                if content[i+1] == ':' or (content[i+1] == 'a' and i + 2 < len(content) and content[i+2] == ':'):
                    emoji_end = content.find('>', i)
                    if emoji_end != -1:
                        temp += content[i:emoji_end+1]
                        skip_until = emoji_end + 1
                        continue
                        
            # Check for default emoji (Unicode)
            if is_valid_emoji(char):
                temp += char
            else:
                temp += unidecode(char)
                
        content = temp
        
        # Remove selfbot prefix and common bot prefixes
        while content.startswith(self.prefix) or re.match(self.cmd_pattern, content):
            content = content[1:]
        
        # Replace age numbers under 16 with 18
        def replace_age(match):
            age = int(match.group(1))
            return match.group(0).replace(str(age), "18") if age < 16 else match.group(0)
        
        # Age-related patterns to rewrite
        age_patterns = [
            r'(?i)(?:age|am|\'m|\s+is)\s*(\d{1,2})(?:\s*(?:y/?o|years?\s*old))?',
            r'(?i)(?:born\s+in\s+(?:19|20)(\d{2}))',
            r'(?i)(?:turn(?:ing|ed)?\s+)(\d{1,2})',
        ]
        
        for pattern in age_patterns:
            content = re.sub(pattern, replace_age, content)
          # Replace self-referential pronouns
        if not is_custom:
            pronouns_map = {
                r'\b(?:i am|im|i\'m)\b': "you're",
                r'\biam\b': "you're", 
                r'\bi\s+am\b': "you're",
                r'\bi\'m\b': "you're",
                r'\bme\b': "you",
                r'\bmy\b': "your",
                r'\bmyself\b': "yourself",
                r'\bi(?:\s+|\b)': "you ",
            }
            
            for pattern, replacement in pronouns_map.items():
                content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)
            
            # Clean up any double spaces only for non-custom responses
            content = ' '.join(content.split())
        
        # For custom responses, we preserve spacing but still trim leading/trailing whitespace
        return content.strip()

    @commands.command(aliases=['mk'])
    async def mock(self, ctx, user: Optional[Union[discord.Member, discord.User]], *, custom_response: str = None):
        """Mimic a user
        mock @user - Copy their messages
        mock @user hello - Reply with custom text"""
        try:
            await ctx.message.delete()
        except:
            pass
    
        if not user:
            await ctx.send(
                format_message("Please specify a user to mock"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
    
        # Use display_name instead of just name for better server context
        display_name = user.display_name if isinstance(user, discord.Member) else user.name
    
        # Check for self mocking or bot mocking 
        if user.id == self.bot.user.id or user.id == ctx.author.id or user.bot:
            await ctx.send(
                format_message("You can't mock yourself or a bot"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
    
        if self.is_mimicking:
            self.is_mimicking = False
            self.mimic_target = None
            self.custom_response = None
            return
    
        self.mimic_target = user.id
        self.is_mimicking = True 
        self.custom_response = custom_response
    

    @rate_limiter(command_only=True)
    async def send_mimic_message(self, original_message: discord.Message, content: str) -> Optional[discord.Message]:
        """Send a mimicked message with rate limiting"""
        try:
            sent_msg = await original_message.reply(content)
            self.sent_messages[original_message.id] = sent_msg
            return sent_msg
        except discord.Forbidden:
            logger.error(f"Failed to send mimic message: Forbidden")
            # stop mimicking if failed
            self.is_mimicking = False
            self.mimic_target = None
            self.custom_response = None
            return None


    async def _handle_message(self, message):
        """Handler for message events"""
        if not self.is_mimicking or message.author.id != self.mimic_target:
            return
            
        if message.author.bot:
            return
        
        if self.custom_response:
            content = self.custom_response
            content = self.clean_message(content, is_custom=True)
        else:
            content = message.content
            content = self.clean_message(content)
        
        if not self.is_safe_content(message):
            return
            
        content = self.clean_mentions(content, message)
        
        if not content.strip():
            return
            
        await self.send_mimic_message(message, content)

    async def _handle_message_delete(self, message):
        """Handler for message delete events"""
        if message.id in self.sent_messages:
            try:
                our_msg = self.sent_messages[message.id]
                await our_msg.delete()
                del self.sent_messages[message.id]
            except (discord.NotFound, discord.HTTPException):
                pass

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)
            event_manager.register_handler('on_message_delete', self.__class__.__name__, self._handle_message_delete)

    async def cog_unload(self):
        """Cleanup and unregister event handlers"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)
        
        self.is_mimicking = False
        self.mimic_target = None
        self.sent_messages.clear()
        if self.session:
            await self.session.close()

    @commands.command(aliases=['smk'])
    async def stopmock(self, ctx):
        """Stop mimicking"""
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass
            
        # Always reset the mimicking state regardless of current state
        self.is_mimicking = False
        # Clear any pending messages
        self.sent_messages.clear()
        self.custom_response = None
        
        if self.mimic_target is not None:
            self.mimic_target = None

async def setup(bot):
    await bot.add_cog(Mock(bot))
