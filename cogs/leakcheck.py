# File: cogs/leakcheck.py
import discord
from discord.ext import commands
import asyncio
import io
import logging
from PIL import Image
from utils.image_utils import generate_card_image
from utils.general import get_max_message_length
import aiohttp
from urllib.parse import quote
import ijson
import logging
import concurrent.futures
from utils.rate_limiter import rate_limiter
import datetime
import re

logger = logging.getLogger(__name__)

def remove_ansi_codes(text):
    ansi_escape = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)

def make_hashable(data):
    """Convert a dictionary into a hashable tuple."""
    if isinstance(data, dict):
        return tuple((k, make_hashable(v)) for k, v in sorted(data.items()))
    elif isinstance(data, list):
        return tuple(make_hashable(item) for item in data)
    else:
        return data

class LeakCheck(commands.Cog):
    BASE_URL = "https://leakcheck.io/api/v2/query/"
    VALID_TYPES = ["auto", "keyword"]
    ERROR_MESSAGES = {
        "no_query": "Error: No query provided.",
        "short_query": "Error: Query must be at least 3 characters long.",
        "invalid_type": f"Error: Invalid search type. Valid types are: {', '.join(VALID_TYPES)}",
        "api_error": "Error: {status} - {text}"
    }

    def __init__(self, bot, api_key):  # Add api_key parameter
        self.bot = bot
        self.api_key = api_key  # Store the api_key
        self.stop_pagination = False
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)  # Adjust the number of workers as needed

    async def can_send_images(self, ctx: commands.Context) -> bool:
        try:
            if isinstance(ctx.channel, (discord.DMChannel, discord.GroupChannel)):
                return True  # Always can send images in DMs and GCs
            if ctx.guild and ctx.guild.me:
                permissions = ctx.channel.permissions_for(ctx.guild.me)
                return permissions.attach_files
            return False
        except Exception as e:
            logging.error(f"Error checking permissions: {e}")
            return False
        
    @commands.command(aliases=['lcp'])
    async def stop(self, ctx: commands.Context):
        try:await ctx.message.delete()
        except:pass
        self.stop_pagination = True

    @commands.command(aliases=['lc'])
    async def check(self, ctx, *args):
        """Check for leaked data
        
        check <data> - Check for leaks (email, username, phone, etc.)
        """
        try:await ctx.message.delete()
        except:pass
        logging.debug("Leakcheck command invoked")

        if not args:
            await self.send_temp_message(ctx, self.ERROR_MESSAGES["no_query"])
            return

        search_type, query = self.parse_args(args)
        if len(query) < 3:
            await self.send_temp_message(ctx, self.ERROR_MESSAGES["short_query"])
            return

        if search_type not in self.VALID_TYPES:
            await self.send_temp_message(ctx, self.ERROR_MESSAGES["invalid_type"])
            return

        await self.perform_check(ctx, search_type, query)

    def parse_args(self, args: tuple) -> tuple:
        if len(args) == 1:
            return "auto", args[0]
        return args[0], " ".join(args[1:])

    async def perform_check(self, ctx: commands.Context, search_type: str, query: str):
        logger.debug(f"Performing check with type: {search_type}, query: {query}")
        try:
            results = []
            unique_results = set()
            result_count = 0
            
            async for data_chunk in self.stream_check_results(search_type, query):
                hashable_data_chunk = make_hashable(data_chunk)
                if hashable_data_chunk not in unique_results:
                    unique_results.add(hashable_data_chunk)
                    results.append(data_chunk)
                    result_count += 1

            logger.debug(f"Total unique results after filtering: {len(results)}")
            
            if results:
                await ctx.send(
                    f"Found {result_count} results" + 
                    (", filtered to breaches from the last 5 years" if result_count >= 50 else ""),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                await self.send_formatted_data(ctx, results)
            else:
                await self.send_temp_message(ctx, "No results found")

        except aiohttp.ClientResponseError as e:
            logger.error(f"API request failed: {e}")
            await self.send_temp_message(ctx, f"API request failed: {e.message}")

    async def stream_check_results(self, search_type: str, query: str):
        headers = {
            "Accept": "application/json",
            "X-API-Key": self.api_key
        }

        encoded_query = quote(query)
        url = f"{self.BASE_URL}{encoded_query}?type={search_type}"
        current_year = datetime.datetime.now().year
        all_results = []  # Store all results first
        
        def is_recent_breach(breach_date: str) -> bool:
            if not breach_date:
                return False
            try:
                # Extract year from YYYY-MM format
                breach_year = int(breach_date.split('-')[0])
                # Check if breach is within last 5 years
                return current_year - breach_year <= 5
            except (ValueError, IndexError):
                return False

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                response_text = ""
                async for chunk in response.content.iter_chunked(4096):
                    response_text += chunk.decode('utf-8')
                    try:
                        for item in ijson.items(response_text, 'result.item'):
                            all_results.append(item)
                        response_text = ""  # Reset response_text after successful parsing
                    except ijson.JSONError:
                        continue

        # Now process all results
        if len(all_results) > 50:
            # Filter to last 5 years
            filtered_results = [
                item for item in all_results 
                if is_recent_breach(item.get('source', {}).get('breach_date', ''))
            ]
            
            if filtered_results:
                logger.info(f"Found {len(all_results)} results, filtered to {len(filtered_results)} from last 5 years")
                for item in filtered_results:
                    yield item
            else:
                # If no recent results, take oldest 50
                logger.info(f"No results from last 5 years, returning first 50 results")
                for item in all_results[:50]:
                    yield item
        else:
            # Under 50 results, return all
            logger.info(f"Found {len(all_results)} results, under threshold - returning all")
            for item in all_results:
                yield item

    async def send_formatted_data(self, ctx: commands.Context, results: list):
        logger.debug(f"send_formatted_data called with {len(results)} results")
        if not results:
            await self.send_temp_message(ctx, "No results found.", delay=60)
            return

        formatted_results = self.format_results(results)
        await self.paginate_results(ctx, formatted_results)

    def format_results(self, results: list) -> list:
        formatted_results = []
        fields = [
            ('Source', lambda r: r['source']['name'] if 'source' in r and 'name' in r['source'] else 'N/A'),
            ('Breach Date', lambda r: r['source']['breach_date'] if 'source' in r and 'breach_date' in r['source'] else 'N/A'),
            ('Username', 'username'),
            ('Email', 'email'),
            ('Password', 'password'),
            ('Origin', lambda r: ', '.join(r['origin']) if 'origin' in r else 'N/A'),
            ('IP', 'ip'),
            ('Date of Birth', 'dob'),
            ('City', 'city'),
            ('Country', 'country'),
            ('First Name', 'first_name'),
            ('Last Name', 'last_name')
        ]
        for result in results:
            formatted_result = []
            for field_name, field_key in fields:
                value = field_key(result) if callable(field_key) else result.get(field_key)
                if value and value != 'N/A':
                    formatted_result.append(f"\u001b[0;37m{field_name}\u001b[0m: \u001b[0m\u001b[40m\u001b[31m{value}\u001b[0m")
            formatted_results.append("\n".join(formatted_result))
        return formatted_results

    async def generate_card_image_async(self, results: list, total_results: int, page: int, total_pages: int) -> Image:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, generate_card_image, results, total_results, page, total_pages)

    @rate_limiter(command_only=True)
    async def paginate_results(self, ctx: commands.Context, formatted_results: list, delay: float = 2.0):
        self.stop_pagination = False
        total_results = len(formatted_results)
        max_results_per_card = 10
        result_chunks = [formatted_results[i:i + max_results_per_card] 
                        for i in range(0, total_results, max_results_per_card)]
        total_pages = len(result_chunks)
        sent_messages = []
        can_send_images = await self.can_send_images(ctx)
        
        for i, chunk in enumerate(result_chunks):
            try:
                if self.stop_pagination:
                    break
                    
                logger.debug(f"Sending page {i + 1} with {len(chunk)} results")
                if can_send_images:
                    chunk_without_ansi = [remove_ansi_codes(result) for result in chunk]
                    image = await self.generate_card_image_async(
                        chunk_without_ansi, total_results, i + 1, total_pages
                    )
                    if image:
                        message = await self.send_image(ctx, image, i + 1)
                    else:
                        message = await self.send_code_block(ctx, chunk, i + 1, total_pages)
                else:
                    message = await self.send_code_block(ctx, chunk, i + 1, total_pages)
                    
                if message:
                    sent_messages.append(message)
                await asyncio.sleep(delay)
                
            except discord.HTTPException as e:
                if e.status == 429:
                    print("Rate limited, waiting...")
                    
        self.schedule_message_deletion(sent_messages)

    async def send_image(self, ctx: commands.Context, image: Image, page_number: int) -> discord.Message:
        with io.BytesIO() as image_binary:
            image.save(image_binary, 'PNG')
            image_binary.seek(0)
            return await ctx.send(file=discord.File(fp=image_binary, filename=f'results_page_{page_number}.png'))

    def quote_block(self, text):
        """Add > prefix to each line while preserving content"""
        return '\n'.join(f'> {line}' for line in text.split('\n'))

    async def send_code_block(self, ctx: commands.Context, chunk: list, page: int, total_pages: int) -> discord.Message:
        """Send formatted results in a code block with consistent ANSI styling"""
        max_length = get_max_message_length(self.bot)
        
        message_parts = [
            "```ansi\n" + \
            f"\u001b[30m\u001b[1m\u001b[4mLeakCheck Results \u001b[0;37m(Page {page}/{total_pages})\n" + \
            "```",  # Thin dotted separator

            "```ansi\n" + \
            "\u001b[30m\u001b[1m\u001b[4mFound Data\u001b[0m\n"
        ]

        header_footer_length = sum(len(part) for part in message_parts) + 50
        max_result_length = max_length - header_footer_length

        separator = "\u001b[0;37m" + "┄" * 20 + "\n"  # Thin dotted separator

        for result in chunk:
            result_text = f"{result}\n"
            if len(message_parts[-1] + result_text) > max_result_length:
                break
            # Add separator between results, but not after the last one
            message_parts[-1] += result_text + separator if chunk.index(result) < len(chunk) - 1 else result_text

        # Add footer 
        message_parts.append(
            "``````ansi\n" + \
            f"Made by: \u001b[1m\u001b[37m{self.bot.config_manager.developer_name}\u001b[0m\n" + \
            f"Version: \u001b[1m\u001b[37m{self.bot.config_manager.version}\u001b[0m```"
        )

        return await ctx.send(self.quote_block(''.join(message_parts)))

    def schedule_message_deletion(self, messages: list):
        """Schedule messages for deletion based on config"""
        if self.bot.config_manager.auto_delete.enabled:
            for message in messages:
                self.bot.loop.create_task(
                    self.delete_message_after_delay(
                        message, 
                        self.bot.config_manager.auto_delete.delay * 2  # Double delay for longer messages
                    )
                )

    async def delete_message_after_delay(self, message, delay):
        await asyncio.sleep(delay)
        try:
            await message.delete()
        except discord.errors.NotFound:
            pass  # Message was already deleted
        except discord.errors.Forbidden:
            logging.warning(f"Bot doesn't have permission to delete message {message.id}")
        except Exception as e:
            logging.error(f"Error deleting message {message.id}: {str(e)}")

    async def send_temp_message(self, ctx: commands.Context, message: str, delay: int = None):
        """Send a temporary message that gets deleted after delay"""
        msg = await ctx.send(message, 
            delete_after=delay if delay else (
                self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
        )
        return msg

async def setup(bot: commands.Bot):
    # Prefer token-specific API key via ConfigManager cache, fallback to global
    cfg = await bot.config_manager._get_cached_config_async()

    api_key = None
    try:
        api_key = cfg.get('user_settings', {}).get(bot.config_manager.token, {}).get('leakcheck_api_key')
    except Exception:
        api_key = None

    if not api_key:
        api_key = cfg.get('leakcheck_api_key')

    if not api_key:
        raise ValueError("No LeakCheck API key found in config")

    await bot.add_cog(LeakCheck(bot, api_key))
