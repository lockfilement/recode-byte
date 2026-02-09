import discord
from discord.ext import commands
import deepl
import logging
from utils.rate_limiter import rate_limiter
from typing import Optional

logger = logging.getLogger(__name__)

class Translate(commands.Cog):
    """A cog for translating text using DeepL API"""
    
    def __init__(self, bot):
        self.bot = bot
        # Use the shared translator instance from bot manager
        self.bot_manager = self.bot._manager
        # Fallback variables if shared translator is not available
        self.api_key = "532b23d1-2a30-4d80-8c27-c53af2272275:fx"
        self.translator = None
        self.source_languages = {}
        self.target_languages = {}
        self.language_code_map = {}
        
        # Initialize translator (shared if available, else local)
        self.initialize_translator()
        
    def initialize_translator(self):
        """Initialize the DeepL translator - use shared if available, else local"""
        try:
            if self.bot_manager and self.bot_manager.shared_deepl_translator:
                logger.info("Using shared DeepL translator from BotManager")
                # Use the shared translator
                self.translator = self.bot_manager.shared_deepl_translator
                # Use the shared language data
                self.source_languages = self.bot_manager.deepl_source_languages
                self.target_languages = self.bot_manager.deepl_target_languages
                self.language_code_map = self.bot_manager.deepl_language_code_map
            else:
                logger.info("Shared DeepL translator not available, initializing local instance")
                # Fall back to local translator if shared one is not available
                if self.api_key and self.api_key != "YOUR_DEEPL_API_KEY_HERE":
                    self.translator = deepl.Translator(self.api_key)
                    
                    # Fetch and cache available languages
                    self.source_languages = {lang.code: lang.name for lang in self.translator.get_source_languages()}
                    self.target_languages = {lang.code: lang.name for lang in self.translator.get_target_languages()}
                    
                    # Initialize language code map
                    self.language_code_map = {
                        "EN": "EN-US",  # Default English to US English
                        "PT": "PT-BR",  # Default Portuguese to Brazilian Portuguese
                        "ZH": "ZH",     # Default Chinese (already exists as is)
                    }
                    
                    logger.info("Local DeepL translator initialized successfully")
                else:
                    logger.warning("No DeepL API key set or using placeholder key")
        except Exception as e:
            logger.error(f"Failed to initialize DeepL translator: {e}")
    
    def get_language_name(self, code, is_target=True):
        """Get language name from code"""
        if is_target:
            return self.target_languages.get(code.upper(), "Unknown language")
        return self.source_languages.get(code.upper(), "Unknown language")
    
    def quote_block(self, text: str) -> str:
        """Add > prefix to each line while preserving the content"""
        return '\n'.join(f'> {line}' for line in text.split('\n'))
    
    @commands.command(aliases=["tr"])
    @rate_limiter(command_only=True)
    async def translate(self, ctx, target_lang: str, *, text: Optional[str] = None):
        """Translate
        
        .translate [target_language] [text]
        You can also reply to a message to translate it
        
        Examples:
        .translate EN Hello world
        .translate DE How are you?
        """
        # Delete the command message if auto-delete is enabled
        try:
            await ctx.message.delete()
        except:
            pass
        
        # Initialize translator if not done already
        if not self.translator and self.api_key != "YOUR_DEEPL_API_KEY_HERE":
            self.initialize_translator()
        
        if not self.translator:
            await ctx.send(
                "❌ DeepL API key not configured. Please set your API key in the translate_cog.py file.",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
        
        # Handle replied message
        if not text and ctx.message.reference:
            replied_message = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            if replied_message:
                text = replied_message.content
        
        # Check if we have text to translate
        if not text:
            await ctx.send(
                "❌ Please provide text to translate or reply to a message",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
        
        # Normalize target language code - ensure properly cased for DeepL API
        original_target_lang = target_lang.upper()
        target_lang = original_target_lang
        
        # Check if we need to map a simple code to DeepL's specific code
        if target_lang in self.language_code_map:
            target_lang = self.language_code_map[target_lang]
            logger.info(f"Mapped language code {original_target_lang} to {target_lang}")
        
        # Log the language code and available languages to help debug
        logger.info(f"Attempting to translate to target language: {target_lang}")
        logger.info(f"Available target languages: {', '.join(self.target_languages.keys())}")
        
        try:
            # First check if the target language is directly supported
            if target_lang not in self.target_languages:
                # If not directly supported, try case-insensitive comparison
                available_langs_upper = [lang.upper() for lang in self.target_languages.keys()]
                if target_lang not in available_langs_upper:
                    # Check if any language code starts with the requested code (e.g. "EN" could match "EN-US")
                    matching_codes = [code for code in self.target_languages.keys() if code.startswith(target_lang)]
                    
                    if matching_codes:
                        # Use the first matching language code
                        target_lang = matching_codes[0]
                        logger.info(f"Found partial match: {original_target_lang} → {target_lang}")
                    else:
                        message = "```ansi\n\u001b[1;31mError: Unsupported language code\u001b[0m```"
                        
                        # Add more details for troubleshooting
                        message += f"\n```Available target language codes: {', '.join(sorted(self.target_languages.keys()))}```"
                        
                        await ctx.send(
                            self.quote_block(message),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                        return
                else:
                    # Find the properly cased language code
                    for lang_code in self.target_languages.keys():
                        if lang_code.upper() == target_lang:
                            target_lang = lang_code
                            break
            
            # Translate the text
            result = self.translator.translate_text(
                text,
                target_lang=target_lang
            )
            
            # Format the response
            translated_text = result.text
            detected_lang = result.detected_source_lang
            detected_lang_name = self.get_language_name(detected_lang, is_target=False)
            target_lang_name = self.get_language_name(target_lang)
            
            # Truncate text if too long
            max_display_length = 600  # Keep messages readable on screen
            orig_text = text[:max_display_length] + "..." if len(text) > max_display_length else text
            trans_text = translated_text[:max_display_length] + "..." if len(translated_text) > max_display_length else translated_text
            
            message_parts = [
                "```ansi\n" + \
                f"\u001b[30m\u001b[1m\u001b[4mTranslation: {detected_lang_name} → {target_lang_name}\u001b[0m```",
                
                "```ansi\n" + \
                f"\u001b[0;36mOriginal:\u001b[0;37m\n{orig_text}```",
                
                "```ansi\n" + \
                f"\u001b[0;36mTranslation:\u001b[0;37m\n{trans_text}```"
            ]
            
            await ctx.send(
                self.quote_block(''.join(message_parts)),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            
        except deepl.exceptions.DeepLException as e:
            await ctx.send(
                self.quote_block(f"```ansi\n\u001b[1;31mDeepL API error: {str(e)}\u001b[0m```"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            logger.error(f"DeepL API error: {e}")
        except Exception as e:
            await ctx.send(
                self.quote_block(f"```ansi\n\u001b[1;31mError: {str(e)}\u001b[0m```"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            logger.error(f"Error in translate command: {e}")
    
    @commands.command(aliases=["langs"])
    async def languages(self, ctx):
        """Show all languages"""
        try:
            await ctx.message.delete()
        except:
            pass
        
        if not self.translator and self.api_key != "YOUR_DEEPL_API_KEY_HERE":
            self.initialize_translator()
            
        if not self.translator:
            await ctx.send(
                "❌ DeepL API key not configured. Please set your API key in the translate_cog.py file.",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
        
        # Format source languages (used for detection)
        source_langs = ", ".join([f"{code}" for code in sorted(self.source_languages.keys())])
        
        # Format target languages (for translation)
        target_langs_list = [(code, name) for code, name in self.target_languages.items()]
        target_langs_list.sort()  # Sort by language code
        
        # Format the list of target languages into columns
        target_langs_formatted = []
        for code, name in target_langs_list:
            target_langs_formatted.append(f"{code}: {name}")
        
        message_parts = [
            "```ansi\n" + \
            "\u001b[30m\u001b[1m\u001b[4mDeepL Supported Languages\u001b[0m```",
            
            "```ansi\n" + \
            "\u001b[0;36mSource Languages (Auto-Detect):\u001b[0;37m\n" + \
            f"{source_langs}```",
            
            "```ansi\n" + \
            "\u001b[0;36mTarget Languages:\u001b[0;37m\n" + \
            "\n".join(target_langs_formatted) + "```",
            
            "```ansi\n" + \
            "\u001b[0;36mSimplified Codes:\u001b[0;37m\n" + \
            "You can use these simplified codes:\n" + \
            "EN → EN-US (English)\n" + \
            "PT → PT-BR (Portuguese)\n" + \
            "Other languages can be used with their exact code.```"
        ]
        
        await ctx.send(
            self.quote_block(''.join(message_parts)),
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )

async def setup(bot):
    await bot.add_cog(Translate(bot))