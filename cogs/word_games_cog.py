import discord
from discord.ext import commands
import asyncio
import random
import re
import logging
from typing import Dict, Set, Optional

logger = logging.getLogger(__name__)

class WordGames(commands.Cog):
    """Unified cog for handling word games from Mudae and Bleed bots"""
    
    # Bot IDs
    MUDAE_ID = 432610292342587392
    BLEED_ID = 593921296224747521
    
    # Game configurations
    GAME_CONFIGS = {
        'mudae_green': {
            'bot_id': MUDAE_ID,
            'start_pattern': "Green Teaword will start!",
            'prompt_pattern': r"Quickly type a word containing: \*\*(\w+)\*\*",
            'requires_mention': False,
            'success_emoji': "🥇",
            'strategy': 'speed',  # Pick randomly, respond quickly
            'delays': {'join': (3, 8), 'response': (0.3, 0.8)}
        },
        'mudae_red': {
            'bot_id': MUDAE_ID,
            'start_pattern': "Red Teaword will start!",
            'prompt_pattern': r"Type the longest word containing: \*\*(\w+)\*\*",
            'requires_mention': False,
            'success_emoji': "🥇",
            'strategy': 'longest',  # Pick longest word
            'delays': {'join': (3, 8), 'response': (0.8, 1.5)}
        },
        'mudae_black': {
            'bot_id': MUDAE_ID,
            'start_pattern': "Black Teaword will start!",
            'prompt_pattern': r"Type a word containing: \*\*(\w+)\*\*",
            'requires_mention': True,
            'success_emoji': "✅",
            'strategy': 'survival',  # Moderate strategy
            'delays': {'join': (3, 8), 'response': (0.5, 1.2)}
        },
        'bleed': {
            'bot_id': BLEED_ID,
            'start_pattern': "Waiting for **players**",
            'prompt_pattern': r"letters: \*\*(\w+)\*\*",
            'requires_mention': True,
            'success_emoji': "✅",
            'strategy': 'shortest',  # Prefer shorter words first
            'delays': {'join': (5, 10), 'response': (0.5, 1.5)}
        }
    }

    def __init__(self, bot):
        self.bot = bot
        # Per-game tracking
        self.enabled_guilds: Dict[str, Set[int]] = {game: set() for game in self.GAME_CONFIGS}
        self.used_words: Dict[str, Set[str]] = {game: set() for game in self.GAME_CONFIGS}
        self.game_active: Dict[str, bool] = {game: False for game in self.GAME_CONFIGS}

    def find_valid_word(self, letters: str, game_type: str) -> Optional[str]:
        """Find a valid word containing the letters in sequence"""
        letters = letters.lower()
        valid_words = []
        
        # Use shared words from bot manager
        for word in self.bot._manager.shared_words:
            if letters in word and word not in self.used_words[game_type]:
                valid_words.append(word)
                
        if not valid_words:
            return None
            
        # Apply strategy based on game type
        strategy = self.GAME_CONFIGS[game_type]['strategy']
        if strategy == 'longest':
            # For red tea, prefer longer words
            valid_words.sort(key=len, reverse=True)
            chosen_word = valid_words[0]
        elif strategy == 'shortest':
            min_length = min(len(word) for word in valid_words)
            shortest_words = [word for word in valid_words if len(word) == min_length]
            chosen_word = random.choice(shortest_words)  # Randomize among the shortest options
        else:
            # For other games, pick randomly
            chosen_word = random.choice(valid_words)
        
        self.used_words[game_type].add(chosen_word)
        return chosen_word

    @commands.command(aliases=['wg'])
    async def wordgames(self, ctx, game_type: str = None, *args):
        """Unified word games controller
        wordgames <game_type> [guild_id] [on|off]
        
        Game types: mudae_green, mudae_red, mudae_black, bleed
        Aliases: mg (mudae_green), mr (mudae_red), mb (mudae_black), bl (bleed)
        
        Examples:
        wordgames mudae_green on
        wordgames bleed 123456789 off
        wordgames mudae_red  (shows status)
        """
        try:
            await ctx.message.delete()
        except:
            pass

        # Handle aliases
        aliases = {
            'mg': 'mudae_green', 'gtea': 'mudae_green', 'greentea': 'mudae_green',
            'mr': 'mudae_red', 'rtea': 'mudae_red', 'redtea': 'mudae_red', 
            'mb': 'mudae_black', 'mudaebt': 'mudae_black', 'mudaeblacktea': 'mudae_black',
            'bl': 'bleed', 'bt': 'bleed', 'blacktea': 'bleed'
        }
        
        if game_type:
            game_type = aliases.get(game_type.lower(), game_type.lower())

        # Show help if no game type or invalid game type
        if not game_type or game_type not in self.GAME_CONFIGS:
            games_list = "\n".join([f"• **{game}** - {config['strategy']} strategy" 
                                   for game, config in self.GAME_CONFIGS.items()])
            await ctx.send(f"**Available Word Games:**\n{games_list}\n\n"
                          f"Usage: `wordgames <game_type> [guild_id] [on|off]`")
            return

        await self._handle_game_command(ctx, args, game_type)

    # Individual command aliases for backward compatibility
    @commands.command(aliases=['gtea'])
    async def greentea(self, ctx, *args):
        """Green Tea auto-player for Mudae (speed-based)"""
        await self._handle_game_command(ctx, args, 'mudae_green')

    @commands.command(aliases=['rtea'])
    async def redtea(self, ctx, *args):
        """Red Tea auto-player for Mudae (longest word)"""
        await self._handle_game_command(ctx, args, 'mudae_red')

    @commands.command(aliases=['mudaebt', 'mudaeblacktea'])
    async def mudaeblack(self, ctx, *args):
        """Mudae black tea auto-player"""
        await self._handle_game_command(ctx, args, 'mudae_black')

    @commands.command(aliases=['bt'])
    async def blacktea(self, ctx, *args):
        """BlackTea auto-player for Bleed bot"""
        await self._handle_game_command(ctx, args, 'bleed')

    async def _handle_game_command(self, ctx, args, game_type: str):
        """Handle game command for any game type"""
        try:
            await ctx.message.delete()
        except:
            pass

        # Parse arguments
        guild_id = None
        setting = None
        
        if not args:
            guild_id = ctx.guild.id
        elif len(args) == 1:
            if args[0].lower() in ['on', 'off', 'enable', 'disable']:
                setting = args[0]
                guild_id = ctx.guild.id
            else:
                try:
                    guild_id = int(args[0])
                except ValueError:
                    await ctx.send("Invalid guild ID format")
                    return
        elif len(args) == 2:
            try:
                guild_id = int(args[0])
                setting = args[1]
            except ValueError:
                await ctx.send("Invalid guild ID format")
                return
    
        if setting and setting.lower() in ['on', 'enable']:
            self.enabled_guilds[game_type].add(guild_id)
        elif setting and setting.lower() in ['off', 'disable']:
            self.enabled_guilds[game_type].discard(guild_id)
            self.used_words[game_type].clear()
        else:
            pass

    async def _handle_message(self, message):
        """Unified message handler for all word games"""
        if not message.guild:
            return

        # Check which games are enabled for this guild
        enabled_games = [game for game, guilds in self.enabled_guilds.items() 
                        if message.guild.id in guilds]
        
        if not enabled_games:
            return

        # Group games by bot ID for efficient processing
        bot_games = {}
        for game in enabled_games:
            bot_id = self.GAME_CONFIGS[game]['bot_id']
            if bot_id not in bot_games:
                bot_games[bot_id] = []
            bot_games[bot_id].append(game)

        # Only process if message is from a relevant bot
        if message.author.id not in bot_games:
            return

        relevant_games = bot_games[message.author.id]

        # Handle embed messages (game starts and bleed prompts)
        if message.embeds and len(message.embeds) > 0:
            await self._handle_embed_message(message, relevant_games)
        
        # Handle text messages (mudae prompts and game ends)
        elif message.content and not message.embeds:
            await self._handle_text_message(message, relevant_games)

    async def _handle_embed_message(self, message, relevant_games):
        """Handle embedded messages"""
        embed = message.embeds[0]
        if not embed.description:
            return

        # Check for game start embeds
        if "To participate, **react** on ✅." in embed.description:
            for game in relevant_games:
                config = self.GAME_CONFIGS[game]
                if config['start_pattern'] in (embed.title or ""):
                    await self._join_game(message, game)
                    break

        # Check for Bleed game start
        elif "Waiting for **players**" in embed.description and 'bleed' in relevant_games:
            await self._join_game(message, 'bleed')

        # Check for Bleed word prompt
        elif ("Type a **word** containing the letters:" in embed.description and 
              self.bot.user in message.mentions and 'bleed' in relevant_games):
            match = re.search(self.GAME_CONFIGS['bleed']['prompt_pattern'], embed.description)
            if match and self.game_active['bleed']:
                await self._respond_to_prompt(message, match.group(1), 'bleed')

        # Check for game end embed (Bleed)
        elif embed.description and "has won the game!" in embed.description and 'bleed' in relevant_games:
            await self._end_game('bleed', message.guild.name)

    async def _handle_text_message(self, message, relevant_games):
        """Handle text messages"""
        # Check for Mudae word prompts
        for game in relevant_games:
            if game.startswith('mudae_'):
                config = self.GAME_CONFIGS[game]
                
                # Check if mention is required and present (or not required)
                mention_check = (not config['requires_mention'] or 
                               self.bot.user in message.mentions)
                
                if mention_check and self.game_active[game]:
                    match = re.search(config['prompt_pattern'], message.content)
                    if match:
                        await self._respond_to_prompt(message, match.group(1), game)
                        break

        # Check for game end messages
        if "**won the game!**" in message.content and "🏆" in message.content:
            for game in relevant_games:
                if game.startswith('mudae_') and self.game_active[game]:
                    await self._end_game(game, message.guild.name)

    async def _join_game(self, message, game_type: str):
        """Join a game by reacting"""
        try:
            config = self.GAME_CONFIGS[game_type]
            delay_range = config['delays']['join']
            await asyncio.sleep(random.uniform(*delay_range))
            await message.add_reaction("✅")
            self.used_words[game_type].clear()
            self.game_active[game_type] = True
            logger.info(f"Joined {game_type} game in {message.guild.name}")
        except discord.HTTPException as e:
            logger.warning(f"Failed to join {game_type} game: {e}")

    async def _respond_to_prompt(self, message, letters: str, game_type: str):
        """Respond to a word prompt"""
        word = self.find_valid_word(letters, game_type)
        if not word:
            logger.warning(f"No valid word found for letters '{letters}' in {game_type}")
            return

        try:
            config = self.GAME_CONFIGS[game_type]
            delay_range = config['delays']['response']
            
            async with message.channel.typing():
                await asyncio.sleep(random.uniform(*delay_range))
                sent_message = await message.channel.send(word)
            
            # Check for success reaction after a delay
            await asyncio.sleep(3)
            sent_message = await message.channel.fetch_message(sent_message.id)
            success_emoji = config['success_emoji']
            
            if not any(reaction.emoji == success_emoji for reaction in sent_message.reactions):
                # Try another word if the first one wasn't accepted
                retry_word = self.find_valid_word(letters, game_type)
                if retry_word:
                    await message.channel.send(retry_word)
                    logger.info(f"Retried with word '{retry_word}' in {game_type}")
                        
        except discord.HTTPException as e:
            logger.warning(f"Failed to respond in {game_type} game: {e}")

    async def _end_game(self, game_type: str, guild_name: str):
        """End a game and reset state"""
        self.game_active[game_type] = False
        self.used_words[game_type].clear()
        logger.info(f"{game_type} game ended in {guild_name}")

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)

    async def cog_unload(self):
        """Cleanup and unregister event handlers"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)
            
        # Clear all game state
        for game_type in self.GAME_CONFIGS:
            self.enabled_guilds.setdefault(game_type, set()).clear()
            self.used_words.setdefault(game_type, set()).clear()
            self.game_active[game_type] = False

async def setup(bot):
    await bot.add_cog(WordGames(bot))
