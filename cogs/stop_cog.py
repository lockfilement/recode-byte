from discord.ext import commands
import logging
from utils.general import format_message, quote_block

logger = logging.getLogger(__name__)

class Stop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(aliases=['stp'])
    async def stopall(self, ctx):
        """Stop all activities"""
        try:await ctx.message.delete()
        except:pass
        
        # Map of cogs and their state attributes/methods to reset
        cog_resets = {
            'Purge': {'attrs': {'is_purging': False}},
            'Spam': {'attrs': {'is_spamming': False}},
            'Pack': {
                'attrs': {'current_target': None},
                'tasks': ['pack_task'],
                'clear': ['sent_insults', 'insults']
            },
            'Rizz': {
                'attrs': {'current_target': None},
                'tasks': ['rizz_task'],
                'clear': ['sent_rizz_lines', 'rizz_lines']
            },
            'Mock': {
                'attrs': {
                    'is_mimicking': False,
                    'mimic_target': None,
                    'custom_response': None
                },
                'clear': ['sent_messages']
            },
            'AutoReact': {
                'nested_clear': {
                    'reaction_manager': ['self_reactions', 'user_reactions']
                }
            },
            'Hush': {
                'attrs': {'is_hushing': False},
                'clear': ['hushed_users', 'deleted_message_counts', 'channel_counts']
            },
            'PackMock': {
                'attrs': {
                    'target': None,
                    'use_hashtag': False,
                    'use_ladder': False,
                    'random_ladder': False
                },
                'clear': ['sent_messages', 'used_insults']
            },
            'RizzMock': {
                'attrs': {
                    'target': None,
                    'use_hashtag': False
                },
                'clear': ['sent_messages', 'used_rizz_lines']
            },
            'LeakCheck': {
                'attrs': {'stop_pagination': True}
            },
            'Nuke': {
                'attrs': {'is_nuking': False}
            },
            'MassDM': {
                'attrs': {'is_dming': False}
            },
            'ServerCopier': {
                'tasks': ['active_copy_task']
            },
            'TicTacToe': {
                'clear': ['board_cache'],
                'attrs': {'enabled_guilds': set()}
            },
            'WordGames': {
                'attrs': {
                    'enabled_guilds': {game: set() for game in ['mudae_green', 'mudae_red', 'mudae_black', 'bleed']},
                    'game_active': {game: False for game in ['mudae_green', 'mudae_red', 'mudae_black', 'bleed']}
                },
                'clear': ['used_words']
            },
            'Skibidi': {
                'attrs': {
                    'current_target': None,
                    'use_hashtag': False,
                    'random_hashtag': False
                },
                'tasks': ['skibidi_task'],
                'clear': ['sent_skibidi_lines']
            },
            'SkibidiMock': {
                'attrs': {
                    'target': None,
                    'use_hashtag': False,
                    'random_hashtag': False
                },
                'clear': ['sent_messages', 'used_skibidi_lines']
            }
        }

        # Process each cog
        for cog_name, reset_info in cog_resets.items():
            cog = self.bot.get_cog(cog_name)
            if not cog:
                continue

            # Reset attributes
            if 'attrs' in reset_info:
                for attr, value in reset_info['attrs'].items():
                    if hasattr(cog, attr):
                        setattr(cog, attr, value)

            # Cancel tasks
            if 'tasks' in reset_info:
                for task_name in reset_info['tasks']:
                    task = getattr(cog, task_name, None)
                    if task and not task.done():
                        task.cancel()

            # Clear collections
            if 'clear' in reset_info:
                for collection_name in reset_info['clear']:
                    collection = getattr(cog, collection_name, None)
                    if collection is not None:
                        if isinstance(collection, (set, list, dict)):
                            collection.clear()

            # Clear nested collections
            if 'nested_clear' in reset_info:
                for obj_name, collections in reset_info['nested_clear'].items():
                    obj = getattr(cog, obj_name, None)
                    if obj:
                        for collection_name in collections:
                            collection = getattr(obj, collection_name, None)
                            if collection is not None:
                                collection.clear()

        await ctx.send(
            format_message("Stopped all running commands and tasks"),
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )

async def setup(bot):
    await bot.add_cog(Stop(bot))