from discord.ext import commands
import logging
from utils.general import format_message, quote_block

logger = logging.getLogger(__name__)

class Config(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(aliases=['ad'])
    async def autodelete(self, ctx, setting: str = None, delay: int = None):
        """Auto-delete settings
        autodelete on/off - Toggle auto-delete
        autodelete delay 10 - Set delay"""
        
        try:await ctx.message.delete()
        except:pass
        
        config_manager = self.bot.config_manager

        if not setting:
            await ctx.send(
                format_message(f"Auto-delete is currently {'enabled' if config_manager.auto_delete.enabled else 'disabled'} with a delay of {config_manager.auto_delete.delay} seconds"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return
        
        # Update config specific to this bot instance
        if setting.lower() in ['on', 'off']:
            config_manager.auto_delete.enabled = setting.lower() == 'on'
            await config_manager.save_config_async()
            await ctx.send(
                format_message(f"Auto-delete has been {'enabled' if setting.lower() == 'on' else 'disabled'}"), 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

        elif setting.lower() == 'delay' and delay is not None:
            if delay < 1:
                await ctx.send(format_message("Delay must be at least 1 second"), delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
            config_manager.auto_delete.delay = delay
            await config_manager.save_config_async()
            await ctx.send(format_message(f"Auto-delete delay set to {delay} seconds"), delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    @commands.command(aliases=['pr'])
    async def prefix(self, ctx, new_prefix: str = None):
        """Change bot's prefix
        .prefix <new_prefix>"""
        try:await ctx.message.delete()
        except:pass
        
        if not new_prefix:
            await ctx.send(
                format_message(f"Current prefix: {self.bot.command_prefix}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return

        # Update prefix in bot instance and config manager
        self.bot.command_prefix = new_prefix
        self.bot.config_manager.command_prefix = new_prefix
        
        # Save config using config manager which handles per-user settings 
        await self.bot.config_manager.save_config_async()
                
        await ctx.send(
            format_message(f"Command prefix changed to: {new_prefix}"),
            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        )

    @commands.command(aliases=['cf'])
    async def config(self, ctx, page: int = 1):
        """Display configuration
        .config [page]
        Pages:
        1 - General Settings
        2 - Presence Settings 
        3 - Status & Activity
        4 - Rich Presence
        5 - Auto Settings"""
    
        try:await ctx.message.delete()
        except:pass
        config_manager = self.bot.config_manager
        uid = config_manager.uid if config_manager.uid is not None else 'Not set'
    
        def escape_backticks(text):
            return text.replace('`', '\\`') if isinstance(text, str) else str(text)
        
        sections = {
            1: ("General Settings", [
                f"\u001b[0;36mUsername: \u001b[0;37m{ctx.author.name}",
                f"\u001b[0;36mUser ID: \u001b[0;37m{ctx.author.id}",
                f"\u001b[0;36mUID: \u001b[0;37m{uid}",
                f"\u001b[0;36mPrefix: \u001b[0;37m{escape_backticks(config_manager.command_prefix)}"
            ]),
            
            2: ("Presence Settings", [
                f"\u001b[0;36mEnabled: \u001b[0;37m{config_manager.presence.get('enabled', False)}",
                f"\u001b[0;36mRotation: \u001b[0;37m{config_manager.presence.get('rotation', {}).get('enabled', False)}",
                f"\u001b[0;36mDelay: \u001b[0;37m{config_manager.presence.get('rotation', {}).get('delay', 60)}s",
                f"\u001b[0;36mStatus: \u001b[0;37m{config_manager.presence.get('status', 'online')}",
                f"\u001b[0;36mActivity: \u001b[0;37m{config_manager.presence.get('type', 'playing')}",
                f"\u001b[0;36mCustom Status: \u001b[0;37m{config_manager.presence.get('custom_status', False)}"
            ]),
    
            3: ("Status & Activity", [
                f"\u001b[0;36mActivity Name: \u001b[0;37m{escape_backticks(config_manager.presence.get('name', 'Not set'))}",
                f"\u001b[0;36mDetails: \u001b[0;37m{escape_backticks(config_manager.presence.get('details', 'Not set'))}",
                f"\u001b[0;36mState: \u001b[0;37m{escape_backticks(config_manager.presence.get('state', 'Not set'))}"
            ]),
    
            4: ("Rich Presence", [
                f"\u001b[0;36mApplication ID: \u001b[0;37m{escape_backticks(config_manager.presence.get('application_id', 'Not set'))}",
                f"\u001b[0;36mLarge Image: \u001b[0;37m{escape_backticks(config_manager.presence.get('large_image', 'Not set'))}",
                f"\u001b[0;36mLarge Text: \u001b[0;37m{escape_backticks(config_manager.presence.get('large_text', 'Not set'))}",
                f"\u001b[0;36mSmall Image: \u001b[0;37m{escape_backticks(config_manager.presence.get('small_image', 'Not set'))}",
                f"\u001b[0;36mSmall Text: \u001b[0;37m{escape_backticks(config_manager.presence.get('small_text', 'Not set'))}",
                f"\u001b[0;36mParty Size: \u001b[0;37m{config_manager.presence.get('party_size', '0')}/{config_manager.presence.get('party_max', '0')}"
            ]),
    
            5: ("Auto Settings", [
                f"\u001b[0;36mAuto-Delete: \u001b[0;37m{config_manager.auto_delete.enabled}",
                f"\u001b[0;36mDelete Delay: \u001b[0;37m{config_manager.auto_delete.delay}s", 
                f"\u001b[0;36mNitro Sniper: \u001b[0;37m{config_manager.nitro_sniper.get('enabled', False)}"
            ])
        }
    
        page = max(1, min(page, len(sections)))
        title, content = sections[page]
    
        message_parts = [
            # Main content block 
            "```ansi\n" + \
            f"\u001b[30m\u001b[1m\u001b[4m{title}\u001b[0m\n" + \
            "\n".join(content) + "```",

            # Footer with page number
            "```ansi\n" + \
            f"Page \u001b[1m\u001b[37m{page}/5\u001b[0m```"
        ]
    
        await ctx.send(quote_block(''.join(message_parts)),
            delete_after=config_manager.auto_delete.delay if config_manager.auto_delete.enabled else None)

async def setup(bot):
    await bot.add_cog(Config(bot))