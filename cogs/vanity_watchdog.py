# cogs/vanity_watchdog.py
import discord
from discord.ext import commands
import asyncio
from utils.rate_limiter import rate_limiter

class VanityWatchdog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.watched_guilds = set()  # Set of guild IDs to watch

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def vanantinuke(self, ctx):
        """Vanity URL watchdog"""
        try:await ctx.message.delete()
        except:pass
        
        guild_id = ctx.guild.id
        if guild_id in self.watched_guilds:
            self.watched_guilds.remove(guild_id)
            await ctx.send("Vanity URL watch disabled for this guild.", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        else:
            self.watched_guilds.add(guild_id)
            await ctx.send("Vanity URL watch enabled for this guild.", 
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    @commands.Cog.listener()
    @rate_limiter(command_only=True)
    async def on_guild_update(self, before, after):
        if before.id not in self.watched_guilds:
            return

        if before.vanity_url_code != after.vanity_url_code:
            # Revert the vanity URL change
            try:
                await after.edit(vanity_code=before.vanity_url_code)
            except discord.HTTPException as e:
                if e.status == 429:
                    print(f"Rate limited when reverting vanity URL change in guild {after.name}, retrying...")
                    await asyncio.sleep(5)
                    try:
                        await after.edit(vanity_code=before.vanity_url_code)
                    except Exception as e:
                        print(f"Error reverting vanity URL change: {e}")
                        return
                print(f"Reverted vanity URL change in guild {after.name}")
            except discord.Forbidden:
                print(f"Failed to revert vanity URL change in guild {after.name} due to insufficient permissions")
                return
            except Exception as e:
                print(f"Error reverting vanity URL change: {e}")
                return

            # Check the audit log for the user who changed the vanity URL
            async for entry in after.audit_logs(limit=1, action=discord.AuditLogAction.guild_update):
                if entry.changes.before.get('vanity_url_code') == before.vanity_url_code:
                    user = entry.user
                    break
            else:
                print("No audit log entry found for vanity URL change")
                return

            # Ensure we do not attempt to kick ourselves
            if user.id == self.bot.user.id:
                print("Attempted to kick self, skipping.")
                return

            # Ensure we do not attempt to kick users with higher roles
            bot_member = after.get_member(self.bot.user.id)
            if not bot_member:
                print("Bot member not found in guild.")
                return

            bot_top_role = bot_member.top_role
            user_top_role = user.top_role

            if user_top_role >= bot_top_role:
                print(f"Cannot kick user {user.name} with higher or equal role.")
                return

            # Kick the user
            try:
                await user.kick(reason="Changed vanity URL")
                print(f"Kicked user {user.name} for changing vanity URL")
            except discord.Forbidden:
                print(f"Failed to kick user {user.name} due to insufficient permissions")
            except Exception as e:
                print(f"Error kicking user {user.name}: {e}")

async def setup(bot):
    await bot.add_cog(VanityWatchdog(bot))