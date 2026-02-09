import discord
from discord.ext import commands
from utils.server_utils import (
    delete_all_roles,
    delete_all_channels,
    copy_roles,
    copy_channels,
    reassign_roles
)
from utils.rate_limiter import rate_limiter

class ServerCopier(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(aliases=['cl'])
    @commands.has_permissions(administrator=True)
    @rate_limiter(command_only=True)
    async def clone(self, ctx, source_server_id: int, target_server_id: int, *options):
        """Clone a server
        
        clone <source_id> <target_id> [-r/-c] - Copy server structure
        """
        try:
            try:await ctx.message.delete()
            except:pass
            source_guild = self.bot.get_guild(source_server_id)
            target_guild = self.bot.get_guild(target_server_id)
            
            if not source_guild or not target_guild:
                await ctx.send("Invalid source or target server ID.")
                return

            # Parse options
            copy_roles_flag = True
            copy_channels_flag = True
            
            if "-r" in options and "-c" not in options:
                copy_roles_flag = True
                copy_channels_flag = False
                await ctx.send("Cloning roles only...", delete_after=5)
            elif "-c" in options and "-r" not in options:
                copy_roles_flag = False
                copy_channels_flag = True
                await ctx.send("Cloning channels only...", delete_after=5)
            else:
                await ctx.send("Cloning everything (roles and channels)...", delete_after=5)

            # Store member roles before deletion
            member_roles = {}
            if copy_roles_flag:
                member_roles = {
                    member.id: [role.id for role in member.roles if role != target_guild.default_role] 
                    for member in target_guild.members
                }

            # Delete existing content with error collection
            failed_role_deletions = []
            failed_channel_deletions = []
            role_map = {}
            failed_role_copies = []
            failed_channel_copies = []
            failed_role_assignments = []

            if copy_roles_flag:
                print("Deleting existing roles...")
                failed_role_deletions = await delete_all_roles(target_guild)
                
                # Copy roles
                print("Copying roles...")
                role_map, failed_role_copies = await copy_roles(source_guild, target_guild)

            if copy_channels_flag:
                print("Deleting existing channels...")
                failed_channel_deletions = await delete_all_channels(target_guild)
                
                # Copy channels
                print("Copying categories and channels...")
                failed_channel_copies = await copy_channels(source_guild, target_guild, role_map)

            # Reassign roles to members if roles were copied
            if copy_roles_flag:
                print("Reassigning roles to members...")
                failed_role_assignments = await reassign_roles(target_guild, member_roles, role_map)

            # Prepare error summary
            error_summary = []
            if failed_role_deletions:
                error_summary.append("Failed to delete roles: " + ", ".join(f"{name} ({error})" for name, error in failed_role_deletions))
            if failed_channel_deletions:
                error_summary.append("Failed to delete channels: " + ", ".join(f"{name} ({error})" for name, error in failed_channel_deletions))
            if failed_role_copies:
                error_summary.append("Failed to copy roles: " + ", ".join(f"{name} ({error})" for name, error in failed_role_copies))
            if failed_channel_copies:
                error_summary.append("Failed to copy channels: " + ", ".join(f"{name} ({error})" for name, error in failed_channel_copies))
            if failed_role_assignments:
                error_summary.append("Failed to reassign roles: " + ", ".join(f"{user} ({error})" for user, error in failed_role_assignments))

            # Send completion message with any errors
            if error_summary:
                await ctx.send(
                    "Server cloning completed with some errors:\n" + "\n".join(error_summary),
                    delete_after=30
                )
            else:
                clone_type = ""
                if copy_roles_flag and copy_channels_flag:
                    clone_type = "Server"
                elif copy_roles_flag:
                    clone_type = "Role"
                else:
                    clone_type = "Channel"
                    
                await ctx.send(
                    f"{clone_type} cloning completed successfully!",
                    delete_after=self.bot.config_manager.auto_delete.delay 
                    if self.bot.config_manager.auto_delete.enabled else None
                )

        except discord.HTTPException as e:
            await ctx.send(f"A critical error occurred: {e}", delete_after=10)
            print(f"Critical error during server cloning: {e}")

async def setup(bot):
    await bot.add_cog(ServerCopier(bot))