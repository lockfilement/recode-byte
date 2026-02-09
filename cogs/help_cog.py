from discord.ext import commands

class Category:
    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.cogs = []

class Help(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.remove_command('help')
        
        self.categories = {
            "General": Category("General", "Config commands"),
            "Tools": Category("Tools", "Tool commands"),
            "Utility": Category("Utility", "Misc commands"),
            "Tracking": Category("Tracking", "Tracking commands"),
            "Server": Category("Server", "Server commands"),
            "Fun": Category("Fun", "Fun commands"),
            "Developer": Category("Developer", "Dev commands")
        }        # Map cogs to categories
        self.category_mapping = {
            # General
            "Config": "General",
            "Help": "General",            
            "Info": "General",
            "Stop": "General",
            "Whitelist": "General",
            "AutoResponder": "General",
            # Tools
            "LeakCheck": "Tools",
            "NitroSniper": "Tools",
            "AutoReact": "Tools",
            "VoiceSitter": "Tools",
            "MassDM": "Tools",
            "Translate": "Tools",
            "Poll": "Tools",
            "Host": "Tools",            # Utility
            "Presence": "Utility",
            "Purge": "Utility",
            "Profile": "Utility",
            "Spam": "Utility",
            "WordGames": "Utility",
            "TicTacToe": "Utility",
            "Hush": "Utility",
            "QuestManager": "Utility",
            "AutoChat": "Utility",
            # Tracking
            "Snipe": "Tracking",
            # Server
            "ServerCopier": "Server",
            "UnbanAll": "Server",
            "VanityWatchdog": "Server",
            "Nuke": "Server",
            # Fun
            "Pack": "Fun",
            "Rizz": "Fun",
            "Ladder": "Fun",
            "PackMock": "Fun",
            "RizzMock": "Fun",
            "Mock": "Fun",
            "Edate": "Fun",
            "Skibidi": "Fun",
            "SkibidiMock": "Fun",            
            "FakePings": "Fun",
            # Developer
            "Developer": "Developer"
        }
        
    def get_category_cogs(self, category_name):
        """Dynamically get cogs for a category based on currently loaded cogs"""
        cogs = []
        for cog_name, cog in self.bot.cogs.items():
            mapped_category = self.category_mapping.get(cog_name, "General")
            if mapped_category == category_name:
                cogs.append(cog_name)
        return cogs


    def get_padding(self, names, min_spacing=2):
        """Calculate padding based on the longest name in the list"""
        if not names:
            return min_spacing
        max_length = max(len(name) - 1 for name in names)
        return max_length + min_spacing

    async def can_run_command(self, ctx, command):
        """Helper method to check if a user can run a specific command based on permissions"""
        try:
            return await command.can_run(ctx)
        except commands.CommandError:
            return False


    @commands.command(aliases=['h'])
    async def help(self, ctx, *args):
        """Display help menu or command information"""
        try:
            try:await ctx.message.delete()
            except:pass
            prefix = self.bot.command_prefix
            delete_after = self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            is_developer = self.bot.config_manager.is_developer(ctx.author.id)
            commands_per_page = 5
    
            def quote_block(text):
                return '\n'.join(f'> {line}' for line in text.split('\n'))
                
            # Main help menu with sleek design
            if not args:
                message_parts = [
                    "```ansi\n" + \
                    f"Category\u001b[30m: \u001b[34m{prefix}help <category> [page]\u001b[0m\n" + \
                    f"Commands\u001b[30m: \u001b[34m{prefix}help <command>\u001b[0m\n```",
    
                    "```ansi\n" + \
                    "\u001b[30m\u001b[1m\u001b[4mCategories\u001b[0m\n"
                ]
                
                # Add categories with clean, modern design
                categories = [(name, cat) for name, cat in self.categories.items() 
                            if name != "Developer" or is_developer]
                
                # Calculate padding based on longest category name
                padding_length = self.get_padding([name for name, _ in categories])
                
                for name, category in categories:
                    name_padding = " " * (padding_length - len(name))
                    message_parts[-1] += f"\u001b[0;37m{name}{name_padding}\u001b[30m| \u001b[0;34m{category.description}\u001b[0m\n"
                
                message_parts[-1] += "```"
                
                # Footer with version only
                message_parts.append(
                    "```ansi\n" + \
                    f"Ver\u001b[30m: \u001b[34m{self.bot.config_manager.version}\u001b[0m```"
                )
                await ctx.send(quote_block(''.join(message_parts)), delete_after=delete_after)
                return
            
            # Command help with sleek design
            if command := self.bot.get_command(args[0].lower()):
                # Only show developer commands to developer
                can_access = await self.can_run_command(ctx, command)
                
                # If command is hidden and user doesn't have permission to access it, pretend it doesn't exist
                if command.hidden and not can_access:
                    await ctx.send("Command not found.", delete_after=delete_after)
                    return
                    
                # Split into 3 code blocks like main menu
                message_parts = [                    # Header
                    "```ansi\n" + \
                    f"\u001b[33mCommand \u001b[30m| \u001b[33m{prefix}{command.name}\u001b[0m\n```",
                    
                    # Main content
                    "```ansi\n" + \
                    "\u001b[30m\u001b[1m\u001b[4mDetails\u001b[0m\n"
                ]
                
                # Calculate padding for labels
                labels = ["Info", "Usage", "Aliases"]
                padding_length = self.get_padding(labels)
                
                # Description first
                description = command.help.split('\n')[0] if command.help else 'No description available'
                message_parts[-1] += f"\u001b[0;37mInfo{' ' * (padding_length - len('Info'))}\u001b[30m| \u001b[0;34m{description}\u001b[0m\n"
                
                # Full help docstring with usage examples
                if command.help and '\n' in command.help:
                    usage_content = '\n'.join(command.help.split('\n')[1:]).strip()
                    if usage_content:
                        message_parts[-1] += f"\u001b[0;37mUsage{' ' * (padding_length - len('Usage'))}\u001b[30m| \u001b[0;34m{prefix}{command.name} {command.signature}\u001b[0m\n"
                        lines = [line for line in usage_content.split('\n') if line.strip()]
                        indent_padding = " " * padding_length
                        for i, line in enumerate(lines):
                            formatted_line = line.replace(f".{command.name}", f"{prefix}{command.name}")
                            message_parts[-1] += f"{indent_padding}\u001b[30m| \u001b[0;34m{formatted_line}\u001b[0m\n"
                else:
                    message_parts[-1] += f"\u001b[0;37mUsage{' ' * (padding_length - len('Usage'))}\u001b[30m| \u001b[0;34m{prefix}{command.name} {command.signature}\u001b[0m\n"
                
                # Aliases
                if command.aliases:
                    message_parts[-1] += f"\u001b[0;37mAliases{' ' * (padding_length - len('Aliases'))}\u001b[30m| \u001b[0;34m{', '.join(command.aliases)}\u001b[0m\n"
                
                message_parts[-1] += "```"
                
                # Footer with version only
                message_parts.append(
                    "```ansi\n" + \
                    f"Ver\u001b[30m: \u001b[34m{self.bot.config_manager.version}\u001b[0m```"
                )
                
                await ctx.send(quote_block(''.join(message_parts)), delete_after=delete_after)
                return
            
            # Category help with sleek design
            if category := next((cat for name, cat in self.categories.items() 
                                if name.lower() == args[0].lower()), None):
                # Hide developer category from non-developers
                if category.name == "Developer" and not is_developer:
                    await ctx.send("Command not found.", delete_after=delete_after)
                    return
                page = int(args[1]) if len(args) > 1 and args[1].isdigit() else 1
                commands_list = []
    
                # Get cogs for this category dynamically
                category_cogs = self.get_category_cogs(category.name)
                for cog_name in category_cogs:
                    if cog := self.bot.get_cog(cog_name):
                        for cmd in cog.get_commands():
                            cmd_accessible = not cmd.hidden or await self.can_run_command(ctx, cmd)
                            if cmd_accessible:
                                commands_list.append((cmd.name, cmd.help.split('\n')[0] if cmd.help else 'No description'))
    
                total_pages = max(1, (len(commands_list) + commands_per_page - 1) // commands_per_page)
                page = max(1, min(page, total_pages))
                start_idx = (page - 1) * commands_per_page
                current_page_commands = commands_list[start_idx:start_idx + commands_per_page]
                
                # Split into 3 code blocks like main menu
                message_parts = [
                    # Header
                    "```ansi\n" + \
                    f"\u001b[33m{category.name} \u001b[30m| \u001b[33mPage {page}/{total_pages}\u001b[0m\n```",
                    
                    # Main content
                    "```ansi\n" + \
                    "\u001b[30m\u001b[1m\u001b[4mCommands\u001b[0m\n"
                ]
                
                # Calculate padding based on longest command name
                if current_page_commands:
                    padding_length = self.get_padding([cmd_name for cmd_name, _ in current_page_commands])
                    
                    for cmd_name, cmd_desc in current_page_commands:
                        name_padding = " " * (padding_length - len(cmd_name))
                        message_parts[-1] += f"\u001b[0;37m{cmd_name}{name_padding}\u001b[30m| \u001b[0;34m{cmd_desc}\u001b[0m\n"
                else:
                    message_parts[-1] += "\u001b[30m\u001b[0;37mNo commands found in this category\u001b[0m\n"
                
                message_parts[-1] += "```"
                
                # Footer with page navigation help
                message_parts.append(
                    "```ansi\n" + \
                    f"\u001b[30m\u001b[0;37mNavigation \u001b[30m| \u001b[0;34m{prefix}help {category.name.lower()} [1-{total_pages}]\u001b[0m```"
                )
                
                await ctx.send(quote_block(''.join(message_parts)), delete_after=delete_after)
                return
    
            await ctx.send(
                "```ansi\n" + \
                f"\u001b[1;31mError \u001b[30m| \u001b[1;31mNo command or category found: '{args[0]}'```",
                delete_after=delete_after
            )
    
        except Exception as e:
            await ctx.send(
                "```ansi\n" + \
                f"\u001b[1;31mError \u001b[30m| \u001b[1;31m{str(e)}```",
                delete_after=delete_after
            )

    @commands.command(hidden=True)
    async def helpdebug(self, ctx):
        """Debug command to show cog categorization (developer only)"""
        if not self.bot.config_manager.is_developer(ctx.author.id):
            return
            
        try: await ctx.message.delete()
        except: pass
        
        delete_after = self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
        
        message_parts = ["```ansi\n\u001b[1;33mHelp System Debug\u001b[0m\n\n"]
        
        # Show all loaded cogs and their categories
        message_parts.append("\u001b[1;36mLoaded Cogs:\u001b[0m\n")
        for cog_name in sorted(self.bot.cogs.keys()):
            category = self.category_mapping.get(cog_name, "General")
            message_parts.append(f"\u001b[0;37m{cog_name}\u001b[0m -> \u001b[0;34m{category}\u001b[0m\n")
        
        # Show category counts
        message_parts.append("\n\u001b[1;36mCategory Counts:\u001b[0m\n")
        for category_name in self.categories.keys():
            cog_count = len(self.get_category_cogs(category_name))
            message_parts.append(f"\u001b[0;37m{category_name}\u001b[0m: \u001b[0;34m{cog_count} cogs\u001b[0m\n")
        
        # Show unmapped cogs
        unmapped = [cog for cog in self.bot.cogs.keys() if cog not in self.category_mapping]
        if unmapped:
            message_parts.append(f"\n\u001b[1;31mUnmapped Cogs:\u001b[0m\n")
            for cog in unmapped:
                message_parts.append(f"\u001b[0;31m{cog}\u001b[0m\n")
        
        message_parts.append("```")
        
        await ctx.send(''.join(message_parts), delete_after=delete_after)

async def setup(bot):
    await bot.add_cog(Help(bot))
