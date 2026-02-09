import discord
from discord.ext import commands
import asyncio
from utils.rate_limiter import rate_limiter
import logging
from utils.services.reaction_manager import ReactionManager
from utils.general import is_valid_emoji, format_message, quote_block  # Added imports for formatting

logger = logging.getLogger(__name__)

class AutoReact(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Pass the bot instance to ReactionManager
        self.reaction_manager = ReactionManager(bot)
        # Store indices for rotating reactions
        self.user_indices = {}

    @commands.command(aliases=['ar'])
    async def react(self, ctx: commands.Context, *args):
        """Configure auto-reactions
        .react [user] [emojis] - For regular reactions
        .react [user] emoji1.emoji2.emoji3 - For rotating reactions (individual)
        .react [user] emoji1emoji2 . emoji3emoji4 - For rotating reactions (groups)
        .react [user] -boost [emojis] - For super reactions (-boost can be placed anywhere)
        .react clear - To clear reactions"""
        try:
            try:await ctx.message.delete()
            except:pass
            
            if not args:
                await ctx.send(
                    format_message("Please provide emojis or 'clear'"), 
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return

            # Handle clear command first if it's the only argument
            if len(args) == 1 and args[0].lower() == 'clear':
                self.reaction_manager.clear_self_reactions()
                self.reaction_manager.clear_rotating_reactions(ctx.author.id)
                self.reaction_manager.clear_super_reactions(ctx.author.id)
                await ctx.send(
                    format_message(f"Cleared auto-reactions for {ctx.author.name}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return

            # Setup target user and starting index
            target_user = ctx.author
            start_idx = 0
            
            # Check if first argument is a user mention or ID
            if args[0].startswith('<@') and args[0].endswith('>') or args[0].isdigit():
                try:
                    target_user = await commands.UserConverter().convert(ctx, args[0])
                    start_idx = 1
                except commands.UserNotFound:
                    await ctx.send(
                        format_message(f"Could not find user: {args[0]}"), 
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return

            # Check if the argument looks like a word (likely a username)
            # Words are primarily alphanumeric with maybe some underscores/dots 
            def looks_like_word(text):
                # Count alphanumeric characters
                alphanum_count = sum(c.isalnum() or c in '_.' for c in text)
                # If most of the string is alphanumeric, it's likely a word/username
                return alphanum_count > len(text) * 0.7 and not text.lower() == 'clear'
             
            # If it looks like a username, try to convert it
            if looks_like_word(args[0]) and not args[0].startswith('<'):
                try:
                    target_user = await commands.UserConverter().convert(ctx, args[0])
                    start_idx = 1
                except commands.UserNotFound:
                    await ctx.send(
                        format_message(f"Could not find user: {args[0]}"), 
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return

            # Handle clear command for specific user
            if args[start_idx:] and args[start_idx].lower() == 'clear':
                if target_user.id == ctx.author.id:
                    self.reaction_manager.clear_self_reactions()
                else:
                    self.reaction_manager.clear_user_reactions(target_user.id)
                self.reaction_manager.clear_rotating_reactions(target_user.id)
                self.reaction_manager.clear_super_reactions(target_user.id)  # Also clear super reactions
                await ctx.send(
                    format_message(f"Cleared auto-reactions for {target_user.name}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return

            # Check for rotating emojis (contains '.')
            remaining = ' '.join(args[start_idx:])
            separator = '.'
            
            # Check for super reactions flag before processing rotating reactions
            use_super_reactions = False
            raw_args = remaining.split()
            if "-boost" in raw_args or "-super" in raw_args:
                # Filter out the flags from remaining
                filtered_parts = []
                for part in raw_args:
                    if part != "-boost" and part != "-super":
                        filtered_parts.append(part)
                remaining = ' '.join(filtered_parts)
                use_super_reactions = True
            
            if separator in remaining:
                # Split by '.' and process each group as potentially multiple emojis
                emoji_groups = []
                raw_groups = [group.strip() for group in remaining.split(separator)]
                
                for group in raw_groups:
                    # Use a better approach to extract emojis from the group
                    group_emojis = await self.extract_emojis_from_text(ctx, group)
                    
                    # If extract_emojis_from_text returned None, it means an error was already shown
                    if group_emojis is None:
                        continue
                    
                    # Only add non-empty groups
                    if group_emojis:
                        emoji_groups.append(group_emojis)
                
                # Validate that we have at least one group with valid emojis
                if not emoji_groups:
                    await ctx.send(
                        format_message("No valid emoji groups found. Make sure your emojis are valid."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
                
                # Set the rotating reactions with super mode if requested
                self.reaction_manager.set_rotating_reactions(target_user.id, emoji_groups, super_mode=use_super_reactions)
                
                # Format the message to show emoji groups
                formatted_groups = []
                for group in emoji_groups:
                    formatted_groups.append(''.join(group))
                
                reaction_type = "super rotating reactions" if use_super_reactions else "rotating reactions"
                await ctx.send(
                    format_message(f"Set {reaction_type} for {target_user.name} to: {' . '.join(formatted_groups)}"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return

            # Check for super reactions (using -boost parameter)
            use_super_reactions = False
            
            # Create a list from args for easier manipulation
            args_list = list(args[start_idx:])
            if "-boost" in args_list or "-super" in args_list:  # Accept both -boost and -super for backward compatibility
                # Find and remove all instances of -boost or -super
                args_list = [arg for arg in args_list if arg != "-boost" and arg != "-super"]
                use_super_reactions = True
            
            # Handle regular reactions
            valid_emojis = await self.validate_emojis(ctx, args_list)
            if valid_emojis:
                if target_user.id == self.bot.user.id:
                    self.reaction_manager.set_self_reactions(valid_emojis)
                else:
                    self.reaction_manager.set_user_reactions(target_user.id, valid_emojis)
                
                # Mark reactions as super reactions if the flag was set
                if use_super_reactions:
                    for emoji in valid_emojis:
                        self.reaction_manager.set_super_reactions(target_user.id, emoji, True)
                    await ctx.send(
                        format_message(f"Set super auto-reactions for {target_user.name} to: {' '.join(valid_emojis)}"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                else:
                    await ctx.send(
                        format_message(f"Set auto-reactions for {target_user.name} to: {' '.join(valid_emojis)}"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
        except Exception as e:
            logger.error(f"Error in react command: {e}", exc_info=True)
            await ctx.send(
                format_message("âŒ An error occurred"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

    @commands.command(aliases=['mr'])
    async def manualreact(self, ctx: commands.Context, amount: str, *args):
        """Manually react to messages
        .manualreact <amount> <emoji> [emoji2] [emoji3] ... - React to last <amount> messages with emoji(s)
        .manualreact <message_id> <emoji> [emoji2] [emoji3] ... - React to specific message with emoji(s)
        .manualreact <amount> -boost <emoji> [emoji2] ... - React with super reactions
        .manualreact <message_id> -boost <emoji> [emoji2] ... - React to specific message with super reactions
        .manualreact <amount> -remove <emoji> [emoji2] ... - Remove reactions from last <amount> messages
        .manualreact <message_id> -remove <emoji> [emoji2] ... - Remove reactions from specific message
        .manualreact <message_id> -num <number> [number2] ... - Use numbers to select existing reactions on the message
        .manualreact <message_id> -remove -num <number> [number2] ... - Remove selected reactions by number from the message
        .manualreact <amount> -num <number> [number2] ... - Use numbers to select existing reactions from reference message
        .manualreact <amount> -remove -num <number> [number2] ... - Remove selected reactions by number from multiple messages"""
        try:
            try:await ctx.message.delete()
            except:pass
            
            # Check for flags
            use_super_reactions = False
            remove_reactions = False
            select_by_number = False
            args_list = list(args)
            
            # Check for flags
            if "-boost" in args_list or "-super" in args_list:
                args_list = [arg for arg in args_list if arg != "-boost" and arg != "-super"]
                use_super_reactions = True
            
            if "-remove" in args_list:
                args_list = [arg for arg in args_list if arg != "-remove"]
                remove_reactions = True
                use_super_reactions = False
            
            # New flag for selecting reactions by number
            if "-num" in args_list:
                args_list = [arg for arg in args_list if arg != "-num"]
                select_by_number = True
                use_super_reactions = False  # Can't reliably combine with -boost
            
            # Determine if we're reacting to a specific message ID or multiple messages
            message_id = None
            target_amount = 0
            
            try:
                # Convert to integer first
                num_value = int(amount)
                
                # Check if this is likely a message ID (Discord message IDs are typically 17+ digits)
                if len(amount) >= 17:
                    message_id = num_value
                    mode = "single"
                else:
                    # Otherwise it's probably a count of messages to react to
                    target_amount = num_value
                    if target_amount <= 0:
                        await ctx.send(
                            format_message("Amount must be a positive number"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                        return
                    mode = "multiple"
            except ValueError:
                # Not a valid number at all
                await ctx.send(
                    format_message("Invalid amount or message ID"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                return
            
            # Handle the reaction number selection logic
            if select_by_number:
                if not args_list:
                    await ctx.send(
                        format_message("Please provide at least one reaction number to use"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
                
                # Validate that all arguments are valid numbers
                try:
                    reaction_indices = [int(arg) - 1 for arg in args_list]  # Convert to 0-indexed
                    for idx in reaction_indices:
                        if idx < 0:
                            await ctx.send(
                                format_message("Reaction numbers must be positive"),
                                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                            )
                            return
                except ValueError:
                    await ctx.send(
                        format_message("Invalid reaction number. Please use only integers."),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    return
                
                # Handle by mode
                if mode == "single":
                    # Get the target message to read reactions from
                    try:
                        message = await ctx.channel.fetch_message(message_id)
                        
                        # Get all existing reactions on the message
                        reaction_list = message.reactions
                        if not reaction_list:
                            await ctx.send(
                                format_message("The specified message has no reactions to select from"),
                                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                            )
                            return
                        
                        # Select reactions by index
                        selected_reactions = []
                        for idx in reaction_indices:
                            if idx < len(reaction_list):
                                selected_reactions.append(reaction_list[idx].emoji)
                            else:
                                await ctx.send(
                                    format_message(f"Reaction number {idx + 1} is out of range. Message has {len(reaction_list)} reactions."),
                                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                                )
                                # Continue with valid indices
                        
                        if not selected_reactions:
                            return  # No valid reactions selected
                        
                        # Add or remove the selected reactions based on the remove_reactions flag
                        reactions_processed = 0
                        for emoji in selected_reactions:
                            try:
                                if remove_reactions:
                                    # Remove the reaction (only works for the bot's own reactions)
                                    await message.remove_reaction(emoji, self.bot.user)
                                    reactions_processed += 1
                                else:
                                    await message.add_reaction(emoji)
                                    reactions_processed += 1
                                await asyncio.sleep(0.5)  # Rate limit between reactions
                            except discord.HTTPException as e:
                                action = "remove" if remove_reactions else "add"
                                logger.error(f"Failed to {action} reaction {emoji}: {e}")
                                continue
                    except discord.NotFound:
                        await ctx.send(
                            format_message(f"Message with ID {message_id} not found"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                        return
                else:  # "multiple" mode
                    # First, we need a reference message to get reactions from
                    try:
                        # Use the most recent message as reference for reactions
                        ref_messages = []
                        async for msg in ctx.channel.history(limit=5):  # Get a few recent messages to find one with reactions
                            if msg.reactions and msg.id != ctx.message.id:
                                ref_messages.append(msg)
                                break
                        
                        if not ref_messages:
                            await ctx.send(
                                format_message("Could not find a recent message with reactions to use as reference"),
                                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                            )
                            return
                            
                        ref_message = ref_messages[0]
                        reaction_list = ref_message.reactions
                        
                        # Select reactions by index
                        selected_reactions = []
                        for idx in reaction_indices:
                            if idx < len(reaction_list):
                                selected_reactions.append(reaction_list[idx].emoji)
                            else:
                                await ctx.send(
                                    format_message(f"Reaction number {idx + 1} is out of range. Reference message has {len(reaction_list)} reactions."),
                                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                                )
                                # Continue with valid indices
                        
                        if not selected_reactions:
                            return  # No valid reactions selected
                        
                        # Get target messages
                        messages = []
                        async for message in ctx.channel.history(limit=target_amount + 1):  # +1 to account for the command message
                            # Skip the command message itself
                            if message.id != ctx.message.id:
                                messages.append(message)
                        
                        # We only need the most recent target_amount messages
                        messages = messages[:target_amount]
                        
                        if not messages:
                            await ctx.send(
                                format_message("No messages found to react to"),
                                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                            )
                            return
                        
                        # Add or remove reactions to messages based on the remove_reactions flag
                        total_reactions = 0
                        for message in messages:
                            for emoji in selected_reactions:
                                try:
                                    if remove_reactions:
                                        # Remove the reaction (only works for the bot's own reactions)
                                        await message.remove_reaction(emoji, self.bot.user)
                                        total_reactions += 1
                                    else:
                                        await message.add_reaction(emoji)
                                        total_reactions += 1
                                    await asyncio.sleep(0.5)  # Rate limit between reactions
                                except discord.HTTPException as e:
                                    action = "remove" if remove_reactions else "add"
                                    logger.error(f"Failed to {action} reaction {emoji} to message {message.id}: {e}")
                                    continue
                    except Exception as e:
                        logger.error(f"Error in manualreact -num command: {e}", exc_info=True)
                        await ctx.send(
                            format_message(f"An error occurred: {str(e)}"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                        return
                
                return  # End processing for -num flag
            
            # Original code for handling emojis continues here
            # Validate emojis
            valid_emojis = await self.validate_emojis(ctx, args_list if use_super_reactions else args)
            if not valid_emojis:
                return  # Error message already sent in validate_emojis
                
            if mode == "single":
                # React to a specific message
                try:
                    message = await ctx.channel.fetch_message(message_id)
                    reactions_added = 0
                    
                    for emoji in valid_emojis:
                        try:
                            if remove_reactions:
                                # Remove the reaction (only works for the bot's own reactions)
                                await message.remove_reaction(emoji, self.bot.user)
                                reactions_added += 1
                            elif use_super_reactions:
                                await message.add_reaction(emoji, boost=True)  # Use boost=True for super reactions
                                reactions_added += 1
                            else:
                                await message.add_reaction(emoji)
                                reactions_added += 1
                        except (discord.HTTPException, TypeError) as e:
                            if remove_reactions:
                                logger.error(f"Failed to remove reaction {emoji}: {e}")
                            # Handle case where boost parameter isn't supported
                            elif use_super_reactions and isinstance(e, TypeError):
                                try:
                                    await message.add_reaction(emoji)  # Fallback to normal reaction
                                    reactions_added += 1
                                except discord.HTTPException as e2:
                                    logger.error(f"Failed to add fallback reaction {emoji}: {e2}")
                            else:
                                logger.error(f"Failed to add reaction {emoji}: {e}")
                            continue
                    
                    # Uncomment if you want action confirmation
                    # action_word = "Removed" if remove_reactions else "Added"
                    # reaction_type = "super reaction" if use_super_reactions else "reaction"
                    # await ctx.send(
                    #     format_message(f"{action_word} {reactions_added} {reaction_type}(s) to message"),
                    #     delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    # )
                except discord.NotFound:
                    await ctx.send(
                        format_message(f"Message with ID {message_id} not found"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                except discord.Forbidden:
                    await ctx.send(
                        format_message("I don't have permission to add reactions to that message"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                except Exception as e:
                    logger.error(f"Error reacting to message {message_id}: {e}")
                    await ctx.send(
                        format_message(f"Failed to add reactions: {str(e)}"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
            else:
                # React to the last target_amount messages
                try:
                    messages = []
                    async for message in ctx.channel.history(limit=target_amount + 1):  # +1 to account for the command message
                        # Skip the command message itself
                        if message.id != ctx.message.id:
                            messages.append(message)
                    
                    # We only need the most recent target_amount messages
                    messages = messages[:target_amount]
                    
                    if not messages:
                        await ctx.send(
                            format_message("No messages found to react to"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                        return
                        
                    total_reactions = 0
                    for message in messages:
                        for emoji in valid_emojis:
                            try:
                                if remove_reactions:
                                    # Remove the reaction (only works for the bot's own reactions)
                                    await message.remove_reaction(emoji, self.bot.user)
                                    total_reactions += 1
                                elif use_super_reactions:
                                    await message.add_reaction(emoji, boost=True)  # Use boost=True for super reactions
                                    total_reactions += 1
                                else:
                                    await message.add_reaction(emoji)
                                    total_reactions += 1
                                await asyncio.sleep(0.5)  # Rate limit between reactions
                            except (discord.HTTPException, TypeError) as e:
                                if remove_reactions:
                                    logger.error(f"Failed to remove reaction {emoji} from message {message.id}: {e}")
                                # Handle case where boost parameter isn't supported
                                elif use_super_reactions and isinstance(e, TypeError):
                                    try:
                                        await message.add_reaction(emoji)  # Fallback to normal reaction
                                        total_reactions += 1
                                        await asyncio.sleep(0.5)
                                    except discord.HTTPException as e2:
                                        logger.error(f"Failed to add fallback reaction {emoji} to message {message.id}: {e2}")
                                else:
                                    logger.error(f"Failed to add reaction {emoji} to message {message.id}: {e}")
                                continue
                    
                    # Uncomment if you want action confirmation
                    # action_word = "Removed" if remove_reactions else "Added"
                    # reaction_type = "super reaction" if use_super_reactions else "reaction"
                    # await ctx.send(
                    #     format_message(f"{action_word} {len(valid_emojis)} {reaction_type}(s) to {len(messages)} message(s) ({total_reactions} total)"),
                    #     delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    # )
                except Exception as e:
                    logger.error(f"Error in manualreact command: {e}", exc_info=True)
                    await ctx.send(
                        format_message(f"An error occurred: {str(e)}"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
        except Exception as e:
            logger.error(f"Error in manualreact command: {e}", exc_info=True)
            await ctx.send(
                format_message("âŒ An error occurred"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

    async def validate_emojis(self, ctx, emoji_list):
        """Helper method to validate emojis"""
        valid_emojis = []
        
        def looks_like_word(text):
            # Don't consider Discord emoji format as a word
            if text.startswith('<') and text.endswith('>'):
                return False
                
            # Count alphanumeric characters
            alphanum_count = sum(c.isalnum() or c in '_.' for c in text)
            # If most of the string is alphanumeric, it's likely a word/username
            return alphanum_count > len(text) * 0.7
            
        # Detect pure-text fake emojis (all caps text)
        def is_fake_emoji(text):
            # If it's all alphabetic characters and longer than 1 character, it's likely not an emoji
            if text.isalpha() and len(text) > 1:
                return True
                
            # Short text strings like "UR" that are all caps
            if text.isupper() and text.isalpha() and len(text) <= 3:
                return True
                
            # Detect invalid emoji formats like :sk: or :xy: that aren't actual Discord emojis
            if text.startswith(':') and text.endswith(':'):
                emoji_name = text[1:-1]
                # If it's a short text between colons (like :sk:), it's not a valid emoji
                if len(emoji_name) <= 3 and emoji_name.isalpha():
                    return True
                
            return False
        
        for emoji in emoji_list:
            # Check for obvious fake emojis first
            if is_fake_emoji(emoji):
                await ctx.send(
                    format_message(f"'{emoji}' is text, not an emoji"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                continue
            
            # Special handling for short colon-wrapped text like :sk: that Discord.py accepts but Discord API rejects
            if emoji.startswith(':') and emoji.endswith(':'):
                emoji_name = emoji[1:-1]
                if len(emoji_name) <= 3 and emoji_name.isalpha():
                    await ctx.send(
                        format_message(f"'{emoji}' is not a valid emoji"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    continue
            
            # Handle custom emoji format: <:name:id> or <a:name:id>
            if emoji.startswith('<') and emoji.endswith('>'):
                try:
                    partial = discord.PartialEmoji.from_str(emoji)
                    # Validate that it's actually a custom emoji with an ID
                    if partial.id is not None:
                        valid_emojis.append(str(partial))
                        continue
                    else:
                        await ctx.send(
                            format_message(f"Invalid custom emoji format: {emoji}"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                        continue
                except Exception as e:
                    await ctx.send(
                        format_message(f"Invalid custom emoji format: {emoji}"),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                    )
                    continue
            
            # Unicode emoji handling
            if is_valid_emoji(emoji):
                # Double-check with Discord's parser
                try:
                    partial_emoji = discord.PartialEmoji.from_str(emoji)
                    if partial_emoji.is_unicode_emoji():
                        valid_emojis.append(str(partial_emoji))
                        continue
                except Exception:
                    pass
                    
                # Only at this point we use our general is_valid_emoji function as final verification
                valid_emojis.append(emoji)
                continue
                
            # Skip obvious words/usernames
            if looks_like_word(emoji) and len(emoji) > 2:
                await ctx.send(
                    format_message(f"'{emoji}' appears to be text, not an emoji"),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                )
                continue
                
            # If nothing worked, it's just an invalid emoji
            await ctx.send(
                format_message(f"Invalid emoji: {emoji}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
        
        if not valid_emojis:
            await ctx.send(
                format_message("No valid emojis provided"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
        
        return valid_emojis

    @commands.command(aliases=['arl'])
    async def reactlist(self, ctx):
        """List all active reactions"""
        try:
            try:await ctx.message.delete()
            except:pass
            
            # Get all reactions
            self_reactions = self.reaction_manager.get_self_reactions()
            self_rotating = self.reaction_manager.get_rotating_reactions(ctx.author.id)
            user_reactions = {}
            user_rotating = {}
            
            # Get combined set of all user IDs from both regular and rotating reactions
            all_user_ids = set(self.reaction_manager.user_reactions.keys()) | set(self.reaction_manager.rotating_reactions.keys())

            for user_id in all_user_ids:
                user = self.bot.get_user(user_id)
                if user:
                    if self.reaction_manager.has_user_reactions(user_id):
                        user_reactions[user] = self.reaction_manager.get_user_reactions(user_id)
                    if self.reaction_manager.has_rotating_reactions(user_id):
                        user_rotating[user] = self.reaction_manager.get_rotating_reactions(user_id)
    
            message_parts = [
                "```ansi\n" + \
                "\u001b[30m\u001b[1m\u001b[4mActive Auto-Reactions\u001b[0m\n"
            ]
    
            # Add self reactions
            if self_reactions or self_rotating:
                message_parts[-1] += "\n\u001b[1;33mSelf Reactions:\n"
                if self_reactions:
                    # Get info about which reactions are super
                    super_reactions = self.reaction_manager.get_super_reactions(ctx.author.id)
                    formatted_reactions = []
                    for r in self_reactions:
                        if r in super_reactions:
                            formatted_reactions.append(f"{r}")  # Add lightning bolt to indicate super reaction
                        else:
                            formatted_reactions.append(r)
                    message_parts[-1] += f"\u001b[0;36mRegular: \u001b[0;37m{' '.join(formatted_reactions)}\n"
                if self_rotating:
                    # Format rotating groups with super indicators
                    formatted_groups = []
                    super_reactions = self.reaction_manager.get_super_reactions(ctx.author.id)
                    
                    for group in self_rotating:
                        # Check if all emojis in this group are super reactions
                        all_super = all(emoji in super_reactions for emoji in group)
                        group_text = ''.join(group)
                        
                        # Add  indicator if all emojis in the group are super reactions
                        if all_super:
                            formatted_groups.append(f"{group_text}")
                        else:
                            formatted_groups.append(group_text)
                            
                    message_parts[-1] += f"\u001b[0;36mRotating: \u001b[0;37m{' . '.join(formatted_groups)}\n"
                message_parts[-1] += "\u001b[0;37m" + "-" * 28 + "\n"
    
            # Add user reactions
            if user_reactions or user_rotating:
                message_parts[-1] += "\n\u001b[1;33mUser Reactions:\n"
                all_users = set(user_reactions.keys()) | set(user_rotating.keys())
                for user in all_users:
                    message_parts[-1] += f"\u001b[0;36mUser: \u001b[0;37m{user.name}\n"
                    if user in user_reactions:
                        # Get info about which reactions are super for this user
                        super_reactions = self.reaction_manager.get_super_reactions(user.id)
                        formatted_reactions = []
                        for r in user_reactions[user]:
                            if r in super_reactions:
                                formatted_reactions.append(f"{r}")  # Add lightning bolt to indicate super reaction
                            else:
                                formatted_reactions.append(r)
                        message_parts[-1] += f"\u001b[0;36mRegular: \u001b[0;37m{' '.join(formatted_reactions)}\n"
                    if user in user_rotating:
                        # Format rotating groups with super indicators
                        formatted_groups = []
                        super_reactions = self.reaction_manager.get_super_reactions(user.id)
                        
                        for group in user_rotating[user]:
                            # Check if all emojis in this group are super reactions
                            all_super = all(emoji in super_reactions for emoji in group)
                            group_text = ''.join(group)
                            
                            # Add  indicator if all emojis in the group are super reactions
                            if all_super:
                                formatted_groups.append(f"{group_text}")
                            else:
                                formatted_groups.append(group_text)
                                
                        message_parts[-1] += f"\u001b[0;36mRotating: \u001b[0;37m{' . '.join(formatted_groups)}\n"
                    message_parts[-1] += "\u001b[0;37m" + "-" * 28 + "\n"
    
            if not self_reactions and not self_rotating and not user_reactions and not user_rotating:
                message_parts[-1] += "\n\u001b[0;37mNo active auto-reactions found"
    
            message_parts[-1] += "```"
    
            await ctx.send(
                quote_block(message_parts[0]),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
    
        except Exception as e:
            logger.error(f"Error in reactlist command: {e}", exc_info=True)
            await ctx.send(
                format_message("âŒ An error occurred"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )

    async def extract_emojis_from_text(self, ctx, text):
        """Extract valid Discord emojis from a text string"""
        result = []
        
        # First try to split by spaces - this helps with groups of single emojis
        parts = text.split()
        if parts:
            for part in parts:
                # Check for invalid emoji formats like :sk: before using Discord.py's parser
                if part.startswith(':') and part.endswith(':'):
                    emoji_name = part[1:-1]
                    if len(emoji_name) <= 3 and emoji_name.isalpha():
                        await ctx.send(
                            format_message(f"'{part}' is not a valid emoji"),
                            delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
                        )
                        continue
                
                # Try to parse each part as an emoji
                try:
                    partial_emoji = discord.PartialEmoji.from_str(part)
                    if partial_emoji.is_unicode_emoji() or partial_emoji.id:
                        # Additional validation for Unicode emojis
                        if partial_emoji.is_unicode_emoji() and part.startswith(':') and part.endswith(':'):
                            emoji_name = part[1:-1]
                            if len(emoji_name) <= 3 and emoji_name.isalpha():
                                continue  # Skip invalid emoji formats like :sk:
                        result.append(str(partial_emoji))
                        continue
                except Exception:
                    pass
                
                # If part wasn't a valid emoji, try more complex extraction
                found_emojis = await self._extract_individual_emoji(part)
                if found_emojis:
                    result.extend(found_emojis)
        
        # If splitting by spaces didn't work, try extracting from the entire text
        if not result:
            result = await self._extract_individual_emoji(text)
            
        # Filter out empty results and duplicates
        result = [emoji for emoji in result if emoji.strip()]
        
        # Final validation to filter out invalid emoji formats
        validated_result = []
        for emoji in result:
            if emoji.startswith(':') and emoji.endswith(':'):
                emoji_name = emoji[1:-1]
                if len(emoji_name) <= 3 and emoji_name.isalpha():
                    continue  # Skip invalid emoji formats like :sk:
            validated_result.append(emoji)
            
        result = validated_result
        
        # If no valid emojis found, show error
        if not result:
            await ctx.send(
                format_message(f"Could not extract valid emojis from: {text}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return None
            
        return result
        
    async def _extract_individual_emoji(self, ctx, text):
        """Helper method to extract emojis from a single piece of text"""
        result = []
        
        # Check if the entire text is an invalid emoji format like :sk:
        if text.startswith(':') and text.endswith(':'):
            emoji_name = text[1:-1]
            if len(emoji_name) <= 3 and emoji_name.isalpha():
                # This is an invalid emoji format like :sk:, don't process it
                return []
                
        # Step 1: Extract custom emojis like <:name:id>
        i = 0
        while i < len(text):
            # Handle custom emoji format
            if text[i:].startswith('<'):
                end_idx = text[i:].find('>')
                if end_idx != -1:
                    end_idx += i + 1  # Convert to global index
                    potential_emoji = text[i:end_idx]
                    
                    try:
                        # Let Discord handle custom emoji parsing
                        partial = discord.PartialEmoji.from_str(potential_emoji)
                        if partial.id:  # Ensure it's a valid custom emoji
                            result.append(str(partial))
                            i = end_idx
                            continue
                    except Exception:
                        pass
            
            i += 1
            
        # Step 2: If no custom emojis found, try to extract unicode emojis
        if not result:
            # Check for invalid colon-wrapped text patterns
            if text.startswith(':') and text.endswith(':'):
                emoji_name = text[1:-1]
                if len(emoji_name) <= 3 and emoji_name.isalpha():
                    return []  # Invalid emoji format like :sk:, return empty list
                    
            # Pass the full text to Discord's parser to attempt extraction
            try:
                partial_emoji = discord.PartialEmoji.from_str(text)
                if partial_emoji.is_unicode_emoji():
                    # Additional validation for patterns like :sk: that discord.py incorrectly accepts
                    if text.startswith(':') and text.endswith(':'):
                        emoji_name = text[1:-1]
                        if len(emoji_name) <= 3 and emoji_name.isalpha():
                            return []  # Invalid emoji format
                    return [str(partial_emoji)]
            except Exception:
                pass
                
            # Manual extraction as fallback
            # Discord handles some emojis as multiple Unicode characters
            # We'll try to detect them character by character
            i = 0
            while i < len(text):
                for j in range(min(10, len(text) - i), 0, -1):  # Try different lengths, max 10
                    potential_emoji = text[i:i+j]
                    
                    # Skip patterns like :sk: that aren't valid emojis
                    if potential_emoji.startswith(':') and potential_emoji.endswith(':'):
                        emoji_name = potential_emoji[1:-1]
                        if len(emoji_name) <= 3 and emoji_name.isalpha():
                            break  # Skip this potential emoji
                    
                    try:
                        partial_emoji = discord.PartialEmoji.from_str(potential_emoji)
                        if partial_emoji.is_unicode_emoji():
                            result.append(str(partial_emoji))
                            i += j - 1  # -1 because the loop will increment i
                            break
                    except Exception:
                        continue
                i += 1
                
        # Filter out invalid emoji formats
        filtered_result = []
        for emoji in result:
            # Skip patterns like :sk: that aren't valid emojis
            if emoji.startswith(':') and emoji.endswith(':'):
                emoji_name = emoji[1:-1]
                if len(emoji_name) <= 3 and emoji_name.isalpha():
                    continue
            filtered_result.append(emoji)
        
            return [emoji for emoji in filtered_result if emoji.strip()]
        
        # If no valid emojis found, show error
        if not result:
            await ctx.send(
                format_message(f"Could not extract valid emojis from: {text}"),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None
            )
            return None
            
        return result

    async def _handle_message(self, message):
        """Handler for message events"""
        try:
            if message.author.bot:
                return

            # Check for rotating reactions
            rotating_reaction_groups = self.reaction_manager.get_rotating_reactions(message.author.id)
            if rotating_reaction_groups:
                # Get next emoji group in rotation
                user_index = (self.user_indices.get(message.author.id, 0) + 1) % len(rotating_reaction_groups)
                self.user_indices[message.author.id] = user_index
                emoji_group = rotating_reaction_groups[user_index]
                
                try:
                    # Add all emojis in the group
                    for emoji in emoji_group:
                        try:
                            # Additional validation for invalid emoji formats like :sk:
                            if isinstance(emoji, str) and emoji.startswith(':') and emoji.endswith(':'):
                                emoji_name = emoji[1:-1]
                                if len(emoji_name) <= 3 and emoji_name.isalpha():
                                    logger.error(f"Skipping invalid emoji format in rotation: {emoji}")
                                    continue
                            
                            # Check if this should be a super reaction
                            # Use the emoji directly as it's already validated and properly formatted
                            is_super = self.reaction_manager.is_super_reaction(message.author.id, emoji)
                            
                            if is_super:
                                try:
                                    await message.add_reaction(emoji, boost=True)  # boost=True makes it a super reaction
                                except (discord.HTTPException, TypeError):
                                    # Fallback in case API changes or unsupported
                                    await message.add_reaction(emoji)
                            else:
                                await message.add_reaction(emoji)
                                
                            await asyncio.sleep(0.5)  # Rate limit between reactions
                        except discord.HTTPException as e:
                            # Check the error code for Unknown Emoji
                            if hasattr(e, 'code') and e.code == 10014:
                                logger.error(f"Failed to add rotating reaction - Unknown Emoji: {emoji}")
                            else:
                                logger.error(f"Failed to add rotating reaction {emoji}: {e}")
                            continue
                        except Exception as e:
                            # Log other unexpected errors but continue with next emoji
                            logger.error(f"Error processing rotating reaction {emoji}: {e}")
                            continue
                    return
                except Exception as e:
                    logger.error(f"Failed to process rotating reaction group {emoji_group}: {e}")
                    return

            # Regular reactions
            reactions = None
            if message.author.id == self.bot.user.id:
                reactions = self.reaction_manager.get_self_reactions()
            else:
                reactions = self.reaction_manager.get_user_reactions(message.author.id)

            if reactions:
                await self.add_reactions(message, reactions)

        except Exception as e:
            logger.error(f"Error in auto-react handler: {e}")

    @rate_limiter(command_only=True)
    async def add_reactions(self, message, reactions):
        """Add reactions with rate limiting"""
        try:
            for reaction in reactions:
                # Additional validation to catch invalid emoji formats before attempting to add them
                # This specifically catches :sk: and similar patterns that discord.py's PartialEmoji accepts
                # but are rejected by Discord's API
                if isinstance(reaction, str) and reaction.startswith(':') and reaction.endswith(':'):
                    emoji_name = reaction[1:-1]
                    if len(emoji_name) <= 3 and emoji_name.isalpha():
                        logger.error(f"Skipping invalid emoji format: {reaction}")
                        continue
                
                try:
                    # Check if this should be a super reaction
                    is_super = self.reaction_manager.is_super_reaction(message.author.id, reaction)
                    
                    if is_super:
                        # Use the super reaction endpoint if supported
                        try:
                            await message.add_reaction(reaction, boost=True)  # boost=True makes it a super reaction
                        except (discord.HTTPException, TypeError) as e:
                            logger.warning(f"Failed to add super reaction {reaction}, falling back to regular: {e}")
                            # Fallback in case API changes or unsupported
                            await message.add_reaction(reaction)
                    else:
                        # Regular reaction
                        await message.add_reaction(reaction)
                        
                    await asyncio.sleep(0.5) # Rate limit between reactions
                except discord.HTTPException as e:
                    # Check the error code
                    if hasattr(e, 'code') and e.code == 10014:
                        logger.error(f"Failed to add reaction - Unknown Emoji: {reaction}")
                    else:
                        logger.error(f"Failed to add reaction {reaction}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error adding reaction {reaction}: {e}")
                    continue
        except Exception as e:
            logger.error(f"Error adding reactions: {e}")

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)
            logger.info("Registered message handler for AutoReactCog with EventManager")

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        # Unregister from event manager
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)
            logger.info("Unregistered AutoReactCog from EventManager")

async def setup(bot):
    await bot.add_cog(AutoReact(bot))
