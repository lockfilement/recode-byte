import discord
import asyncio

class ReactionManager:
    def __init__(self, bot):
        self.bot = bot
        self.self_reactions = []
        self.user_reactions = {}
        self.rotating_reactions = {}  # Store rotating emoji sets per user
        self.super_reactions = {}  # Store which reactions should be super reactions

    async def load_reactions(self):
        # Do not load reactions from the database
        pass

    async def save_self_reactions(self):
        # Do not save self reactions
        pass

    async def save_user_reactions(self, user_id):
        # Do not save user reactions
        pass

    def set_self_reactions(self, reactions):
        """Set reactions for self"""
        self.self_reactions = list(reactions)
        asyncio.create_task(self.save_self_reactions())

    def get_self_reactions(self):
        """Get reactions for self"""
        return self.self_reactions

    def has_self_reactions(self):
        """Check if self reactions are configured"""
        return bool(self.self_reactions)

    def clear_self_reactions(self):
        """Clear all self reactions"""
        self.self_reactions = []
        asyncio.create_task(self.save_self_reactions())

    def set_user_reactions(self, user_id, reactions):
        """Set reactions for specific user"""
        self.user_reactions[user_id] = list(reactions)
        asyncio.create_task(self.save_user_reactions(user_id))

    def get_user_reactions(self, user_id):
        """Get reactions for specific user"""
        return self.user_reactions.get(user_id, [])

    def has_user_reactions(self, user_id):
        """Check if user has reactions configured"""
        return user_id in self.user_reactions and bool(self.user_reactions[user_id])

    def clear_user_reactions(self, user_id):
        """Clear reactions for specific user"""
        self.user_reactions.pop(user_id, None)
        asyncio.create_task(self.save_user_reactions(user_id))

    def set_rotating_reactions(self, user_id, reactions, super_mode=False):
        """Set rotating reactions for a user
        If super_mode is True, all emojis in all groups will be set as super reactions"""
        # Process each reaction to ensure valid emojis
        processed_reactions = []
        for group in reactions:
            processed_group = []
            for emoji in group:
                # Validate emoji using Discord's parser
                try:
                    partial_emoji = discord.PartialEmoji.from_str(emoji)
                    if partial_emoji.is_unicode_emoji() or partial_emoji.id:
                        # Store the properly formatted emoji string
                        processed_group.append(str(partial_emoji))
                except Exception:
                    # If invalid, skip this emoji
                    continue
            
            # Only add the group if it contains valid emojis
            if processed_group:
                processed_reactions.append(processed_group)
                
        # Store the validated emoji groups
        self.rotating_reactions[user_id] = processed_reactions
        
        # If super_mode is enabled, mark all emojis in all groups as super reactions
        if super_mode:
            for group in processed_reactions:
                for emoji in group:
                    self.set_super_reactions(user_id, emoji, True)
        
    def get_rotating_reactions(self, user_id):
        """Get rotating reactions for a user"""
        return self.rotating_reactions.get(user_id, [])
        
    def has_rotating_reactions(self, user_id):
        """Check if user has rotating reactions"""
        return user_id in self.rotating_reactions and bool(self.rotating_reactions[user_id])

    def clear_rotating_reactions(self, user_id):
        """Clear rotating reactions for a user"""
        self.rotating_reactions.pop(user_id, None)

    def set_super_reactions(self, user_id, emoji, is_super=True):
        """Set which emojis should be used as super reactions for a user"""
        if user_id not in self.super_reactions:
            self.super_reactions[user_id] = {}
        self.super_reactions[user_id][emoji] = is_super

    def is_super_reaction(self, user_id, emoji):
        """Check if the emoji is configured as a super reaction for this user"""
        if user_id not in self.super_reactions:
            return False
        return self.super_reactions[user_id].get(emoji, False)
        
    def get_super_reactions(self, user_id):
        """Get all emojis configured as super reactions for a user"""
        if user_id not in self.super_reactions:
            return {}
        return {emoji: is_super for emoji, is_super in self.super_reactions[user_id].items() if is_super}
        
    def clear_super_reactions(self, user_id):
        """Clear super reaction settings for a user"""
        if user_id in self.super_reactions:
            self.super_reactions.pop(user_id)
