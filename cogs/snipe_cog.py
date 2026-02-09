import discord
from discord.ext import commands, tasks
import asyncio
from datetime import datetime
from typing import Union, Optional
import logging
from utils.general import get_max_message_length
from pymongo.errors import BulkWriteError
from pymongo import WriteConcern, UpdateOne, DeleteOne # Added import

logger = logging.getLogger(__name__)

# Configuration for the buffer - optimized for high throughput
BUFFER_FLUSH_INTERVAL = 5   # seconds - flush more frequently
BUFFER_MAX_SIZE = 2000      # Much larger batch size for high volume processing
USER_MESSAGE_LIMIT = 100 # Max messages per user to keep
# Note: deleted_messages, edited_messages, and mentions use immediate storage for real-time snipe functionality

class Snipe(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.db
        # Track ghost friend requests
        self.pending_friend_requests = {}    
        # Buffer for user messages
        self.message_buffer = []
        self.buffer_lock = asyncio.Lock()
        
        # Note: Removed batch processors for snipe operations to maintain real-time functionality
        # Only user_messages uses batching via the existing buffer system (10-second intervals)
        # deleted_messages, edited_messages, and mentions need immediate storage for snipe commands
        # Start the periodic flush task if db is active
        if self.bot.db.is_active:
            self.flush_buffer_task.start() # Start the task loop
        
    async def get_user_message_limit(self, user_id: int) -> int:
        """Get custom message tracking limit for a user, or default if none set"""
        if not self.bot.db.is_active:
            return USER_MESSAGE_LIMIT
            
        try:
            # Check if user has a custom limit
            limit_doc = await self.bot.db.db.tracking_limits.find_one({"user_id": user_id})
            if limit_doc and "message_limit" in limit_doc:
                return limit_doc["message_limit"]
        except Exception as e:
            logger.error(f"Error fetching custom message limit for user {user_id}: {e}")
            
        # Return default if no custom limit or error
        return USER_MESSAGE_LIMIT
    
    def clean_content(self, content: str) -> str:
        """Clean and escape special characters in message content"""
        # Replace common formatting characters with escaped versions
        content = (content
            .replace('\\', '')  # Escape backslashes first
            .replace('```', '')  # Replace code blocks with zero-width joiner to preserve visible format but prevent conflicts
            .replace('`', '')  # Replace backticks with zero-width joiner
            .replace('|', '')  # Escape pipes
            .replace('*', '')  # Escape asterisks
        )
        return content
    
    def truncate_content(self, content: str, max_length: int = 512) -> str:
        """Truncate content if it's longer than max_length"""
        if content and len(content) > max_length:
            return content[:max_length-3] + "..."
        return content
    
    def quote_block(self, text: str) -> str:
        """Add > prefix to each line while preserving the content"""
        return '\n'.join(f'> {line}' for line in text.split('\n'))
    
    async def _periodic_flush(self):
        """Periodically flushes the message buffer to the database."""
        messages_to_insert = [] # Initialize empty list
        async with self.buffer_lock:
            if not self.message_buffer:
                return # Nothing to flush

            messages_to_insert = self.message_buffer[:] # Copy buffer
            self.message_buffer.clear() # Clear original buffer

        if messages_to_insert:
            # logger.debug(f"Flushing {len(messages_to_insert)} messages to user_messages collection.")
            flush_successful = False # Flag to track success
            try:
                # Use insert_many for bulk insertion with explicit write concern
                # Get the collection with the desired write concern
                user_messages_collection = self.bot.db.db.get_collection(
                    "user_messages",
                    write_concern=WriteConcern(w="majority") # Explicitly use majority write concern
                )
                await user_messages_collection.insert_many(
                    messages_to_insert,
                    ordered=False # Continue inserting even if some fail (e.g., duplicates)
                )
                logger.debug(f"Successfully flushed {len(messages_to_insert)} messages (initial attempt).") # Log success
                flush_successful = True # Mark as successful for limit enforcement

            except BulkWriteError as bwe:
                # Log duplicate key errors specifically if needed, ignore otherwise
                write_errors = bwe.details.get('writeErrors', [])
                duplicates = sum(1 for e in write_errors if e.get('code') == 11000)
                if duplicates != len(write_errors): # If there are errors other than duplicates
                     logger.error(f"Error during bulk insert to user_messages (excluding duplicates): {bwe.details}", exc_info=False)
                     # Add messages back to buffer if non-duplicate errors occurred
                     async with self.buffer_lock:
                         self.message_buffer.extend(messages_to_insert)
                     logger.warning(f"Added {len(messages_to_insert)} messages back to buffer due to non-duplicate BulkWriteError.")
                     flush_successful = False # Mark as failed
                else:
                    # logger.debug(f"Ignored {duplicates} duplicate messages during flush.")
                    flush_successful = True # Duplicates are okay, proceed with limit enforcement

            except Exception as e:
                logger.error(f"Unexpected error flushing message buffer: {e}", exc_info=True)
                # Add messages back to the buffer to retry later
                async with self.buffer_lock:
                    self.message_buffer.extend(messages_to_insert)
                logger.warning(f"Added {len(messages_to_insert)} messages back to buffer due to unexpected error: {e}")
                flush_successful = False # Mark as failed
                return # Stop processing this batch, will retry next interval

            # --- Handle message limit enforcement after flushing (only if insert was successful or only duplicates) ---
            if flush_successful:
                # Batch limit enforcement for better performance
                await self._batch_enforce_message_limits(messages_to_insert)


    @tasks.loop(seconds=BUFFER_FLUSH_INTERVAL)
    async def flush_buffer_task(self):
        """Wrapper task for periodic flushing."""
        if not self.bot.db.is_active:
            logger.warning("Database is not active. Skipping buffer flush.")
            return
        await self._periodic_flush()

    @flush_buffer_task.before_loop
    async def before_flush_buffer_task(self):
        """Wait until the bot is ready before starting the loop."""
        await self.bot.wait_until_ready()
        logger.info("Starting periodic user message buffer flush task.")

    async def _handle_message(self, message):
        """Handler for message events"""
        if not self.bot.db.is_active:
            return
        try:
            if message.author.bot:
                return
            
            # --- Start: User Message Tracking (Buffered) ---
            try:
                # Check if message was sent by the current bot instance
                is_self = message.author.id == self.bot.user.id
                # Prepare user message data with more details                
                user_message_data = {
                    "_id": message.id, # Use message ID as MongoDB _id for uniqueness
                    "message_id": message.id,
                    "user_id": message.author.id,
                    "username": message.author.name,
                    "content": message.content or "",
                    "created_at": datetime.utcnow(),
                    "channel_id": message.channel.id,
                    "channel_name": "DMs" if isinstance(message.channel, discord.DMChannel) else message.channel.name,
                    "channel_type": "group" if hasattr(message.channel, "recipients") and len(message.channel.recipients) > 1 else "DMs" if isinstance(message.channel, discord.DMChannel) else "text",
                    "is_group": hasattr(message.channel, "recipients") and len(message.channel.recipients) > 1,
                    "attachments": [a.proxy_url for a in message.attachments] if message.attachments else [],
                    "instance_id": self.bot.user.id,
                    "is_self": is_self
                }
                
                # Add DM recipient information for better context
                if isinstance(message.channel, discord.DMChannel) and hasattr(message.channel, "recipient"):
                    user_message_data["dm_recipient_id"] = message.channel.recipient.id
                    user_message_data["dm_recipient_name"] = message.channel.recipient.name
                
                # Add guild information if available
                if hasattr(message, 'guild') and message.guild:
                    user_message_data["guild_id"] = message.guild.id
                    user_message_data["guild_name"] = message.guild.name
                
                # Capture reply information if message is a reply
                if hasattr(message, 'reference') and message.reference:
                    if hasattr(message.reference, 'resolved') and message.reference.resolved:
                        ref_msg = message.reference.resolved
                        
                        # Handle replies to normal messages
                        if hasattr(ref_msg, 'author') and ref_msg.author:
                            user_message_data["reply_to_user_id"] = ref_msg.author.id
                            user_message_data["reply_to_username"] = ref_msg.author.name
                            # Also capture the content of the replied message
                            user_message_data["reply_to_content"] = ref_msg.content if hasattr(ref_msg, 'content') else ""
                            # Capture attachments from the replied message
                            if hasattr(ref_msg, 'attachments') and ref_msg.attachments:
                                user_message_data["reply_to_attachments"] = [a.proxy_url for a in ref_msg.attachments]
                        
                        # Handle replies to message snapshots (forwarded messages)
                        if hasattr(ref_msg, 'message_snapshots') and ref_msg.message_snapshots:
                            # Get the first message snapshot (forwarded message)
                            snapshot = ref_msg.message_snapshots[0]
                            user_message_data["reply_to_snapshot"] = True
                            user_message_data["reply_to_content"] = snapshot.content if hasattr(snapshot, 'content') else ""
                            

                            # Capture attachments from the snapshot
                            if hasattr(snapshot, 'attachments') and snapshot.attachments:
                                user_message_data["reply_to_attachments"] = [
                                    a.proxy_url if hasattr(a, 'proxy_url') else a.url 
                                    for a in snapshot.attachments
                                ]
                
                # Check for message snapshots (forwarded messages)
                if hasattr(message, 'message_snapshots') and message.message_snapshots:
                    snapshots_data = []
                    for snapshot in message.message_snapshots:
                        snapshot_attachments = []
                        if hasattr(snapshot, 'attachments'):
                            for attachment in snapshot.attachments:
                                if hasattr(attachment, 'proxy_url'):
                                    snapshot_attachments.append(attachment.proxy_url)
                                else:
                                    snapshot_attachments.append(attachment.url)
                        
                        snapshot_data = {
                            "snapshot_id": snapshot.id,
                            "snapshot_content": snapshot.content if hasattr(snapshot, 'content') else "",
                            "snapshot_created_at": snapshot.created_at.isoformat() if hasattr(snapshot, 'created_at') else None,
                            "snapshot_attachments": snapshot_attachments
                        }
                        
                        # Try to get author information if available
                        cached_message = snapshot.cached_message if hasattr(snapshot, 'cached_message') else None
                        if cached_message and hasattr(cached_message, 'author'):
                            snapshot_data["snapshot_author_id"] = cached_message.author.id
                            snapshot_data["snapshot_author_name"] = cached_message.author.name
                        
                        snapshots_data.append(snapshot_data)
                    
                    user_message_data["message_snapshots"] = snapshots_data
                
                # Add the message data to the buffer instead of writing directly
                async with self.buffer_lock:
                    self.message_buffer.append(user_message_data)

                # Trigger flush if buffer size exceeds max (optimized for high traffic)
                async with self.buffer_lock:
                    if len(self.message_buffer) >= BUFFER_MAX_SIZE:
                        # Schedule immediate flush to prevent memory buildup
                        logger.debug(f"Buffer size {len(self.message_buffer)} >= {BUFFER_MAX_SIZE}, triggering early flush.")
                        # Don't await to avoid blocking message processing
                        asyncio.create_task(self._periodic_flush())

                # --- REMOVED direct DB write and limit check ---
                # await self.bot.db.db.user_messages.update_one(...)
                # count = await self.bot.db.db.user_messages.count_documents(...)
                # if count > 100: ... delete logic ...

            except Exception as e:
                logger.error(f"Error preparing or buffering user message: {e}", exc_info=True)
            # --- End: User Message Tracking (Buffered) ---


            # --- Start: Mention Tracking (Remains unchanged, direct write) ---
            if message.author.id == self.bot.user.id:
                 return # Skip mention tracking for self
                
            was_mentioned = False
            
            # Check if selfbot user was directly mentioned
            if self.bot.user in message.mentions:
                was_mentioned = True
                
            # Check if selfbot user was replied to
            elif message.reference and isinstance(message.reference.resolved, discord.Message):
                reply_author = message.reference.resolved.author
                if reply_author.id == self.bot.user.id:
                    was_mentioned = True
                    
            if not was_mentioned:
                return            # Store mention data
            mention_data = {
                "message_id": message.id,
                "author_id": message.author.id,
                "author_name": message.author.name,
                "content": message.content,                
                "created_at": datetime.utcnow(),
                "channel_id": message.channel.id,
                "channel_name": "DMs" if isinstance(message.channel, discord.DMChannel) else message.channel.name,
                "attachments": [a.proxy_url for a in message.attachments] if message.attachments else [],
                "channel_type": 3 if hasattr(message.channel, "recipients") and len(message.channel.recipients) > 1 else 1 if isinstance(message.channel, discord.DMChannel) else 0,
                "is_group": hasattr(message.channel, "recipients") and len(message.channel.recipients) > 1,
                "target_id": self.bot.user.id  # Store only selfbot user ID
            }
            
            # Capture reply information if this is a reply to someone other than the selfbot
            if message.reference and isinstance(message.reference.resolved, discord.Message):
                # Only store reply data if it's a reply to another user's message
                if message.reference.resolved.author.id != message.author.id:
                    reply_msg = message.reference.resolved
                    mention_data["reply_to_user_id"] = reply_msg.author.id
                    mention_data["reply_to_username"] = reply_msg.author.name
                    mention_data["reply_to_content"] = reply_msg.content
                    
                    # Store reply attachments if any
                    if reply_msg.attachments:
                        mention_data["reply_to_attachments"] = [a.proxy_url for a in reply_msg.attachments]
                    
                    # Handle replies to message snapshots (forwarded messages)
                    if hasattr(reply_msg, 'message_snapshots') and reply_msg.message_snapshots:
                        snapshot = reply_msg.message_snapshots[0]
                        mention_data["reply_to_snapshot"] = True
                        mention_data["reply_to_content"] = snapshot.content if hasattr(snapshot, 'content') else ""
                        
                        # Capture attachments from the snapshot
                        if hasattr(snapshot, 'attachments') and snapshot.attachments:
                            mention_data["reply_to_attachments"] = [
                                a.proxy_url if hasattr(a, 'proxy_url') else a.url 
                                for a in snapshot.attachments
                            ]
    
            if hasattr(message, 'guild') and message.guild:
                mention_data["guild_id"] = message.guild.id
                mention_data["guild_name"] = message.guild.name
    
            # Store mentions immediately for real-time functionality  
            await self.bot.db.db.mentions.update_one(
                {"message_id": message.id},
                {"$setOnInsert": mention_data},
                upsert=True
            )
            # --- End: Mention Tracking ---

        except Exception as e:
            # General error handling for _handle_message
            logger.error(f"Error in _handle_message: {e}", exc_info=True)

    # Optional: Helper for early flush trigger to reset flag
    # async def _trigger_early_flush(self):
    #     try:
    #         await self._periodic_flush()
    #     finally:
    #         # Reset the flag after the flush attempt completes or fails
    #         self._flush_triggered = False

    async def _batch_enforce_message_limits(self, messages_to_insert):
        """Enforce message limits using batch operations"""
        try:
            user_ids_in_batch = {msg['user_id'] for msg in messages_to_insert}
            delete_operations = []
            
            for user_id in user_ids_in_batch:
                try:
                    # Get custom limit for this user
                    user_limit = await self.get_user_message_limit(user_id)
                    
                    count = await self.bot.db.count_documents('user_messages', {"user_id": user_id})
                    if count > user_limit:
                        excess = count - user_limit
                        if excess > 0:
                            # Find the oldest messages for this user to delete
                            oldest_messages = await self.bot.db.find_many(
                                'user_messages',
                                query={"user_id": user_id},
                                projection={"_id": 1},
                                sort=[("created_at", 1)],
                                limit=excess
                            )
                            
                            oldest_ids = [msg["_id"] for msg in oldest_messages]
                            if oldest_ids:
                                # Add to batch delete operations
                                delete_operations.extend(
                                    DeleteOne({"_id": msg_id}) for msg_id in oldest_ids
                                )
                except Exception as e_limit:
                    logger.error(f"Error preparing limit enforcement for user {user_id}: {e_limit}", exc_info=True)
            
            # Execute batch delete if we have operations
            if delete_operations:
                try:
                    result = await self.bot.db.bulk_write('user_messages', delete_operations, ordered=False)
                    
                    # Parse result using helper function
                    parsed = self.bot.db.parse_bulk_result(result, "delete")
                    # Removed debug logging to prevent console spam
                        
                except Exception as e:
                    logger.error(f"Error in batch limit enforcement: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error in batch message limit enforcement: {e}", exc_info=True)

    async def _handle_message_delete(self, message):
        """Handler for message delete events"""
        if not self.bot.db.is_active:
            return
        try:
            if not hasattr(message, 'content') or not hasattr(message, 'author'):
                return
            if message.author.bot or message.author.id == self.bot.user.id:
                return

            # Convert attachment URLs to media proxy URLs
            proxy_attachments = []
            if hasattr(message, 'attachments'):
                for attachment in message.attachments:
                    # Replace cdn.discordapp.com with media.discordapp.net
                    if hasattr(attachment, 'proxy_url'):
                        proxy_attachments.append(attachment.proxy_url)
                    else:
                        # Fallback to direct URL if proxy not available
                        proxy_attachments.append(attachment.url)   
            message_data = {
                "message_id": message.id,
                "user_id": message.author.id,
                "content": message.content or "",
                "deleted_at": datetime.utcnow(),
                "channel_id": message.channel.id,
                "channel_name": "DMs" if isinstance(message.channel, discord.DMChannel) else message.channel.name,
                "channel_type": "group" if hasattr(message.channel, "recipients") and len(message.channel.recipients) > 1 else "DMs" if isinstance(message.channel, discord.DMChannel) else "text",
                "is_group": hasattr(message.channel, "recipients") and len(message.channel.recipients) > 1,
                "attachments": proxy_attachments  # Store proxy URLs instead of direct CDN URLs
            }

            # Handle forwarded messages (MessageSnapshot)
            if hasattr(message, 'message_reference') and isinstance(message.message_reference, discord.MessageReference):
                if message.message_reference.resolved and hasattr(message.message_reference.resolved, 'to_message_reference_dict'):
                    snapshot_dict = message.message_reference.resolved.to_message_reference_dict()
                    message_data["has_forwarded_message"] = True
                    message_data["forwarded_message_id"] = snapshot_dict.get("message_id")
                    message_data["forwarded_message_channel_id"] = snapshot_dict.get("channel_id")
                    message_data["forwarded_message_guild_id"] = snapshot_dict.get("guild_id")
            
            # Check for message snapshots (forwarded messages)
            if hasattr(message, 'message_snapshots') and message.message_snapshots:
                snapshots_data = []
                for snapshot in message.message_snapshots:
                    snapshot_attachments = []
                    if hasattr(snapshot, 'attachments'):
                        for attachment in snapshot.attachments:
                            if hasattr(attachment, 'proxy_url'):
                                snapshot_attachments.append(attachment.proxy_url)
                            else:
                                snapshot_attachments.append(attachment.url)
                    
                    snapshot_data = {
                        "snapshot_id": snapshot.id,
                        "snapshot_content": snapshot.content if hasattr(snapshot, 'content') else "",
                        "snapshot_created_at": snapshot.created_at.isoformat() if hasattr(snapshot, 'created_at') else None,
                        "snapshot_attachments": snapshot_attachments
                    }
                    
                    # Try to get author information if available
                    cached_message = snapshot.cached_message if hasattr(snapshot, 'cached_message') else None
                    if cached_message and hasattr(cached_message, 'author'):
                        snapshot_data["snapshot_author_id"] = cached_message.author.id
                        snapshot_data["snapshot_author_name"] = cached_message.author.name
                    
                    snapshots_data.append(snapshot_data)
                
                message_data["message_snapshots"] = snapshots_data

            # Capture reply information if message is a reply
            if hasattr(message, 'reference') and message.reference:
                if hasattr(message.reference, 'resolved') and message.reference.resolved:
                    ref_msg = message.reference.resolved
                    
                    # Handle replies to normal messages
                    if hasattr(ref_msg, 'author') and ref_msg.author:
                        message_data["reply_to_user_id"] = ref_msg.author.id
                        message_data["reply_to_username"] = ref_msg.author.name
                        # Also capture the content of the replied message
                        message_data["reply_to_content"] = ref_msg.content if hasattr(ref_msg, 'content') else ""
                        # Capture attachments from the replied message
                        if hasattr(ref_msg, 'attachments') and ref_msg.attachments:
                            message_data["reply_to_attachments"] = [a.proxy_url for a in ref_msg.attachments]
                    
                    # Handle replies to message snapshots (forwarded messages)
                    # Check the resolved message for message_snapshots
                    if hasattr(ref_msg, 'message_snapshots') and ref_msg.message_snapshots:
                        # Get the first message snapshot (forwarded message)
                        snapshot = ref_msg.message_snapshots[0]
                        message_data["reply_to_snapshot"] = True
                        message_data["reply_to_content"] = snapshot.content if hasattr(snapshot, 'content') else ""
                        
                        # Capture attachments from the snapshot
                        if hasattr(snapshot, 'attachments') and snapshot.attachments:
                            message_data["reply_to_attachments"] = [
                                a.proxy_url if hasattr(a, 'proxy_url') else a.url 
                                for a in snapshot.attachments
                            ]

            if hasattr(message, 'guild') and message.guild:
                message_data["guild_id"] = message.guild.id
                message_data["guild_name"] = message.guild.name

            # Store deleted messages immediately for real-time snipe functionality
            await self.bot.db.db.deleted_messages.update_one(
                {"message_id": message.id},
                {"$setOnInsert": message_data},
                upsert=True
            )

        except Exception as e:
            logger.error(f"Error storing deleted message: {e}", exc_info=True)

    async def _handle_message_edit(self, before, after):
        """Handler for message edit events"""
        if not self.bot.db.is_active:
            return
        try:
            if not hasattr(before, 'content') or not hasattr(before, 'author'):
                return
            if before.author.bot or before.author.id == self.bot.user.id:
                return
            if before.content == after.content and before.attachments == after.attachments:
                return  # Skip if neither content nor attachments changed

            # Convert attachment URLs to media proxy URLs
            before_attachments = []
            after_attachments = []

            if hasattr(before, 'attachments'):
                for attachment in before.attachments:
                    if hasattr(attachment, 'proxy_url'):
                        before_attachments.append(attachment.proxy_url)
                    else:
                        before_attachments.append(attachment.url)

            if hasattr(after, 'attachments'):
                for attachment in after.attachments:
                    if hasattr(attachment, 'proxy_url'):
                        after_attachments.append(attachment.proxy_url)
                    else:
                        after_attachments.append(attachment.url)     

            edit_data = {
                "message_id": before.id,
                "user_id": before.author.id,
                "before_content": before.content or "",
                "after_content": after.content or "",
                "before_attachments": before_attachments,
                "after_attachments": after_attachments,
                "edited_at": datetime.utcnow(),                
                "channel_id": before.channel.id,
                "channel_name": "DMs" if isinstance(before.channel, discord.DMChannel) else before.channel.name,
                "channel_type": "group" if hasattr(before.channel, "recipients") and len(before.channel.recipients) > 1 else "DMs" if isinstance(before.channel, discord.DMChannel) else "text",
                "is_group": hasattr(before.channel, "recipients") and len(before.channel.recipients) > 1
            }
            
            # Capture reply information if message is a reply
            if hasattr(before, 'reference') and before.reference:
                if hasattr(before.reference, 'resolved') and before.reference.resolved:
                    ref_msg = before.reference.resolved
                    
                    # Handle replies to normal messages
                    if hasattr(ref_msg, 'author') and ref_msg.author:
                        edit_data["reply_to_user_id"] = ref_msg.author.id
                        edit_data["reply_to_username"] = ref_msg.author.name
                        # Also capture the content of the replied message
                        edit_data["reply_to_content"] = ref_msg.content if hasattr(ref_msg, 'content') else ""
                        # Capture attachments from the replied message
                        if hasattr(ref_msg, 'attachments') and ref_msg.attachments:
                            edit_data["reply_to_attachments"] = [a.proxy_url for a in ref_msg.attachments]
                    
                    # Handle replies to message snapshots (forwarded messages)
                    # Check the resolved message for message_snapshots
                    if hasattr(ref_msg, 'message_snapshots') and ref_msg.message_snapshots:
                        # Get the first message snapshot (forwarded message)
                        snapshot = ref_msg.message_snapshots[0]
                        edit_data["reply_to_snapshot"] = True
                        edit_data["reply_to_content"] = snapshot.content if hasattr(snapshot, 'content') else ""
                        
                        # Capture attachments from the snapshot
                        if hasattr(snapshot, 'attachments') and snapshot.attachments:
                            edit_data["reply_to_attachments"] = [
                                a.proxy_url if hasattr(a, 'proxy_url') else a.url 
                                for a in snapshot.attachments
                            ]

            if hasattr(before, 'guild') and before.guild:
                edit_data["guild_id"] = before.guild.id
                edit_data["guild_name"] = before.guild.name

            # Store edited messages immediately for real-time snipe functionality
            await self.bot.db.db.edited_messages.update_one(
                {"message_id": before.id},
                {"$setOnInsert": edit_data},
                upsert=True
            )

        except Exception as e:
            logger.error(f"Error storing edited message: {e}", exc_info=True)    
            
    async def _handle_relationship_add(self, relationship):
        """Track when a friend request is received"""
        if not self.bot.db.is_active:
            return
        
        try:
            # Only track incoming friend requests (when someone sends to us)
            if relationship.type == discord.RelationshipType.incoming_request:                
                request_data = {
                    "user_id": relationship.user.id,
                    "username": relationship.user.name,
                    "sent_at": datetime.utcnow(),
                    "status": "pending",
                    "avatar_url": str(relationship.user.avatar.url) if relationship.user.avatar else None,
                    "instance_id": self.bot.user.id
                }
                
                # Store in memory for quicker access
                self.pending_friend_requests[relationship.user.id] = request_data
                
                # Store in database
                await self.bot.db.db.friend_requests.update_one(
                    {"user_id": relationship.user.id},
                    {"$set": request_data},
                    upsert=True
                )
                
                logger.info(f"Tracked incoming friend request from {relationship.user.name} ({relationship.user.id})")
        except Exception as e:
            logger.error(f"Error tracking friend request: {e}")    
            
    async def _handle_relationship_remove(self, relationship):
        """Track when a friend request is cancelled"""
        if not self.bot.db.is_active:
            return
        
        try:
            # Check if this was a pending incoming request that got removed
            user_id = relationship.user.id
            if user_id in self.pending_friend_requests or await self.bot.db.db.friend_requests.find_one({"user_id": user_id, "status": "pending"}):
                # Mark as ghosted - the request was removed/cancelled
                update_data = {
                    "removed_at": datetime.utcnow(),
                    "status": "ghosted",
                }
                
                # Update in-memory record
                if user_id in self.pending_friend_requests:
                    self.pending_friend_requests[user_id].update(update_data)
                
                # Update database record
                await self.bot.db.db.friend_requests.update_one(
                    {"user_id": user_id},
                    {"$set": update_data}
                )
                
                logger.info(f"Marked friend request from {relationship.user.name} ({user_id}) as ghosted")
        except Exception as e:
            logger.error(f"Error tracking removed friend request: {e}")    
            
    async def _handle_relationship_update(self, before, after):
        """Track when a friend request status changes"""
        if not self.bot.db.is_active:
            return
            
        try:
            # If relationship changed from incoming request to friend, clean up the tracking
            if before.type == discord.RelationshipType.incoming_request and after.type == discord.RelationshipType.friend:
                user_id = after.user.id
                
                # Remove from pending dict if present
                self.pending_friend_requests.pop(user_id, None)
                
                # Remove from database to keep it clean
                await self.bot.db.db.friend_requests.delete_one({"user_id": user_id})
                
                logger.info(f"Accepted friend request from {after.user.name} ({user_id}), removed from tracking")
        except Exception as e:
            logger.error(f"Error tracking relationship update: {e}")

    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        # Note: Snipe operations (delete/edit/mention) use immediate storage for real-time functionality
        # Only user_messages continues to use the existing 10-second buffer system
        
        # Start the flush task if not already started (e.g., if cog is reloaded)
        if self.bot.db.is_active and not self.flush_buffer_task.is_running():
             try:
                 self.flush_buffer_task.start()
             except RuntimeError: # Already running (shouldn't happen with check, but safety)
                 pass

        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            # Make sure the handler registration uses the correct method name
            event_manager.register_handler('on_message', self.__class__.__name__, self._handle_message)
            event_manager.register_handler('on_message_delete', self.__class__.__name__, self._handle_message_delete)
            event_manager.register_handler('on_message_edit', self.__class__.__name__, self._handle_message_edit)
            event_manager.register_handler('on_relationship_add', self.__class__.__name__, self._handle_relationship_add)
            event_manager.register_handler('on_relationship_remove', self.__class__.__name__, self._handle_relationship_remove)
            event_manager.register_handler('on_relationship_update', self.__class__.__name__, self._handle_relationship_update)
        else:
             logger.warning("EventManager cog not found. SnipeCog event handlers may not be registered.")

    @commands.command(aliases=['sn'])
    async def snipe(self, ctx, target: Optional[Union[int, discord.Member, discord.User, discord.TextChannel]] = None, 
                    amount: Optional[int] = None, 
                    channel: Optional[discord.TextChannel] = None):
        """
        Snipe deleted messages
        
        .snipe - Show most recent deleted message in current channel
        .snipe @user/ID - Show most recent deleted message from user
        .snipe <amount> - Show X most recent deleted messages in current channel
        .snipe @user/ID <amount> - Show X most recent deleted messages from user
        .snipe #channel - Show most recent deleted message in specified channel
        .snipe <amount> #channel - Show X most recent deleted messages in specified channel
        .snipe @user/ID #channel - Show most recent deleted message from user in specified channel
        .snipe @user/ID <amount> #channel - Show X most recent deleted messages from user in specified channel
        .snipe <channel_id> - Show most recent deleted message in specified channel by ID
        """

        def quote_block(text):
        # Add > prefix to each line while preserving the content
            return '\n'.join(f'> {line}' for line in text.split('\n'))

        try:await ctx.message.delete()
        except:pass
        
        # Initialize query
        query = {
            "user_id": {"$ne": self.bot.user.id}  # Filter out selfbot's messages
        }
        limit = 1
        user = None
        target_channel = None

        # Check if the channel was specified in the target parameter
        if isinstance(target, discord.TextChannel):
            target_channel = target
            target = None
        
        # Check if channel was specified as third parameter
        if channel:
            target_channel = channel

        # Handle first argument - could be amount, user, or channel_id
        if isinstance(target, (discord.Member, discord.User)):
            user = target
        elif isinstance(target, int):
            # First check if it's a channel ID
            found_channel = self.bot.get_channel(target)
            if found_channel:
                target_channel = found_channel
            else:
                # Next check if it's a user ID
                found_user = await self.bot.GetUser(target)
                if found_user:
                    user = found_user
                else:
                    # If not a channel ID or user ID, treat as amount
                    limit = target

        # If we identified a user, set up user query
        if user:
            query["user_id"] = user.id
            # If amount specified after user, update limit
            if amount:
                limit = amount
            # Only add channel_id if a specific channel was requested
            if target_channel:
                query["channel_id"] = target_channel.id
        else:
            # No user specified, so filter by channel
            if target_channel:
                query["channel_id"] = target_channel.id
            else:
                # Default to current channel if no user and no specific channel
                query["channel_id"] = ctx.channel.id
                
            if amount:  
                # If amount specified but no user, show channel messages
                limit = amount

        # Apply reasonable limits
        limit = min(max(1, limit), 1000)  # Between 1 and 1000 messages

        try:
            # Fetch messages with proper sorting
            cursor = self.bot.db.db.deleted_messages.find(query)
            cursor.sort("deleted_at", -1)
            
            # Apply limit if not "all"
            messages = await cursor.to_list(length=limit if limit > 0 else None)

            if not messages:
                if user:
                    no_message_text = f"No deleted messages found from {user.name}"
                    if target_channel:
                        no_message_text += f" in #{target_channel.name}"
                else:
                    channel_name = f"#{target_channel.name}" if target_channel else "this channel"
                    no_message_text = f"No deleted messages found in {channel_name}"
                    
                message_parts = [
                    "```ansi\n" +
                    "\u001b[1;35mNo Messages Found\n" +
                    f"\u001b[0;37m{'─' * 17}\n" +
                    f"\u001b[0;37m{no_message_text}```"
                ]
                await ctx.send(quote_block(''.join(message_parts)),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return

            # Format messages
            sent_messages = []
            for chunk_start in range(0, len(messages), 10):  # Process in chunks of 10
                chunk = messages[chunk_start:chunk_start + 10]
                message_parts = [
                    "```ansi\n" + \
                    "\u001b[30m\u001b[1m\u001b[4mMessages\u001b[0m\n"
                ]
                
                attachments_to_send = []  # Store attachments for sending after codeblock
                
                # Pre-fetch all user IDs in this chunk
                user_ids = [msg["user_id"] for msg in chunk]
                users_dict = {}
                
                # First try to get users from cache
                for user_id in user_ids:
                    user = self.bot.get_user(user_id)
                    if user:
                        users_dict[user_id] = user
                
                # For any users not found in cache, try to get them in a single batch
                missing_user_ids = [user_id for user_id in user_ids if user_id not in users_dict]
                if missing_user_ids:
                    for user_id in missing_user_ids:
                        try:
                            user = await self.bot.GetUser(user_id)
                            if user:
                                users_dict[user_id] = user
                        except Exception as e:
                            logger.error(f"Error fetching user {user_id}: {e}")
                
                # Add each message
                for idx, msg in enumerate(chunk, chunk_start + 1):
                    user = users_dict.get(msg["user_id"])
                    username = user.name if user else f"Unknown User ({msg['user_id']})"
                    timestamp = msg["deleted_at"].strftime("%I:%M %p")

                    message_parts[-1] += f"\u001b[1;33m#{idx}\n"
                    
                    # Add reply information with the line format
                    if "reply_to_user_id" in msg and "reply_to_username" in msg:
                        reply_content = msg.get("reply_to_content", "")
                        # Truncate reply content if too long
                        if len(reply_content) > 190:
                            reply_content = reply_content[:187] + "..."
                        message_parts[-1] += f"┌─── \u001b[0m{msg['reply_to_username']} \u001b[30m{reply_content}\n"
                        
                        # Show if reply had attachments
                        if "reply_to_attachments" in msg and msg["reply_to_attachments"]:
                            if len(msg["reply_to_attachments"]) == 1:
                                message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                            else:
                                message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['reply_to_attachments'])} Attachments ]\n"
                            # Add reply attachments to the list to be displayed
                            attachments_to_send.extend(msg["reply_to_attachments"])
                    # Handle replies to message snapshots (forwarded messages)
                    elif "reply_to_snapshot" in msg and msg.get("reply_to_content"):
                        reply_content = msg.get("reply_to_content", "")
                        # Truncate reply content if too long
                        if len(reply_content) > 190:
                            reply_content = reply_content[:187] + "..."
                            # clean_content(reply_content)
                            reply_content = self.clean_content(reply_content)
                        message_parts[-1] += f"┌─── \u001b[0;33m[Forwarded Message] \u001b[30m{reply_content}\n"
                        
                        # Show if reply had attachments
                        if "reply_to_attachments" in msg and msg["reply_to_attachments"]:
                            if len(msg["reply_to_attachments"]) == 1:
                                message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                            else:
                                message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['reply_to_attachments'])} Attachments ]\n"
                            # Add reply attachments to the list to be displayed
                            attachments_to_send.extend(msg["reply_to_attachments"])
                    # New formatting: username Today at time
                    message_parts[-1] += f"\u001b[1;37m{username} \u001b[0mToday at {timestamp}\n"                    
                    if msg.get("content"):
                        content = self.clean_content(msg["content"])
                        # Truncate content if it's too long
                        content = self.truncate_content(content, 256)
                        # Content directly below the username line with color
                        for line in content.split('\n'):
                            message_parts[-1] += f"\u001b[1;31m{line}\n"

                    # Display forwarded messages (message snapshots) if any
                    if "message_snapshots" in msg and msg["message_snapshots"]:
                        for i, snapshot in enumerate(msg["message_snapshots"]):
                            # Show simplified forwarded message header
                            message_parts[-1] += f"┌─── \u001b[0;33m[Forwarded Message]\n"
                            
                            # Show snapshot content if any
                            if snapshot.get("snapshot_content"):
                                snapshot_content = self.clean_content(snapshot["snapshot_content"])
                                if len(snapshot_content) > 100:  # Truncate if too long
                                    snapshot_content = snapshot_content[:97] + "..."
                                message_parts[-1] += f"│    \u001b[0;37m{snapshot_content}\n"
                            

                            # Show snapshot attachments if any
                            if snapshot.get("snapshot_attachments"):
                                if len(snapshot["snapshot_attachments"]) == 1:
                                    message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                                else:
                                    message_parts[-1] += f"└─── \u001b[0;36m[ {len(snapshot['snapshot_attachments'])} Attachments ]\n"
                                # Add snapshot attachments to the list to be displayed
                                attachments_to_send.extend(snapshot["snapshot_attachments"])
                            else:
                                message_parts[-1] += f"└───\n"

                    if msg.get("attachments"):
                        # Update to show number of attachments in simpler format
                        if len(msg["attachments"]) == 1:
                            message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                        else:
                            message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['attachments'])} Attachments ]\n"
                        attachments_to_send.extend(msg["attachments"])                    # Add server/channel info in a more compact format
                    if "guild_name" in msg:
                        location_info = f"#{msg['channel_name']} in {msg['guild_name']}"
                    elif msg.get("channel_type") == "group" or msg.get("is_group"):
                        # Enhanced group chat display - if no name, try to show participants
                        if not msg['channel_name'] or msg['channel_name'] == "None":
                            # Try to get the channel object to access recipients
                            channel = self.bot.get_channel(msg["channel_id"])
                            if channel and hasattr(channel, "recipients") and len(channel.recipients) > 0:
                                # Format up to 3 recipient usernames
                                recipient_names = [r.name for r in channel.recipients[:3]]
                                if len(channel.recipients) > 3:
                                    recipient_names.append(f"+{len(channel.recipients) - 3} more")
                                participants = ", ".join(recipient_names)
                                location_info = f"Group with: {participants}"
                            else:
                                location_info = f"Group chat"
                        else:
                            location_info = f"Group: {msg['channel_name']}"
                    else:
                        # For DMs, display the username of the message author
                        user = users_dict.get(msg["user_id"])
                        dm_username = user.name if user else f"Unknown User ({msg['user_id']})"
                        location_info = f"DM with {dm_username}"
                            

                    message_parts[-1] += f"\u001b[0;36m{location_info}\n"
                    message_parts[-1] += "\u001b[0;37m" + "─" * 28 + "\n"

                message_parts[-1] += "```"

                # Send the formatted message first
                msg = await ctx.send(quote_block(''.join(message_parts)))
                sent_messages.append(msg)
                
                # Then send attachments if any
                if attachments_to_send:
                    attachment_msg = await ctx.send("\n".join(attachments_to_send))
                    sent_messages.append(attachment_msg)

            # Auto-delete sent messages if configured
            if self.bot.config_manager.auto_delete.enabled:
                await asyncio.sleep(self.bot.config_manager.auto_delete.delay)
                for message in sent_messages:
                    try:
                        await message.delete()
                    except (discord.NotFound, discord.Forbidden):
                        pass

        except Exception as e:
            logger.error(f"Error fetching deleted messages: {e}")

    @commands.command(aliases=['es'])
    async def editsnipe(self, ctx, target: Optional[Union[int, discord.Member, discord.User, discord.TextChannel]] = None,
                       amount: Optional[int] = None,
                       channel: Optional[discord.TextChannel] = None):
        """
        Snipe edited messages
        
        .editsnipe - Show most recent edited message in current channel
        .editsnipe @user/ID - Show most recent edited message from user
        .editsnipe <amount> - Show X most recent edited messages in channel
        .editsnipe @user/ID <amount> - Show X most recent edited messages from user
        .editsnipe #channel - Show most recent edited message in specified channel
        .editsnipe <amount> #channel - Show X most recent edited messages in specified channel
        .editsnipe @user/ID #channel - Show most recent edited message from user in specified channel
        .editsnipe @user/ID <amount> #channel - Show X most recent edited messages from user in specified channel
        .editsnipe <channel_id> - Show most recent edited message in specified channel by ID
        """

        def quote_block(text):
        # Add > prefix to each line while preserving the content
            return '\n'.join(f'> {line}' for line in text.split('\n'))

        try:await ctx.message.delete()
        except:pass
        
        # Initialize query
        query = {
            "user_id": {"$ne": self.bot.user.id}  # Filter out selfbot's messages
        }
        limit = 1
        user = None
        target_channel = None

        # Check if the channel was specified in the target parameter
        if isinstance(target, discord.TextChannel):
            target_channel = target
            target = None
        
        # Check if channel was specified as third parameter
        if channel:
            target_channel = channel

        # Handle first argument - could be amount, user, or channel_id
        if isinstance(target, (discord.Member, discord.User)):
            user = target
        elif isinstance(target, int):
            # First check if it's a channel ID
            found_channel = self.bot.get_channel(target)
            if found_channel:
                target_channel = found_channel
            else:
                # Next check if it's a user ID
                found_user = await self.bot.GetUser(target)
                if found_user:
                    user = found_user
                else:
                    # If not a channel ID or user ID, treat as amount
                    limit = target

        # If we identified a user, set up user query
        if user:
            query["user_id"] = user.id
            # If amount specified after user, update limit
            if amount:
                limit = amount
            # Only add channel_id if a specific channel was requested
            if target_channel:
                query["channel_id"] = target_channel.id
        else:
            # No user specified, so filter by channel
            if target_channel:
                query["channel_id"] = target_channel.id
            else:
                # Default to current channel if no user and no specific channel
                query["channel_id"] = ctx.channel.id
                
            if amount:  
                # If amount specified but no user, show channel messages
                limit = amount

        limit = min(max(1, limit), 1000)  # Between 1 and 1000 messages

        try:
            cursor = self.bot.db.db.edited_messages.find(query)
            cursor.sort("edited_at", -1)
            messages = await cursor.to_list(length=limit)

            if not messages:
                if user:
                    no_message_text = f"No edited messages found from {user.name}"
                    if target_channel:
                        no_message_text += f" in #{target_channel.name}"
                else:
                    channel_name = f"#{target_channel.name}" if target_channel else "this channel"
                    no_message_text = f"No edited messages found in {channel_name}"
                    
                message_parts = [
                    "```ansi\n" +
                    "\u001b[1;35mNo Messages Found\n" +
                    f"\u001b[0;37m{'─' * 17}\n" +
                    f"\u001b[0;37m{no_message_text}```"
                ]
                await ctx.send(quote_block(''.join(message_parts)),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return

            sent_messages = []
            for chunk_start in range(0, len(messages), 10):  # Process in chunks of 10 (edits take more space)
                chunk = messages[chunk_start:chunk_start + 10]
                message_parts = [
                    "```ansi\n" + \
                    "\u001b[30m\u001b[1m\u001b[4mMessages\u001b[0m\n"
                ]
                
                attachments_to_send = []  # Store attachments for sending after codeblock

                # Pre-fetch all user IDs in this chunk
                user_ids = [msg["user_id"] for msg in chunk]
                users_dict = {}
                
                # First try to get users from cache
                for user_id in user_ids:
                    user = self.bot.get_user(user_id)
                    if user:
                        users_dict[user_id] = user
                
                # For any users not found in cache, try to get them in a single batch
                missing_user_ids = [user_id for user_id in user_ids if user_id not in users_dict]
                if missing_user_ids:
                    for user_id in missing_user_ids:
                        try:
                            user = await self.bot.GetUser(user_id)
                            if user:
                                users_dict[user_id] = user
                        except Exception as e:
                            logger.error(f"Error fetching user {user_id}: {e}")

                for idx, msg in enumerate(chunk, chunk_start + 1):
                    user = users_dict.get(msg["user_id"])
                    username = user.name if user else f"Unknown User ({msg['user_id']})"
                    timestamp = msg["edited_at"].strftime("%I:%M %p")                    
                    before_content = self.clean_content(msg["before_content"])
                    after_content = self.clean_content(msg["after_content"])
                    
                    # Truncate content if it's too long
                    before_content = self.truncate_content(before_content, 256)
                    after_content = self.truncate_content(after_content, 256)

                    message_parts[-1] += f"\u001b[1;33m#{idx}\n"
                    
                    # Add reply information with the line format
                    if "reply_to_user_id" in msg and "reply_to_username" in msg:
                        reply_content = msg.get("reply_to_content", "")
                        # Truncate reply content if too long
                        if len(reply_content) > 190:
                            reply_content = reply_content[:187] + "..."
                            # clean_content(reply_content)
                            reply_content = self.clean_content(reply_content)
                        message_parts[-1] += f"┌─── \u001b[0m{msg['reply_to_username']} \u001b[30m{reply_content}\n"
                        
                        # Show if reply had attachments
                        if "reply_to_attachments" in msg and msg["reply_to_attachments"]:
                            if len(msg["reply_to_attachments"]) == 1:
                                message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                            else:
                                message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['reply_to_attachments'])} Attachments ]\n"
                            # Add reply attachments to the list to be displayed
                            attachments_to_send.extend(msg["reply_to_attachments"])
                    
                    # Add username and timestamp like in the snipe command
                    message_parts[-1] += f"\u001b[1;37m{username} \u001b[0mToday at {timestamp}\n"
                    
                    # New compact format: before_content -> after_content
                    message_parts[-1] += f"\u001b[1;31m{before_content} -> {after_content}\n"

                    # Handle attachments
                    if msg.get("before_attachments") and msg.get("after_attachments"):
                        if len(msg["before_attachments"]) != len(msg["after_attachments"]) or any(b != a for b, a in zip(msg["before_attachments"], msg["after_attachments"])):
                            # Attachments changed - show new attachments
                            if len(msg["after_attachments"]) == 1:
                                message_parts[-1] += f"└─── \u001b[0;36m[ 1 New Attachment ]\n"
                            else:
                                message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['after_attachments'])} New Attachments ]\n"
                            attachments_to_send.extend(msg["after_attachments"])
                        else:
                            # Same attachments
                            if len(msg["after_attachments"]) == 1:
                                message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                            else:
                                message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['after_attachments'])} Attachments ]\n"
                            attachments_to_send.extend(msg["after_attachments"])
                    elif msg.get("before_attachments"):
                        # Attachments were removed
                        if len(msg["before_attachments"]) == 1:
                            message_parts[-1] += f"└─── \u001b[0;36m[ 1 Removed Attachment ]\n"
                        else:
                            message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['before_attachments'])} Removed Attachments ]\n"
                        attachments_to_send.extend(msg["before_attachments"])
                    elif msg.get("after_attachments"):
                        # Attachments were added
                        if len(msg["after_attachments"]) == 1:
                            message_parts[-1] += f"└─── \u001b[0;36m[ 1 New Attachment ]\n"
                        else:
                            message_parts[-1] += f"└─── \u001b[0;36m[ {len(msg['after_attachments'])} New Attachments ]\n"
                        attachments_to_send.extend(msg["after_attachments"])                    # Add server/channel info in a more compact format
                    if "guild_name" in msg:
                        location_info = f"#{msg['channel_name']} in {msg['guild_name']}"
                    elif msg.get("channel_type") == "group" or msg.get("is_group"):
                        # Enhanced group chat display - if no name, try to show participants
                        if not msg['channel_name'] or msg['channel_name'] == "None":
                            # Try to get the channel object to access recipients
                            channel = self.bot.get_channel(msg["channel_id"])
                            if channel and hasattr(channel, "recipients") and len(channel.recipients) > 0:
                                # Format up to 3 recipient usernames
                                recipient_names = [r.name for r in channel.recipients[:3]]
                                if len(channel.recipients) > 3:
                                    recipient_names.append(f"+{len(channel.recipients) - 3} more")
                                participants = ", ".join(recipient_names)
                                location_info = f"Group with: {participants}"
                            else:
                                location_info = f"Group chat"
                        else:
                            location_info = f"Group: {msg['channel_name']}"
                    else:
                        # For DMs, display the username of the message author
                        user = users_dict.get(msg["user_id"])
                        dm_username = user.name if user else f"Unknown User ({msg['user_id']})"
                        location_info = f"DM with {dm_username}"
                        
                    message_parts[-1] += f"\u001b[0;36m{location_info}\n"
                    message_parts[-1] += "\u001b[0;37m" + "─" * 28 + "\n"

                message_parts[-1] += "```"

                # Send the formatted message
                msg = await ctx.send(quote_block(''.join(message_parts)))
                sent_messages.append(msg)
                
                # Then send attachments if any
                if attachments_to_send:
                    attachment_msg = await ctx.send("\n".join(attachments_to_send))
                    sent_messages.append(attachment_msg)

            # Auto-delete sent messages if configured
            if self.bot.config_manager.auto_delete.enabled:
                await asyncio.sleep(self.bot.config_manager.auto_delete.delay)
                for message in sent_messages:
                    try:
                        await message.delete()
                    except (discord.NotFound, discord.Forbidden):
                        pass

        except Exception as e:
            logger.error(f"Error fetching edited messages: {e}")

    @commands.command(aliases=['lp'])
    async def lastping(self, ctx, amount: Optional[int] = None):
        """Show your recent mentions
        
        .lastping - Show your 5 most recent mentions
        .lastping 10 - Show your last 10 mentions (max 50)
        """
        try:await ctx.message.delete()
        except:pass
        
        limit = min(max(1, amount or 5), 1000)  # Default 5, max 1000
        
        try:
            cursor = self.bot.db.db.mentions.find({"target_id": self.bot.user.id})
            cursor.sort("created_at", -1) 
            mentions = await cursor.to_list(length=limit)
    
            if not mentions:
                message_parts = [
                    "```ansi\n" +
                    "\u001b[1;35mNo Mentions Found\n" +
                    f"\u001b[0;37m{'─' * 17}\n" +
                    "\u001b[0;37mNo mentions found for you```"
                ]
                await ctx.send(self.quote_block(''.join(message_parts)),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
    
            # Process mentions in chunks of 10 for better display
            sent_messages = []
            for chunk_start in range(0, len(mentions), 10):
                chunk = mentions[chunk_start:chunk_start + 10]
                message_parts = [
                    "```ansi\n" + \
                    f"\u001b[30m\u001b[1m\u001b[4mLast Mentions\u001b[0m\n"
                ]
                
                attachments_to_send = []  # Store attachments for sending after codeblock
                
                now = datetime.utcnow()
                max_length = get_max_message_length(self.bot) - 100  # Leave room for formatting
                
                for idx, mention in enumerate(chunk, chunk_start + 1):                    
                    timestamp = mention["created_at"]
                    is_yesterday = (now - timestamp).days == 1
                    time_str = timestamp.strftime("%I:%M %p")
                    if is_yesterday:
                        time_str = f"Yesterday at {time_str}"
                    else:
                        time_str = f"Today at {time_str}"# Build location string
                    if mention.get("guild_name"):
                        location = f"#{mention['channel_name']} in {mention['guild_name']}"
                    elif mention.get("channel_type") == 3 or mention.get("is_group"):  # Group DM
                        if mention.get("channel_name") and mention.get("channel_name") != "None":
                            location = f"Group: {mention['channel_name']}"
                        else:
                            # Try to get the channel object to access recipients
                            channel = self.bot.get_channel(mention["channel_id"])
                            if channel and hasattr(channel, "recipients") and len(channel.recipients) > 0:
                                # Format up to 3 recipient usernames
                                recipient_names = [r.name for r in channel.recipients[:3]]
                                if len(channel.recipients) > 3:
                                    recipient_names.append(f"+{len(channel.recipients) - 3} more")
                                participants = ", ".join(recipient_names)
                                location = f"Group with: {participants}"
                            else:
                                location = f"Group chat"
                    else:
                        location = "DM"
                        
                    # Add entry number and basic info
                    message_parts[-1] += f"\u001b[1;33m#{idx}\n"
                    
                    # Add reply information if present - similar to snipe command
                    if "reply_to_user_id" in mention and "reply_to_username" in mention:
                        reply_content = mention.get("reply_to_content", "")
                        # Truncate reply content if too long
                        if len(reply_content) > 190:
                            reply_content = reply_content[:187] + "..."
                            # clean_content(reply_content)
                            reply_content = self.clean_content(reply_content)
                        message_parts[-1] += f"┌─── \u001b[0m{mention['reply_to_username']} \u001b[30m{reply_content}\n"
                        
                        # Show if reply had attachments
                        if "reply_to_attachments" in mention and mention["reply_to_attachments"]:
                            if len(mention["reply_to_attachments"]) == 1:
                                message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                            else:
                                message_parts[-1] += f"└─── \u001b[0;36m[ {len(mention['reply_to_attachments'])} Attachments ]\n"
                            # Add reply attachments to the list to be displayed
                            attachments_to_send.extend(mention["reply_to_attachments"])
                    # Handle replies to message snapshots (forwarded messages)
                    elif "reply_to_snapshot" in mention and mention.get("reply_to_content"):
                        reply_content = mention.get("reply_to_content", "")
                        # Truncate reply content if too long
                        if len(reply_content) > 190:
                            reply_content = reply_content[:187] + "..."
                        message_parts[-1] += f"┌─── \u001b[0;33m[Forwarded Message] \u001b[30m{reply_content}\n"
                        
                        # Show if reply had attachments
                        if "reply_to_attachments" in mention and mention["reply_to_attachments"]:
                            if len(mention["reply_to_attachments"]) == 1:
                                message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                            else:
                                message_parts[-1] += f"└─── \u001b[0;36m[ {len(mention['reply_to_attachments'])} Attachments ]\n"
                            # Add reply attachments to the list to be displayed
                            attachments_to_send.extend(mention["reply_to_attachments"])
                                                  

                    # Add timestamp and author formatted like snipe command
                    message_parts[-1] += f"\u001b[1;37m{mention['author_name']} \u001b[0m{time_str}\n"
                    
                    # Add the message content
                    content = self.clean_content(mention['content'])
                    if len(content) > 512:  # Reasonable limit for display
                        content = content[:512] + "..."
                    
                    # Content directly below with color
                    for line in content.split('\n'):
                        message_parts[-1] += f"\u001b[1;31m{line}\n"
                    
                    # Show attachments if any
                    if mention.get("attachments"):
                        if len(mention["attachments"]) == 1:
                            message_parts[-1] += f"└─── \u001b[0;36m[ 1 Attachment ]\n"
                        else:
                            message_parts[-1] += f"└─── \u001b[0;36m[ {len(mention['attachments'])} Attachments ]\n"
                        attachments_to_send.extend(mention["attachments"])
                    
                    # Add location info
                    message_parts[-1] += f"\u001b[0;36m{location}\n"
                    message_parts[-1] += "\u001b[0;37m" + "─" * 28 + "\n"
                    
                    # Check if we're approaching the max length
                    if len(''.join(message_parts)) + 10 > max_length:
                        message_parts[-1] += "... (more mentions truncated) ..."
                        break
                
                message_parts[-1] += "```"
                
                # Send the formatted message
                msg = await ctx.send(self.quote_block(''.join(message_parts)),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                sent_messages.append(msg)
                
                # Then send attachments if any
                if attachments_to_send:
                    attachment_msg = await ctx.send("\n".join(attachments_to_send),
                        delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                    sent_messages.append(attachment_msg)
    
        except Exception as e:
            logger.error(f"Error in lastping: {e}")
            await ctx.send("An error occurred while fetching mentions",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)    
                          

    @commands.command(aliases=['gfr'])
    async def ghostreqs(self, ctx, amount: Optional[int] = None):
        """sent & cancelled frs
        
        .ghostreqs - Show your 5 most recent ghost friend requests
        .ghostreqs 10 - Show your last 10 ghost friend requests (max 50)
        """
        try:
            await ctx.message.delete()
        except:
            pass
        
        limit = min(max(1, amount or 5), 50)  # Default 5, max 50
        
        try:            # Find only ghosted friend requests for the current instance
            query = {"status": "ghosted", "instance_id": self.bot.user.id}
            cursor = self.bot.db.db.friend_requests.find(query)
            cursor.sort("sent_at", -1)
            ghosted = await cursor.to_list(length=limit)
            
            if not ghosted:
                message = (
                    "```ansi\n"
                    "\u001b[33mGhost Friend Requests \u001b[30m| \u001b[33mNo Requests Found\n\n"
                    f"\u001b[0;37mNo cancelled friend requests have been tracked for {self.bot.user.name}\n"
                    "```"
                )
                await ctx.send(self.quote_block(message),
                    delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
                return
            
            # Get total count for header - only for this instance
            total_ghosted = await self.bot.db.db.friend_requests.count_documents(query)
            
            # Header with serverinfo-style formatting
            message = (
                "```ansi\n"
                f"\u001b[33mGhost Friend Requests \u001b[30m| \u001b[33mTotal: {total_ghosted}\n\n"
            )
            
            # Display ghosted requests using a combination of serverinfo and snipe styles
            for idx, req in enumerate(ghosted, 1):
                username = req.get("username", "Unknown User")
                user_id = req.get("user_id", "Unknown ID")
                sent_time = req.get("sent_at").strftime("%b %d, %Y %I:%M %p") if req.get("sent_at") else "Unknown"
                removed_time = req.get("removed_at").strftime("%b %d, %Y %I:%M %p") if req.get("removed_at") else "Unknown"
                duration = self._format_duration(req.get('sent_at'), req.get('removed_at'))
                
                # User header with number and formatting like in snipe
                message += f"\u001b[1;33m#{idx} \u001b[1;37m{username} \u001b[30m|\u001b[0;37m {user_id}\n"
                
                # Detail lines with serverinfo-style brackets and tree characters from snipe
                message += f"\u001b[30m├─ \u001b[0;37mSent \u001b[30m[\u001b[0;34m{sent_time}\u001b[30m]\n"
                message += f"\u001b[30m├─ \u001b[0;37mCancelled \u001b[30m[\u001b[0;34m{removed_time}\u001b[30m]\n" 
                message += f"\u001b[30m└─ \u001b[0;37mDuration \u001b[30m[\u001b[0;34m{duration}\u001b[30m]\n"
                
                # Add separator between entries like in lastping, except for the last one
                if idx < len(ghosted):
                    message += f"\u001b[30m{'─' * 40}\n"
            
            message += "```"
            
            # Send the message
            await ctx.send(self.quote_block(message),
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
        
        except Exception as e:
            logger.error(f"Error in ghostfriendrequests: {e}")
            await ctx.send("An error occurred while fetching ghost friend requests",
                delete_after=self.bot.config_manager.auto_delete.delay if self.bot.config_manager.auto_delete.enabled else None)
    
    def _format_duration(self, start_time, end_time):
        """Format the duration between two times"""
        if not start_time or not end_time:
            return "Unknown"
            
        # Calculate duration
        duration = end_time - start_time
        days = duration.days
        hours, remainder = divmod(duration.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Format nicely
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
        

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        # Unregister from event manager
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            event_manager.unregister_cog(self.__class__.__name__)
        self.flush_buffer_task.cancel()
        # Perform final flushes if the DB is active
        if self.bot.db.is_active:
            logger.info("Performing final flush of all buffers...")
            await self._periodic_flush()
            
            # Note: No batch processors to flush for snipe operations
            # They use immediate storage for real-time functionality
            
            logger.info("Final flush complete.")
        
    



async def setup(bot):
    await bot.add_cog(Snipe(bot))
