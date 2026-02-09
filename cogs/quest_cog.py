import discord
from discord.ext import commands
import asyncio
import logging
import json
import random
import math
import time
import aiohttp
from datetime import datetime, timedelta
from utils.general import format_message, quote_block
import traceback

logger = logging.getLogger(__name__)

class Quest:
    def __init__(self, id, title, description, task_type, status=None):
        self.id = id
        self.title = title
        self.description = description
        self.task_type = task_type  # "WatchVideo", "PlayOnDesktop", or other types
        self.status = status  # None, "enrolled", "completed", "expired"
        self.enrolled_at = None
        self.completed_at = None
        self.progress = 0.0
        self.expires_at = None
        self.starts_at = None
        self.is_expired = False
        self.last_updated = time.time()

    def is_supported(self):
        """Check if this quest type is supported for automation."""
        # Use case-insensitive comparison to allow different capitalizations
        task_type_lower = self.task_type.lower() if self.task_type else ""
        return (("watch" in task_type_lower and "video" in task_type_lower) or 
                ("play" in task_type_lower and "desktop" in task_type_lower)) and not self.is_expired
    
    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'task_type': self.task_type,
            'status': self.status,
            'enrolled_at': self.enrolled_at,
            'completed_at': self.completed_at,
            'progress': self.progress,
            'expires_at': self.expires_at,
            'starts_at': self.starts_at,
            'is_expired': self.is_expired,
            'last_updated': self.last_updated
        }
        
    def check_expiration(self):
        """Check if the quest has expired based on expires_at date."""
        if not self.expires_at:
            return False
            
        try:
            # Parse expiration date and compare with current time
            expiration_time = datetime.fromisoformat(self.expires_at.replace('Z', '+00:00'))
            current_time = datetime.now(expiration_time.tzinfo)
            self.is_expired = current_time > expiration_time
            return self.is_expired
        except Exception as e:
            logger.error(f"Error checking expiration for quest {self.id}: {e}")
            return False
            
    @classmethod
    def from_dict(cls, data):
        quest = cls(
            id=data['id'],
            title=data['title'],
            description=data['description'],
            task_type=data['task_type'],
            status=data['status']
        )
        quest.enrolled_at = data.get('enrolled_at')
        quest.completed_at = data.get('completed_at')
        quest.progress = data.get('progress', 0.0)
        quest.expires_at = data.get('expires_at')
        quest.starts_at = data.get('starts_at')
        quest.is_expired = data.get('is_expired', False)
        quest.last_updated = data.get('last_updated', time.time())
        return quest


class QuestManager(commands.Cog):    
    def __init__(self, bot):
        self.bot = bot
        self.quests = {}  # Store quests data
        self.auto_complete = False
        self.last_fetch_time = 0
        self.quest_completion_task = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord/1.0.9191 Chrome/134.0.6998.179 Electron/35.1.5 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json"
        }
        self.cache = {}  # For storing quest completion progress
        self.refresh_interval = 30 * 60  # Refresh quests every 30 minutes (same as Go implementation)
        self.excluded_quests = set()  # Quest IDs to exclude from processing
        
    def get_padding(self, labels: list) -> int:
        """Calculate padding for consistent alignment based on longest label"""
        return max(len(label) for label in labels) + 2 if labels else 2

    async def cog_load(self):
        """Initialize the cog when it's loaded."""
        logger.info("Quest cog is being loaded")
    
    async def cog_unload(self):
        """Clean up resources when the cog is unloaded."""
        logger.info("Quest cog is being unloaded")
        if self.quest_completion_task:
            self.quest_completion_task.cancel()
            try:
                await self.quest_completion_task
            except asyncio.CancelledError:
                pass
            logger.info("Quest completion task canceled")

    def random_decimal(self):
        """Generate a random decimal for quest progress simulation."""
        return round(random.random() * 1000000) / 1000000
    async def get_quests(self):
        """Fetch available quests from Discord API."""
        try:
            # Use INFO level logging to ensure it shows in console
            logger.info("Fetching quests from Discord API...")
            
            # Get proper authentication headers
            headers = self.get_auth_headers()
                
            # Use aiohttp directly for better control of the request
            async with aiohttp.ClientSession() as session:
                url = "https://discord.com/api/v9/quests/@me"
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch quests: HTTP {response.status}: {await response.text()}")
                        return False
                    
                    data = await response.json()
            
            logger.info(f"Received quest API response")
            
            quest_list = []
            
            # Based on provided API response format, we expect a dict with "quests" key
            if isinstance(data, dict) and 'quests' in data:
                quest_list = data['quests']
                # Handle excluded quests if available
                if 'excluded_quests' in data:
                    for excluded_quest in data['excluded_quests']:
                        if 'id' in excluded_quest:
                            self.excluded_quests.add(excluded_quest['id'])
            elif isinstance(data, list):
                quest_list = data
            
            # Ensure quest_list is a list
            if not isinstance(quest_list, list):
                logger.error(f"Could not convert response to a quest list. Type: {type(quest_list)}")
                return False
            
            logger.info(f"Processing {len(quest_list)} quests...")
            
            # Process quests data
            for quest_data in quest_list:
                if not isinstance(quest_data, dict):
                    continue
                
                quest_id = quest_data.get('id')
                if not quest_id:
                    continue
                
                # Extract quest details from the config structure
                config = quest_data.get('config', {})
                
                # Get expiration and start dates
                starts_at = config.get('starts_at')
                expires_at = config.get('expires_at')
                
                # Get title from application.name (most reliable source based on API data)
                title = None
                if isinstance(config.get('application'), dict) and config['application'].get('name'):
                    title = config['application']['name']
                elif config.get('messages', {}).get('game_title'):
                    title = config['messages']['game_title']
                else:
                    title = 'Unknown Quest'
                
                # Get description
                description = config.get('messages', {}).get('quest_name', 'No description')
                
                # Determine task type using our helper method
                task_type = self.determine_quest_task_type(config)
                logger.debug(f"Detected quest: {title} (ID: {quest_id}, Type: {task_type})")
                  
                # Create or update quest object
                if quest_id in self.quests:
                    # Update existing quest
                    self.quests[quest_id].task_type = task_type
                    self.quests[quest_id].title = title
                    self.quests[quest_id].description = description
                    self.quests[quest_id].starts_at = starts_at
                    self.quests[quest_id].expires_at = expires_at
                    self.quests[quest_id].last_updated = time.time()
                else:
                    # Create new quest
                    self.quests[quest_id] = Quest(
                        id=quest_id,
                        title=title,
                        description=description,
                        task_type=task_type
                    )
                    self.quests[quest_id].starts_at = starts_at
                    self.quests[quest_id].expires_at = expires_at
                
                # Check if quest has expired
                if expires_at:
                    self.quests[quest_id].check_expiration()
                
                # Process user_status data
                user_status = quest_data.get('user_status')
                
                if user_status:
                    # Check for completion
                    if user_status.get('completed_at'):
                        self.quests[quest_id].status = "completed"
                        self.quests[quest_id].completed_at = user_status.get('completed_at')
                        # Clear from cache if completed
                        if quest_id in self.cache:
                            del self.cache[quest_id]
                    # Check for enrollment
                    elif user_status.get('enrolled_at'):
                        self.quests[quest_id].status = "enrolled"
                        self.quests[quest_id].enrolled_at = user_status.get('enrolled_at')
                    
                    # Update progress if available
                    progress_data = user_status.get('progress', {})
                    
                    if progress_data:
                        # Try to extract progress value based on task type
                        if task_type == "WatchVideo" and "watch_video" in progress_data:
                            watch_video = progress_data.get("watch_video", {})
                            if isinstance(watch_video, dict) and "value" in watch_video:
                                self.quests[quest_id].progress = watch_video["value"]
                                # Also update cache to ensure we continue from this point
                                self.cache[quest_id] = watch_video["value"]
                        elif task_type == "PlayOnDesktop" and "play_on_desktop" in progress_data:
                            play_desktop = progress_data.get("play_on_desktop", {})
                            if isinstance(play_desktop, dict) and "value" in play_desktop:
                                self.quests[quest_id].progress = play_desktop["value"]
            
            logger.info(f"Successfully processed {len(self.quests)} quests")
            self.last_fetch_time = time.time()
            return True
            
        except Exception as e:
            logger.error(f"Error fetching quests: {e}")
            logger.error(f"Exception details: {traceback.format_exc()}")
            return False      
    async def check_enrollment_status(self, quest_id):
        """Check if a quest is enrolled (but don't attempt enrollment as it requires CAPTCHA)."""
        if quest_id not in self.quests:
            logger.warning(f"Cannot check enrollment for unknown quest {quest_id}")
            return False
        
        quest = self.quests[quest_id]
          # Skip if quest has expired
        if quest.is_expired:
            logger.info(f"Skipping expired quest: {quest.title}")
            return False
            
        # Skip if quest is completed (to prevent processing completed quests)
        if quest.status == "completed":
            logger.debug(f"Skipping completed quest: {quest.title}")
            return False
            
        # Just check if the quest is already marked as enrolled
        if quest.status == "enrolled":
            return True
              
        # Quest needs manual enrollment (logging is now done in quest_completer)
        return False
        
    async def update_quest_progress(self, quest_id):
        """Update the progress for a specific quest."""
        if quest_id not in self.quests:
            return False
        
        quest = self.quests[quest_id]
        # Skip if quest is not supported, not enrolled, or already completed
        if not quest.is_supported() or quest.status != "enrolled" or quest.status == "completed":
            logger.debug(f"Skipping quest progress update for {quest.title}: not supported or not enrolled or completed")
            return False
        
        try:
            data = {}
            url = ""
            
            # Get proper authentication headers
            headers = self.get_auth_headers()
            
            # Normalize task type for case-insensitive comparison
            task_type_lower = quest.task_type.lower()
            
            if "watch" in task_type_lower and "video" in task_type_lower:
                # Match Golang implementation exactly:
                # Default progress of -29 if nothing in cache
                default_progress = -29
                
                # If we have progress in cache, use that
                if quest_id in self.cache:
                    default_progress = self.cache[quest_id]
                # If there's progress in the quest object, use that instead
                elif quest.progress:
                    default_progress = quest.progress
                  # Calculate new progress (match Golang implementation)
                new_progress = default_progress + 30 + self.random_decimal()
                # Store in cache before sending request
                self.cache[quest_id] = new_progress
                data["timestamp"] = new_progress
                
                # Calculate percentage for logging
                progress_pct = 0
                if new_progress < 0:
                    progress_pct = ((new_progress + 29) / 30) * 100
                else:
                    progress_pct = min(100, (new_progress / 30) * 100)
                
                url = f"https://discord.com/api/v9/quests/{quest_id}/video-progress"
                logger.debug(f"Sending WatchVideo progress update: {new_progress} ({progress_pct:.0f}%)")
                
            elif "play" in task_type_lower and "desktop" in task_type_lower:
                # Match Golang implementation: only include stream_key and terminal=false
                data["stream_key"] = f"call:{quest_id}:1"
                data["terminal"] = False
                
                url = f"https://discord.com/api/v9/quests/{quest_id}/heartbeat"
                logger.debug(f"Sending PlayOnDesktop heartbeat")
            
            if url and data:
                # Use aiohttp directly for better control over the request
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=data, headers=headers) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"Request failed with status {response.status}: {error_text}")
                            
                            if response.status == 401:
                                logger.error(f"Authorization error for quest {quest_id} ({quest.title})")
                                self.excluded_quests.add(quest_id)
                            elif response.status == 404:
                                logger.warning(f"Quest {quest_id} ({quest.title}) not found, excluding from further attempts")
                                self.excluded_quests.add(quest_id)
                            return False
                        
                        response_data = await response.json()
                        
                        # Update quest status based on response
                        if response_data.get('completed_at'):
                            quest.status = "completed"
                            quest.completed_at = response_data.get('completed_at')
                            # Remove from cache when completed
                            if quest_id in self.cache:
                                del self.cache[quest_id]
                            logger.info(f"Quest {quest.title} (ID: {quest_id}) marked as completed")
                          # Update progress if available
                        if response_data.get('progress'):
                            progress_data = response_data['progress']
                            if "watch" in task_type_lower and "video" in task_type_lower:
                                watch_video = progress_data.get('watch_video', {})
                                if isinstance(watch_video, dict) and "value" in watch_video:
                                    # Calculate percentage for logging
                                    progress_value = watch_video["value"]
                                    progress_pct = 0
                                    if progress_value < 0:
                                        progress_pct = ((progress_value + 29) / 30) * 100
                                    else:
                                        progress_pct = min(100, (progress_value / 30) * 100)
                                    
                                    logger.info(f"Watch progress updated: {progress_value} ({progress_pct:.0f}%)")
                                    quest.progress = progress_value
                                    # Also update cache to ensure we continue from this point
                                    self.cache[quest_id] = progress_value
                            elif "play" in task_type_lower:
                                play_desktop = progress_data.get('play_on_desktop', {})
                                if isinstance(play_desktop, dict) and "value" in play_desktop:
                                    # Calculate percentage for logging
                                    progress_value = play_desktop["value"]
                                    progress_pct = min(100, (progress_value / 30) * 100)
                                    
                                    logger.info(f"Play progress updated: {progress_value} ({progress_pct:.0f}%)")
                                    quest.progress = progress_value
                        
                        return True
                
            return False
                
        except Exception as e:
            logger.error(f"Error updating quest progress for {quest_id}: {e}")
            logger.error(traceback.format_exc())
            return False
    async def quest_completer(self):
        """Background task to automatically complete quests, processing all enrolled quests in each iteration."""
        while self.auto_complete:
            try:
                # Fetch quests if it's been more than the refresh interval
                if time.time() - self.last_fetch_time > self.refresh_interval:
                    logger.info("Refresh interval reached, fetching new quests")
                    await self.get_quests()                    
                
                # Find all valid quests to process
                available_quests = []
                for quest_id, quest in self.quests.items():
                    # Skip completed quests
                    if quest.status == "completed":
                        # Remove from cache when completed
                        if quest_id in self.cache:
                            del self.cache[quest_id]
                        continue
                    
                    # Skip expired or unsupported quests
                    if quest.is_expired or not quest.is_supported():
                        continue
                    
                    # Skip excluded quests
                    if quest_id in self.excluded_quests:
                        continue
                    
                    # Add supported quests to process list
                    available_quests.append(quest_id)
                
                if not available_quests:
                    # No available quests to process - check if all are completed
                    all_completed = True
                    for quest in self.quests.values():
                        if not quest.is_expired and quest.is_supported() and quest.status != "completed":
                            all_completed = False
                            break
                    if all_completed and self.quests:
                        logger.info("All available quests are completed, stopping auto-completer")
                        self.auto_complete = False
                        # Clean up task reference to prevent race conditions
                        self.quest_completion_task = None
                        break
                    else:
                        # No currently available quests, sleep before trying again
                        logger.debug("No available quests to process")
                        await asyncio.sleep(random.randint(45, 60))
                        continue

                # Filter out quests that aren't enrolled and might need manual enrollment
                enrolled_quests = []
                manual_enrollment_needed = False
                for quest_id in available_quests:
                    quest = self.quests[quest_id]
                    # Skip completed quests (double-check)
                    if quest.status == "completed":
                        logger.debug(f"Skipping completed quest during enrollment check: {quest.title}")
                        continue
                    
                    if quest.status == "enrolled":
                        enrolled_quests.append(quest_id)
                    else:
                        # Check enrollment without trying to enroll
                        enrolled = await self.check_enrollment_status(quest_id)
                        if enrolled:
                            enrolled_quests.append(quest_id)

                if not enrolled_quests:
                    # If we have available quests but none are enrolled, they likely need manual enrollment
                    for quest_id in available_quests:
                        quest = self.quests[quest_id]
                        if quest.status != "enrolled" and not quest.is_expired and quest.is_supported() and quest_id not in self.excluded_quests:
                            manual_enrollment_needed = True
                            break
                        
                    if manual_enrollment_needed:
                        logger.info("No enrolled quests available, manual enrollment is required. Stopping auto-completer.")
                        self.auto_complete = False
                        # Clean up task reference to prevent race conditions
                        self.quest_completion_task = None
                        break
                        
                    logger.debug("No enrolled quests ready to process")
                    await asyncio.sleep(random.randint(45, 60))
                    continue
                    
                logger.info(f"Processing {len(enrolled_quests)} enrolled quests")
                
                # Process each enrolled quest
                for quest_id in enrolled_quests:
                    quest = self.quests[quest_id]
                    
                    # Double-check that the quest is not completed (could have changed since filtering)
                    if quest.status == "completed":
                        logger.debug(f"Skipping completed quest: {quest.title}")
                        continue
                    
                    # Update progress for the current quest
                    logger.info(f"Updating progress for quest: {quest.title}")
                    success = await self.update_quest_progress(quest_id)
                    if success:
                        if quest.status == "completed":
                            logger.info(f"Completed quest: {quest.title}")
                
                # Sleep before next cycle - use random duration between 45-60 seconds
                sleep_duration = random.randint(45, 60)
                logger.debug(f"Sleeping for {sleep_duration} seconds before next cycle")
                await asyncio.sleep(sleep_duration)
                
            except asyncio.CancelledError:
                logger.info("Quest completer task canceled")
                break
            except Exception as e:                
                logger.error(f"Error in quest completer: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(30)  # Longer sleep on error
    def start_auto_completer(self):
        """Start the auto completer task."""
        if self.auto_complete and not self.quest_completion_task:
            logger.info("Starting quest auto-completer task")
            
            # Reset the counter for no enrolled quests
            self.no_enrolled_quests_count = 0
            
            # Check for quests that need manual enrollment
            manual_enrollment_quests = []
            for quest_id, quest in self.quests.items():
                # Skip completed, expired, or unsupported quests
                if quest.status == "completed" or quest.is_expired or not quest.is_supported():
                    continue
                # Skip excluded quests
                if quest_id in self.excluded_quests:
                    continue
                # If not enrolled, it needs manual enrollment
                if quest.status != "enrolled":
                    manual_enrollment_quests.append(quest.title)
            
            # Show notification about quests that need manual enrollment
            if manual_enrollment_quests:
                logger.warning(f"Found {len(manual_enrollment_quests)} quests that need manual enrollment through Discord's interface")
                for quest_title in manual_enrollment_quests:
                    logger.warning(f"Quest needing manual enrollment: {quest_title}")
            
            self.quest_completion_task = asyncio.create_task(self.quest_completer())
            return True
        return False
    
    def stop_auto_completer(self):
        """Stop the auto completer task."""
        logger.info("Stopping quest auto-completer task")
        if self.quest_completion_task:
            self.quest_completion_task.cancel()
            self.quest_completion_task = None
            return True
        return False
    @commands.command(aliases=['qlist', 'ql', 'qstat', 'qstatus', 'qst'])
    async def questlist(self, ctx):
        """List all available quests and their status
        
        .questlist - Show all available quests and statuses"""
        try:
            await ctx.message.delete()
        except:
            pass
        
        # Fetch fresh quest data
        await self.get_quests()
        
        if not self.quests:
            await ctx.send(format_message("No quests available", "error"), delete_after=10)
            return
        
        # Group quests by status and expiration
        active_quests = []       # Not expired and available
        in_progress_quests = []  # Enrolled
        completed_quests = []    # Completed
        expired_quests = []      # Expired
        upcoming_quests = []     # Not started yet
        
        current_time = datetime.now()
        
        for quest_id, quest in self.quests.items():
            # Check for upcoming quests (if starts_at is in the future)
            if quest.starts_at:
                try:
                    start_time = datetime.fromisoformat(quest.starts_at.replace('Z', '+00:00'))
                    if start_time > current_time:
                        upcoming_quests.append(quest)
                        continue
                except Exception:
                    pass
                    
            # Check if quest is expired
            if quest.is_expired:
                expired_quests.append(quest)
            # Group by status
            elif quest.status == "completed":
                completed_quests.append(quest)
            elif quest.status == "enrolled":
                in_progress_quests.append(quest)
            else:
                active_quests.append(quest)
        
        # Count totals for statistics
        quest_total = len(self.quests)
        available_count = len(active_quests)
        in_progress_count = len(in_progress_quests)
        completed_count = len(completed_quests)
        expired_count = len(expired_quests)
        upcoming_count = len(upcoming_quests)
        
        # Build ANSI formatted message
        message_parts = ["```ansi\n"]
        
        # STATUS SECTION
        message_parts.append("\u001b[30m\u001b[1m\u001b[4mStatus\u001b[0m\n")
        
        # Status color based on whether auto-complete is active
        status_color = "\u001b[0;32m" if self.auto_complete else "\u001b[0;31m"
        status_text = "Running" if self.auto_complete else "Stopped"
        
        # Calculate padding for status labels
        status_labels = ["Auto-completer", "Quest count", "Last fetch"]
        status_padding = self.get_padding(status_labels)
        
        # Format status information
        message_parts.append(f"\u001b[0;37mAuto-completer{' ' * (status_padding - len('Auto-completer'))}\u001b[30m| {status_color}{status_text}\u001b[0m\n")
        message_parts.append(f"\u001b[0;37mQuest count{' ' * (status_padding - len('Quest count'))}\u001b[30m| \u001b[0;34m{quest_total}\u001b[0m\n")
        
        # Add last fetch time if available
        if self.last_fetch_time > 0:
            fetch_time = datetime.fromtimestamp(self.last_fetch_time).strftime('%H:%M:%S')
            message_parts.append(f"\u001b[0;37mLast fetch{' ' * (status_padding - len('Last fetch'))}\u001b[30m| \u001b[0;34m{fetch_time}\u001b[0m\n")
        
        # STATISTICS SECTION
        message_parts.append("\n\u001b[30m\u001b[1m\u001b[4mQuest Statistics\u001b[0m\n")
        
        # Calculate padding for quest stats labels
        stats_labels = ["Available", "In Progress", "Completed", "Expired", "Upcoming"]
        stats_padding = self.get_padding(stats_labels)
        
        # Format quest statistics
        message_parts.append(f"\u001b[0;37mAvailable{' ' * (stats_padding - len('Available'))}\u001b[30m| \u001b[0;34m{available_count}\u001b[0m\n")
        message_parts.append(f"\u001b[0;37mIn Progress{' ' * (stats_padding - len('In Progress'))}\u001b[30m| \u001b[0;33m{in_progress_count}\u001b[0m\n")
        message_parts.append(f"\u001b[0;37mCompleted{' ' * (stats_padding - len('Completed'))}\u001b[30m| \u001b[0;32m{completed_count}\u001b[0m\n")        
        message_parts.append(f"\u001b[0;37mExpired{' ' * (stats_padding - len('Expired'))}\u001b[30m| \u001b[0;31m{expired_count}\u001b[0m\n")
        if upcoming_count > 0:
            message_parts.append(f"\u001b[0;37mUpcoming{' ' * (stats_padding - len('Upcoming'))}\u001b[30m| \u001b[0;35m{upcoming_count}\u001b[0m\n")
        
        # QUEST DETAILS SECTION (if there are active quests)
        if in_progress_quests:
            message_parts.append("\n\u001b[30m\u001b[1m\u001b[4mIn Progress Quests\u001b[0m\n")
            for quest in in_progress_quests:
                # Calculate progress based on quest type
                progress_display = 0
                progress_str = ""
                
                if isinstance(quest.progress, (int, float)):
                    task_type_lower = quest.task_type.lower() if quest.task_type else ""
                    
                    if "watch" in task_type_lower and "video" in task_type_lower:
                        # WatchVideo quests start at -29 and increment by 30
                        # Calculate percentage based on this range
                        if quest.progress < 0:
                            # Still in negative range
                            progress_display = ((quest.progress + 29) / 30) * 100
                        else:
                            # Already in positive range
                            progress_display = min(100, (quest.progress / 30) * 100)
                              # Ensure progress is between 0-100%
                        progress_display = max(0, min(100, progress_display))
                        progress_str = f" ({progress_display:.0f}% | {quest.progress:.1f})"
                        
                    elif "play" in task_type_lower and "desktop" in task_type_lower:
                        # PlayOnDesktop quests have their own progress calculation
                        # Typically these increment by 1 for each heartbeat
                        # Assuming 100% is reached at approximately 30 updates
                        progress_display = min(100, (quest.progress / 30) * 100)
                        progress_str = f" ({progress_display:.0f}% | {quest.progress:.1f})"
                        
                    else:
                        # Generic progress handling for other quest types
                        # Simply show the raw value as percentage if between 0-100
                        if 0 <= quest.progress <= 100:
                            progress_display = quest.progress
                        else:
                            # Otherwise scale it to a percentage
                            progress_display = min(100, max(0, (quest.progress / 30) * 100))
                        progress_str = f" ({progress_display:.0f}% | {quest.progress:.1f})"
                        
                message_parts.append(f"\u001b[0;33m• {quest.title}{progress_str} [{quest.task_type}]\u001b[0m\n")
        
        if active_quests:
            message_parts.append("\n\u001b[30m\u001b[1m\u001b[4mAvailable Quests\u001b[0m\n")
            for quest in active_quests:
                message_parts.append(f"\u001b[0;34m• {quest.title} [{quest.task_type}]\u001b[0m\n")
        
        # COMMANDS SECTION
        message_parts.append("\n\u001b[30m\u001b[1m\u001b[4mCommands\u001b[0m\n")
        commands_labels = [".queststart", ".queststop", ".questrefresh"]
        commands_padding = self.get_padding(commands_labels)
        
        message_parts.append(f"\u001b[0;37m.queststart{' ' * (commands_padding - len('.queststart'))}\u001b[30m| \u001b[0;34mStart auto-completing\u001b[0m\n")
        message_parts.append(f"\u001b[0;37m.queststop{' ' * (commands_padding - len('.queststop'))}\u001b[30m| \u001b[0;34mStop auto-completing\u001b[0m\n")
        message_parts.append(f"\u001b[0;37m.questrefresh{' ' * (commands_padding - len('.questrefresh'))}\u001b[30m| \u001b[0;34mRefresh quest data\u001b[0m\n")
        
        # Close the code block
        message_parts.append("```")
        
        # Send the formatted message
        await ctx.send(quote_block(''.join(message_parts)), 
            delete_after=self.bot.config_manager.auto_delete.delay if hasattr(self.bot, 'config_manager') and hasattr(self.bot.config_manager, 'auto_delete') and self.bot.config_manager.auto_delete.enabled else 30
        )    
    
    @commands.command(aliases=['qstart', 'qs'])
    async def queststart(self, ctx):
        """Start automatic quest completion
        
        .queststart - Begin auto-completing quests"""
        try:
            await ctx.message.delete()
        except:
            pass
        
        if self.auto_complete:
            await ctx.send(format_message("Quest auto-completer is already running", "warn"), delete_after=10)
            return
        
        # First fetch the latest quest data
        await self.get_quests()
        
        # Check for enrolled quests
        enrolled_quests = []
        manual_enrollment_quests = []
        
        for quest_id, quest in self.quests.items():
            # Skip completed, expired, or unsupported quests
            if quest.status == "completed" or quest.is_expired or not quest.is_supported():
                continue
            # Skip excluded quests
            if quest_id in self.excluded_quests:
                continue
            
            # Add to appropriate list based on enrollment status
            if quest.status == "enrolled":
                enrolled_quests.append(quest.title)
            else:
                manual_enrollment_quests.append(quest.title)
        
        # If no quests are available at all
        if not enrolled_quests and not manual_enrollment_quests:
            await ctx.send(format_message("No available quests to process. All quests may be completed or expired.", "warn"), delete_after=10)
            return
        
        # If no enrolled quests but some need manual enrollment
        if not enrolled_quests and manual_enrollment_quests:
            manual_quest_message = f"Cannot start quest completer - no enrolled quests to process.\n\nFound {len(manual_enrollment_quests)} quests that need manual enrollment through Discord's interface:\n"
            manual_quest_message += "\n".join([f"• {title}" for title in manual_enrollment_quests])
            await ctx.send(format_message(manual_quest_message, "error"), delete_after=20)
            return
        
        # If we have some enrolled quests but also some that need manual enrollment
        if manual_enrollment_quests:
            manual_quest_message = f"Found {len(manual_enrollment_quests)} quests that need manual enrollment through Discord's interface:\n"
            manual_quest_message += "\n".join([f"• {title}" for title in manual_enrollment_quests])
            await ctx.send(format_message(manual_quest_message, "warn"), delete_after=20)
        
        # Start quest completer with enrolled quests
        self.auto_complete = True
        started = self.start_auto_completer()
        
        if started:
            await ctx.send(format_message(f"Quest auto-completer started successfully with {len(enrolled_quests)} enrolled quests", "success"), delete_after=10)
        else:
            await ctx.send(format_message("Failed to start quest auto-completer", "error"), delete_after=10)
    
    @commands.command(aliases=['qstop', 'qx'])
    async def queststop(self, ctx):
        """Stop automatic quest completion
        
        .queststop - Stop auto-completing quests"""
        try:
            await ctx.message.delete()
        except:
            pass
        
        if not self.auto_complete:
            await ctx.send(format_message("Quest auto-completer is not running", "warn"), delete_after=10)
            return
        
        self.auto_complete = False
        stopped = self.stop_auto_completer()
        
        if stopped:
            await ctx.send(format_message("Quest auto-completer stopped successfully", "success"), delete_after=10)
        else:
            await ctx.send(format_message("Failed to stop quest auto-completer", "error"), delete_after=10)
            
    @commands.command(aliases=['qrefresh', 'qr'])
    async def questrefresh(self, ctx):
        """Refresh quest data
        
        .questrefresh - Manually refresh quest data from Discord"""
        try:
            await ctx.message.delete()
        except:
            pass
        
        success = await self.get_quests()
        
        if success:
            await ctx.send(format_message("Quest data refreshed successfully", "success"), delete_after=10)
        else:
            await ctx.send(format_message("Failed to refresh quest data", "error"), delete_after=10)
    def get_next_quest(self):
        """Find the next quest to process."""
        available_quests = []
          # Collect all quests that are enrolled, supported, not expired, and not excluded
        for quest_id, quest in self.quests.items():
            if (quest.status == "enrolled" and not quest.is_expired and
                quest.is_supported() and quest_id not in self.excluded_quests):
                available_quests.append(quest_id)
        
        if not available_quests:
            logger.debug("No available quests to process")
            return None
        
        # If we have a current quest that's still valid, find the next one
        if self.current_quest_id in available_quests:
            current_idx = available_quests.index(self.current_quest_id)
            next_idx = (current_idx + 1) % len(available_quests)
            return available_quests[next_idx]
        
        # Otherwise, return the first available quest
        return available_quests[0]
    
    def determine_quest_task_type(self, config):
        """Determine the correct task type from quest config data."""
        # Start with unknown task type
        task_type = "Unknown"
        
        # Check task_config_v2 first (based on provided API structure)
        task_config_v2 = config.get('task_config_v2', {})
        tasks_v2 = task_config_v2.get('tasks', {})
        
        if "WATCH_VIDEO" in tasks_v2:
            task_type = "WatchVideo"
        elif "PLAY_ON_DESKTOP" in tasks_v2:
            task_type = "PlayOnDesktop"
            
        # If not found in v2 config, check task_config
        if task_type == "Unknown":
            task_config = config.get('task_config', {})
            tasks = task_config.get('tasks', {})
            
            if "WATCH_VIDEO" in tasks:
                task_type = "WatchVideo"
            elif "PLAY_ON_DESKTOP" in tasks:
                task_type = "PlayOnDesktop"
                
        # Final fallback - check features array
        if task_type == "Unknown" and 'features' in config:
            features = config.get('features', [])
            # Feature code 3 = watch video, 4 = play on desktop (from provided data)
            if 3 in features:
                task_type = "WatchVideo"
            elif 4 in features:
                task_type = "PlayOnDesktop"
                
        return task_type
    
    def get_auth_headers(self):
        """Get proper authentication headers for Discord API requests."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord/1.0.9191 Chrome/134.0.6998.179 Electron/35.1.5 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Authorization": self.bot.http.token,
        }
        
        # Add X-Super-Properties if available
        if hasattr(self.bot.http, "headers") and hasattr(self.bot.http.headers, "encoded_super_properties"):
            headers["X-Super-Properties"] = self.bot.http.headers.encoded_super_properties
            
        return headers
        

async def setup(bot):
    await bot.add_cog(QuestManager(bot))