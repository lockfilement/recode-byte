import discord
from discord.ext import commands
import asyncio
import logging
import time
from utils.general import format_message, quote_block
from utils.voice_patches import initialize_patches

logger = logging.getLogger(__name__)


class VoiceSitter(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Initialize voice patches when cog is created
        initialize_patches()
        self.target_channels = {}  # {guild_id: {user_id: current_channel_id}}
        self.original_target_channels = {} # {guild_id: {user_id: initial_channel_id}} # Added
        self.last_channels = {}  # {guild_id: {user_id: channel_id}}
        self.reconnection_attempts = {}  # {guild_id: {user_id: attempts}}
        self.last_reconnect_time = {}  # {guild_id: {user_id: timestamp}}
        self.last_successful_connection = {}  # {guild_id: {user_id: timestamp}}
        self.connection_in_progress = {}  # {guild_id: {user_id: bool}}
        self.base_delay = 8
        self.max_delay = 300
        self.stabilization_delay = 5
        self.j2c_stabilization_delay = 10
        self.max_rapid_attempts = 3
        self.locks = {}  # {guild_id: {user_id: asyncio.Lock()}}
        self.j2c_channels = {}  # {guild_id: {user_id: bool}} - Track if channel is J2C

    def _get_lock(self, guild_id, user_id):
        """Get or create an asyncio.Lock for a guild/user pair."""
        if guild_id not in self.locks:
            self.locks[guild_id] = {}
        if user_id not in self.locks[guild_id]:
            self.locks[guild_id][user_id] = asyncio.Lock()
        return self.locks[guild_id][user_id]

    def _is_j2c_channel(self, channel):
        """Detect if a channel is a join-to-create channel"""
        if not channel:
            return False
        # Check for common J2C indicators
        j2c_keywords = ['create', 'join', 'j2c', '+', '➕']
        channel_name_lower = channel.name.lower()
        return any(keyword in channel_name_lower for keyword in j2c_keywords)

    def _get_stabilization_delay(self, guild_id, user_id):
        """Get appropriate stabilization delay based on channel type"""
        is_j2c = self.j2c_channels.get(guild_id, {}).get(user_id, False)
        return self.j2c_stabilization_delay if is_j2c else self.stabilization_delay

    async def _connect_with_retry(self, channel, max_attempts=3):
        """Connect to voice channel with retry logic for failed handshakes"""
        guild_id = channel.guild.id
        is_j2c = self._is_j2c_channel(channel)
        connect_timeout = 120 if is_j2c else 90
        
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"[{guild_id}] Connection attempt {attempt}/{max_attempts} to {channel.name} (J2C: {is_j2c}, timeout: {connect_timeout}s)")
                
                # Use Discord.py-self's built-in timeout parameter and enable reconnection
                await channel.connect(
                    timeout=connect_timeout,
                    self_deaf=True, 
                    reconnect=True
                )
                logger.info(f"[{guild_id}] Successfully connected to {channel.name} on attempt {attempt}")
                return True
                
            except asyncio.TimeoutError:
                logger.warning(f"[{guild_id}] Connection attempt {attempt} timed out after {connect_timeout}s")
                if attempt < max_attempts:
                    delay = min(5 * attempt, 15)  # Progressive delay: 5s, 10s, 15s
                    logger.info(f"[{guild_id}] Waiting {delay}s before retry...")
                    await asyncio.sleep(delay)
                    
            except discord.ClientException as e:
                if "already connected" in str(e).lower():
                    logger.info(f"[{guild_id}] Already connected to voice channel")
                    return True
                logger.warning(f"[{guild_id}] ClientException on attempt {attempt}: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(3)
                    
            except Exception as e:
                logger.error(f"[{guild_id}] Connection attempt {attempt} failed: {e}")
                if "curl" in str(e).lower() or "websocket" in str(e).lower():
                    # Websocket/curl errors - retry with delay
                    if attempt < max_attempts:
                        delay = min(3 * attempt, 10)
                        logger.info(f"[{guild_id}] Websocket error, retrying in {delay}s...")
                        await asyncio.sleep(delay)
                else:
                    # Other errors - don't retry
                    logger.error(f"[{guild_id}] Non-retryable error: {e}")
                    return False
                    
        logger.error(f"[{guild_id}] Failed to connect to {channel.name} after {max_attempts} attempts")
        return False


    async def cog_load(self):
        """Register event handlers when cog is loaded"""
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            # Assuming EventManager exists and has this method
            try:
                event_manager.register_handler('on_voice_state_update', self.__class__.__name__, self._handle_voice_state_update)
                logger.info("Registered voice state update handler with EventManager.")
            except AttributeError:
                 logger.error("EventManager cog found, but register_handler method is missing.")
            except Exception as e:
                 logger.exception(f"Error registering handler with EventManager: {e}")
        else:
            logger.warning("EventManager cog not found. Voice state updates will not be handled by VoiceSitterCog.")
            # Fallback or alternative handling might be needed if EventManager is optional

    def _get_backoff_delay(self, guild_id: int, user_id: int) -> float:
        """Calculate exponential backoff delay for reconnection attempts"""
        attempts = self.reconnection_attempts.get(guild_id, {}).get(user_id, 0)
        delay = min(self.base_delay * (2 ** attempts), self.max_delay)
        return delay

    def _should_attempt_reconnect(self, guild_id: int, user_id: int) -> bool:
        """Determine if reconnection should be attempted based on current state"""
        current_time = time.time()
        
        # Don't reconnect if we're already trying to connect
        if self.connection_in_progress.get(guild_id, {}).get(user_id, False):
            return False
            
        # Check if we've exceeded max rapid attempts
        if self.reconnection_attempts.get(guild_id, {}).get(user_id, 0) >= self.max_rapid_attempts:
            last_success = self.last_successful_connection.get(guild_id, {}).get(user_id, 0)
            # Only try again if it's been a while since our last successful connection
            if current_time - last_success < self.max_delay:
                return False
                
        # Don't attempt if we're within the stabilization period of last attempt
        last_attempt = self.last_reconnect_time.get(guild_id, {}).get(user_id, 0)
        if current_time - last_attempt < self.stabilization_delay:
            return False
            
        return True

    async def _handle_reconnection(self, guild_id: int, channel_id: int):
        """Handle reconnection with exponential backoff and race condition protection"""
        user_id = self.bot.user.id
        lock = self._get_lock(guild_id, user_id)
        async with lock:
            # Double check if we should attempt connection right before starting
            current_target_id = self.target_channels.get(guild_id, {}).get(user_id)
            original_target_exists = user_id in self.original_target_channels.get(guild_id, {})

            if not original_target_exists or not self._should_attempt_reconnect(guild_id, user_id):
                logger.info(f"[{guild_id}] Reconnection attempt to {channel_id} aborted due to state conditions changing or target removed.")
                if guild_id in self.connection_in_progress and user_id in self.connection_in_progress.get(guild_id, {}):
                    self.connection_in_progress[guild_id][user_id] = False
                return

            self.reconnection_attempts.setdefault(guild_id, {})
            self.last_reconnect_time.setdefault(guild_id, {})
            self.connection_in_progress.setdefault(guild_id, {})
            self.last_successful_connection.setdefault(guild_id, {})

            self.connection_in_progress[guild_id][user_id] = True
            self.last_reconnect_time[guild_id][user_id] = time.time()

            try:
                self.reconnection_attempts[guild_id][user_id] = self.reconnection_attempts[guild_id].get(user_id, 0) + 1
                attempts = self.reconnection_attempts[guild_id][user_id]
                delay = self._get_backoff_delay(guild_id, user_id)
                logger.info(f"[{guild_id}] Attempting reconnection {attempts} to channel {channel_id} after delay {delay:.1f}s.")
                await asyncio.sleep(delay)

                if guild_id not in self.original_target_channels or user_id not in self.original_target_channels.get(guild_id, {}):
                    logger.info(f"[{guild_id}] Reconnection to {channel_id} cancelled: Original target channel removed during delay.")
                    self.connection_in_progress[guild_id][user_id] = False
                    await self.cleanup_voice_state(guild_id)
                    return

                channel = self.bot.get_channel(channel_id)
                if channel and isinstance(channel, discord.VoiceChannel):
                    logger.info(f"[{guild_id}] Reconnecting to {channel.name} ({channel_id})...")
                    success = await self._connect_with_retry(channel)
                    if success:
                        logger.info(f"[{guild_id}] Successfully reconnected to {channel.name} ({channel_id}).")
                    else:
                        raise Exception(f"Failed to reconnect to {channel.name} ({channel_id}) after multiple attempts")
                else:
                    logger.error(f"[{guild_id}] Cannot reconnect: Channel {channel_id} not found or invalid.")
                    current_target = self.target_channels.get(guild_id, {}).get(user_id)
                    original_target = self.original_target_channels.get(guild_id, {}).get(user_id)
                    if original_target is None:
                        logger.error(f"[{guild_id}] Original target is missing during failed reconnect handling. Cleaning up.")
                        await self.cleanup_voice_state(guild_id)
                        return
                    if channel_id == current_target and channel_id != original_target:
                        logger.warning(f"[{guild_id}] Failed to connect to temporary channel {channel_id}. Resetting target to original {original_target}.")
                        self.target_channels[guild_id][user_id] = original_target
                    elif channel_id == original_target:
                        logger.error(f"[{guild_id}] Failed to connect to original target channel {channel_id}. Cleaning up.")
                        await self.cleanup_voice_state(guild_id)
                    else:
                        logger.error(f"[{guild_id}] Failed connecting to unexpected channel {channel_id}. Current target: {current_target}, Original: {original_target}. Cleaning up.")
                        await self.cleanup_voice_state(guild_id)

            except discord.errors.ClientException as e:
                logger.warning(f"[{guild_id}] Reconnection failed (ClientException): {e}. Already connected elsewhere?")
                vc = self.bot.get_guild(guild_id).me.voice
                current_target_id = self.target_channels.get(guild_id, {}).get(user_id)
                if vc and current_target_id and vc.channel.id == current_target_id:
                    logger.info(f"[{guild_id}] Already in the current target channel {vc.channel.id}. Resetting attempts.")
                    self.reconnection_attempts[guild_id][user_id] = 0
                    self.last_successful_connection[guild_id][user_id] = time.time()
                else:
                    logger.warning(f"[{guild_id}] Not in target channel ({current_target_id}) despite ClientException.")
            except Exception as e:
                logger.exception(f"[{guild_id}] Reconnection attempt failed: {e}")
            finally:
                if guild_id in self.connection_in_progress and user_id in self.connection_in_progress.get(guild_id, {}):
                    self.connection_in_progress[guild_id][user_id] = False
                if guild_id in self.last_reconnect_time and user_id in self.last_reconnect_time.get(guild_id, {}):
                    self.last_reconnect_time[guild_id][user_id] = time.time()


    async def cleanup_voice_state(self, guild_id: int):
        """Clean up voice state and tracking data"""
        user_id = self.bot.user.id
        logger.info(f"[{guild_id}] Cleaning up voice state for user {user_id}.")
        try:
            # Clear target channels first to prevent immediate reconnection attempts by VSU
            if guild_id in self.target_channels:
                self.target_channels[guild_id].pop(user_id, None)
                if not self.target_channels[guild_id]:
                    self.target_channels.pop(guild_id, None)
            # Clear original target channel
            if guild_id in self.original_target_channels:
                self.original_target_channels[guild_id].pop(user_id, None)
                if not self.original_target_channels[guild_id]:
                    self.original_target_channels.pop(guild_id, None)


            voice_client = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
            if voice_client and voice_client.is_connected():
                logger.info(f"[{guild_id}] Disconnecting from voice channel {voice_client.channel.id}.")
                await voice_client.disconnect(force=True)
            # else: # No need to log if not connected, already logged if disconnect fails
            #      logger.info(f"[{guild_id}] No active voice client found to disconnect.")

            # Clean up all other tracking dictionaries for the user in this guild
            for tracking_dict in [self.last_channels,
                                self.reconnection_attempts,
                                self.last_reconnect_time, self.last_successful_connection,
                                self.connection_in_progress, self.j2c_channels]:
                if guild_id in tracking_dict:
                    tracking_dict[guild_id].pop(user_id, None)
                    if not tracking_dict[guild_id]: # Remove guild entry if empty
                        tracking_dict.pop(guild_id, None)

            logger.info(f"[{guild_id}] Finished cleaning up voice state.")
        except Exception as e:
            logger.exception(f"[{guild_id}] Error cleaning up voice state: {e}")

    async def _handle_voice_state_update(self, member, before, after):
        """Handle voice state updates"""
        if member.id != self.bot.user.id:
            return

        # Get the guild from the voice state objects, not from the member
        # This ensures we have a guild even when member.guild might not be available
        guild = None
        if after and after.channel:
            guild = after.channel.guild
        elif before and before.channel:
            guild = before.channel.guild
        
        if not guild:
            logger.warning("VSU: Could not determine guild from voice state update.")
            return
            
        guild_id = guild.id
        user_id = self.bot.user.id

        # Initialize tracking dictionaries for the guild if they don't exist (safety)
        self.target_channels.setdefault(guild_id, {})
        self.original_target_channels.setdefault(guild_id, {}) # Added init
        self.last_channels.setdefault(guild_id, {})
        self.reconnection_attempts.setdefault(guild_id, {})
        self.last_reconnect_time.setdefault(guild_id, {})
        self.last_successful_connection.setdefault(guild_id, {})
        self.connection_in_progress.setdefault(guild_id, {})
        self.j2c_channels.setdefault(guild_id, {})

        try:
            # Ignore updates if we are not supposed to be in a channel for this guild (check original target)
            if user_id not in self.original_target_channels.get(guild_id, {}):
                # If we disconnected but have no original target, ensure cleanup (handles edge cases like manual disconnect after cog load failure)
                if before.channel and not after.channel:
                     # Check if voice client exists before logging disconnect message
                     vc = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                     if vc and vc.channel == before.channel: # Ensure it was *our* client that disconnected
                         logger.info(f"[{guild_id}] Bot disconnected from {before.channel.id} but had no original target. Ensuring cleanup.")
                         await self.cleanup_voice_state(guild_id) # Cleanup state and ensure disconnect
                     elif not vc and before.channel: # Disconnected, no client, no target - state likely already clean
                         logger.debug(f"[{guild_id}] Bot disconnected from {before.channel.id} but had no original target and no active VC. Assuming already cleaned up.")

                return # Exit if no original target is set

            # Get current and original targets safely
            original_target_channel_id = self.original_target_channels.get(guild_id, {}).get(user_id) # Should exist based on check above
            current_target_channel_id = self.target_channels.get(guild_id, {}).get(user_id)

            if not original_target_channel_id: # Should not happen if check above passed, but safety first
                 logger.error(f"[{guild_id}] VSU triggered but original_target_channel_id is None after initial check. Cleaning up.")
                 await self.cleanup_voice_state(guild_id)
                 return
            # If current target is missing but original exists, set current to original initially
            if not current_target_channel_id:
                logger.debug(f"[{guild_id}] Current target missing, initializing with original target {original_target_channel_id}.")
                current_target_channel_id = original_target_channel_id
                self.target_channels[guild_id][user_id] = original_target_channel_id


            # Don't process updates if we're in the middle of an explicit connection attempt
            if self.connection_in_progress.get(guild_id, {}).get(user_id, False):
                logger.debug(f"[{guild_id}] Skipping voice state update for {member} due to connection in progress.")
                return

            current_channel_id = after.channel.id if after.channel else None
            previous_channel_id = before.channel.id if before.channel else None

            logger.debug(f"[{guild_id}] VSU: Before={previous_channel_id}, After={current_channel_id}, CurrentTarget={current_target_channel_id}, OriginalTarget={original_target_channel_id}")

            if current_channel_id:
                # Bot joined or moved to a channel
                self.last_channels[guild_id][user_id] = current_channel_id

                if current_channel_id == current_target_channel_id:
                    # Bot is in the current target channel
                    logger.info(f"[{guild_id}] Bot confirmed in current target channel {current_target_channel_id}.")
                    if self.reconnection_attempts.get(guild_id, {}).get(user_id, 0) > 0:
                         logger.info(f"[{guild_id}] Resetting reconnection attempts.")
                         self.reconnection_attempts[guild_id][user_id] = 0
                    self.last_successful_connection[guild_id][user_id] = time.time()

                else:
                    # Bot is in a different channel than the current target
                    # Check if this is a move *from* the original target channel (potential join-to-create)
                    # Use original_target_channel_id for J2C detection
                    is_potential_j2c_move = (previous_channel_id == original_target_channel_id)

                    if is_potential_j2c_move:
                        # Assume this is a join-to-create move. Update *current* target to the new channel.
                        is_j2c = self.j2c_channels.get(guild_id, {}).get(user_id, False)
                        logger.info(f"[{guild_id}] Detected move from original target {original_target_channel_id} to {current_channel_id}. J2C channel: {is_j2c}")
                        if is_j2c:
                            logger.info(f"[{guild_id}] J2C channel created temp channel {current_channel_id}, updating target.")
                            self.target_channels[guild_id][user_id] = current_channel_id
                            current_target_channel_id = current_channel_id # Update local var too
                            if self.reconnection_attempts.get(guild_id, {}).get(user_id, 0) > 0:
                                logger.info(f"[{guild_id}] Resetting reconnection attempts after J2C move.")
                                self.reconnection_attempts[guild_id][user_id] = 0
                            self.last_successful_connection[guild_id][user_id] = time.time()
                        else:
                            logger.warning(f"[{guild_id}] Unexpected move from original target (non-J2C). Attempting to return to original.")
                    else:
                        # Bot was moved from a non-target channel or joined directly into wrong channel. Try to move back to *current* target.
                        logger.warning(f"[{guild_id}] Bot is in {current_channel_id} but should be in {current_target_channel_id}. Attempting move back.")
                        # Check if we are still connected before attempting move back
                        vc = guild.me.voice
                        if vc and vc.channel.id == current_channel_id:
                             await self._handle_reconnection(guild_id, current_target_channel_id)
                        elif not vc:
                             logger.info(f"[{guild_id}] Bot disconnected before move back could be initiated from {current_channel_id}.")
                             # Disconnect logic below will handle this.
                             pass
                        else: # In a different channel already?
                             logger.warning(f"[{guild_id}] Bot moved again to {vc.channel.id if vc else 'None'} before move back could be initiated.")


            elif previous_channel_id:
                # Bot was disconnected (current_channel_id is None)
                logger.warning(f"[{guild_id}] Bot disconnected from {previous_channel_id}. Current Target={current_target_channel_id}, Original Target={original_target_channel_id}.")

                # Clear last known channel only if we were actually in it
                if self.last_channels.get(guild_id, {}).get(user_id) == previous_channel_id:
                    self.last_channels[guild_id].pop(user_id, None)

                # Wait briefly ONLY IF a reconnection attempt isn't already queued or running
                # And only if we are still supposed to be connected
                should_wait = (not self.connection_in_progress.get(guild_id, {}).get(user_id, False) and
                               user_id in self.original_target_channels.get(guild_id, {}))

                if should_wait:
                     delay = self._get_stabilization_delay(guild_id, user_id)
                     logger.debug(f"[{guild_id}] Waiting {delay}s for voice state stabilization")
                     await asyncio.sleep(delay)

                # Check if we're still disconnected AND if we are still supposed to be connected (check original target)
                # This check is crucial to prevent reconnecting after leavevc or cog unload
                if (guild_id in self.original_target_channels and
                    user_id in self.original_target_channels[guild_id]):
                    
                    # Check current voice state directly, but safely
                    voice_client = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                    is_connected = bool(voice_client and voice_client.is_connected())
                    
                    if not is_connected:
                        # Determine which channel to reconnect to
                        reconnect_channel_id = None # Default to None
                        target_to_check = current_target_channel_id # Check current target first
                        channel_object = self.bot.get_channel(target_to_check)

                        # If the current target was different from original (likely J2C temp channel) AND it no longer exists
                        if target_to_check != original_target_channel_id and not channel_object:
                            logger.info(f"[{guild_id}] Current target channel {target_to_check} not found. Assuming deleted J2C channel. Reverting to original target {original_target_channel_id}.")
                            reconnect_channel_id = original_target_channel_id
                            # Reset the current target back to the original one in our state
                            self.target_channels[guild_id][user_id] = original_target_channel_id
                        elif channel_object:
                            # Current target channel still exists, try reconnecting there
                            logger.info(f"[{guild_id}] Current target channel {target_to_check} still exists. Attempting reconnect there.")
                            reconnect_channel_id = target_to_check
                        elif not channel_object and target_to_check == original_target_channel_id:
                             # Original target channel doesn't exist
                             logger.error(f"[{guild_id}] Original target channel {original_target_channel_id} not found. Cannot reconnect. Cleaning up.")
                             await self.cleanup_voice_state(guild_id)
                             # reconnect_channel_id remains None
                        else:
                             # Should not happen: target_to_check != original_target_channel_id and channel_object exists
                             # This case is covered by the elif channel_object: block above.
                             # If target_to_check == original_target_channel_id and not channel_object, covered above.
                             logger.error(f"[{guild_id}] Unexpected state in disconnect logic. TargetToCheck={target_to_check}, Original={original_target_channel_id}, Exists={bool(channel_object)}. Cleaning up.")
                             await self.cleanup_voice_state(guild_id)
                             # reconnect_channel_id remains None


                        # If we determined a channel to reconnect to
                        if reconnect_channel_id:
                            logger.info(f"[{guild_id}] Bot still disconnected after delay. Scheduling reconnect to {reconnect_channel_id}.")
                            await self._handle_reconnection(guild_id, reconnect_channel_id)
                        else:
                             logger.info(f"[{guild_id}] No valid channel determined for reconnection after disconnect.")


                else:
                    # This block now covers:
                    # - Bot reconnected quickly (voice client exists)
                    # - Bot left intentionally (leavevc called, original target removed)
                    # - Cog unloaded (original target removed)
                    # - Bot was never supposed to be connected (original target missing - though initial check should prevent this)
                    voice_client = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
                    current_vc_state = "connected" if voice_client and voice_client.is_connected() else "disconnected"
                    target_state = "target exists" if user_id in self.original_target_channels.get(guild_id, {}) else "no target"
                    logger.info(f"[{guild_id}] Disconnection event ignored: Bot is {current_vc_state}, {target_state}.")
                    # If original target was removed but we somehow got disconnected, ensure full cleanup
                    if user_id not in self.original_target_channels.get(guild_id, {}):
                         await self.cleanup_voice_state(guild_id) # Ensure cleanup if target is gone

        except Exception as e:
            logger.exception(f"[{guild_id}] Error in voice state update: {e}")    
            
    @commands.command(aliases=['jvc', 'sitvc'])
    async def joinvc(self, ctx, *, channel_lookup: str):
        """Join & sit in a vc
        Usage:
        !jvc <channel_id> - join using a channel ID
        !jvc <channel_name> - channel by name or partial name
        """
        try:
            await ctx.message.delete()
        except discord.errors.Forbidden:
            pass
        except discord.errors.NotFound:
            pass
        except Exception as e:
            logger.warning(f"Error deleting joinvc command message: {e}")

        user_id = self.bot.user.id
        channel = None
        guild = None
        guild_id = None

        def get_delete_delay(base=5):
            try:
                if self.bot.config_manager.auto_delete.enabled:
                    return self.bot.config_manager.auto_delete.delay
            except AttributeError:
                pass
            return base

        # Channel lookup logic (by ID or name)
        try:
            channel_id_int = int(channel_lookup)
            channel = self.bot.get_channel(channel_id_int)
            if not isinstance(channel, discord.VoiceChannel):
                logger.debug(f"Channel ID {channel_id_int} found but is not a valid voice channel.")
                await ctx.send(format_message(f"Channel ID {channel_id_int} is not a voice channel."), delete_after=get_delete_delay(10))
                return
            guild = channel.guild
            guild_id = guild.id
            logger.debug(f"Found voice channel by ID: {channel.name} ({channel.id}) in guild {guild.name} ({guild_id})")
        except ValueError:
            if not ctx.guild:
                await ctx.send(format_message("When using this command in DMs, you must provide a channel ID."), delete_after=get_delete_delay(10))
                return
            guild = ctx.guild
            guild_id = guild.id
            logger.debug(f"'{channel_lookup}' is not an ID, searching by name in guild {guild.name}.")
            found_channels = [c for c in guild.voice_channels if channel_lookup.lower() in c.name.lower()]
            if len(found_channels) == 1:
                channel = found_channels[0]
                logger.debug(f"Found unique channel by name: {channel.name} ({channel.id})")
            elif len(found_channels) > 1:
                names = [f"'{c.name}' ({c.id})" for c in found_channels]
                await ctx.send(format_message(f"Multiple channels found matching '{channel_lookup}': {', '.join(names)}. Please use ID or a more specific name."), delete_after=15)
                return
            else:
                logger.debug(f"No channels found matching name '{channel_lookup}'.")
                await ctx.send(format_message(f"Could not find a voice channel matching '{channel_lookup}' in this server."), delete_after=get_delete_delay(10))
                return

        if not channel or not guild:
            await ctx.send(format_message(f"Could not find a voice channel matching '{channel_lookup}'."), delete_after=get_delete_delay(10))
            return

        channel_id = channel.id
        logger.info(f"[{guild_id}] Received joinvc command for channel {channel.name} ({channel_id}) from {ctx.guild.name if ctx.guild else 'DMs'}")

        # Prevent multiple simultaneous connection attempts
        if self.connection_in_progress.get(guild_id, {}).get(user_id, False):
            logger.warning(f"[{guild_id}] Connection already in progress for user {user_id}. Aborting new joinvc request.")
            await ctx.send(format_message("A connection attempt is already in progress. Please wait."), delete_after=get_delete_delay(5))
            return

        # If already connected to a voice channel in this guild, disconnect first
        voice_client = discord.utils.get(self.bot.voice_clients, guild__id=guild_id)
        if voice_client and voice_client.is_connected():
            if voice_client.channel.id == channel_id:
                logger.info(f"[{guild_id}] Already connected to the requested channel {channel_id}. Refreshing state.")
                # Clean up state and re-join to reset attempts, etc.
                await self.cleanup_voice_state(guild_id)
                await asyncio.sleep(0.5)
            else:
                logger.info(f"[{guild_id}] Connected to a different channel ({voice_client.channel.id}). Disconnecting before joining {channel_id}.")
                await self.cleanup_voice_state(guild_id)
                await asyncio.sleep(1)

        # Initialize tracking dictionaries for the guild if they don't exist
        self.target_channels.setdefault(guild_id, {})
        self.original_target_channels.setdefault(guild_id, {})
        self.last_channels.setdefault(guild_id, {})
        self.reconnection_attempts.setdefault(guild_id, {})
        self.last_reconnect_time.setdefault(guild_id, {})
        self.last_successful_connection.setdefault(guild_id, {})
        self.connection_in_progress.setdefault(guild_id, {})
        self.j2c_channels.setdefault(guild_id, {})

        # Update tracking - Set both current and original target initially
        self.target_channels[guild_id][user_id] = channel_id
        self.original_target_channels[guild_id][user_id] = channel_id
        self.j2c_channels[guild_id][user_id] = self._is_j2c_channel(channel)
        self.reconnection_attempts[guild_id].pop(user_id, None)
        self.last_reconnect_time[guild_id].pop(user_id, None)
        self.last_successful_connection[guild_id].pop(user_id, None)
        self.connection_in_progress[guild_id][user_id] = True
        self.last_reconnect_time[guild_id][user_id] = time.time()
        
        logger.info(f"[{guild_id}] Channel '{channel.name}' detected as J2C: {self.j2c_channels[guild_id][user_id]}")

        try:
            logger.info(f"[{guild_id}] Attempting initial connection to {channel.name} ({channel_id}).")
            success = await self._connect_with_retry(channel)
            if success:
                logger.info(f"[{guild_id}] Initial connection successful for {channel.name} ({channel_id}).")
                await ctx.send(format_message(f"Successfully joined voice channel: {channel.name}"), delete_after=get_delete_delay(5))
            else:
                raise Exception(f"Connection to {channel.name} failed after multiple attempts")
        except discord.errors.ClientException as e:
            logger.warning(f"[{guild_id}] Initial connection failed (ClientException): {e}.")
            # Check if already connected to the target channel
            vc = guild.me.voice
            if vc and vc.channel.id == channel_id:
                logger.info(f"[{guild_id}] Already connected to target channel {channel_id}.")
                await ctx.send(format_message(f"Already sitting in voice channel: {channel.name}"), delete_after=get_delete_delay(5))
                self.target_channels[guild_id][user_id] = channel_id
                self.original_target_channels[guild_id][user_id] = channel_id
                self.reconnection_attempts[guild_id][user_id] = 0
                self.last_successful_connection[guild_id][user_id] = time.time()
            elif vc:
                logger.error(f"[{guild_id}] ClientException: Bot is connected to {vc.channel.name} ({vc.channel.id}) instead of target {channel_id}. Cleaning up state.")
                await ctx.send(format_message(f"Error joining {channel.name}: Bot is stuck in {vc.channel.name}. Cleaning up..."), delete_after=get_delete_delay(10))
                await self.cleanup_voice_state(guild_id)
            else:
                logger.error(f"[{guild_id}] ClientException but bot is not connected to any voice channel. Cleaning up state.")
                await ctx.send(format_message(f"Error joining {channel.name}: Connection state issue. Cleaning up..."), delete_after=get_delete_delay(10))
                await self.cleanup_voice_state(guild_id)
        except discord.errors.Forbidden:
            logger.error(f"[{guild_id}] Permission error connecting to {channel.name} ({channel_id}).")
            await ctx.send(format_message(f"Failed to join {channel.name}: Check bot permissions (Connect, Speak)."), delete_after=get_delete_delay(10))
            await self.cleanup_voice_state(guild_id)
        except Exception as e:
            logger.exception(f"[{guild_id}] Initial voice connection error to {channel.name} ({channel_id}): {e}")
            await ctx.send(format_message(f"Failed to join {channel.name}. An unexpected error occurred."), delete_after=get_delete_delay(10))
            await self.cleanup_voice_state(guild_id)
        finally:
            if guild_id in self.connection_in_progress and user_id in self.connection_in_progress.get(guild_id, {}):
                self.connection_in_progress[guild_id][user_id] = False
            if guild_id in self.last_reconnect_time and user_id in self.last_reconnect_time.get(guild_id, {}):
                self.last_reconnect_time[guild_id][user_id] = time.time()
                 
    @commands.command(aliases=['lvc', 'stopsitvc'])
    async def leavevc(self, ctx, guild_id=None):
        """Stop sitting in vc
        
        Usage:
            !lvc - leaves all voice channels.
            !lvc all - Leaves all voice channels in all servers.
            !lvc [guild_id] - Leaves voice in the specified guild ID.
        """
        try: await ctx.message.delete()
        except discord.errors.Forbidden: pass
        except discord.errors.NotFound: pass
        except Exception as e: logger.warning(f"Error deleting leavevc command message: {e}")

        user_id = self.bot.user.id

        # Use helper function or attribute for auto-delete delay
        def get_delete_delay(base=5):
             try:
                 if self.bot.config_manager.auto_delete.enabled:
                     return self.bot.config_manager.auto_delete.delay
             except AttributeError: pass
             return base
             
        # Case 1: Leave all voice channels
        if guild_id == "all" or (not ctx.guild and not guild_id):
            guilds_left = 0
            # Create a copy of keys to iterate over as cleanup modifies the dict
            guild_ids_to_cleanup = list(self.original_target_channels.keys())
            
            for gid in guild_ids_to_cleanup:
                if user_id in self.original_target_channels.get(gid, {}):
                    original_target = self.original_target_channels[gid][user_id]
                    logger.info(f"[{gid}] Leaving voice channel {original_target} from global command.")
                    await self.cleanup_voice_state(int(gid))
                    guilds_left += 1
            
            message = f"Left voice channels in {guilds_left} server{'s' if guilds_left != 1 else ''}."
            await ctx.send(format_message(message), delete_after=get_delete_delay(5))
            return
        
        # Case 2: Leave specific guild by ID
        if guild_id:
            try:
                target_guild_id = int(guild_id)
                if target_guild_id in self.original_target_channels and user_id in self.original_target_channels.get(target_guild_id, {}):
                    original_target = self.original_target_channels[target_guild_id][user_id]
                    logger.info(f"[{target_guild_id}] Received leavevc command with specific guild ID. Original target was {original_target}. Cleaning up state.")
                    await self.cleanup_voice_state(target_guild_id)
                    guild_name = self.bot.get_guild(target_guild_id)
                    guild_name = guild_name.name if guild_name else str(target_guild_id)
                    await ctx.send(format_message(f"Left the voice channel in {guild_name}."), delete_after=get_delete_delay(5))
                else:
                    await ctx.send(format_message(f"Not sitting in a voice channel in server with ID {target_guild_id}."), delete_after=get_delete_delay(5))
                return
            except ValueError:
                await ctx.send(format_message(f"Invalid guild ID: {guild_id}. Please provide a valid server ID."), delete_after=get_delete_delay(5))
                return
        
        # Case 3: Used in a guild without parameters - leave that guild
        if ctx.guild:
            guild_id = ctx.guild.id
            # Check original target to see if we were *supposed* to be sitting
            if guild_id in self.original_target_channels and user_id in self.original_target_channels.get(guild_id, {}):
                original_target = self.original_target_channels[guild_id][user_id]
                logger.info(f"[{guild_id}] Received leavevc command in guild. Original target was {original_target}. Cleaning up state.")
                await self.cleanup_voice_state(guild_id) # This now clears both targets and disconnects
                await ctx.send(
                    format_message("Left the voice channel."),
                    delete_after=get_delete_delay(5)
                )
            else:
                logger.info(f"[{guild_id}] Received leavevc command, but bot was not targeting a channel in this guild.")
                # Ensure disconnected anyway, in case state is inconsistent
                vc = ctx.guild.me.voice
                if vc:
                    logger.info(f"[{guild_id}] Disconnecting from {vc.channel.name} ({vc.channel.id}) as a precaution.")
                    await vc.disconnect(force=True)
                    # Also run cleanup_voice_state to clear any potentially inconsistent state variables
                    await self.cleanup_voice_state(guild_id)
                await ctx.send(
                    format_message("Was not sitting in a voice channel in this server."),
                    delete_after=get_delete_delay(5)
                )
        else:
            # This shouldn't happen given the cases above, but as a fallback
            await ctx.send(format_message("Please use 'all' or provide a server ID when using this command in DMs."), delete_after=get_delete_delay(5))


    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        logger.info("VoiceSitter cog unloading. Cleaning up all voice connections...")
        event_manager = self.bot.get_cog('EventManager')
        if event_manager:
            try:
                event_manager.unregister_cog(self.__class__.__name__)
                logger.info("Unregistered voice state update handler from EventManager.")
            except Exception as e:
                 logger.exception(f"Error unregistering handler from EventManager: {e}")


        # Create a copy of keys to iterate over as cleanup modifies the dict
        # Use original_target_channels as the source of truth for cleanup
        guild_ids_to_cleanup = list(self.original_target_channels.keys())

        cleanup_tasks = []
        user_id = self.bot.user.id # Get user_id once
        for guild_id in guild_ids_to_cleanup:
            # Check if the bot user ID is actually in the dictionary for this guild
            if user_id in self.original_target_channels.get(guild_id, {}):
                 logger.info(f"[{guild_id}] Scheduling cleanup for cog unload.")
                 # Use create_task for potentially faster scheduling if many guilds
                 cleanup_tasks.append(asyncio.create_task(self.cleanup_voice_state(guild_id)))

        if cleanup_tasks:
            try:
                # Wait for all cleanup tasks to complete
                results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                         # Log exceptions that occurred during cleanup
                         guild_id = guild_ids_to_cleanup[i] # Assuming order is preserved
                         logger.error(f"[{guild_id}] Exception during cog unload cleanup: {result}")
            except Exception as e:
                 logger.exception(f"Error during asyncio.gather for cog unload cleanup: {e}")


        logger.info("VoiceSitter cog unloaded")

async def setup(bot):
    # Ensure logging is configured before adding the cog
    # Example: if not logging.getLogger(__name__).hasHandlers(): logging.basicConfig(level=logging.INFO)

    # Check for necessary dependencies or bot attributes if required
    # Example: if not hasattr(bot, 'config_manager'): logger.error("VoiceSitterCog requires bot.config_manager"); return

    try:
        cog_instance = VoiceSitter(bot)
        await bot.add_cog(cog_instance)
        logger.info("VoiceSitter added successfully.")
    except Exception as e:
        logger.exception(f"Failed to load VoiceSitter: {e}")
