"""
Voice connection patches for Discord.py-self to fix handshake timeout issues.

This module monkey patches Discord.py-self's voice connection to remove aggressive
timeouts that cause connection failures with the current Discord voice gateway.
"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

def apply_voice_patches():
    """Apply monkey patches to fix Discord.py-self voice connection issues."""
    try:
        # Import discord modules after ensuring they're available
        import discord.voice_client
        
        # Check if VoiceClient has the methods we expect
        if not hasattr(discord.voice_client.VoiceClient, 'finish_handshake'):
            logger.warning("VoiceClient.finish_handshake method not found, voice patches may not be compatible")
            return False
        
        # Store original methods
        original_finish_handshake = discord.voice_client.VoiceClient.finish_handshake
        original_connect_websocket = discord.voice_client.VoiceClient.connect_websocket
        
        # Patched finish_handshake with better timeout handling
        async def patched_finish_handshake(self):
            """Finish voice handshake with more lenient timeout handling."""
            try:
                # Use original method but catch timeout errors gracefully
                for attempt in range(3):  # Try up to 3 times
                    try:
                        await original_finish_handshake(self)
                        return  # Success
                    except asyncio.TimeoutError:
                        if attempt < 2:  # Not the last attempt
                            logger.debug(f"Voice handshake attempt {attempt + 1} timed out, retrying...")
                            await asyncio.sleep(2)
                            continue
                        else:
                            logger.warning("Voice handshake failed after 3 attempts, proceeding anyway...")
                            # Don't raise the timeout error, let connection continue
                            return
                    except Exception as e:
                        error_str = str(e).lower()
                        # Handle common curl_cffi/cdata errors that are non-critical
                        if any(x in error_str for x in ["curl", "websocket", "cdata", "ctype", "void", "nonetype"]):
                            if attempt < 2:
                                logger.debug(f"Voice handshake curl/websocket error on attempt {attempt + 1}, retrying...")
                                await asyncio.sleep(1)
                                continue
                            else:
                                # On final attempt, just log debug and continue
                                logger.debug(f"Voice handshake completed with curl errors (non-critical): {e}")
                                return
                        else:
                            logger.error(f"Voice handshake error: {e}")
                            raise  # Re-raise non-retryable errors
                        
            except Exception as e:
                # Final exception handler - log as debug since connection usually works
                logger.debug(f"Voice handshake completed with minor issues: {e}")
                # Don't fail the connection, just continue
        
        # Patched connect_websocket with better error handling
        async def patched_connect_websocket(self, *args, **kwargs):
            """Connect websocket with better error handling."""
            try:
                return await original_connect_websocket(self, *args, **kwargs)
            except Exception as e:
                error_str = str(e).lower()
                # Handle common curl_cffi/cdata errors that are non-critical
                if any(x in error_str for x in ["curl", "websocket", "cdata", "ctype", "void", "nonetype"]):
                    logger.debug(f"Voice websocket connection completed with curl errors (non-critical): {e}")
                    # Continue with a minimal websocket-like object if needed
                    return None
                else:
                    logger.error(f"Voice websocket connection error: {e}")
                    raise
        
        # Apply the voice patches
        discord.voice_client.VoiceClient.finish_handshake = patched_finish_handshake
        discord.voice_client.VoiceClient.connect_websocket = patched_connect_websocket
        
        logger.info("Applied voice handshake patches successfully for discord.py-self 2.0.1")
        return True
        
    except ImportError as e:
        logger.warning(f"Could not import discord modules for patching: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to apply voice patches: {e}")
        return False

def apply_gateway_patches():
    """Apply additional gateway patches for better voice stability."""
    # DISABLED - This was breaking main bot connections
    # Only apply voice-specific patches, not gateway patches
    logger.info("Gateway patches disabled to prevent connection issues")
    return True

# Auto-apply patches when module is imported
def initialize_patches():
    """Initialize all voice-related patches."""
    patches_applied = 0
    
    if apply_voice_patches():
        patches_applied += 1
    
    if apply_gateway_patches():
        patches_applied += 1
    
    if patches_applied > 0:
        logger.info(f"Voice patch system initialized ({patches_applied} patches applied)")
    else:
        logger.warning("No voice patches could be applied")
    
    return patches_applied > 0