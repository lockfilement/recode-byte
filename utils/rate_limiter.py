# utils/rate_limiter.py
from time import time
import asyncio
from functools import wraps
from collections import defaultdict
import logging
import inspect

logger = logging.getLogger(__name__)

class RateLimiter:
    _instances = {}  # Store per-instance rate limiters
    
    def __init__(self):
        # Instance-specific buckets for per-command limiting
        self.instance_buckets = defaultdict(lambda: defaultdict(lambda: {
            'actions': 0,
            'last_action': time(),
            'lock': asyncio.Lock(),
            'initial_limit': 30,
            'cooldown': 2.5,
            'reset_after': 30,
            'min_cooldown': 2.0,
            'rate_limited_count': 0,
            'cooldown_reduction_threshold': 5,
            'current_cooldown': 2.5
        }))
        
        # Global rate limit settings per instance
        self.global_actions = defaultdict(int)
        self.global_last_action = defaultdict(time)
        self.global_lock = defaultdict(asyncio.Lock)
        self.global_settings = defaultdict(lambda: {
            'initial_limit': 30,
            'cooldown': 2.5,
            'reset_after': 30,
            'min_cooldown': 2.0,
            'rate_limited_count': 0,
            'cooldown_reduction_threshold': 5,
            'current_cooldown': 2.5
        })

    @classmethod
    def get_instance(cls, instance_id):
        """Get or create rate limiter for specific instance"""
        if instance_id not in cls._instances:
            cls._instances[instance_id] = RateLimiter()
        return cls._instances[instance_id]

    def __call__(self, global_only=False, command_only=False):
        """Decorator factory"""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                instance_id = self._get_instance_id(args)
                if instance_id is None:
                    # Handle the case where instance_id cannot be determined
                    logger.warning("Instance ID could not be determined.")
                    return await func(*args, **kwargs)

                key = f"{func.__module__}.{func.__qualname__}"
                bucket = self.instance_buckets[instance_id][key]

                # Use per-instance global settings
                global_actions = self.global_actions[instance_id]
                global_last_action = self.global_last_action[instance_id]
                global_lock = self.global_lock[instance_id]
                global_settings = self.global_settings[instance_id]

                if global_only:
                    # Handle global rate limiting first
                    async with global_lock:
                        now = time()
                        time_since_last_global = now - global_last_action

                        if time_since_last_global >= global_settings['reset_after']:
                            # Reset global actions and settings for this instance
                            self.global_actions[instance_id] = 0
                            global_settings['rate_limited_count'] = 0
                            global_settings['current_cooldown'] = global_settings['cooldown']
                            self.global_last_action[instance_id] = now
                            global_last_action = now
                            global_actions = 0

                        if global_actions >= global_settings['initial_limit']:
                            global_settings['rate_limited_count'] += 1

                            if global_settings['rate_limited_count'] >= global_settings['cooldown_reduction_threshold']:
                                reduction_factor = min(
                                    (global_settings['rate_limited_count'] - global_settings['cooldown_reduction_threshold']) / 7.0,
                                    1.0
                                )
                                global_settings['current_cooldown'] = max(
                                    global_settings['min_cooldown'],
                                    global_settings['cooldown'] - (reduction_factor * (global_settings['cooldown'] - global_settings['min_cooldown']))
                                )

                            await asyncio.sleep(global_settings['current_cooldown'])

                        # Update global actions and last action time for this instance
                        self.global_actions[instance_id] += 1
                        self.global_last_action[instance_id] = now

                if command_only:
                    # Handle per-command rate limiting
                    async with bucket['lock']:
                        now = time()
                        time_since_last = now - bucket['last_action']

                        if time_since_last >= bucket['reset_after']:
                            bucket['actions'] = 0
                            bucket['rate_limited_count'] = 0
                            bucket['current_cooldown'] = bucket['cooldown']
                            bucket['last_action'] = now

                        if bucket['actions'] >= bucket['initial_limit']:
                            bucket['rate_limited_count'] += 1
                            
                            if bucket['rate_limited_count'] >= bucket['cooldown_reduction_threshold']:
                                reduction_factor = min(
                                    (bucket['rate_limited_count'] - bucket['cooldown_reduction_threshold']) / 7.0,
                                    1.0
                                )
                                bucket['current_cooldown'] = max(
                                    bucket['min_cooldown'],
                                    bucket['cooldown'] - (reduction_factor * (bucket['cooldown'] - bucket['min_cooldown']))
                                )
                            
                            await asyncio.sleep(bucket['current_cooldown'])

                        bucket['actions'] += 1
                        bucket['last_action'] = now

                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if hasattr(e, 'status') and e.status == 429:
                        logger.warning(f"Discord rate limit hit in {key}, retrying...")
                        await asyncio.sleep(2.5)
                        return await wrapper(*args, **kwargs)
                    raise

            return wrapper
        return decorator

    def _get_instance_id(self, args):
        """Extract instance ID from arguments"""
        instance_id = None
        frame = inspect.currentframe()
        try:
            while frame and not instance_id:
                for var in frame.f_locals.values():
                    if hasattr(var, 'bot'):
                        instance_id = var.bot.user.id
                        break
                    elif hasattr(var, 'user'):
                        instance_id = var.user.id
                        break
                frame = frame.f_back

            if not instance_id:
                for arg in args:
                    if hasattr(arg, 'bot'):
                        instance_id = arg.bot.user.id
                        break
                    elif hasattr(arg, 'user'):
                        instance_id = arg.user.id
                        break

            return instance_id
        finally:
            del frame  # Break reference cycle to prevent memory leak

# Create global instance
rate_limiter = RateLimiter()