"""
Intelligent caching layer for MongoDB operations.
Optimized for Discord selfbot with smart cache invalidation and memory management.
"""
import asyncio
import hashlib
import json
import logging
import time
from typing import Dict, Any, Optional, List, Union, Set
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    data: Any
    created_at: float
    access_count: int
    last_accessed: float
    ttl: float
    cache_key: str
    collection: str
    invalidation_patterns: Set[str]

class IntelligentCache:
    """
    High-performance caching layer with:
    - LRU eviction with access patterns
    - Smart invalidation based on write operations
    - Memory-efficient storage
    - Query pattern optimization
    - Automatic TTL management
    """
    
    def __init__(self, max_size: int = 10000, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        
        # Cache storage with LRU ordering
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._cache_lock = asyncio.Lock()
        
        # Invalidation tracking
        self._invalidation_patterns: Dict[str, Set[str]] = defaultdict(set)
        self._collection_keys: Dict[str, Set[str]] = defaultdict(set)
        
        # Performance metrics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'invalidations': 0,
            'memory_pressure_evictions': 0
        }
        
        # Cache configurations for different query types
        self.cache_configs = {
            'user_lookup': {'ttl': 600, 'priority': 'high'},      # 10 minutes
            'user_history': {'ttl': 1800, 'priority': 'medium'},  # 30 minutes
            'message_search': {'ttl': 300, 'priority': 'low'},    # 5 minutes
            'guild_stats': {'ttl': 900, 'priority': 'medium'},    # 15 minutes
            'presence_data': {'ttl': 120, 'priority': 'low'},     # 2 minutes
            'system_config': {'ttl': 3600, 'priority': 'high'}    # 1 hour
        }
        
        # Cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Start the cache cleanup task"""
        if not self._running:
            self._running = True
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("[Cache] Started intelligent cache manager")

    async def stop(self):
        """Stop the cache and cleanup"""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("[Cache] Stopped cache manager")

    def _create_cache_key(self, collection: str, operation: str, query: Dict, 
                         projection: Dict = None, sort: List = None, 
                         limit: int = None, skip: int = None) -> str:
        """Create a deterministic cache key"""
        key_data = {
            'collection': collection,
            'operation': operation,
            'query': query,
            'projection': projection,
            'sort': sort,
            'limit': limit,
            'skip': skip
        }
        
        # Create hash of the serialized data
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]

    def _determine_cache_type(self, collection: str, query: Dict) -> str:
        """Determine cache type based on collection and query patterns"""
        if collection == 'users':
            if 'user_id' in query:
                return 'user_lookup'
            elif any(field.endswith('_history') for field in query.keys()):
                return 'user_history'
            return 'user_lookup'
        elif collection in ['deleted_messages', 'edited_messages']:
            return 'message_search'
        elif 'guild_id' in query:
            return 'guild_stats'
        elif 'status' in query or 'presence' in str(query).lower():
            return 'presence_data'
        else:
            return 'user_lookup'  # Default

    def _create_invalidation_patterns(self, collection: str, query: Dict) -> Set[str]:
        """Create invalidation patterns for cache entries"""
        patterns = {f"collection:{collection}"}
        
        # Add specific field patterns
        for key, value in query.items():
            if key in ['user_id', 'guild_id', 'channel_id', 'message_id']:
                patterns.add(f"{key}:{value}")
        
        return patterns

    async def get(self, collection: str, operation: str, query: Dict, 
                 projection: Dict = None, sort: List = None, 
                 limit: int = None, skip: int = None) -> Optional[Any]:
        """Get cached result if available"""
        cache_key = self._create_cache_key(collection, operation, query, projection, sort, limit, skip)
        
        async with self._cache_lock:
            if cache_key in self._cache:
                entry = self._cache[cache_key]
                current_time = time.time()
                
                # Check TTL
                if current_time - entry.created_at > entry.ttl:
                    del self._cache[cache_key]
                    self._collection_keys[collection].discard(cache_key)
                    self.stats['misses'] += 1
                    return None
                
                # Update access pattern
                entry.access_count += 1
                entry.last_accessed = current_time
                
                # Move to end (most recently used)
                self._cache.move_to_end(cache_key)
                
                self.stats['hits'] += 1
                logger.debug(f"[Cache] Hit for {collection}.{operation}")
                return entry.data
            
            self.stats['misses'] += 1
            return None

    async def set(self, collection: str, operation: str, query: Dict, result: Any,
                 projection: Dict = None, sort: List = None, 
                 limit: int = None, skip: int = None):
        """Cache a query result"""
        if result is None or (isinstance(result, (list, dict)) and not result):
            return  # Don't cache empty results
        
        cache_key = self._create_cache_key(collection, operation, query, projection, sort, limit, skip)
        cache_type = self._determine_cache_type(collection, query)
        config = self.cache_configs.get(cache_type, {'ttl': self.default_ttl, 'priority': 'medium'})
        
        current_time = time.time()
        invalidation_patterns = self._create_invalidation_patterns(collection, query)
        
        entry = CacheEntry(
            data=result,
            created_at=current_time,
            access_count=1,
            last_accessed=current_time,
            ttl=config['ttl'],
            cache_key=cache_key,
            collection=collection,
            invalidation_patterns=invalidation_patterns
        )
        
        async with self._cache_lock:
            # Remove old entry if exists
            if cache_key in self._cache:
                del self._cache[cache_key]
            
            # Add new entry
            self._cache[cache_key] = entry
            self._collection_keys[collection].add(cache_key)
            
            # Update invalidation patterns
            for pattern in invalidation_patterns:
                self._invalidation_patterns[pattern].add(cache_key)
            
            # Evict if necessary
            await self._evict_if_needed(config['priority'])
            
            logger.debug(f"[Cache] Cached {collection}.{operation} (TTL: {config['ttl']}s)")

    async def _evict_if_needed(self, priority: str = 'medium'):
        """Evict entries if cache is full"""
        if len(self._cache) <= self.max_size:
            return
        
        # Priority-based eviction
        priority_scores = {'low': 1, 'medium': 2, 'high': 3}
        current_priority = priority_scores.get(priority, 2)
        
        # Find candidates for eviction
        eviction_candidates = []
        current_time = time.time()
        
        for key, entry in self._cache.items():
            # Score based on: age, access pattern, priority
            age_score = (current_time - entry.last_accessed) / 3600  # Hours since last access
            access_score = 1 / (entry.access_count + 1)  # Lower is better for eviction
            
            # Determine entry priority from cache type
            cache_type = self._determine_cache_type(entry.collection, {})
            entry_priority = priority_scores.get(
                self.cache_configs.get(cache_type, {}).get('priority', 'medium'), 2
            )
            
            # Don't evict higher priority items for lower priority requests
            if entry_priority > current_priority:
                continue
            
            eviction_score = age_score + access_score - (entry_priority * 0.5)
            eviction_candidates.append((eviction_score, key, entry))
        
        # Sort by eviction score (highest first)
        eviction_candidates.sort(reverse=True)
        
        # Evict until we're under the limit
        evicted = 0
        target_evictions = len(self._cache) - int(self.max_size * 0.9)  # Leave some headroom
        
        for _, key, entry in eviction_candidates:
            if evicted >= target_evictions:
                break
                
            # Remove from cache
            del self._cache[key]
            self._collection_keys[entry.collection].discard(key)
            
            # Remove from invalidation patterns
            for pattern in entry.invalidation_patterns:
                self._invalidation_patterns[pattern].discard(key)
            
            evicted += 1
            
        self.stats['evictions'] += evicted
        if evicted > 0:
            logger.debug(f"[Cache] Evicted {evicted} entries due to size limit")

    async def invalidate_by_write(self, collection: str, operation: str, query: Dict, update: Dict = None):
        """Invalidate cache entries based on write operations"""
        invalidation_patterns = set()
        
        # Always invalidate collection-wide patterns
        invalidation_patterns.add(f"collection:{collection}")
        
        # Add specific field patterns based on query
        for key, value in query.items():
            if key in ['user_id', 'guild_id', 'channel_id', 'message_id']:
                invalidation_patterns.add(f"{key}:{value}")
        
        # Add patterns from update operations
        if update and '$set' in update:
            for key, value in update['$set'].items():
                if key in ['user_id', 'guild_id', 'channel_id', 'message_id']:
                    invalidation_patterns.add(f"{key}:{value}")
        
        # Invalidate matching cache entries
        keys_to_remove = set()
        async with self._cache_lock:
            for pattern in invalidation_patterns:
                keys_to_remove.update(self._invalidation_patterns.get(pattern, set()))
            
            for key in keys_to_remove:
                if key in self._cache:
                    entry = self._cache[key]
                    del self._cache[key]
                    self._collection_keys[entry.collection].discard(key)
                    
                    # Remove from invalidation patterns
                    for p in entry.invalidation_patterns:
                        self._invalidation_patterns[p].discard(key)
            
            self.stats['invalidations'] += len(keys_to_remove)
            
            if keys_to_remove:
                logger.debug(f"[Cache] Invalidated {len(keys_to_remove)} entries for {collection}.{operation}")

    async def invalidate_collection(self, collection: str):
        """Invalidate all cache entries for a collection"""
        async with self._cache_lock:
            keys_to_remove = self._collection_keys[collection].copy()
            
            for key in keys_to_remove:
                if key in self._cache:
                    entry = self._cache[key]
                    del self._cache[key]
                    
                    # Remove from invalidation patterns
                    for pattern in entry.invalidation_patterns:
                        self._invalidation_patterns[pattern].discard(key)
            
            self._collection_keys[collection].clear()
            self.stats['invalidations'] += len(keys_to_remove)
            
            if keys_to_remove:
                logger.info(f"[Cache] Invalidated {len(keys_to_remove)} entries for collection {collection}")

    async def _cleanup_loop(self):
        """Background cleanup of expired entries"""
        while self._running:
            try:
                await asyncio.sleep(60)  # Run every minute
                await self._cleanup_expired()
                await self._optimize_memory()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Cache] Cleanup error: {e}")

    async def _cleanup_expired(self):
        """Remove expired cache entries"""
        current_time = time.time()
        expired_keys = []
        
        async with self._cache_lock:
            for key, entry in self._cache.items():
                if current_time - entry.created_at > entry.ttl:
                    expired_keys.append(key)
            
            for key in expired_keys:
                entry = self._cache[key]
                del self._cache[key]
                self._collection_keys[entry.collection].discard(key)
                
                # Remove from invalidation patterns
                for pattern in entry.invalidation_patterns:
                    self._invalidation_patterns[pattern].discard(key)
        
        if expired_keys:
            logger.debug(f"[Cache] Cleaned up {len(expired_keys)} expired entries")

    async def _optimize_memory(self):
        """Optimize memory usage by cleaning up empty pattern sets"""
        async with self._cache_lock:
            # Clean up empty invalidation patterns
            empty_patterns = [p for p, keys in self._invalidation_patterns.items() if not keys]
            for pattern in empty_patterns:
                del self._invalidation_patterns[pattern]
            
            # Clean up empty collection key sets
            empty_collections = [c for c, keys in self._collection_keys.items() if not keys]
            for collection in empty_collections:
                del self._collection_keys[collection]

    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / max(1, total_requests)) * 100
        
        return {
            'cache_size': len(self._cache),
            'max_size': self.max_size,
            'hit_rate': hit_rate,
            'total_requests': total_requests,
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'evictions': self.stats['evictions'],
            'invalidations': self.stats['invalidations'],
            'collections_cached': len(self._collection_keys),
            'invalidation_patterns': len(self._invalidation_patterns)
        }

    def clear_stats(self):
        """Reset performance statistics"""
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'invalidations': 0,
            'memory_pressure_evictions': 0
        }

    async def clear_all(self):
        """Clear all cache entries"""
        async with self._cache_lock:
            self._cache.clear()
            self._collection_keys.clear()
            self._invalidation_patterns.clear()
        logger.info("[Cache] Cleared all cache entries")

# Global cache instance
_global_cache = IntelligentCache()

async def start_cache():
    """Start the global cache"""
    await _global_cache.start()

async def stop_cache():
    """Stop the global cache"""
    await _global_cache.stop()

def get_cache() -> IntelligentCache:
    """Get the global cache instance"""
    return _global_cache