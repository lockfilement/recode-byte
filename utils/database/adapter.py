"""
Enhanced database adapter with intelligent caching and query optimization.
Optimized for high-scale Discord selfbot operations on single machine.
"""
import logging
import time
from typing import Dict, Any, List, Optional
from .global_manager import (
    global_find_one, global_update_one, global_update_many, 
    global_insert_one, global_delete_one, global_write_operation,
    get_global_db, is_global_db_active,
    global_bulk_write, global_insert_many, global_update_many_bulk,
    global_find_many, global_aggregate, global_count_documents,
    create_batch_processor
)
from .cache_manager import get_cache
from .query_optimizer import get_query_optimizer
from .aggregation_optimizer import get_aggregation_optimizer
from .performance_monitor import get_performance_monitor

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Enhanced database adapter with caching, optimization, and monitoring"""
    
    def __init__(self):
        self._instance_name = f"adapter_{id(self)}"
        self._cache = get_cache()
        self._query_optimizer = None
        self._aggregation_optimizer = None
        self._performance_monitor = None
        
    async def initialize(self):
        """Initialize with full optimization suite"""
        # Start caching
        await self._cache.start()
        
        # Initialize optimizers
        self._query_optimizer = get_query_optimizer(self)
        self._aggregation_optimizer = get_aggregation_optimizer(self)
        self._performance_monitor = get_performance_monitor(self)
        
        # Start performance monitoring
        await self._performance_monitor.start_monitoring()
        
        logger.info(f"[{self._instance_name}] Database adapter ready with full optimization suite")

    async def find_one(self, collection: str, query: dict, projection: dict = None):
        """Find one document with caching and optimization"""
        start_time = time.time()
        
        # Try cache first
        cached_result = await self._cache.get(collection, 'find_one', query, projection)
        if cached_result is not None:
            execution_time = (time.time() - start_time) * 1000
            if self._performance_monitor:
                self._performance_monitor.record_query(
                    collection, 'find_one', query, execution_time, 
                    1, cache_hit=True
                )
            return cached_result
        
        # Optimize query
        if self._query_optimizer:
            optimization = self._query_optimizer.optimize_query(
                collection, 'find_one', query, projection
            )
            query = optimization['query']
            projection = optimization['projection'] or projection
        
        # Execute query with explain for troubleshooting if needed
        result = await global_find_one(collection, query, projection)
        execution_time = (time.time() - start_time) * 1000
        
        # Get explain data for slow queries to debug index usage
        explain_data = None
        if execution_time > 100:  # Explain slow queries
            try:
                db = self.db
                explain_result = await db[collection].find(query, projection).explain()
                explain_data = explain_result
            except Exception as e:
                logger.debug(f"Could not get explain data: {e}")
        
        # Record performance metrics
        if self._performance_monitor:
            self._performance_monitor.record_query(
                collection, 'find_one', query, execution_time,
                1 if result else 0, cache_hit=False, explain_data=explain_data
            )
        
        # Cache the result if it exists
        if result is not None:
            await self._cache.set(collection, 'find_one', query, result, projection)
        
        return result

    async def update_one(self, collection: str, filter: dict, update: dict, upsert: bool = False):
        """Update one document with cache invalidation"""
        result = await global_update_one(collection, filter, update, upsert)
        
        # Invalidate related cache entries
        await self._cache.invalidate_by_write(collection, 'update_one', filter, update)
        
        return result
    
    async def update_many(self, collection: str, filter: dict, update: dict, upsert: bool = False):
        """Update many documents with cache invalidation"""
        result = await global_update_many(collection, filter, update, upsert)
        
        # Invalidate related cache entries
        await self._cache.invalidate_by_write(collection, 'update_many', filter, update)
        
        return result

    async def ensure_write(self, collection: str, operation: str, *args, **kwargs):
        """Generic write operation"""
        return await global_write_operation(collection, operation, *args, **kwargs)

    async def close(self):
        """Close adapter and stop all services"""
        if self._performance_monitor:
            await self._performance_monitor.stop_monitoring()
        await self._cache.stop()
        logger.debug(f"[{self._instance_name}] Adapter closed")

    @property
    def db(self):
        """Get global database instance"""
        return get_global_db()

    @property
    def is_active(self):
        """Check if global database is active"""
        return is_global_db_active()

    @property
    def instance_id(self):
        """Return adapter instance name"""
        return self._instance_name
    
    # Batch operations
    async def bulk_write(self, collection: str, requests, ordered: bool = False):
        """Perform bulk write operations"""
        return await global_bulk_write(collection, requests, ordered)
    
    async def insert_many(self, collection: str, documents, ordered: bool = False):
        """Insert many documents"""
        return await global_insert_many(collection, documents, ordered)
    
    async def update_many_bulk(self, collection: str, operations, ordered: bool = False):
        """Perform multiple update operations in bulk"""
        return await global_update_many_bulk(collection, operations, ordered)
    
    async def find_many(self, collection: str, query=None, projection=None, sort=None, limit=None, skip=None):
        """Find many documents with caching"""
        # Try cache first
        cached_result = await self._cache.get(collection, 'find_many', query or {}, projection, sort, limit, skip)
        if cached_result is not None:
            return cached_result
        
        # Execute query
        result = await global_find_many(collection, query, projection, sort, limit, skip)
        
        # Cache the result if it's not empty and not too large
        if result and len(result) <= 1000:  # Don't cache very large results
            await self._cache.set(collection, 'find_many', query or {}, result, projection, sort, limit, skip)
        
        return result
    
    async def aggregate(self, collection: str, pipeline):
        """Perform aggregation"""
        return await global_aggregate(collection, pipeline)
    
    async def count_documents(self, collection: str, filter=None):
        """Count documents"""
        return await global_count_documents(collection, filter)
    
    def create_batch_processor(self, collection: str, batch_size: int = 100, ordered: bool = False):
        """Create a batch processor for this collection"""
        return create_batch_processor(collection, batch_size, ordered)
    
    async def get_cache_stats(self):
        """Get cache performance statistics"""
        return self._cache.get_stats()
    
    async def clear_cache(self, collection: str = None):
        """Clear cache entries"""
        if collection:
            await self._cache.invalidate_collection(collection)
        else:
            await self._cache.clear_all()
    
    # Optimized query methods
    async def find_user_by_id(self, user_id: int, projection: dict = None):
        """Optimized user lookup by ID with minimal projection and index hint"""
        if not projection:
            projection = {
                'user_id': 1, 'current_username': 1, 'current_displayname': 1,
                'current_avatar': 1, 'last_seen': 1, 'first_seen': 1
            }
        
        # Use direct MongoDB access with hint for optimal performance
        if self.is_active:
            collection = self.db.users
            return await collection.find_one(
                {'user_id': user_id}, 
                projection=projection
            ).hint([("user_id", 1)])
        
        return await self.find_one('users', {'user_id': user_id}, projection)
    
    async def find_recent_messages(self, user_id: int, limit: int = 50):
        """Optimized recent message lookup with index hint"""
        projection = {'user_id': 1, 'content': 1, 'created_at': 1, 'guild_id': 1, 'channel_id': 1}
        
        # Use direct MongoDB access with hint for optimal performance
        if self.is_active:
            collection = self.db.user_messages
            cursor = collection.find(
                {'user_id': user_id},
                projection=projection
            ).hint([("user_id", 1), ("created_at", -1)]).sort([('created_at', -1)]).limit(limit)
            
            return await cursor.to_list(length=limit)
        
        return await self.find_many(
            'user_messages',
            {'user_id': user_id},
            projection=projection,
            sort=[('created_at', -1)],
            limit=limit
        )
    
    async def find_user_mentions(self, user_id: int, days: int = 7, limit: int = 100):
        """Optimized user mention lookup"""
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        projection = {'target_id': 1, 'mentioner_id': 1, 'created_at': 1, 'content': 1}
        
        return await self.find_many(
            'mentions',
            {'target_id': user_id, 'created_at': {'$gte': cutoff_date}},
            projection=projection,
            sort=[('created_at', -1)],
            limit=limit
        )
    
    # Performance and optimization methods
    async def get_performance_report(self):
        """Get comprehensive performance report"""
        if self._performance_monitor:
            return self._performance_monitor.get_performance_report()
        return {'error': 'Performance monitoring not available'}
    
    async def get_optimization_recommendations(self):
        """Get optimization recommendations"""
        if self._performance_monitor:
            return self._performance_monitor.get_optimization_recommendations()
        return []
    
    async def explain_query(self, collection: str, query: dict, projection: dict = None, 
                           sort: list = None, limit: int = None):
        """Explain query execution plan"""
        if self._query_optimizer:
            return await self._query_optimizer.explain_query(collection, query, projection, sort, limit)
        return {'error': 'Query optimizer not available'}
    
    async def execute_aggregation(self, collection: str, pipeline: list, 
                                 cache_ttl: int = 300, hint_index: dict = None):
        """Execute optimized aggregation pipeline with optional index hint"""
        if self._aggregation_optimizer:
            return await self._aggregation_optimizer.execute_optimized_aggregation(
                collection, pipeline, cache_ttl=cache_ttl, hint_index=hint_index
            )
        else:
            return await global_aggregate(collection, pipeline)
    
    # Pre-built analytics methods using aggregation optimizer
    async def get_user_activity_summary(self, user_id: int, days: int = 30):
        """Get user activity summary using optimized aggregation with index hint"""
        if self._aggregation_optimizer:
            pipeline = self._aggregation_optimizer.user_activity_summary(user_id, days)
            # Use the user_id + created_at index for optimal performance
            hint_index = {"user_id": 1, "created_at": -1}
            return await self.execute_aggregation('user_messages', pipeline, hint_index=hint_index)
        return []
    
    async def get_guild_leaderboard(self, guild_id: int, days: int = 7, limit: int = 50):
        """Get guild user leaderboard using optimized aggregation with index hint"""
        if self._aggregation_optimizer:
            pipeline = self._aggregation_optimizer.guild_user_leaderboard(guild_id, days, limit)
            # Use the guild_id + user_id + created_at index for optimal performance
            hint_index = {"guild_id": 1, "user_id": 1, "created_at": -1}
            return await self.execute_aggregation('user_messages', pipeline, hint_index=hint_index)
        return []
    
    def parse_bulk_result(self, result, operation_type: str = "operation"):
        """Parse bulk write results, handling both single results and lists"""
        if result is None:
            return {"success": False, "count": 0, "batches": 0}
        
        if isinstance(result, list):
            # Multiple batches - sum up all results
            total_count = 0
            for r in result:
                if hasattr(r, 'deleted_count'):
                    total_count += r.deleted_count
                elif hasattr(r, 'modified_count'):
                    total_count += r.modified_count
                elif hasattr(r, 'inserted_count'):
                    total_count += r.inserted_count
                elif hasattr(r, 'upserted_count'):
                    total_count += r.upserted_count
            
            return {
                "success": True,
                "count": total_count,
                "batches": len(result),
                "operation_type": operation_type
            }
        else:
            # Single batch result
            count = 0
            if hasattr(result, 'deleted_count'):
                count = result.deleted_count
            elif hasattr(result, 'modified_count'):
                count = result.modified_count
            elif hasattr(result, 'inserted_count'):
                count = result.inserted_count
            elif hasattr(result, 'upserted_count'):
                count = result.upserted_count
            
            return {
                "success": True,
                "count": count,
                "batches": 1,
                "operation_type": operation_type
            }
