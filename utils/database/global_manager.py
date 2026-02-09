"""
Global MongoDB manager that runs independently of any bot instance.
Simple global connection - no locking or complex management needed.
"""
import asyncio
import logging
import json
import os
from typing import Optional, List, Dict, Any, Union
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import WriteConcern, InsertOne, UpdateOne, UpdateMany, DeleteOne, DeleteMany, ReplaceOne
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)

# Global variables for the database connection
_global_client: Optional[AsyncIOMotorClient] = None
_global_db = None
_global_connection_active = False
_global_health_task: Optional[asyncio.Task] = None

# Configuration
_connection_string = None
_db_name = None
_options = None

async def initialize_global_database():
    """Initialize the global MongoDB connection - called once at startup"""
    global _global_client, _global_db, _global_connection_active
    global _global_health_task, _connection_string, _db_name, _options
    
    if _global_connection_active:
        logger.info("[GlobalDB] Database already initialized")
        return
            
    try:
        # Load configuration
        await _load_global_config()
        
        # Create MongoDB client
        logger.info("[GlobalDB] Creating global MongoDB connection...")
        _global_client = AsyncIOMotorClient(_connection_string, **_options)
        _global_db = _global_client[_db_name]
        
        # Test connection
        await _global_client.admin.command('ping')
        _global_connection_active = True
        
        # Create indexes
        await _create_global_indexes()
        
        # Start health check task
        _global_health_task = asyncio.create_task(_global_health_check())
        _global_health_task.set_name('global_mongodb_health_check')
        
        logger.info("[GlobalDB] Global MongoDB connection established successfully")
        
    except Exception as e:
        logger.error(f"[GlobalDB] Failed to initialize global MongoDB: {e}")
        await _cleanup_global_database()
        raise

async def _load_global_config():
    """Load MongoDB configuration globally"""
    global _connection_string, _db_name, _options
    
    try:
        config_path = os.path.join('config', 'database.json')
        with open(config_path) as f:
            config = json.load(f)
            _connection_string = config['mongodb']['uri']
            _db_name = config['mongodb']['database']
            
            # Optimize connection pooling for single machine high-performance operations
            _options = config['mongodb']['options'].copy()
            
            # Enhance connection pool for high-scale operations
            _options.update({
                'maxPoolSize': min(100, _options.get('maxPoolSize', 15) * 3),  # Increase pool size significantly
                'minPoolSize': max(10, _options.get('minPoolSize', 3) * 2),    # Maintain more minimum connections
                'maxIdleTimeMS': 120000,    # Keep connections alive longer (2 minutes)
                'waitQueueTimeoutMS': 15000, # Allow more time for connection acquisition
                'serverSelectionTimeoutMS': 10000,
                'readConcernLevel': 'local',       # Faster reads for single instance
                'retryReads': True,
                'retryWrites': True,
                'compressors': _options.get('compressors', 'zstd'),  # Enable compression
                'readPreference': 'primary'  # Direct reads to primary for consistency (removed maxStalenessSeconds)
            })
            
        logger.info("[GlobalDB] Loaded global MongoDB configuration")
    except Exception as e:
        logger.error(f"[GlobalDB] Failed to load MongoDB config: {e}")
        raise

async def _create_global_indexes():
    """Create optimized indexes for better performance"""
    try:
        logger.info("[GlobalDB] Checking and creating database indexes...")
        
        # Get existing indexes for all collections to avoid unnecessary creation attempts
        existing_indexes = {}
        collections_to_check = ['user_messages', 'deleted_messages', 'edited_messages', 
                              'mentions', 'authorized_hosts', 'hosted_tokens', 'blacklisted_users']
        
        for collection in collections_to_check:
            try:
                indexes = await _global_db[collection].list_indexes().to_list(length=None)
                existing_indexes[collection] = [idx.get('name', '') for idx in indexes]
                logger.info(f"[GlobalDB] Existing indexes for {collection}: {existing_indexes[collection]}")
            except Exception as e:
                logger.debug(f"[GlobalDB] Could not list indexes for {collection}: {e}")
                existing_indexes[collection] = []
        
        # Create indexes in background to avoid blocking startup
        index_tasks = []
        
        def should_create_index(collection: str, index_name: str) -> bool:
            """Check if an index should be created based on existing indexes"""
            exists = index_name in existing_indexes.get(collection, [])
            if not exists:
                logger.info(f"[GlobalDB] Missing index: {collection}.{index_name}")
            return not exists
        
        # Enhanced User collection indexes - REMOVED (User tracking disabled)
        # Old user indexes blocks were here

        # Enhanced Message indexes for high-volume operations
        # Primary message lookup by user with time ordering
        if should_create_index("user_messages", "user_id_1_created_at_-1"):
            index_tasks.append(_global_db.user_messages.create_index([
                ("user_id", 1),
                ("created_at", -1)
            ], background=True))
        
        # Guild-specific message queries
        if should_create_index("user_messages", "guild_id_1_user_id_1_created_at_-1"):
            index_tasks.append(_global_db.user_messages.create_index([
                ("guild_id", 1),
                ("user_id", 1),
                ("created_at", -1)
            ], background=True))
        
        # Channel-specific message queries
        if should_create_index("user_messages", "channel_id_1_created_at_-1"):
            index_tasks.append(_global_db.user_messages.create_index([
                ("channel_id", 1),
                ("created_at", -1)
            ], background=True))
        
        # Message content search index - only create if actually needed for search functionality
        # Commented out as text indexes are expensive - uncomment only if you do content searches
        # index_tasks.append(_global_db.user_messages.create_index([
        #     ("content", "text"),
        #     ("user_id", 1)
        # ], background=True, sparse=True))

        # Enhanced Collection-specific indexes for high-volume operations
        for collection in ['deleted_messages', 'edited_messages', 'mentions']:
            # Primary lookup index
            if should_create_index(collection, "message_id_1"):
                index_tasks.append(_global_db[collection].create_index([("message_id", 1)], unique=True, background=True))
            
            # Compound index for user-based queries with location context
            if should_create_index(collection, "user_id_1_channel_id_1_guild_id_1"):
                index_tasks.append(_global_db[collection].create_index([
                    ("user_id", 1),
                    ("channel_id", 1),
                    ("guild_id", 1)
                ], background=True))
            
            # Time-based queries for recent activity
            if collection == 'deleted_messages':
                if should_create_index(collection, "user_id_1_deleted_at_-1"):
                    index_tasks.append(_global_db[collection].create_index([
                        ("user_id", 1),
                        ("deleted_at", -1)
                    ], background=True))
            elif collection == 'edited_messages':
                if should_create_index(collection, "user_id_1_edited_at_-1"):
                    index_tasks.append(_global_db[collection].create_index([
                        ("user_id", 1),
                        ("edited_at", -1)
                    ], background=True))
            elif collection == 'mentions':
                if should_create_index(collection, "target_id_1_mentioner_id_1_created_at_-1"):
                    index_tasks.append(_global_db[collection].create_index([
                        ("target_id", 1),
                        ("mentioner_id", 1),
                        ("created_at", -1)
                    ], background=True))
            

        # TTL indexes - optimized to avoid redundancy
        # TTL on compound indexes for better query performance
        if should_create_index("deleted_messages", "deleted_at_-1_channel_id_1"):
            index_tasks.append(_global_db.deleted_messages.create_index(
                [("deleted_at", -1), ("channel_id", 1)], 
                expireAfterSeconds=48 * 60 * 60,
                background=True
            ))
        if should_create_index("edited_messages", "edited_at_-1_channel_id_1"):
            index_tasks.append(_global_db.edited_messages.create_index(
                [("edited_at", -1), ("channel_id", 1)],
                expireAfterSeconds=48 * 60 * 60,
                background=True
            ))
        # Mentions TTL - using expires_at field
        if should_create_index("mentions", "expires_at_1"):
            index_tasks.append(_global_db.mentions.create_index(
                [("expires_at", 1)],
                expireAfterSeconds=48 * 60 * 60,
                background=True
            ))
        
        # Additional indexes
        if should_create_index("authorized_hosts", "user_id_1"):
            index_tasks.append(_global_db.authorized_hosts.create_index(
                [("user_id", 1)],
                unique=True,
                background=True
            ))
        
        if should_create_index("hosted_tokens", "host_user_id_1_token_owner_id_1"):
            index_tasks.append(_global_db.hosted_tokens.create_index([
                ("host_user_id", 1),
                ("token_owner_id", 1)
            ], unique=True, background=True))
        
        # Removed redundant single-field index - compound index covers this

        if should_create_index("blacklisted_users", "user_id_1"):
            index_tasks.append(_global_db.blacklisted_users.create_index(
                [("user_id", 1)],
                unique=True,
                background=True
            ))

        # Handle index creation 
        if index_tasks:
            logger.info(f"[GlobalDB] Creating {len(index_tasks)} missing indexes...")
            logger.info("[GlobalDB] Note: users collection uses _id field (already optimally indexed)")
            
            # Execute all indexes without timeout - they're all needed
            results = await asyncio.gather(*index_tasks, return_exceptions=True)
            
            # Check for any errors
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                logger.warning(f"[GlobalDB] {len(errors)} index creation errors: {[str(e)[:100] for e in errors[:2]]}")
            
            success_count = len(results) - len(errors)
            logger.info(f"[GlobalDB] Index creation completed: {success_count}/{len(results)} successful")
            
            if success_count == len(results):
                logger.info("[GlobalDB] ✅ All indexes created successfully - optimal performance enabled!")
        else:
            logger.info("[GlobalDB] All indexes already exist, skipping creation")
    except Exception as e:
        logger.error(f"[GlobalDB] Error creating database indexes: {e}")

async def _global_health_check():
    """Simple periodic health check of global connection"""
    global _global_connection_active
    
    while True:
        try:
            await asyncio.sleep(120)  # Check every 2 minutes
            
            # Quick ping
            await _global_client.admin.command('ping')
            
            if not _global_connection_active:
                logger.info("[GlobalDB] Global MongoDB connection restored")
                _global_connection_active = True
                
        except Exception as e:
            logger.error(f"[GlobalDB] Health check failed: {e}")
            _global_connection_active = False
            
            # If connection is lost, try to reinitialize
            if "connection closed" in str(e).lower():
                logger.warning("[GlobalDB] Connection lost - attempting to reinitialize")
                try:
                    await _cleanup_global_database()
                    await initialize_global_database()
                except Exception as reinit_e:
                    logger.error(f"[GlobalDB] Failed to reinitialize: {reinit_e}")
                    await asyncio.sleep(10)

async def _cleanup_global_database():
    """Clean up global database resources"""
    global _global_client, _global_db, _global_connection_active, _global_health_task
    
    if _global_health_task:
        _global_health_task.cancel()
        try:
            await _global_health_task
        except asyncio.CancelledError:
            pass
        _global_health_task = None

    if _global_client:
        _global_client.close()
        _global_client = None
        _global_db = None
        
    _global_connection_active = False
    logger.info("[GlobalDB] Cleaned up global MongoDB resources")

async def shutdown_global_database():
    """Shutdown the global database connection"""
    logger.info("[GlobalDB] Shutting down global database...")
    await _cleanup_global_database()

# Public API functions - simplified, no locking needed
async def global_find_one(collection: str, query: dict, projection: dict = None):
    """Perform a find_one operation on the global database"""
    if not _global_connection_active:
        raise RuntimeError("[GlobalDB] Global database not active")
    
    return await _global_db[collection].find_one(query, projection=projection)

async def global_write_operation(collection: str, operation: str, *args, **kwargs):
    """Perform a write operation on the global database"""
    if not _global_connection_active:
        raise RuntimeError("[GlobalDB] Global database not active")
    
    wc = WriteConcern(w='majority', j=True)
    coll = _global_db.get_collection(collection, write_concern=wc)
    operation_method = getattr(coll, operation)
    
    return await operation_method(*args, **kwargs)

def get_global_db():
    """Get the global database instance"""
    if not _global_connection_active or _global_db is None:
        raise RuntimeError("[GlobalDB] Global database not initialized or inactive")
    return _global_db

def is_global_db_active():
    """Check if global database is active"""
    return _global_connection_active

# Convenience methods
async def global_update_one(collection: str, filter: dict, update: dict, upsert: bool = False):
    """Update one document in the global database"""
    return await global_write_operation(collection, 'update_one', filter, update, upsert=upsert)

async def global_update_many(collection: str, filter: dict, update: dict, upsert: bool = False):
    """Update many documents in the global database"""
    return await global_write_operation(collection, 'update_many', filter, update, upsert=upsert)

async def global_insert_one(collection: str, document: dict):
    """Insert one document in the global database"""
    return await global_write_operation(collection, 'insert_one', document)

async def global_delete_one(collection: str, filter: dict):
    """Delete one document in the global database"""
    return await global_write_operation(collection, 'delete_one', filter)

# Batch Operations
async def global_bulk_write(collection: str, requests: List[Union[InsertOne, UpdateOne, UpdateMany, DeleteOne, DeleteMany, ReplaceOne]], ordered: bool = False, bypass_document_validation: bool = False):
    """Perform bulk write operations on the global database with connection management"""
    if not _global_connection_active:
        raise RuntimeError("[GlobalDB] Global database not active")
    
    if not requests:
        return None
    
    # Limit batch size to prevent connection timeouts
    max_batch_size = 100
    if len(requests) > max_batch_size:
        logger.warning(f"[GlobalDB] Large batch ({len(requests)} operations) being split for connection stability")
        
        # Split into smaller batches
        results = []
        for i in range(0, len(requests), max_batch_size):
            batch = requests[i:i + max_batch_size]
            result = await global_bulk_write(collection, batch, ordered, bypass_document_validation)
            if result:
                results.append(result)
        return results
    
    wc = WriteConcern(w='majority', j=True)
    coll = _global_db.get_collection(collection, write_concern=wc)
    
    try:
        result = await coll.bulk_write(
            requests, 
            ordered=ordered,
            bypass_document_validation=bypass_document_validation
        )
        
        # Log large operations for monitoring
        if len(requests) > 50:
            logger.debug(f"[GlobalDB] Completed bulk write: {len(requests)} operations to {collection}")
        
        return result
        
    except BulkWriteError as bwe:
        # Log details but let caller handle the error
        error_count = len(bwe.details.get('writeErrors', []))
        logger.warning(f"[GlobalDB] Bulk write error in {collection}: {error_count}/{len(requests)} operations failed")
        raise
    except Exception as e:
        logger.error(f"[GlobalDB] Bulk write failed for {collection}: {e}")
        raise

async def global_insert_many(collection: str, documents: List[Dict[str, Any]], ordered: bool = False, bypass_document_validation: bool = False):
    """Insert many documents in the global database"""
    if not documents:
        return None
        
    requests = [InsertOne(doc) for doc in documents]
    return await global_bulk_write(collection, requests, ordered, bypass_document_validation)

async def global_update_many_bulk(collection: str, operations: List[Dict[str, Any]], ordered: bool = False):
    """Perform multiple update operations in a single bulk write
    
    operations should be a list of dicts with keys: 'filter', 'update', 'upsert' (optional)
    Example: [{'filter': {'_id': 1}, 'update': {'$set': {'name': 'test'}}, 'upsert': True}]
    """
    if not operations:
        return None
        
    requests = []
    for op in operations:
        upsert = op.get('upsert', False)
        requests.append(UpdateOne(op['filter'], op['update'], upsert=upsert))
    
    return await global_bulk_write(collection, requests, ordered)

async def global_find_many(collection: str, query: dict = None, projection: dict = None, sort: List[tuple] = None, limit: int = None, skip: int = None):
    """Find many documents with cursor options"""
    if not _global_connection_active:
        raise RuntimeError("[GlobalDB] Global database not active")
    
    cursor = _global_db[collection].find(query or {}, projection=projection)
    
    if sort:
        cursor = cursor.sort(sort)
    if skip:
        cursor = cursor.skip(skip)
    if limit:
        cursor = cursor.limit(limit)
    
    # Fix: to_list should use None for unlimited length, not limit
    return await cursor.to_list(length=None if limit is None else limit)

async def global_aggregate(collection: str, pipeline: List[Dict[str, Any]], allow_disk_use: bool = True):
    """Perform aggregation on the global database"""
    if not _global_connection_active:
        raise RuntimeError("[GlobalDB] Global database not active")
    
    cursor = _global_db[collection].aggregate(pipeline, allowDiskUse=allow_disk_use)
    return await cursor.to_list(length=None)

async def global_count_documents(collection: str, filter: dict = None):
    """Count documents in the global database"""
    if not _global_connection_active:
        raise RuntimeError("[GlobalDB] Global database not active")
    
    return await _global_db[collection].count_documents(filter or {})

# Batch operation helpers for common patterns
class BatchProcessor:
    """Helper class for batching operations with connection management"""
    
    def __init__(self, collection: str, batch_size: int = 100, ordered: bool = False):
        self.collection = collection
        self.batch_size = batch_size
        self.ordered = ordered
        self.requests = []
        self.stats = {'inserts': 0, 'updates': 0, 'deletes': 0, 'errors': 0, 'batches_executed': 0}
        self._last_execution = None
        self._connection_timeout = 30  # Force connection cleanup after 30s
    
    def add_insert(self, document: Dict[str, Any]):
        """Add an insert operation to the batch"""
        self.requests.append(InsertOne(document))
        self.stats['inserts'] += 1
        return len(self.requests)
    
    def add_update(self, filter: dict, update: dict, upsert: bool = False):
        """Add an update operation to the batch"""
        self.requests.append(UpdateOne(filter, update, upsert=upsert))
        self.stats['updates'] += 1
        return len(self.requests)
    
    def add_delete(self, filter: dict):
        """Add a delete operation to the batch"""
        self.requests.append(DeleteOne(filter))
        self.stats['deletes'] += 1
        return len(self.requests)
    
    def add_replace(self, filter: dict, replacement: dict, upsert: bool = False):
        """Add a replace operation to the batch"""
        self.requests.append(ReplaceOne(filter, replacement, upsert=upsert))
        self.stats['updates'] += 1
        return len(self.requests)
    
    async def execute_batch(self, force: bool = False):
        """Execute the current batch if it's full or if forced"""
        if not self.requests or (not force and len(self.requests) < self.batch_size):
            return None
        
        import time
        current_time = time.time()
        
        # Force execution if connection has been idle too long
        if (self._last_execution and 
            current_time - self._last_execution > self._connection_timeout):
            force = True
        
        try:
            # Make a copy of requests before clearing to avoid issues
            requests_to_execute = self.requests.copy()
            executed_count = len(requests_to_execute)
            self.requests.clear()
            
            # Add connection health check
            if not _global_connection_active:
                logger.warning(f"[BatchProcessor] Global DB inactive, skipping batch of {executed_count} operations")
                return None, 0
            
            result = await global_bulk_write(self.collection, requests_to_execute, self.ordered)
            self._last_execution = current_time
            self.stats['batches_executed'] += 1
            
            # Log batch execution for monitoring
            if executed_count > 10:  # Only log larger batches
                logger.debug(f"[BatchProcessor] Executed batch: {executed_count} operations to {self.collection}")
            
            return result, executed_count
        except BulkWriteError as e:
            self.stats['errors'] += len(e.details.get('writeErrors', []))
            self._last_execution = current_time
            raise
        except Exception as e:
            # Don't clear requests on unexpected errors - let caller handle retry
            logger.error(f"[BatchProcessor] Unexpected error in execute_batch: {e}")
            raise
    
    async def flush(self):
        """Execute any remaining operations in the batch"""
        if self.requests:
            try:
                return await self.execute_batch(force=True)
            finally:
                # Always clear requests on flush to prevent memory leaks
                self.requests.clear()
        return None
    
    def size(self):
        """Get current batch size"""
        return len(self.requests)
    
    def is_ready(self):
        """Check if batch is ready for execution"""
        return len(self.requests) >= self.batch_size
    
    def get_stats(self):
        """Get operation statistics with additional metrics"""
        stats = self.stats.copy()
        stats['pending_operations'] = len(self.requests)
        stats['collection'] = self.collection
        return stats
    
    def reset_stats(self):
        """Reset statistics counters"""
        self.stats = {'inserts': 0, 'updates': 0, 'deletes': 0, 'errors': 0, 'batches_executed': 0}

# Convenience function to create batch processors
def create_batch_processor(collection: str, batch_size: int = 100, ordered: bool = False) -> BatchProcessor:
    """Create a new batch processor for the given collection"""
    return BatchProcessor(collection, batch_size, ordered)

# Utility function to handle bulk write result parsing
def parse_bulk_result(result, operation_type: str = "operation"):
    """Parse bulk write results, handling both single results and lists from batch splitting
    
    Args:
        result: Result from bulk_write operation (single result or list)
        operation_type: Type of operation for logging (e.g., "delete", "update", "insert")
        
    Returns:
        dict: Summary of the operation with counts
    """
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
