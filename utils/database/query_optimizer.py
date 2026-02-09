"""
Query optimization engine for MongoDB operations.
Ensures optimal index utilization and query performance.
"""
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class QueryOptimizer:
    """
    Advanced query optimizer that:
    - Adds appropriate index hints
    - Optimizes projections
    - Rewrites queries for better performance
    - Monitors query execution plans
    """
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
        # Index hint mappings for common query patterns
        self.index_hints = {
            'users': {
                'user_id_lookup': {'user_id': 1},
                'username_search': 'name_search_text_index',
                'recent_activity': {'last_seen': -1, 'user_id': 1},
                'guild_activity': {'guild_count': -1, 'last_seen': -1}
            },
            'user_messages': {
                'user_messages_recent': {'user_id': 1, 'created_at': -1},
                'channel_messages': {'guild_id': 1, 'channel_id': 1, 'created_at': -1},
                'message_content_search': 'message_content_text'
            },
            'deleted_messages': {
                'user_deleted': {'user_id': 1, 'deleted_at': -1},
                'channel_deleted': {'guild_id': 1, 'channel_id': 1, 'deleted_at': -1},
                'recent_deleted': {'deleted_at': -1}
            },
            'edited_messages': {
                'user_edited': {'user_id': 1, 'edited_at': -1},
                'channel_edited': {'guild_id': 1, 'channel_id': 1, 'edited_at': -1}
            },
            'mentions': {
                'user_mentions': {'target_id': 1, 'created_at': -1},
                'mentioner_history': {'mentioner_id': 1, 'created_at': -1},
                'channel_mentions': {'guild_id': 1, 'channel_id': 1, 'created_at': -1}
            }
        }
        
        # Optimal projections for common operations
        self.optimal_projections = {
            'user_basic': {
                'user_id': 1, 'current_username': 1, 'current_displayname': 1,
                'current_avatar': 1, 'last_seen': 1, 'first_seen': 1
            },
            'user_search': {
                'user_id': 1, 'current_username': 1, 'current_displayname': 1,
                'username_history': {'$slice': 5}, 'displayname_history': {'$slice': 5}
            },
            'message_basic': {
                'message_id': 1, 'user_id': 1, 'content': 1, 'created_at': 1,
                'guild_id': 1, 'channel_id': 1
            },
            'message_minimal': {
                'user_id': 1, 'created_at': 1, 'guild_id': 1
            },
            'mention_basic': {
                'target_id': 1, 'mentioner_id': 1, 'created_at': 1,
                'content': 1, 'message_id': 1
            }
        }

    def optimize_query(self, collection: str, operation: str, query: Dict[str, Any], 
                      projection: Dict[str, Any] = None, sort: List[Tuple] = None,
                      limit: int = None, skip: int = None) -> Dict[str, Any]:
        """
        Optimize a query by adding hints, improving projections, and rewriting conditions
        """
        optimized = {
            'query': query.copy(),
            'projection': projection.copy() if projection else None,
            'sort': sort.copy() if sort else None,
            'limit': limit,
            'skip': skip,
            'hint': None,
            'optimizations': []
        }
        
        # Add index hint
        hint = self._determine_index_hint(collection, query, sort)
        if hint:
            optimized['hint'] = hint
            optimized['optimizations'].append(f'Added index hint: {hint}')
        
        # Optimize projection
        if not projection:
            suggested_projection = self._suggest_projection(collection, operation)
            if suggested_projection:
                optimized['projection'] = suggested_projection
                optimized['optimizations'].append('Added optimal projection')
        
        # Optimize query conditions
        optimized_query = self._optimize_query_conditions(collection, query)
        if optimized_query != query:
            optimized['query'] = optimized_query
            optimized['optimizations'].append('Optimized query conditions')
        
        # Optimize sort
        if sort:
            optimized_sort = self._optimize_sort_conditions(collection, sort, query)
            if optimized_sort != sort:
                optimized['sort'] = optimized_sort
                optimized['optimizations'].append('Optimized sort conditions')
        
        return optimized

    def _determine_index_hint(self, collection: str, query: Dict[str, Any], 
                            sort: List[Tuple] = None) -> Optional[Any]:
        """Determine the best index hint for a query"""
        if collection not in self.index_hints:
            return None
        
        collection_hints = self.index_hints[collection]
        
        # Check for specific query patterns
        query_fields = set(query.keys())
        
        # User ID lookups
        if 'user_id' in query_fields and len(query_fields) == 1:
            return collection_hints.get('user_id_lookup')
        
        # Text search queries
        if any('$text' in str(v) for v in query.values()) or \
           any(field in ['current_username', 'current_displayname'] for field in query_fields):
            return collection_hints.get('username_search')
        
        # Time-based queries with user
        if 'user_id' in query_fields and sort and sort[0][0] in ['created_at', 'last_seen', 'edited_at', 'deleted_at']:
            if sort[0][0] == 'created_at':
                return collection_hints.get('user_messages_recent')
            elif sort[0][0] == 'last_seen':
                return collection_hints.get('recent_activity')
            elif sort[0][0] == 'deleted_at':
                return collection_hints.get('user_deleted')
            elif sort[0][0] == 'edited_at':
                return collection_hints.get('user_edited')
        
        # Channel-based queries
        if 'guild_id' in query_fields and 'channel_id' in query_fields:
            if collection == 'user_messages':
                return collection_hints.get('channel_messages')
            elif collection == 'deleted_messages':
                return collection_hints.get('channel_deleted')
            elif collection == 'edited_messages':
                return collection_hints.get('channel_edited')
            elif collection == 'mentions':
                return collection_hints.get('channel_mentions')
        
        # Mention-specific queries
        if collection == 'mentions':
            if 'target_id' in query_fields:
                return collection_hints.get('user_mentions')
            elif 'mentioner_id' in query_fields:
                return collection_hints.get('mentioner_history')
        
        return None

    def _suggest_projection(self, collection: str, operation: str) -> Optional[Dict[str, Any]]:
        """Suggest optimal projection based on collection and operation"""
        projection_map = {
            ('users', 'find_one'): 'user_basic',
            ('users', 'find_many'): 'user_basic',
            ('users', 'search'): 'user_search',
            ('user_messages', 'find_many'): 'message_basic',
            ('user_messages', 'recent'): 'message_minimal',
            ('mentions', 'find_many'): 'mention_basic',
            ('deleted_messages', 'find_many'): 'message_basic',
            ('edited_messages', 'find_many'): 'message_basic'
        }
        
        projection_key = projection_map.get((collection, operation))
        return self.optimal_projections.get(projection_key) if projection_key else None

    def _optimize_query_conditions(self, collection: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize query conditions for better index utilization"""
        optimized = query.copy()
        
        # Convert string IDs to integers for better index performance
        id_fields = ['user_id', 'guild_id', 'channel_id', 'message_id', 'target_id', 'mentioner_id']
        for field in id_fields:
            if field in optimized and isinstance(optimized[field], str):
                try:
                    optimized[field] = int(optimized[field])
                except (ValueError, TypeError):
                    pass
        
        # Optimize date range queries
        if collection in ['user_messages', 'deleted_messages', 'edited_messages', 'mentions']:
            date_field = {
                'user_messages': 'created_at',
                'deleted_messages': 'deleted_at',
                'edited_messages': 'edited_at',
                'mentions': 'created_at'
            }[collection]
            
            # Add implicit date range for recent data if not specified
            if date_field not in optimized and 'limit' in str(query).lower():
                # Add recent data filter (last 30 days) to improve index utilization
                recent_date = datetime.utcnow() - timedelta(days=30)
                optimized[date_field] = {'$gte': recent_date}
        
        # Optimize text search queries
        if collection == 'users':
            text_fields = ['current_username', 'current_displayname']
            for field in text_fields:
                if field in optimized and isinstance(optimized[field], str):
                    # Convert simple string matches to regex for case-insensitive search
                    if not optimized[field].startswith('$'):
                        optimized[field] = {'$regex': optimized[field], '$options': 'i'}
        
        return optimized

    def _optimize_sort_conditions(self, collection: str, sort: List[Tuple], 
                                query: Dict[str, Any]) -> List[Tuple]:
        """Optimize sort conditions to match available indexes"""
        if not sort:
            return sort
        
        optimized_sort = sort.copy()
        
        # For user queries with time-based sorting, ensure proper compound index usage
        if collection in ['user_messages', 'deleted_messages', 'edited_messages'] and 'user_id' in query:
            # If sorting by time, make sure user_id is considered for compound index
            time_fields = ['created_at', 'deleted_at', 'edited_at']
            if optimized_sort and optimized_sort[0][0] in time_fields:
                # The compound index (user_id, created_at) will be used automatically
                pass
        
        # For mentions, optimize target_id + created_at sorting
        if collection == 'mentions' and 'target_id' in query:
            if optimized_sort and optimized_sort[0][0] == 'created_at':
                # Compound index (target_id, created_at) will be used
                pass
        
        return optimized_sort

    async def explain_query(self, collection: str, query: Dict[str, Any], 
                          projection: Dict[str, Any] = None, sort: List[Tuple] = None,
                          limit: int = None) -> Dict[str, Any]:
        """
        Explain query execution plan to verify index usage
        """
        try:
            db = self.db_manager.db
            cursor = db[collection].find(query, projection)
            
            if sort:
                cursor = cursor.sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            
            # Get the query plan
            plan = await cursor.explain()
            
            # Extract relevant information
            execution_stats = plan.get('executionStats', {})
            winning_plan = plan.get('queryPlanner', {}).get('winningPlan', {})
            
            analysis = {
                'collection': collection,
                'query': query,
                'index_used': self._extract_index_info(winning_plan),
                'execution_time': execution_stats.get('executionTimeMillis', 0),
                'docs_examined': execution_stats.get('totalDocsExamined', 0),
                'docs_returned': execution_stats.get('totalDocsReturned', 0),
                'is_covered_query': execution_stats.get('totalDocsExamined', 0) == 0,
                'efficiency_ratio': self._calculate_efficiency(execution_stats)
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"[QueryOptimizer] Error explaining query: {e}")
            return {'error': str(e)}

    def _extract_index_info(self, winning_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Extract index information from query plan"""
        if 'inputStage' in winning_plan:
            input_stage = winning_plan['inputStage']
            if input_stage.get('stage') == 'IXSCAN':
                return {
                    'type': 'index_scan',
                    'index_name': input_stage.get('indexName'),
                    'keys_examined': input_stage.get('keysExamined', 0),
                    'direction': input_stage.get('direction', 'forward')
                }
            elif input_stage.get('stage') == 'COLLSCAN':
                return {
                    'type': 'collection_scan',
                    'warning': 'Full collection scan - consider adding index'
                }
        
        return {'type': 'unknown'}

    def _calculate_efficiency(self, execution_stats: Dict[str, Any]) -> float:
        """Calculate query efficiency ratio"""
        docs_examined = execution_stats.get('totalDocsExamined', 0)
        docs_returned = execution_stats.get('totalDocsReturned', 0)
        
        if docs_examined == 0:
            return 1.0  # Covered query - perfect efficiency
        
        return docs_returned / docs_examined if docs_examined > 0 else 0.0

    def create_optimized_aggregation_pipeline(self, collection: str, 
                                            operations: List[str]) -> List[Dict[str, Any]]:
        """Create optimized aggregation pipeline"""
        pipeline = []
        
        # Add $match as early as possible for index utilization
        if 'filter' in operations:
            pipeline.append({'$match': {}})  # Placeholder - will be filled by caller
        
        # Add $sort before $limit for better index usage
        if 'sort' in operations:
            pipeline.append({'$sort': {}})  # Placeholder
        
        # Add $limit early to reduce document processing
        if 'limit' in operations:
            pipeline.append({'$limit': 1000})  # Default limit
        
        # Add projections to reduce data transfer
        if 'project' in operations:
            pipeline.append({'$project': {}})  # Placeholder
        
        # Add grouping operations
        if 'group' in operations:
            pipeline.append({'$group': {}})  # Placeholder
        
        return pipeline

    def get_index_suggestions(self, collection: str, 
                            recent_queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze queries and suggest new indexes"""
        suggestions = []
        
        # Analyze query patterns
        field_combinations = {}
        sort_patterns = {}
        
        for query_info in recent_queries:
            query = query_info.get('query', {})
            sort = query_info.get('sort', [])
            
            # Track field combinations
            query_fields = tuple(sorted(query.keys()))
            if query_fields not in field_combinations:
                field_combinations[query_fields] = 0
            field_combinations[query_fields] += 1
            
            # Track sort patterns
            if sort:
                sort_pattern = tuple(sort)
                if sort_pattern not in sort_patterns:
                    sort_patterns[sort_pattern] = 0
                sort_patterns[sort_pattern] += 1
        
        # Suggest compound indexes for common field combinations
        for fields, count in field_combinations.items():
            if count >= 5 and len(fields) > 1:  # Suggest if used 5+ times
                index_spec = {(field, 1) for field in fields}
                suggestions.append({
                    'type': 'compound_index',
                    'collection': collection,
                    'fields': list(index_spec),
                    'usage_count': count,
                    'reason': f'Frequently queried fields: {", ".join(fields)}'
                })
        
        # Suggest indexes for common sort patterns
        for sort_pattern, count in sort_patterns.items():
            if count >= 3:  # Suggest if used 3+ times
                suggestions.append({
                    'type': 'sort_index',
                    'collection': collection,
                    'fields': list(sort_pattern),
                    'usage_count': count,
                    'reason': f'Frequently used sort pattern'
                })
        
        return suggestions

# Global optimizer instance
_optimizer = None

def get_query_optimizer(db_manager) -> QueryOptimizer:
    """Get or create query optimizer instance"""
    global _optimizer
    if _optimizer is None:
        _optimizer = QueryOptimizer(db_manager)
    return _optimizer