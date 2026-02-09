"""
Aggregation pipeline optimizer for complex MongoDB operations.
Provides pre-built, optimized pipelines for common Discord selfbot analytics.
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AggregationOptimizer:
    """
    Optimized aggregation pipelines for Discord selfbot operations:
    - User activity analytics
    - Message statistics
    - Guild participation metrics
    - Performance-optimized stages
    """
    
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def user_activity_summary(self, user_id: int, days: int = 30) -> List[Dict[str, Any]]:
        """
        Optimized pipeline for user activity summary with message counts, guilds active in, etc.
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return [
            # Stage 1: Match user and recent messages (optimized for user_id + created_at index)
            {
                '$match': {
                    'user_id': user_id,
                    'created_at': {'$gte': cutoff_date}
                }
            },
            # Stage 2: Group by day and guild to get daily activity
            {
                '$group': {
                    '_id': {
                        'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$created_at'}},
                        'guild_id': '$guild_id'
                    },
                    'message_count': {'$sum': 1},
                    'unique_channels': {'$addToSet': '$channel_id'},
                    'first_message': {'$min': '$created_at'},
                    'last_message': {'$max': '$created_at'}
                }
            },
            # Stage 3: Calculate per-day metrics
            {
                '$group': {
                    '_id': '$_id.date',
                    'total_messages': {'$sum': '$message_count'},
                    'guilds_active': {'$addToSet': '$_id.guild_id'},
                    'total_channels': {'$sum': {'$size': '$unique_channels'}},
                    'activity_span': {
                        '$max': {
                            '$subtract': ['$last_message', '$first_message']
                        }
                    }
                }
            },
            # Stage 4: Add computed fields
            {
                '$addFields': {
                    'guild_count': {'$size': '$guilds_active'},
                    'activity_hours': {'$divide': ['$activity_span', 3600000]}  # Convert to hours
                }
            },
            # Stage 5: Sort by date
            {
                '$sort': {'_id': -1}
            },
            # Stage 6: Project final format
            {
                '$project': {
                    'date': '$_id',
                    'messages': '$total_messages',
                    'guilds': '$guild_count',
                    'channels': '$total_channels',
                    'activity_hours': {'$round': ['$activity_hours', 2]},
                    '_id': 0
                }
            }
        ]

    def guild_user_leaderboard(self, guild_id: int, days: int = 7, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Optimized pipeline for guild user activity leaderboard
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return [
            # Stage 1: Match guild and recent messages (uses index: guild_id + created_at)
            {
                '$match': {
                    'guild_id': guild_id,
                    'created_at': {'$gte': cutoff_date}
                }
            },
            # Stage 2: Group by user
            {
                '$group': {
                    '_id': '$user_id',
                    'message_count': {'$sum': 1},
                    'unique_channels': {'$addToSet': '$channel_id'},
                    'first_message': {'$min': '$created_at'},
                    'last_message': {'$max': '$created_at'},
                    'total_chars': {'$sum': {'$strLenCP': '$content'}}
                }
            },
            # Stage 3: Add computed metrics
            {
                '$addFields': {
                    'channel_count': {'$size': '$unique_channels'},
                    'avg_message_length': {'$divide': ['$total_chars', '$message_count']},
                    'activity_span_hours': {
                        '$divide': [
                            {'$subtract': ['$last_message', '$first_message']},
                            3600000
                        ]
                    }
                }
            },
            # Stage 4: Sort by message count
            {
                '$sort': {'message_count': -1}
            },
            # Stage 5: Limit results
            {
                '$limit': limit
            },
            # Stage 6: Lookup user details (only for top users to minimize lookups)
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': 'user_id',
                    'as': 'user_info',
                    'pipeline': [
                        {
                            '$project': {
                                'current_username': 1,
                                'current_displayname': 1,
                                'current_avatar': 1
                            }
                        }
                    ]
                }
            },
            # Stage 7: Project final format
            {
                '$project': {
                    'user_id': '$_id',
                    'username': {'$arrayElemAt': ['$user_info.current_username', 0]},
                    'displayname': {'$arrayElemAt': ['$user_info.current_displayname', 0]},
                    'messages': '$message_count',
                    'channels': '$channel_count',
                    'avg_length': {'$round': ['$avg_message_length', 1]},
                    'activity_hours': {'$round': ['$activity_span_hours', 1]},
                    '_id': 0
                }
            }
        ]

    def message_timeline_analysis(self, user_id: int, days: int = 30) -> List[Dict[str, Any]]:
        """
        Optimized pipeline for user message timeline analysis (hourly breakdown)
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return [
            # Stage 1: Match user and recent messages
            {
                '$match': {
                    'user_id': user_id,
                    'created_at': {'$gte': cutoff_date}
                }
            },
            # Stage 2: Group by hour of day
            {
                '$group': {
                    '_id': {'$hour': '$created_at'},
                    'message_count': {'$sum': 1},
                    'unique_guilds': {'$addToSet': '$guild_id'},
                    'avg_length': {'$avg': {'$strLenCP': '$content'}}
                }
            },
            # Stage 3: Add hour formatting
            {
                '$addFields': {
                    'hour': '$_id',
                    'hour_formatted': {
                        '$concat': [
                            {'$cond': [{'$lt': ['$_id', 10]}, '0', '']},
                            {'$toString': '$_id'},
                            ':00'
                        ]
                    },
                    'guild_count': {'$size': '$unique_guilds'}
                }
            },
            # Stage 4: Sort by hour
            {
                '$sort': {'hour': 1}
            },
            # Stage 5: Project final format
            {
                '$project': {
                    'hour': '$hour_formatted',
                    'messages': '$message_count',
                    'guilds': '$guild_count',
                    'avg_length': {'$round': ['$avg_length', 1]},
                    '_id': 0
                }
            }
        ]

    def user_mention_analysis(self, user_id: int, days: int = 30) -> List[Dict[str, Any]]:
        """
        Optimized pipeline for analyzing who mentions the user most
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return [
            # Stage 1: Match mentions of the user
            {
                '$match': {
                    'target_id': user_id,
                    'created_at': {'$gte': cutoff_date}
                }
            },
            # Stage 2: Group by mentioner
            {
                '$group': {
                    '_id': '$mentioner_id',
                    'mention_count': {'$sum': 1},
                    'unique_guilds': {'$addToSet': '$guild_id'},
                    'unique_channels': {'$addToSet': '$channel_id'},
                    'first_mention': {'$min': '$created_at'},
                    'last_mention': {'$max': '$created_at'}
                }
            },
            # Stage 3: Add computed fields
            {
                '$addFields': {
                    'guild_count': {'$size': '$unique_guilds'},
                    'channel_count': {'$size': '$unique_channels'}
                }
            },
            # Stage 4: Sort by mention count
            {
                '$sort': {'mention_count': -1}
            },
            # Stage 5: Limit to top 20
            {
                '$limit': 20
            },
            # Stage 6: Lookup mentioner details
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': 'user_id',
                    'as': 'mentioner_info',
                    'pipeline': [
                        {
                            '$project': {
                                'current_username': 1,
                                'current_displayname': 1
                            }
                        }
                    ]
                }
            },
            # Stage 7: Project final format
            {
                '$project': {
                    'mentioner_id': '$_id',
                    'username': {'$arrayElemAt': ['$mentioner_info.current_username', 0]},
                    'displayname': {'$arrayElemAt': ['$mentioner_info.current_displayname', 0]},
                    'mentions': '$mention_count',
                    'guilds': '$guild_count',
                    'channels': '$channel_count',
                    'first_mention': '$first_mention',
                    'last_mention': '$last_mention',
                    '_id': 0
                }
            }
        ]

    def deleted_messages_analysis(self, days: int = 7, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Optimized pipeline for analyzing recent deleted messages patterns
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return [
            # Stage 1: Match recent deletions
            {
                '$match': {
                    'deleted_at': {'$gte': cutoff_date}
                }
            },
            # Stage 2: Group by user
            {
                '$group': {
                    '_id': '$user_id',
                    'deleted_count': {'$sum': 1},
                    'unique_guilds': {'$addToSet': '$guild_id'},
                    'unique_channels': {'$addToSet': '$channel_id'},
                    'avg_content_length': {'$avg': {'$strLenCP': '$content'}},
                    'deletion_times': {'$push': '$deleted_at'}
                }
            },
            # Stage 3: Add computed metrics
            {
                '$addFields': {
                    'guild_count': {'$size': '$unique_guilds'},
                    'channel_count': {'$size': '$unique_channels'},
                    'avg_length': {'$round': ['$avg_content_length', 1]}
                }
            },
            # Stage 4: Filter users with significant deletions
            {
                '$match': {
                    'deleted_count': {'$gte': 3}  # Only users with 3+ deletions
                }
            },
            # Stage 5: Sort by deletion count
            {
                '$sort': {'deleted_count': -1}
            },
            # Stage 6: Limit results
            {
                '$limit': limit
            },
            # Stage 7: Lookup user details
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': 'user_id',
                    'as': 'user_info',
                    'pipeline': [
                        {
                            '$project': {
                                'current_username': 1,
                                'current_displayname': 1
                            }
                        }
                    ]
                }
            },
            # Stage 8: Project final format
            {
                '$project': {
                    'user_id': '$_id',
                    'username': {'$arrayElemAt': ['$user_info.current_username', 0]},
                    'displayname': {'$arrayElemAt': ['$user_info.current_displayname', 0]},
                    'deletions': '$deleted_count',
                    'guilds': '$guild_count',
                    'channels': '$channel_count',
                    'avg_message_length': '$avg_length',
                    '_id': 0
                }
            }
        ]

    def guild_activity_overview(self, guild_id: int, days: int = 30) -> List[Dict[str, Any]]:
        """
        Optimized pipeline for guild activity overview with channel breakdowns
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return [
            # Stage 1: Match guild and recent messages
            {
                '$match': {
                    'guild_id': guild_id,
                    'created_at': {'$gte': cutoff_date}
                }
            },
            # Stage 2: Group by channel
            {
                '$group': {
                    '_id': '$channel_id',
                    'message_count': {'$sum': 1},
                    'unique_users': {'$addToSet': '$user_id'},
                    'total_chars': {'$sum': {'$strLenCP': '$content'}},
                    'first_message': {'$min': '$created_at'},
                    'last_message': {'$max': '$created_at'}
                }
            },
            # Stage 3: Add computed metrics
            {
                '$addFields': {
                    'user_count': {'$size': '$unique_users'},
                    'avg_message_length': {'$divide': ['$total_chars', '$message_count']},
                    'activity_span_days': {
                        '$divide': [
                            {'$subtract': ['$last_message', '$first_message']},
                            86400000  # Convert to days
                        ]
                    }
                }
            },
            # Stage 4: Sort by message count
            {
                '$sort': {'message_count': -1}
            },
            # Stage 5: Add ranking
            {
                '$group': {
                    '_id': null,
                    'channels': {'$push': '$$ROOT'}
                }
            },
            # Stage 6: Unwind with index for ranking
            {
                '$unwind': {
                    'path': '$channels',
                    'includeArrayIndex': 'rank'
                }
            },
            # Stage 7: Project final format with rank
            {
                '$project': {
                    'channel_id': '$channels._id',
                    'rank': {'$add': ['$rank', 1]},
                    'messages': '$channels.message_count',
                    'users': '$channels.user_count',
                    'avg_length': {'$round': ['$channels.avg_message_length', 1]},
                    'activity_days': {'$round': ['$channels.activity_span_days', 1]},
                    'first_message': '$channels.first_message',
                    'last_message': '$channels.last_message',
                    '_id': 0
                }
            }
        ]

    def user_growth_trend(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        Optimized pipeline for tracking new user registrations/first appearances
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return [
            # Stage 1: Match users first seen in the period
            {
                '$match': {
                    'first_seen': {'$gte': cutoff_date}
                }
            },
            # Stage 2: Group by day
            {
                '$group': {
                    '_id': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$first_seen'
                        }
                    },
                    'new_users': {'$sum': 1},
                    'user_ids': {'$push': '$user_id'}
                }
            },
            # Stage 3: Sort by date
            {
                '$sort': {'_id': 1}
            },
            # Stage 4: Add cumulative count
            {
                '$group': {
                    '_id': null,
                    'daily_data': {'$push': '$$ROOT'}
                }
            },
            # Stage 5: Calculate running total
            {
                '$project': {
                    'daily_data': {
                        '$map': {
                            'input': {'$range': [0, {'$size': '$daily_data'}]},
                            'in': {
                                'date': {'$arrayElemAt': ['$daily_data._id', '$$this']},
                                'new_users': {'$arrayElemAt': ['$daily_data.new_users', '$$this']},
                                'cumulative': {
                                    '$sum': {
                                        '$slice': [
                                            '$daily_data.new_users',
                                            0,
                                            {'$add': ['$$this', 1]}
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            },
            # Stage 6: Unwind for final output
            {
                '$unwind': '$daily_data'
            },
            # Stage 7: Project final format
            {
                '$project': {
                    'date': '$daily_data.date',
                    'new_users': '$daily_data.new_users',
                    'total_users': '$daily_data.cumulative',
                    '_id': 0
                }
            }
        ]

    async def execute_optimized_aggregation(self, collection: str, 
                                          pipeline: List[Dict[str, Any]], 
                                          cache_key: str = None,
                                          cache_ttl: int = 300,
                                          hint_index: Dict[str, int] = None) -> List[Dict[str, Any]]:
        """
        Execute an optimized aggregation pipeline with optional caching and index hints
        """
        try:
            # Optimize aggregation options for high-volume operations
            options = {
                'allowDiskUse': True,  # Allow spilling to disk for large operations
                'maxTimeMS': 60000,    # 60 second timeout for complex aggregations
                'batchSize': 1000      # Optimize batch size for memory usage
            }
            
            # Add index hint if provided
            if hint_index:
                options['hint'] = hint_index
            
            # Execute the aggregation
            db = self.db_manager.db
            cursor = db[collection].aggregate(pipeline, **options)
            result = await cursor.to_list(length=None)
            
            logger.info(f"[AggregationOptimizer] Executed pipeline on {collection}: {len(result)} results")
            return result
            
        except Exception as e:
            logger.error(f"[AggregationOptimizer] Pipeline execution failed: {e}")
            return []

    def create_text_search_pipeline(self, collection: str, search_term: str, 
                                   limit: int = 50) -> List[Dict[str, Any]]:
        """
        Optimized text search pipeline with scoring
        """
        return [
            # Stage 1: Text search with index
            {
                '$match': {
                    '$text': {
                        '$search': search_term,
                        '$caseSensitive': False
                    }
                }
            },
            # Stage 2: Add search score
            {
                '$addFields': {
                    'search_score': {'$meta': 'textScore'}
                }
            },
            # Stage 3: Sort by relevance
            {
                '$sort': {
                    'search_score': -1,
                    'created_at': -1  # Secondary sort for recency
                }
            },
            # Stage 4: Limit results
            {
                '$limit': limit
            },
            # Stage 5: Remove internal score field
            {
                '$project': {
                    'search_score': 0
                }
            }
        ]

# Global aggregation optimizer instance
_agg_optimizer = None

def get_aggregation_optimizer(db_manager) -> AggregationOptimizer:
    """Get or create aggregation optimizer instance"""
    global _agg_optimizer
    if _agg_optimizer is None:
        _agg_optimizer = AggregationOptimizer(db_manager)
    return _agg_optimizer