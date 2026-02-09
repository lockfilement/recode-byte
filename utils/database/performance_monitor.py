"""
Performance monitoring and query analysis for MongoDB operations.
Tracks query performance, index usage, and provides optimization recommendations.
"""
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class QueryPerformanceMetric:
    """Performance metric for a single query execution"""
    collection: str
    operation: str
    query_signature: str
    execution_time: float
    docs_examined: int
    docs_returned: int
    index_used: Optional[str]
    timestamp: datetime
    cache_hit: bool = False
    optimization_suggestions: List[str] = field(default_factory=list)

class PerformanceMonitor:
    """
    Comprehensive performance monitoring system:
    - Query execution tracking
    - Index utilization analysis
    - Slow query detection
    - Performance trend analysis
    - Optimization recommendations
    """
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
        # Performance tracking
        self.query_metrics: deque = deque(maxlen=10000)  # Last 10k queries
        self.slow_queries: deque = deque(maxlen=1000)    # Last 1k slow queries
        self.index_usage: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # Performance thresholds
        self.slow_query_threshold = 100  # milliseconds
        self.efficiency_threshold = 0.1  # docs_returned / docs_examined
        
        # Monitoring state
        self._monitoring_active = False
        self._monitoring_task: Optional[asyncio.Task] = None
        self._profile_collection = None
        
        # Statistics
        self.stats = {
            'total_queries': 0,
            'slow_queries': 0,
            'cache_hits': 0,
            'index_scans': 0,
            'collection_scans': 0,
            'avg_execution_time': 0.0,
            'queries_per_second': 0.0
        }
        
        # Query pattern analysis
        self.query_patterns = defaultdict(int)
        self.collection_access_patterns = defaultdict(lambda: defaultdict(int))

    async def start_monitoring(self):
        """Start performance monitoring"""
        if not self._monitoring_active:
            self._monitoring_active = True
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            logger.info("[PerfMonitor] Started performance monitoring")

    async def stop_monitoring(self):
        """Stop performance monitoring"""
        self._monitoring_active = False
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("[PerfMonitor] Stopped performance monitoring")

    def record_query(self, collection: str, operation: str, query: Dict[str, Any],
                    execution_time: float, result_count: int = 0, 
                    cache_hit: bool = False, explain_data: Dict[str, Any] = None):
        """Record a query execution for performance analysis"""
        
        # Create query signature for pattern analysis
        query_signature = self._create_query_signature(collection, operation, query)
        
        # Extract execution stats from explain data
        docs_examined = 0
        docs_returned = result_count
        index_used = None
        
        if explain_data:
            execution_stats = explain_data.get('executionStats', {})
            docs_examined = execution_stats.get('totalDocsExamined', 0)
            docs_returned = execution_stats.get('totalDocsReturned', result_count)
            
            winning_plan = explain_data.get('queryPlanner', {}).get('winningPlan', {})
            index_used = self._extract_index_name(winning_plan)
        
        # Create performance metric
        metric = QueryPerformanceMetric(
            collection=collection,
            operation=operation,
            query_signature=query_signature,
            execution_time=execution_time,
            docs_examined=docs_examined,
            docs_returned=docs_returned,
            index_used=index_used,
            timestamp=datetime.utcnow(),
            cache_hit=cache_hit
        )
        
        # Analyze and add optimization suggestions
        metric.optimization_suggestions = self._analyze_query_performance(metric)
        
        # Record the metric
        self.query_metrics.append(metric)
        
        # Update statistics
        self._update_statistics(metric)
        
        # Track slow queries
        if execution_time > self.slow_query_threshold:
            self.slow_queries.append(metric)
            self.stats['slow_queries'] += 1
            logger.warning(f"[PerfMonitor] Slow query detected: {collection}.{operation} took {execution_time:.2f}ms")
        
        # Update query patterns
        self.query_patterns[query_signature] += 1
        self.collection_access_patterns[collection][operation] += 1
        
        # Update index usage tracking
        if index_used:
            self.index_usage[collection][index_used] += 1
            self.stats['index_scans'] += 1
        else:
            self.stats['collection_scans'] += 1
            # Log problematic queries for debugging
            if execution_time > 50:  # Log slow queries without indexes
                logger.warning(f"[PerfMonitor] Slow collection scan: {collection}.{operation} "
                             f"({execution_time:.1f}ms) Query: {query}")

    def _create_query_signature(self, collection: str, operation: str, query: Dict[str, Any]) -> str:
        """Create a signature for query pattern analysis"""
        # Extract field patterns from query
        field_patterns = []
        for field, value in query.items():
            if field.startswith('$'):
                field_patterns.append(field)
            elif isinstance(value, dict):
                # Handle operators like {field: {$gt: value}}
                operators = [k for k in value.keys() if k.startswith('$')]
                if operators:
                    field_patterns.append(f"{field}:{':'.join(operators)}")
                else:
                    field_patterns.append(field)
            else:
                field_patterns.append(field)
        
        signature = f"{collection}.{operation}({','.join(sorted(field_patterns))})"
        return signature

    def _extract_index_name(self, winning_plan: Dict[str, Any]) -> Optional[str]:
        """Extract index name from query execution plan"""
        if 'inputStage' in winning_plan:
            input_stage = winning_plan['inputStage']
            if input_stage.get('stage') == 'IXSCAN':
                return input_stage.get('indexName')
        elif winning_plan.get('stage') == 'IXSCAN':
            return winning_plan.get('indexName')
        
        return None

    def _analyze_query_performance(self, metric: QueryPerformanceMetric) -> List[str]:
        """Analyze query performance and provide optimization suggestions"""
        suggestions = []
        
        # Check execution time
        if metric.execution_time > self.slow_query_threshold:
            suggestions.append(f"Query execution time ({metric.execution_time:.2f}ms) exceeds threshold")
        
        # Check index usage
        if not metric.index_used:
            suggestions.append("Query performed collection scan - consider adding index")
        
        # Check efficiency ratio
        if metric.docs_examined > 0:
            efficiency = metric.docs_returned / metric.docs_examined
            if efficiency < self.efficiency_threshold:
                suggestions.append(f"Low efficiency ratio ({efficiency:.3f}) - query examines too many documents")
        
        # Check for potential optimizations based on collection and operation
        if metric.collection == 'users' and 'user_id' not in metric.query_signature:
            suggestions.append("Consider adding user_id filter for better performance")
        
        if metric.operation == 'find_many' and metric.docs_returned > 1000:
            suggestions.append("Large result set - consider adding limit or pagination")
        
        return suggestions

    def _update_statistics(self, metric: QueryPerformanceMetric):
        """Update running statistics"""
        self.stats['total_queries'] += 1
        
        if metric.cache_hit:
            self.stats['cache_hits'] += 1
        
        # Update average execution time
        current_avg = self.stats['avg_execution_time']
        total_queries = self.stats['total_queries']
        self.stats['avg_execution_time'] = (
            (current_avg * (total_queries - 1) + metric.execution_time) / total_queries
        )

    async def _monitoring_loop(self):
        """Background monitoring loop"""
        while self._monitoring_active:
            try:
                await asyncio.sleep(60)  # Run every minute
                await self._update_qps_metric()
                await self._analyze_trends()
                await self._check_performance_alerts()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[PerfMonitor] Monitoring loop error: {e}")

    async def _update_qps_metric(self):
        """Update queries per second metric"""
        if len(self.query_metrics) >= 2:
            # Calculate QPS based on recent queries
            recent_queries = list(self.query_metrics)[-60:]  # Last 60 queries
            if len(recent_queries) >= 2:
                time_span = (recent_queries[-1].timestamp - recent_queries[0].timestamp).total_seconds()
                if time_span > 0:
                    self.stats['queries_per_second'] = len(recent_queries) / time_span

    async def _analyze_trends(self):
        """Analyze performance trends"""
        if len(self.query_metrics) < 100:
            return
        
        # Analyze recent performance vs historical
        recent_metrics = list(self.query_metrics)[-100:]
        historical_metrics = list(self.query_metrics)[-500:-100] if len(self.query_metrics) >= 500 else []
        
        if not historical_metrics:
            return
        
        recent_avg = sum(m.execution_time for m in recent_metrics) / len(recent_metrics)
        historical_avg = sum(m.execution_time for m in historical_metrics) / len(historical_metrics)
        
        # Check for performance degradation
        if recent_avg > historical_avg * 1.5:
            logger.warning(f"[PerfMonitor] Performance degradation detected: "
                         f"recent avg {recent_avg:.2f}ms vs historical {historical_avg:.2f}ms")

    async def _check_performance_alerts(self):
        """Check for performance alerts that need attention"""
        # Check for high collection scan ratio
        total_queries = self.stats['index_scans'] + self.stats['collection_scans']
        if total_queries > 0:
            collection_scan_ratio = self.stats['collection_scans'] / total_queries
            if collection_scan_ratio > 0.3:  # More than 30% collection scans
                logger.warning(f"[PerfMonitor] High collection scan ratio: {collection_scan_ratio:.2f}")
        
        # Check cache hit rate
        if self.stats['total_queries'] > 100:
            cache_hit_rate = self.stats['cache_hits'] / self.stats['total_queries']
            if cache_hit_rate < 0.2:  # Less than 20% cache hits
                logger.info(f"[PerfMonitor] Low cache hit rate: {cache_hit_rate:.2f}")

    def get_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        # Calculate additional metrics
        recent_metrics = list(self.query_metrics)[-1000:] if self.query_metrics else []
        slow_query_ratio = len([m for m in recent_metrics if m.execution_time > self.slow_query_threshold]) / max(1, len(recent_metrics))
        
        # Top slow queries
        top_slow_queries = sorted(
            self.slow_queries, 
            key=lambda x: x.execution_time, 
            reverse=True
        )[:10]
        
        # Most frequent query patterns
        top_patterns = sorted(
            self.query_patterns.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:10]
        
        # Index usage summary
        index_usage_summary = {}
        for collection, indexes in self.index_usage.items():
            total_usage = sum(indexes.values())
            index_usage_summary[collection] = {
                'total_index_usage': total_usage,
                'indexes': dict(sorted(indexes.items(), key=lambda x: x[1], reverse=True))
            }
        
        return {
            'summary': {
                'total_queries': self.stats['total_queries'],
                'avg_execution_time': round(self.stats['avg_execution_time'], 2),
                'queries_per_second': round(self.stats['queries_per_second'], 2),
                'slow_query_ratio': round(slow_query_ratio, 3),
                'cache_hit_rate': round(self.stats['cache_hits'] / max(1, self.stats['total_queries']), 3),
                'index_scan_ratio': round(self.stats['index_scans'] / max(1, self.stats['total_queries']), 3)
            },
            'top_slow_queries': [
                {
                    'signature': q.query_signature,
                    'execution_time': round(q.execution_time, 2),
                    'docs_examined': q.docs_examined,
                    'docs_returned': q.docs_returned,
                    'index_used': q.index_used,
                    'suggestions': q.optimization_suggestions
                }
                for q in top_slow_queries
            ],
            'query_patterns': [
                {'pattern': pattern, 'count': count}
                for pattern, count in top_patterns
            ],
            'index_usage': index_usage_summary,
            'collection_access': dict(self.collection_access_patterns)
        }

    def get_optimization_recommendations(self) -> List[Dict[str, Any]]:
        """Get optimization recommendations based on collected data"""
        recommendations = []
        
        # Analyze slow queries for index recommendations
        slow_query_patterns = defaultdict(int)
        for query in self.slow_queries:
            if not query.index_used:
                slow_query_patterns[query.query_signature] += 1
        
        for pattern, count in slow_query_patterns.items():
            if count >= 3:  # Pattern appears in multiple slow queries
                recommendations.append({
                    'type': 'missing_index',
                    'priority': 'high',
                    'description': f"Add index for pattern: {pattern}",
                    'affected_queries': count,
                    'estimated_impact': 'high'
                })
        
        # Check for collections with high collection scan ratios
        for collection, access_patterns in self.collection_access_patterns.items():
            total_access = sum(access_patterns.values())
            if total_access > 100:  # Significant usage
                index_usage = self.index_usage[collection]
                total_index_usage = sum(index_usage.values())
                
                if total_index_usage / total_access < 0.5:  # Less than 50% index usage
                    recommendations.append({
                        'type': 'low_index_usage',
                        'priority': 'medium',
                        'description': f"Collection '{collection}' has low index usage ratio",
                        'collection': collection,
                        'estimated_impact': 'medium'
                    })
        
        # Check for potential caching opportunities
        frequent_patterns = [p for p, c in self.query_patterns.items() if c >= 10]
        uncached_frequent = []
        
        recent_metrics = list(self.query_metrics)[-1000:]
        for pattern in frequent_patterns:
            pattern_metrics = [m for m in recent_metrics if m.query_signature == pattern]
            cache_hit_rate = sum(1 for m in pattern_metrics if m.cache_hit) / max(1, len(pattern_metrics))
            
            if cache_hit_rate < 0.3:  # Low cache hit rate
                uncached_frequent.append((pattern, len(pattern_metrics), cache_hit_rate))
        
        for pattern, count, hit_rate in uncached_frequent[:5]:
            recommendations.append({
                'type': 'caching_opportunity',
                'priority': 'low',
                'description': f"Increase cache TTL for frequent pattern: {pattern}",
                'query_count': count,
                'current_hit_rate': round(hit_rate, 2),
                'estimated_impact': 'medium'
            })
        
        return sorted(recommendations, key=lambda x: {'high': 3, 'medium': 2, 'low': 1}[x['priority']], reverse=True)

    def clear_metrics(self):
        """Clear all collected metrics"""
        self.query_metrics.clear()
        self.slow_queries.clear()
        self.index_usage.clear()
        self.query_patterns.clear()
        self.collection_access_patterns.clear()
        
        # Reset stats
        self.stats = {
            'total_queries': 0,
            'slow_queries': 0,
            'cache_hits': 0,
            'index_scans': 0,
            'collection_scans': 0,
            'avg_execution_time': 0.0,
            'queries_per_second': 0.0
        }
        
        logger.info("[PerfMonitor] Cleared all performance metrics")

# Global performance monitor
_performance_monitor = None

def get_performance_monitor(db_manager) -> PerformanceMonitor:
    """Get or create performance monitor instance"""
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = PerformanceMonitor(db_manager)
    return _performance_monitor