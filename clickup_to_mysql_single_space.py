import os
from dotenv import load_dotenv
import requests
from sqlalchemy import create_engine, text
import logging
from datetime import datetime, timezone
import signal
import sys
import json
import traceback
import time
import gc
import re
import random  # Added for jitter in retry logic
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

# Global cache for efficient operations and async session
task_cache = {}
task_details_cache = {}
smart_cache = {}
async_session_cache = None
connection_health_cache = {}  # Track connection health

# Enhanced LRU cache for duplicate detection with circuit breaker pattern
@lru_cache(maxsize=3000)  # Increased cache size
def check_duplicate_task(task_id: str, modified_time: str) -> bool:
    """Smart duplicate detection using LRU cache with circuit breaker"""
    cache_key = f"{task_id}_{modified_time}"
    return cache_key in smart_cache

# Circuit breaker for API calls
class CircuitBreaker:
    """Circuit breaker pattern for API resilience"""
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
                logging.info("Circuit breaker transitioning to HALF_OPEN")
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                self.failure_count = 0
                logging.info("Circuit breaker closed - API calls restored")
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
                logging.error(f"Circuit breaker opened after {self.failure_count} failures")
            raise e

# Initialize circuit breaker
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

@dataclass
class SyncMetrics:
    """Comprehensive metrics tracking for sync operations"""
    start_time: float = field(default_factory=time.time)
    
    # API Metrics
    api_calls_total: int = 0
    api_calls_successful: int = 0
    rate_limits_hit: int = 0
    api_errors: int = 0
    total_api_time: float = 0.0
    
    # Processing Metrics
    lists_processed: int = 0
    tasks_processed: int = 0
    subtasks_processed: int = 0
    status_records_processed: int = 0
    
    # Database Metrics
    db_operations: int = 0
    bulk_inserts: int = 0
    individual_inserts: int = 0
    
    # Memory Metrics
    peak_memory_mb: float = 0.0
    memory_warnings: int = 0
    cache_clears: int = 0
    
    # Error Tracking
    errors_by_type: Dict[str, int] = field(default_factory=dict)
    failed_items: List[str] = field(default_factory=list)
    
    def record_api_call(self, success: bool = True, duration: float = 0):
        self.api_calls_total += 1
        self.total_api_time += duration
        if success:
            self.api_calls_successful += 1
        else:
            self.api_errors += 1
    
    def record_rate_limit(self):
        self.rate_limits_hit += 1
    
    def record_error(self, error_type: str, item_id: str = None):
        self.errors_by_type[error_type] = self.errors_by_type.get(error_type, 0) + 1
        if item_id:
            self.failed_items.append(f"{error_type}: {item_id}")
        
        # Auto-recovery mechanism for high error rates
        if len(self.failed_items) > 50:  # If too many errors, trigger recovery
            logging.warning(f"High error count detected: {len(self.failed_items)} failed items. Triggering recovery mechanisms.")
            self.trigger_recovery()
    
    def trigger_recovery(self):
        """Trigger recovery mechanisms when error rate is high"""
        global smart_cache, api_request_cache
        # Clear caches to force fresh data
        smart_cache.clear()
        api_request_cache.clear()
        # Reset circuit breaker
        circuit_breaker.failure_count = 0
        circuit_breaker.state = 'CLOSED'
        logging.info("Recovery mechanisms triggered - caches cleared, circuit breaker reset")
    
    def update_memory(self, current_memory_mb: float):
        self.peak_memory_mb = max(self.peak_memory_mb, current_memory_mb)
        if current_memory_mb > 1000:
            self.memory_warnings += 1
    
    def log_summary(self):
        duration = time.time() - self.start_time
        api_success_rate = (self.api_calls_successful / max(1, self.api_calls_total)) * 100
        avg_api_time = self.total_api_time / max(1, self.api_calls_total)
        
        logging.info("=" * 60)
        logging.info("SYNC SUMMARY REPORT")
        logging.info("=" * 60)
        logging.info(f"Total Duration: {duration:.1f} seconds")
        logging.info(f"Records Processed: {self.tasks_processed} tasks, {self.subtasks_processed} subtasks")
        logging.info(f"API Calls: {self.api_calls_total} total, {api_success_rate:.1f}% success rate")
        logging.info(f"Rate Limits: {self.rate_limits_hit} encounters")
        logging.info(f"Average API Response Time: {avg_api_time:.2f}s")
        if hasattr(rate_limiter, 'consecutive_rate_limits'):
            logging.info(f"Current API Health: {rate_limiter.api_health_score}% (consecutive rate limits: {rate_limiter.consecutive_rate_limits})")
        logging.info(f"Peak Memory Usage: {self.peak_memory_mb:.1f}MB")
        logging.info(f"Database Operations: {self.db_operations} total, {self.bulk_inserts} bulk")
        
        if self.errors_by_type:
            logging.warning("Errors encountered:")
            for error_type, count in self.errors_by_type.items():
                logging.warning(f"  {error_type}: {count}")

class ProgressTracker:
    """Enhanced progress tracking with time estimates"""
    
    def __init__(self, total_items: int, operation_name: str):
        self.total_items = total_items
        self.operation_name = operation_name
        self.processed_items = 0
        self.start_time = time.time()
        self.last_log_time = self.start_time
        self.last_log_count = 0
        
    def update(self, items_processed: int = 1):
        self.processed_items += items_processed
        
        # Log progress every 10% or every 30 seconds
        current_time = time.time()
        progress_percent = (self.processed_items / self.total_items) * 100
        
        should_log = (
            progress_percent >= (self.last_log_count + 20) or  # Every 20% instead of 15%
            (current_time - self.last_log_time) >= 60  # Every 60 seconds instead of 45
        )
        
        if should_log:
            self._log_progress(current_time, progress_percent)
            self.last_log_time = current_time
            self.last_log_count = int(progress_percent // 20) * 20  # Update to match 20% intervals
    
    def _log_progress(self, current_time: float, progress_percent: float):
        elapsed = current_time - self.start_time
        
        if self.processed_items > 0:
            avg_time_per_item = elapsed / self.processed_items
            remaining_items = self.total_items - self.processed_items
            estimated_remaining = avg_time_per_item * remaining_items
            
            logging.info(
                f"{self.operation_name}: {self.processed_items}/{self.total_items} "
                f"({progress_percent:.1f}%) - "
                f"Elapsed: {elapsed:.1f}s, "
                f"ETA: {estimated_remaining:.1f}s"
            )
        else:
            logging.info(f"{self.operation_name}: Starting...")
    
    def complete(self):
        total_time = time.time() - self.start_time
        avg_time = total_time / max(1, self.processed_items)
        logging.info(
            f"{self.operation_name}: Completed {self.processed_items} items "
            f"in {total_time:.1f}s (avg: {avg_time:.2f}s/item)"
        )

class EnhancedProgressTracker:
    """Enhanced progress tracking with visual indicators, detailed metrics, and better ETA"""
    
    def __init__(self, total_items: int, operation_name: str, update_interval: int = 30):
        self.total_items = total_items
        self.operation_name = operation_name
        self.processed_items = 0
        self.start_time = time.time()
        self.last_report = time.time()
        self.update_interval = update_interval
        self.error_count = 0
        self.success_count = 0
        
    def update(self, items_processed: int = 1, errors: int = 0):
        self.processed_items += items_processed
        self.error_count += errors
        self.success_count += (items_processed - errors)
        
        current_time = time.time()
        progress_percent = (self.processed_items / self.total_items) * 100
        
        # Update every interval or at key milestones
        should_log = (
            (current_time - self.last_report) >= self.update_interval or
            progress_percent >= 100 or
            progress_percent in [10, 25, 50, 75, 90]  # Key milestones
        )
        
        if should_log:
            self._log_enhanced_progress(current_time, progress_percent)
            self.last_report = current_time
    
    def _log_enhanced_progress(self, current_time: float, progress_percent: float):
        elapsed = current_time - self.start_time
        
        if self.processed_items > 0:
            # Calculate rates and timing
            rate_per_minute = (self.processed_items / elapsed) * 60
            avg_time_per_item = elapsed / self.processed_items
            remaining_items = self.total_items - self.processed_items
            estimated_remaining = avg_time_per_item * remaining_items
            
            # Format ETA nicely
            eta_formatted = self._format_duration(estimated_remaining)
            elapsed_formatted = self._format_duration(elapsed)
            
            # Create visual progress bar (20 characters wide)
            progress_bar_length = 20
            filled_length = int(progress_percent / 100 * progress_bar_length)
            progress_bar = "█" * filled_length + "░" * (progress_bar_length - filled_length)
            
            # Calculate success rate
            success_rate = (self.success_count / max(1, self.processed_items)) * 100
            
            # Enhanced progress log with visual elements
            timestamp = datetime.now().strftime('%H:%M:%S')
            logging.info(f"┌─ {self.operation_name} Progress Report ─────────────────")
            logging.info(f"│ [{timestamp}] Progress: [{progress_bar}] {progress_percent:.1f}%")
            logging.info(f"│ Items: {self.processed_items:,}/{self.total_items:,} | Speed: {rate_per_minute:.1f}/min")
            logging.info(f"│ Elapsed: {elapsed_formatted} | ETA: {eta_formatted}")
            logging.info(f"│ Success Rate: {success_rate:.1f}% | Errors: {self.error_count}")
            
            # Add API health and memory info if available
            try:
                if hasattr(rate_limiter, 'api_health_score'):
                    logging.info(f"│ API Health: {rate_limiter.api_health_score:.0f}% | Delay: {rate_limiter.current_delay:.1f}s")
                
                # Memory usage if available
                memory_info = self._get_memory_info()
                if memory_info:
                    logging.info(f"│ Memory: {memory_info}")
                    
            except NameError:
                pass  # rate_limiter not available in this context
            
            logging.info(f"└─────────────────────────────────────────────────────")
        else:
            logging.info(f"🚀 Starting {self.operation_name}...")
    
    def _format_duration(self, seconds):
        """Format duration in a human-readable way"""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds//60)}m {int(seconds%60)}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    
    def _get_memory_info(self):
        """Get current memory usage if psutil is available"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            return f"{memory_mb:.0f}MB"
        except ImportError:
            return None
    
    def complete(self):
        total_time = time.time() - self.start_time
        avg_time = total_time / max(1, self.processed_items)
        success_rate = (self.success_count / max(1, self.processed_items)) * 100
        
        logging.info(f"✅ {self.operation_name} Complete!")
        logging.info(f"   Processed: {self.processed_items:,} items in {self._format_duration(total_time)}")
        logging.info(f"   Average: {avg_time:.2f}s/item | Success Rate: {success_rate:.1f}%")
        if self.error_count > 0:
            logging.info(f"   ⚠️  Errors: {self.error_count}")

class DataValidator:
    """Enhanced validate and sanitize data from ClickUp API with safety checks"""
    
    @staticmethod
    def validate_task_data(task_data: Dict[str, Any]) -> bool:
        """Validate essential task fields with enhanced safety checks"""
        if not isinstance(task_data, dict):
            logging.warning("Task data is not a dictionary")
            metrics.record_error("validation_error", "not_dict")
            return False
        
        required_fields = ['id', 'name']
        
        for field in required_fields:
            value = task_data.get(field)
            if not value or (isinstance(value, str) and not value.strip()):
                logging.warning(f"Task missing or empty required field: {field}")
                metrics.record_error("validation_error", task_data.get('id', 'unknown'))
                return False
        
        # Validate task ID format with enhanced checks
        task_id = task_data.get('id')
        if not isinstance(task_id, (str, int)) or len(str(task_id)) < 3:
            logging.warning(f"Invalid task ID format: {task_id}")
            metrics.record_error("invalid_task_id", str(task_id))
            return False
        
        # Check for suspicious data patterns
        name = task_data.get('name', '')
        if len(name) > 1000 or any(char in name for char in ['\x00', '\x01', '\x02']):
            logging.warning(f"Suspicious task name detected: {name[:50]}...")
            metrics.record_error("suspicious_data", task_id)
            return False
        
        return True
    
    @staticmethod
    def sanitize_text_content(text: Any, max_length: int = 65535) -> Optional[str]:
        """Enhanced sanitize text content for database storage with safety checks"""
        if not text:
            return None
        
        # Convert to string if not already
        text = str(text)
        
        # Remove null bytes and other dangerous characters
        text = text.replace('\x00', '')
        
        # Remove other problematic control characters
        text = re.sub(r'[\x01-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', text)
        
        # Remove potential SQL injection patterns (basic protection)
        dangerous_patterns = [
            r'(DROP\s+TABLE)', r'(DELETE\s+FROM)', r'(INSERT\s+INTO)', 
            r'(UPDATE\s+\w+\s+SET)', r'(TRUNCATE\s+TABLE)', r'(ALTER\s+TABLE)'
        ]
        for pattern in dangerous_patterns:
            text = re.sub(pattern, '[FILTERED]', text, flags=re.IGNORECASE)
        
        # Truncate if too long with safety margin
        if len(text) > max_length:
            safe_length = max_length - 10  # Extra margin for safety
            text = text[:safe_length] + "..."
            logging.warning(f"Text truncated to {safe_length} characters for safety")
        
        return text.strip()
    
    @staticmethod
    def validate_time_value(time_value: Any) -> int:
        """Validate and convert time values to minutes"""
        if time_value is None:
            return 0
        
        try:
            if isinstance(time_value, str):
                time_value = float(time_value)
            
            # Convert milliseconds to minutes
            if time_value > 1000000:  # Likely milliseconds
                time_value = int(time_value / (1000 * 60))
            else:
                time_value = int(time_value)
            
            # Ensure non-negative
            return max(0, time_value)
            
        except (ValueError, TypeError):
            logging.warning(f"Invalid time value: {time_value}, defaulting to 0")
            metrics.record_error("invalid_time_value", str(time_value))
            return 0
    
    @staticmethod
    def validate_api_response(response_data: Dict[str, Any], expected_keys: List[str] = None) -> bool:
        """Validate API response structure"""
        if not isinstance(response_data, dict):
            logging.error("API response is not a dictionary")
            metrics.record_error("invalid_api_response", "not_dict")
            return False
        
        if expected_keys:
            for key in expected_keys:
                if key not in response_data:
                    logging.warning(f"API response missing expected key: {key}")
                    metrics.record_error("missing_api_key", key)
                    return False
        
        return True

class AdaptiveRateLimiter:
    """Enhanced adaptive rate limiter with predictive adjustment and safety mechanisms"""
    def __init__(self, base_delay=0.5, max_delay=12.0):  # Even more conservative to prevent cascades
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.max_delay = max_delay
        self.consecutive_rate_limits = 0
        self.success_count = 0
        self.api_health_score = 100  # Track API health (0-100)
        self.recent_response_times = []  # Track recent response times
        self.rate_limit_backoff_multiplier = 1.1  # Much more conservative scaling
        
    def on_rate_limit(self):
        """Called when a rate limit (429) is encountered"""
        self.consecutive_rate_limits += 1
        self.api_health_score = max(0, self.api_health_score - 10)  # Even less aggressive health reduction
        old_delay = self.current_delay
        
        # Much more conservative delay increase to prevent escalation
        if self.consecutive_rate_limits <= 1:
            self.current_delay = min(self.current_delay * self.rate_limit_backoff_multiplier, self.max_delay)
        elif self.consecutive_rate_limits <= 3:
            self.current_delay = min(self.current_delay * 1.3, self.max_delay)
        else:
            # Cap at max_delay to prevent infinite escalation
            self.current_delay = self.max_delay
            
        logging.warning(f"Rate limit detected (#{self.consecutive_rate_limits}). API health: {self.api_health_score}%. Adjusting delay from {old_delay:.1f}s to {self.current_delay:.1f}s")
        self.success_count = 0  # Reset success counter
    
    def on_success(self, response_time: float = 0):
        """Called when a request succeeds with response time tracking"""
        self.success_count += 1
        self.api_health_score = min(100, self.api_health_score + 3)  # Faster health recovery
        
        # Track response times for predictive adjustment
        self.recent_response_times.append(response_time)
        if len(self.recent_response_times) > 20:  # Keep only recent 20 response times
            self.recent_response_times.pop(0)
        
        # More aggressive delay reduction when API is healthy
        if self.success_count >= 8 and self.api_health_score > 80:  # Higher threshold for stability
            old_delay = self.current_delay
            # More gradual reduction when healthy
            if self.api_health_score > 95:
                reduction_factor = 0.94  # Slower reduction
            elif self.api_health_score > 90:
                reduction_factor = 0.96
            else:
                reduction_factor = 0.98
                
            self.current_delay = max(self.current_delay * reduction_factor, self.base_delay)
            
            if old_delay != self.current_delay and old_delay - self.current_delay > 0.05:
                logging.info(f"API stable (health: {self.api_health_score}%). Reducing delay from {old_delay:.1f}s to {self.current_delay:.1f}s")
            self.consecutive_rate_limits = max(0, self.consecutive_rate_limits - 1)
    
    def get_delay(self):
        """Get current delay time with predictive adjustment"""
        # If response times are consistently high, increase delay slightly
        if len(self.recent_response_times) >= 5:
            avg_response_time = sum(self.recent_response_times) / len(self.recent_response_times)
            if avg_response_time > 2.0:  # If average response time > 2 seconds
                return self.current_delay * 1.2
        
        # If we've had many consecutive rate limits, add extra delay
        if self.consecutive_rate_limits > 3:
            return min(self.current_delay * 1.5, self.max_delay)
        
        return self.current_delay
    
    def get_health_status(self):
        """Get current API health status for monitoring"""
        return {
            'health_score': self.api_health_score,
            'current_delay': self.current_delay,
            'consecutive_failures': self.consecutive_rate_limits,
            'avg_response_time': sum(self.recent_response_times) / len(self.recent_response_times) if self.recent_response_times else 0
        }

# Initialize global instances
metrics = SyncMetrics()
validator = DataValidator()

# Graceful shutdown handler
def signal_handler(signum, frame):
    logging.info("Received termination signal. Exiting...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper functions
def should_process(date_updated):
    """Determine if a task should be processed based on its update time"""
    if mode == 'A' and last_run:
        return date_updated and date_updated > last_run
    return True

def to_number(val):
    if val is None or val == '':
        logging.warning("ID value is None or empty string!")
        return None
    try:
        return int(val)
    except Exception:
        try:
            return float(val)
        except Exception as e:
            logging.warning(f"Could not convert {val} to number: {e}")
            return None

def ms_to_datetime(timestamp):
    """Convert milliseconds or minutes timestamp to datetime.
    If the timestamp appears to be in milliseconds (> 1e10), converts to minutes first."""
    try:
        if not timestamp:
            return None
            
        # Try to convert to integer first
        try:
            timestamp = int(timestamp)
        except (ValueError, TypeError):
            try:
                timestamp = int(float(timestamp))
            except (ValueError, TypeError):
                logging.warning(f"Could not convert timestamp {timestamp} to number")
                return None

        # If timestamp is in milliseconds (larger than 10 billion), convert to minutes
        if timestamp > 1e10:
            timestamp = timestamp // (1000 * 60)  # Convert ms to minutes
            
        # Now timestamp should be in minutes, convert to seconds for fromtimestamp
        seconds = timestamp * 60
        
        # Check if timestamp is within reasonable range (between 1970 and 2100)
        if seconds < 0 or seconds > 4102444800:  # 4102444800 is timestamp for year 2100
            logging.warning(f"Timestamp {timestamp} minutes ({seconds} seconds) is out of reasonable range, defaulting to None")
            return None
            
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (ValueError, TypeError, OverflowError) as e:
        logging.warning(f"Could not convert timestamp {timestamp} to datetime: {str(e)}")
        return None

def extract_comma_separated(values, key):
    return ", ".join([str(v.get(key, "")) for v in values]) if isinstance(values, list) else None

def process_in_batches(items, batch_size=10):
    """Split items into smaller batches for processing"""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]

def log_progress(current, total, operation="Processing"):
    """Log progress for long-running operations"""
    if total > 0 and current % max(1, total // 10) == 0:  # Log every 10%
        percentage = (current / total) * 100
        logging.info(f"{operation}: {current}/{total} ({percentage:.1f}%)")

def check_memory_usage():
    """Monitor memory usage and trigger garbage collection if needed"""
    try:
        import psutil
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        metrics.update_memory(memory_mb)  # Track peak memory
        
        if memory_mb > 1000:  # Alert if using >1GB
            logging.warning(f"High memory usage: {memory_mb:.1f}MB - triggering garbage collection")
            gc.collect()
            return True
        return False
    except ImportError:
        # psutil not available, use basic gc
        gc.collect()
        return False

def clear_caches_if_needed():
    """Clear LRU caches if they're getting too large"""
    cache_info = fetch_subtask_details.cache_info()
    if cache_info.currsize > 2400:  # Clear when cache is 80% full (2400/3000)
        logging.info(f"Clearing subtask cache (was {cache_info.currsize}/3000 items)")
        fetch_subtask_details.cache_clear()
        metrics.cache_clears += 1  # Track cache clears

def bulk_insert_subtasks(conn, subtask_records, batch_size=1000):  # Increased for maximum speed
    """Enhanced insert subtasks with transaction safety and optimized batch processing"""
    if not subtask_records:
        return
    
    total_inserted = 0
    total_failed = 0
    
    for batch_num, batch in enumerate(process_in_batches(subtask_records, batch_size), 1):
        try:
            # Sanitize data before insertion with enhanced validation
            sanitized_batch = []
            for record in batch:
                # Validate record structure
                if not isinstance(record, dict) or not record.get('subtask_id'):
                    logging.warning(f"Invalid subtask record structure: {record}")
                    total_failed += 1
                    continue
                
                sanitized_record = record.copy()
                sanitized_record['subtask_name'] = validator.sanitize_text_content(
                    record.get('subtask_name'), 250  # Slightly reduced for safety margin
                )
                sanitized_record['description'] = validator.sanitize_text_content(
                    record.get('description')
                )
                sanitized_record['tags_names'] = validator.sanitize_text_content(
                    record.get('tags_names'), 995  # Safety margin
                )
                
                # Validate time values
                sanitized_record['time_estimate'] = validator.validate_time_value(
                    record.get('time_estimate', 0)
                )
                sanitized_record['time_spent'] = validator.validate_time_value(
                    record.get('time_spent', 0)
                )
                
                sanitized_batch.append(sanitized_record)
            
            if not sanitized_batch:
                logging.warning(f"Batch {batch_num}: No valid records to insert")
                continue
            
            # Perform bulk insert with enhanced error handling (no savepoints in autocommit mode)
            result = conn.execute(text("""
                INSERT INTO subtasks (
                    task_id, task_name, subtask_id, subtask_name, description,
                    time_estimate, time_spent, status, tags_names
                ) VALUES (
                    :task_id, :task_name, :subtask_id, :subtask_name, :description,
                    :time_estimate, :time_spent, :status, :tags_names
                )
                ON DUPLICATE KEY UPDATE
                    subtask_name = VALUES(subtask_name),
                    description = VALUES(description),
                    time_estimate = VALUES(time_estimate),
                    time_spent = VALUES(time_spent),
                    status = VALUES(status),
                    tags_names = VALUES(tags_names)
            """), sanitized_batch)
                
            batch_inserted = len(sanitized_batch)
            total_inserted += batch_inserted
            metrics.bulk_inserts += 1
            metrics.db_operations += 1
            
            logging.debug(f"Batch {batch_num}: Successfully inserted {batch_inserted} subtasks")
            
        except Exception as e:
            logging.error(f"Batch {batch_num}: Failed to bulk insert subtask batch: {str(e)}")
            metrics.record_error("bulk_insert_failed", f"batch_{batch_num}_size_{len(batch)}")
            
            # Fallback to individual inserts for this batch with enhanced error handling
            individual_success = 0
            for record in batch:
                try:
                    # Validate and sanitize individual record
                    if not isinstance(record, dict) or not record.get('subtask_id'):
                        total_failed += 1
                        continue
                    
                    sanitized_record = record.copy()
                    sanitized_record['subtask_name'] = validator.sanitize_text_content(
                        record.get('subtask_name'), 250
                    )
                    sanitized_record['description'] = validator.sanitize_text_content(
                        record.get('description')
                    )
                    sanitized_record['tags_names'] = validator.sanitize_text_content(
                        record.get('tags_names'), 995
                    )
                    sanitized_record['time_estimate'] = validator.validate_time_value(
                        record.get('time_estimate', 0)
                    )
                    sanitized_record['time_spent'] = validator.validate_time_value(
                        record.get('time_spent', 0)
                    )
                    
                    conn.execute(text("""
                        INSERT INTO subtasks (
                            task_id, task_name, subtask_id, subtask_name, description,
                            time_estimate, time_spent, status, tags_names
                        ) VALUES (
                            :task_id, :task_name, :subtask_id, :subtask_name, :description,
                            :time_estimate, :time_spent, :status, :tags_names
                        )
                        ON DUPLICATE KEY UPDATE
                            subtask_name = VALUES(subtask_name),
                            description = VALUES(description),
                            time_estimate = VALUES(time_estimate),
                            time_spent = VALUES(time_spent),
                            status = VALUES(status),
                            tags_names = VALUES(tags_names)
                    """), sanitized_record)
                    
                    individual_success += 1
                    metrics.individual_inserts += 1
                    metrics.db_operations += 1
                    
                except Exception as e2:
                    logging.error(f"Failed to insert individual subtask {record.get('subtask_id', 'unknown')}: {str(e2)}")
                    metrics.record_error("individual_insert_failed", record.get('subtask_id', 'unknown'))
                    total_failed += 1
            
            total_inserted += individual_success
            logging.info(f"Batch {batch_num}: Fallback completed - {individual_success} successful, {len(batch) - individual_success} failed")
    
    if total_inserted > 0:
        logging.info(f"Successfully bulk inserted {total_inserted} subtasks ({total_failed} failed)")
        metrics.subtasks_processed += total_inserted
        
        if total_failed > 0:
            failure_rate = (total_failed / (total_inserted + total_failed)) * 100
            if failure_rate > 10:
                logging.warning(f"High subtask insert failure rate: {failure_rate:.1f}%")
                metrics.record_error("high_failure_rate", f"{failure_rate:.1f}%")

# Prompt for mode
mode = input("Enter R to REFETCH all data, or A to ACCUMULATE only changes: ").strip().upper()
last_run = None
if mode == 'A':
    try:
        with open('last_run.json', 'r') as f:
            last_run = json.load(f)['timestamp']
        logging.info(f"Last run timestamp: {last_run}")
    except FileNotFoundError:
        logging.warning("No previous run found, will fetch all data")
        mode = 'R'
else:
    logging.info("Running in REFETCH mode - will get all data")

# Load environment variables
load_dotenv()
CLICKUP_API_KEY = os.getenv("CLICKUP_API_KEY")
TEAM_ID = os.getenv("TEAM_ID")
SPACE_ID = os.getenv("SPACE_ID")
DB_CONN_STRING = os.getenv("DB_CONN_STRING")

def validate_configuration():
    """Comprehensive configuration validation before starting sync"""
    logging.info("Validating configuration...")
    config_issues = []
    
    # Validate required environment variables
    required_vars = {
        "CLICKUP_API_KEY": CLICKUP_API_KEY,
        "TEAM_ID": TEAM_ID,
        "SPACE_ID": SPACE_ID,
        "DB_CONN_STRING": DB_CONN_STRING
    }
    
    for name, value in required_vars.items():
        if not value:
            config_issues.append(f"Missing required environment variable: {name}")
    
    if config_issues:
        for issue in config_issues:
            logging.error(f"Configuration issue: {issue}")
        sys.exit(1)
    
    # Validate API key format (ClickUp API keys are typically 56+ characters)
    if len(CLICKUP_API_KEY) < 20:
        config_issues.append("CLICKUP_API_KEY appears to be invalid (too short)")
    
    # Validate numeric IDs
    try:
        space_id_num = int(SPACE_ID)
        team_id_num = int(TEAM_ID)
        if space_id_num <= 0 or team_id_num <= 0:
            config_issues.append("SPACE_ID and TEAM_ID must be positive numbers")
    except ValueError:
        config_issues.append("SPACE_ID and TEAM_ID must be numeric")
    
    # Validate database connection string format
    if not any(db_type in DB_CONN_STRING.lower() for db_type in ['mysql', 'postgresql', 'sqlite']):
        config_issues.append("DB_CONN_STRING doesn't appear to be a valid database connection string")
    
    # Test API connectivity with a simple request
    try:
        logging.info("Testing ClickUp API connectivity...")
        test_response = requests.get("https://api.clickup.com/api/v2/user", 
                                   headers={"Authorization": CLICKUP_API_KEY},
                                   timeout=10)
        if test_response.status_code == 401:
            config_issues.append("Invalid CLICKUP_API_KEY - authentication failed")
        elif test_response.status_code != 200:
            config_issues.append(f"ClickUp API connectivity test failed: HTTP {test_response.status_code}")
        else:
            user_data = test_response.json()
            logging.info(f"✅ API connectivity test passed - authenticated as: {user_data.get('user', {}).get('username', 'Unknown')}")
    except requests.exceptions.RequestException as e:
        config_issues.append(f"ClickUp API connectivity test failed: {str(e)}")
    
    # Test database connectivity
    try:
        logging.info("Testing database connectivity...")
        test_engine = create_engine(DB_CONN_STRING, connect_args={"connect_timeout": 10})
        test_conn = test_engine.connect()
        test_conn.execute(text("SELECT 1"))
        test_conn.close()
        test_engine.dispose()
        logging.info("✅ Database connectivity test passed")
    except Exception as e:
        config_issues.append(f"Database connectivity test failed: {str(e)}")
    
    # Test ClickUp space access
    try:
        logging.info(f"Testing access to ClickUp space {SPACE_ID}...")
        space_response = requests.get(f"https://api.clickup.com/api/v2/space/{SPACE_ID}",
                                    headers={"Authorization": CLICKUP_API_KEY},
                                    timeout=10)
        if space_response.status_code == 404:
            config_issues.append(f"ClickUp space {SPACE_ID} not found or no access")
        elif space_response.status_code != 200:
            config_issues.append(f"Cannot access ClickUp space {SPACE_ID}: HTTP {space_response.status_code}")
        else:
            space_data = space_response.json()
            logging.info(f"✅ Space access test passed - space name: '{space_data.get('name', 'Unknown')}'")
    except requests.exceptions.RequestException as e:
        config_issues.append(f"ClickUp space access test failed: {str(e)}")
    
    # Report configuration issues
    if config_issues:
        logging.error("❌ Configuration validation failed:")
        for issue in config_issues:
            logging.error(f"  • {issue}")
        logging.error("Please fix the configuration issues above and try again.")
        sys.exit(1)
    
    logging.info("✅ All configuration validation checks passed")

# Validate configuration before proceeding
validate_configuration()

for name, value in [("CLICKUP_API_KEY", CLICKUP_API_KEY), ("TEAM_ID", TEAM_ID), ("SPACE_ID", SPACE_ID), ("DB_CONN_STRING", DB_CONN_STRING)]:
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

headers = {"Authorization": CLICKUP_API_KEY}

# Create a session for connection pooling and rate limiting
session = requests.Session()
session.headers.update(headers)

# Global async session for concurrent requests (disabled for sync version)
# async_session = None

# async def close_async_session():
#     """Close the async session properly"""
#     global async_session_cache
#     if async_session_cache:
#         try:
#             await async_session_cache.close()
#             async_session_cache = None
#         except Exception as e:
#             logging.error(f"Error closing async session: {str(e)}")

# def create_async_session():
#     """Create async session with connection pooling"""
#     global async_session
#     if async_session is None:
#         connector = aiohttp.TCPConnector(
#             limit=20,  # Total connection pool size
#             limit_per_host=10,  # Per host connection limit
#             ttl_dns_cache=300,  # DNS cache TTL
#             use_dns_cache=True,
#         )
#         timeout = aiohttp.ClientTimeout(total=30, connect=10)
#         async_session = aiohttp.ClientSession(
#             connector=connector,
#             timeout=timeout,
#             headers=headers
#         )
#     return async_session

# async def close_async_session():
#     """Clean up async session"""
#     global async_session
#     if async_session:
#         await async_session.close()
#         async_session = None

# Initialize ultra-conservative rate limiter for better performance
rate_limiter = AdaptiveRateLimiter(base_delay=0.5, max_delay=12.0)  # Even more conservative settings

# Add performance monitoring
last_performance_log = time.time()
performance_log_interval = 30  # Log performance every 30 seconds

def log_performance_metrics():
    """Enhanced performance metrics logging with better formatting"""
    global last_performance_log
    current_time = time.time()
    
    if current_time - last_performance_log >= performance_log_interval:
        health_status = rate_limiter.get_health_status()
        uptime = current_time - metrics.start_time
        
        # Get memory info if available
        memory_info = "N/A"
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_info = f"{memory_mb:.0f}MB"
        except ImportError:
            pass
        
        # Enhanced performance dashboard
        timestamp = datetime.now().strftime('%H:%M:%S')
        logging.info(f"📊 [{timestamp}] Performance Dashboard")
        logging.info(f"   ⏱️  Uptime: {uptime//3600:.0f}h {(uptime%3600)//60:.0f}m {uptime%60:.0f}s")
        logging.info(f"   📈 Progress: Lists {metrics.lists_processed}/110, Tasks {metrics.tasks_processed}, Subtasks {metrics.subtasks_processed}")
        logging.info(f"   🔗 API Health: {health_status['health_score']:.0f}% | Delay: {health_status['current_delay']:.1f}s | Success: {metrics.api_calls_successful}/{metrics.api_calls_total}")
        logging.info(f"   💾 Memory: {memory_info} | Cache Hits: {cache_hit_count} | DB Ops: {metrics.db_operations}")
        if metrics.rate_limits_hit > 0:
            logging.info(f"   ⚠️  Rate Limits: {metrics.rate_limits_hit} | Errors: {len(metrics.failed_items)}")
        
        last_performance_log = current_time# Async functions disabled for sync optimization
# async def make_async_api_request(url, params=None, semaphore=None):
#     """High-performance async API request with rate limiting and retry logic"""
#     session = create_async_session()
#     
#     # Use semaphore to control concurrency
#     async with semaphore if semaphore else asyncio.Semaphore(10):
#         # Adaptive delay
#         await asyncio.sleep(rate_limiter.get_delay())
#         
#         request_start_time = time.time()
#         max_retries = 3
#         
#         for attempt in range(max_retries):
#             try:
#                 async with session.get(url, params=params) as response:
#                     request_duration = time.time() - request_start_time
#                     
#                     if response.status == 429:  # Rate limited
#                         rate_limiter.on_rate_limit()
#                         metrics.record_rate_limit()
#                         wait_time = min(2 ** attempt * 2, 10)  # Reduced wait time
#                         logging.warning(f"Rate limited, waiting {wait_time} seconds...")
#                         await asyncio.sleep(wait_time)
#                         continue
#                     
#                     response.raise_for_status()
#                     rate_limiter.on_success()
#                     metrics.record_api_call(success=True, duration=request_duration)
#                     return await response.json()
#                     
#             except Exception as e:
#                 if attempt == max_retries - 1:
#                     metrics.record_api_call(success=False, duration=time.time() - request_start_time)
#                     metrics.record_error("async_api_request_failed", url)
#                     raise
#                 wait_time = min(2 ** attempt, 3)  # Reduced wait time
#                 await asyncio.sleep(wait_time)
#         
#         return None

def make_api_request(url, params=None):
    """Enhanced rate-limited API request with circuit breaker, health monitoring and safety mechanisms"""
    
    def _make_request():
        # Use adaptive delay with health-based adjustment
        delay = rate_limiter.get_delay()
        time.sleep(delay)
        
        request_start_time = time.time()
        max_retries = 3  # Increased retries for better reliability
        
        for attempt in range(max_retries):
            try:
                # Check circuit breaker before making request
                if circuit_breaker.state == 'OPEN':
                    raise Exception("Circuit breaker is OPEN - API temporarily unavailable")
                
                response = session.get(url, params=params, timeout=20)  # Increased timeout for safety
                request_duration = time.time() - request_start_time
                
                if response.status_code == 429:  # Rate limited
                    rate_limiter.on_rate_limit()
                    metrics.record_rate_limit()
                    
                    # More aggressive exponential backoff with circuit breaker logic
                    if rate_limiter.consecutive_rate_limits > 3:
                        # Trigger circuit breaker mode for severe rate limiting
                        wait_time = min(45 + random.uniform(0, 15), 90)  # 45-90 second break
                        logging.warning(f"Severe rate limiting detected. Taking extended break for {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                        # Reset rate limiter after break
                        rate_limiter.consecutive_rate_limits = max(0, rate_limiter.consecutive_rate_limits - 3)
                        rate_limiter.api_health_score = min(100, rate_limiter.api_health_score + 20)  # Boost health after break
                        continue
                    else:
                        # Normal exponential backoff with jitter
                        wait_time = min((2 ** attempt) * 3 + random.uniform(0, 2), 20)
                        logging.warning(f"Rate limited (attempt {attempt + 1}/{max_retries}), waiting {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                        continue
                
                # Check for other HTTP errors
                if response.status_code >= 500:
                    raise requests.exceptions.RequestException(f"Server error: {response.status_code}")
                elif response.status_code >= 400:
                    raise requests.exceptions.RequestException(f"Client error: {response.status_code}")
                    
                response.raise_for_status()
                
                # Success - update rate limiter with response time
                rate_limiter.on_success(request_duration)
                metrics.record_api_call(success=True, duration=request_duration)
                
                # Validate JSON response with enhanced error handling
                try:
                    json_data = response.json()
                    if not isinstance(json_data, dict):
                        logging.error(f"API response is not a dictionary from {url}: {type(json_data)}")
                        metrics.record_error("invalid_json_response", url)
                        return None
                    return json_data
                except ValueError as e:
                    logging.error(f"Invalid JSON response from {url}: {str(e)}")
                    metrics.record_error("invalid_json_response", url)
                    return None
                except Exception as e:
                    logging.error(f"Unexpected error parsing JSON from {url}: {str(e)}")
                    metrics.record_error("json_parse_error", url)
                    return None
                
            except requests.exceptions.Timeout:
                logging.warning(f"Request timeout (attempt {attempt + 1}/{max_retries}) for {url}")
                if attempt == max_retries - 1:
                    metrics.record_api_call(success=False, duration=time.time() - request_start_time)
                    metrics.record_error("timeout", url)
                    raise
                time.sleep(min(2 ** attempt, 5))
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    metrics.record_api_call(success=False, duration=time.time() - request_start_time)
                    metrics.record_error("api_request_failed", url)
                    raise
                wait_time = min(2 ** attempt + random.uniform(0, 1), 5)  # Jitter for better distribution
                logging.warning(f"Request failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.1f} seconds: {str(e)}")
                time.sleep(wait_time)
        
        return None
    
    # Use circuit breaker pattern
    try:
        return circuit_breaker.call(_make_request)
    except Exception as e:
        logging.error(f"Circuit breaker or API request failed for {url}: {str(e)}")
        metrics.record_error("circuit_breaker_failure", url)
        return None

# Enhanced caching with smart pre-filtering, duplicate detection and memory management
processed_subtask_ids = set()  # Track processed subtasks to avoid duplicates
api_request_cache = {}  # Simple in-memory cache for API responses
cache_hit_count = 0  # Track cache efficiency

@lru_cache(maxsize=4000)  # Increased cache size for better hit rate
def fetch_subtask_details(subtask_id):
    """Enhanced cache subtask details with circuit breaker and health monitoring"""
    global cache_hit_count
    
    try:
        # Skip if already processed recently
        if subtask_id in processed_subtask_ids:
            cache_hit_count += 1
            return None
        
        # Check simple cache first
        cache_key = f"subtask_{subtask_id}"
        if cache_key in api_request_cache:
            cache_hit_count += 1
            return api_request_cache[cache_key]
        
        # Make API request with circuit breaker
        result = make_api_request(f"https://api.clickup.com/api/v2/task/{subtask_id}")
        if result:
            processed_subtask_ids.add(subtask_id)
            api_request_cache[cache_key] = result  # Cache for reuse
            
            # Enhanced cache management with memory safety
            if len(api_request_cache) > 2000:  # Increased threshold
                # Remove oldest 25% of cache entries more aggressively
                keys_to_remove = list(api_request_cache.keys())[:500]
                for key in keys_to_remove:
                    del api_request_cache[key]
                logging.info(f"Cache pruned: removed {len(keys_to_remove)} entries. Cache hit rate: {cache_hit_count}/{cache_hit_count + len(api_request_cache)}")
                    
        return result
    except Exception as e:
        logging.warning(f"Failed to fetch cached subtask details for {subtask_id}: {str(e)}")
        metrics.record_error("subtask_cache_failure", subtask_id)
        return None

# Initialize DB engine with heavily optimized connection pooling and safety
try:
    engine = create_engine(
        DB_CONN_STRING,
        pool_size=20,         # Increased for higher throughput
        max_overflow=40,      # Increased overflow capacity
        pool_pre_ping=True,   # Verify connections before use
        pool_recycle=3600,    # Recycle connections every hour
        pool_timeout=15,      # Reduced timeout for faster failover
        echo=False,           # Set to True for SQL debugging
        # Additional optimizations for MySQL with safety
        connect_args={
            "charset": "utf8mb4",
            "autocommit": True,   # Use autocommit to avoid transaction conflicts
            "init_command": "SET SESSION innodb_lock_wait_timeout = 120"  # Increase lock wait timeout
        },
        # Set isolation level at the engine level instead
        isolation_level="AUTOCOMMIT"  # Use autocommit mode
    )
    logging.info("Database engine initialized with enhanced connection pooling and safety measures")
except Exception as e:
    logging.error("Failed to connect to DB", exc_info=True)
    exit(1)

def create_tables(conn):
    # Create spaces table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS spaces (
            space_id BIGINT PRIMARY KEY,
            space_name VARCHAR(255),
            private_status BOOLEAN,
            archived_status BOOLEAN
        )
    """))

    # Create folders table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS folders (
            folder_id BIGINT PRIMARY KEY,
            space_id BIGINT,
            folder_name VARCHAR(255),
            folder_hidden BOOLEAN,
            task_count INT,
            FOREIGN KEY (space_id) REFERENCES spaces(space_id) ON DELETE CASCADE
        )
    """))

    # Create lists table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS lists (
            list_id BIGINT PRIMARY KEY,
            folder_id BIGINT,
            list_name VARCHAR(255),
            start_date DATETIME NULL,
            due_date DATETIME NULL,
            task_count INT,
            sprint_check BOOLEAN,
            sprint_name VARCHAR(255),
            FOREIGN KEY (folder_id) REFERENCES folders(folder_id) ON DELETE CASCADE
        )
    """))

    # Create tasks table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS tasks (
            task_id VARCHAR(255) PRIMARY KEY,
            list_id BIGINT,
            custom_item_id VARCHAR(255),
            name VARCHAR(500),
            text_content TEXT,
            description TEXT,
            tags_names TEXT,
            points INT,
            current_status VARCHAR(255),
            time_estimate DOUBLE,
            time_spent DOUBLE,
            date_created DATETIME,
            date_updated DATETIME,
            date_closed DATETIME,
            date_done DATETIME,
            time_track DOUBLE,
            locations_ids TEXT,
            locations_names TEXT,
            subtasks_time_estimate DOUBLE,
            subtasks_time_spent DOUBLE,
            us_cycle_time DOUBLE DEFAULT 0,
            FOREIGN KEY (list_id) REFERENCES lists(list_id) ON DELETE CASCADE
        )
    """))

    # Create relations table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS relations (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            task_id VARCHAR(255),
            task_name VARCHAR(500),
            linked_id VARCHAR(255),
            linked_name VARCHAR(500),
            FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
        )
    """))

    # Create sprints table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sprints (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            sprint_id VARCHAR(255),
            sprint_name VARCHAR(500),
            linked_task_id VARCHAR(255),
            task_name VARCHAR(500),
            FOREIGN KEY (linked_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
        )
    """))

    # Create subtasks table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS subtasks (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            task_id VARCHAR(255),
            task_name VARCHAR(255),
            subtask_id VARCHAR(255),
            subtask_name VARCHAR(255),
            description TEXT,
            time_estimate DOUBLE,
            time_spent DOUBLE,
            status VARCHAR(255),
            tags_names TEXT,
            FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
        )
    """))

    # Create task_time_in_status table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS task_time_in_status (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            task_id VARCHAR(255),
            task_name TEXT,
            status VARCHAR(255),
            total_time INT,
            from_date DATETIME,
            to_date DATETIME,
            FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
        )
    """))
    
    # Migrate existing task_time_in_status table to support longer task names
    try:
        conn.execute(text("ALTER TABLE task_time_in_status MODIFY COLUMN task_name TEXT"))
        logging.info("Updated task_time_in_status.task_name column to TEXT for longer task names")
    except Exception as e:
        if "doesn't exist" not in str(e).lower():
            logging.debug(f"Task name column migration: {str(e)}")  # Column might already be TEXT
    
    # Create essential performance indexes
    logging.info("Creating database indexes for optimal performance...")
    
    # MySQL-compatible index creation with error handling
    indexes_to_create = [
        ("idx_folders_space_id", "folders", "space_id"),
        ("idx_lists_folder_id", "lists", "folder_id"),
        ("idx_tasks_list_id", "tasks", "list_id"),
        ("idx_subtasks_task_id", "subtasks", "task_id"),
        ("idx_relations_task_id", "relations", "task_id"),
        ("idx_task_time_status_task_id", "task_time_in_status", "task_id"),
        ("idx_sprints_linked_task_id", "sprints", "linked_task_id"),
        ("idx_tasks_date_updated", "tasks", "date_updated"),
        ("idx_tasks_status", "tasks", "current_status"),
        ("idx_subtasks_status", "subtasks", "status")
    ]
    
    indexes_created = 0
    indexes_skipped = 0
    
    for index_name, table_name, column_name in indexes_to_create:
        try:
            # Try to create the index
            conn.execute(text(f"CREATE INDEX {index_name} ON {table_name}({column_name})"))
            indexes_created += 1
            logging.debug(f"Created index {index_name} on {table_name}({column_name})")
        except Exception as e:
            if "already exists" in str(e).lower() or "duplicate key name" in str(e).lower():
                indexes_skipped += 1
                logging.debug(f"Index {index_name} already exists, skipping")
            else:
                logging.warning(f"Failed to create index {index_name}: {str(e)}")
    
    logging.info(f"Database indexes processed: {indexes_created} created, {indexes_skipped} already existed")

def insert_space(conn):
    """Insert or update space information with enhanced error handling"""
    logging.info(f"Fetching space information for space {SPACE_ID}")
    
    # In REFETCH mode, clear only this space with retry logic
    if mode == 'R':
        logging.info(f"REFETCH mode: Clearing data for space {SPACE_ID}...")
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Use DELETE instead of TRUNCATE to avoid table-level locks
                logging.info(f"Clearing space data (attempt {attempt + 1}/{max_retries})...")
                result = conn.execute(text("DELETE FROM spaces WHERE space_id = :space_id"), {"space_id": to_number(SPACE_ID)})
                deleted_count = result.rowcount if hasattr(result, 'rowcount') else 0
                logging.info(f"Successfully cleared space data - deleted {deleted_count} records")
                break  # Success, exit retry loop
                
            except Exception as e:
                if "Lock wait timeout" in str(e) and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                    logging.warning(f"Database lock timeout (attempt {attempt + 1}/{max_retries}), retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to clear space data after {max_retries} attempts: {str(e)}")
                    # Don't raise - continue with insert/update logic
                    logging.info("Continuing with space insert/update despite clearing failure...")
                    break
    
    space = make_api_request(f"https://api.clickup.com/api/v2/space/{SPACE_ID}")
    
    space_record = {
        "space_id": to_number(space.get("id")),
        "space_name": space.get("name", ""),
        "private_status": space.get("private") is True,
        "archived_status": space.get("archived") is True
    }
    
    conn.execute(text("""
        INSERT INTO spaces (space_id, space_name, private_status, archived_status)
        VALUES (:space_id, :space_name, :private_status, :archived_status)
        ON DUPLICATE KEY UPDATE
            space_name = VALUES(space_name),
            private_status = VALUES(private_status),
            archived_status = VALUES(archived_status)
    """), space_record)
    logging.info(f"Inserted/Updated space: {space_record['space_name']}")

def insert_folders(conn):
    """Insert or update folders with enhanced error handling"""
    logging.info(f"Fetching folders for space {SPACE_ID}")
    
    # In REFETCH mode, clear folders for this space only with retry logic
    if mode == 'R':
        logging.info(f"REFETCH mode: Clearing folders data for space {SPACE_ID}...")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = conn.execute(text("DELETE FROM folders WHERE space_id = :space_id"), {"space_id": to_number(SPACE_ID)})
                deleted_count = result.rowcount if hasattr(result, 'rowcount') else 0
                logging.info(f"Successfully cleared folders data - deleted {deleted_count} records")
                break  # Success, exit retry loop
            except Exception as e:
                if "Lock wait timeout" in str(e) and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                    logging.warning(f"Database lock timeout (attempt {attempt + 1}/{max_retries}), retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to clear folders data after {max_retries} attempts: {str(e)}")
                    logging.info("Continuing with folders insert/update despite clearing failure...")
                    break
    
    folders_data = make_api_request(f"https://api.clickup.com/api/v2/space/{SPACE_ID}/folder")
    folders = folders_data.get("folders", [])
    
    for folder in folders:
        folder_record = {
            "folder_id": to_number(folder.get("id")),
            "space_id": to_number(SPACE_ID),
            "folder_name": folder.get("name", ""),
            "folder_hidden": folder.get("hidden", False),
            "task_count": folder.get("task_count", 0)
        }
        
        conn.execute(text("""
            INSERT INTO folders (folder_id, space_id, folder_name, folder_hidden, task_count)
            VALUES (:folder_id, :space_id, :folder_name, :folder_hidden, :task_count)
            ON DUPLICATE KEY UPDATE
                folder_name = VALUES(folder_name),
                folder_hidden = VALUES(folder_hidden),
                task_count = VALUES(task_count)
        """), folder_record)
    logging.info(f"Inserted/Updated {len(folders)} folders")

def insert_lists(conn):
    """Insert or update lists with enhanced error handling"""
    # In REFETCH mode, clear lists for folders in this space only with retry logic
    if mode == 'R':
        logging.info(f"REFETCH mode: Clearing lists data for space {SPACE_ID}...")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = conn.execute(text("""
                    DELETE FROM lists 
                    WHERE folder_id IN (
                        SELECT folder_id FROM folders WHERE space_id = :space_id
                    )
                """), {"space_id": to_number(SPACE_ID)})
                deleted_count = result.rowcount if hasattr(result, 'rowcount') else 0
                logging.info(f"Successfully cleared lists data - deleted {deleted_count} records")
                break  # Success, exit retry loop
            except Exception as e:
                if "Lock wait timeout" in str(e) and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                    logging.warning(f"Database lock timeout (attempt {attempt + 1}/{max_retries}), retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to clear lists data after {max_retries} attempts: {str(e)}")
                    logging.info("Continuing with lists insert/update despite clearing failure...")
                    break
    
    folder_ids = [row[0] for row in conn.execute(text("SELECT folder_id FROM folders WHERE space_id = :sid"), {"sid": to_number(SPACE_ID)})]
    total = 0
    
    # Process folders in parallel
    def process_folder(folder_id):
        try:
            logging.info(f"Fetching lists for folder {folder_id}")
            lists_data = make_api_request(f"https://api.clickup.com/api/v2/folder/{folder_id}/list")
            lists = lists_data.get("lists", [])
            
            list_records = []
            for lst in lists:
                list_name = lst.get("name", "")
                is_sprint = "sprint" in list_name.lower()
                
                list_record = {
                    "list_id": to_number(lst.get("id")),
                    "folder_id": to_number(folder_id),
                    "list_name": list_name,
                    "start_date": ms_to_datetime(lst.get("start_date")),
                    "due_date": ms_to_datetime(lst.get("due_date")),
                    "task_count": lst.get("task_count", 0),
                    "sprint_check": is_sprint,
                    "sprint_name": list_name if is_sprint else None
                }
                
                if list_record["sprint_check"]:
                    logging.info(f"Sprint detected: {list_record['list_name']} with sprint_name: {list_record['sprint_name']}")
                
                list_records.append(list_record)
            
            return list_records
        except Exception as e:
            logging.error(f"Failed to fetch/process lists for folder {folder_id}: {e}")
            return []
    
    # Use ThreadPoolExecutor for parallel processing (reduced concurrency to prevent rate limits)
    with ThreadPoolExecutor(max_workers=2) as executor:  # Reduced from 3 to prevent rate limits
        future_to_folder = {executor.submit(process_folder, folder_id): folder_id for folder_id in folder_ids}
        
        for future in as_completed(future_to_folder):
            folder_id = future_to_folder[future]
            try:
                list_records = future.result(timeout=60)  # Added timeout for safety
                
                # Bulk insert the list records with transaction safety
                for list_record in list_records:
                    try:
                        conn.execute(text("""
                            INSERT INTO lists (list_id, folder_id, list_name, start_date, due_date, task_count, sprint_check, sprint_name)
                            VALUES (:list_id, :folder_id, :list_name, :start_date, :due_date, :task_count, :sprint_check, :sprint_name)
                            ON DUPLICATE KEY UPDATE
                                list_name = VALUES(list_name),
                                start_date = VALUES(start_date),
                                due_date = VALUES(due_date),
                                task_count = VALUES(task_count),
                                sprint_check = VALUES(sprint_check),
                                sprint_name = VALUES(sprint_name)
                        """), list_record)
                    except Exception as e:
                        logging.error(f"Failed to insert list {list_record.get('list_id', 'unknown')}: {str(e)}")
                        metrics.record_error("list_insert_failed", list_record.get('list_id', 'unknown'))
                
                total += len(list_records)
                
            except Exception as e:
                logging.error(f"Failed to process folder {folder_id}: {str(e)}")
                metrics.record_error("folder_processing_failed", folder_id)
    
    logging.info(f"Inserted/Updated {total} lists")

# Async functions disabled for sync optimization
# async def async_process_subtasks_batch(subtask_ids):
#     """Ultra-fast async batch processing for subtasks"""
#     if not subtask_ids:
#         return {}
#     
#     logging.info(f"Processing {len(subtask_ids)} subtasks with async batch processing...")
#     progress = ProgressTracker(len(subtask_ids), "Async fetching subtask details")
#     results = {}
#     
#     # Create semaphore to control concurrency (higher than sync version)
#     semaphore = asyncio.Semaphore(15)  # Allow 15 concurrent requests
#     
#     async def fetch_single_subtask(subtask_id):
#         """Fetch a single subtask with error handling"""
#         try:
#             url = f"https://api.clickup.com/api/v2/task/{subtask_id}"
#             data = await make_async_api_request(url, semaphore=semaphore)
#             progress.update(1)
#             return subtask_id, data
#         except Exception as e:
#             logging.error(f"Failed to fetch async subtask {subtask_id}: {str(e)}")
#             metrics.record_error("async_subtask_fetch_failed", subtask_id)
#             progress.update(1)
#             return subtask_id, None
#     
#     # Process all subtasks concurrently
#     tasks = [fetch_single_subtask(subtask_id) for subtask_id in subtask_ids]
#     
#     # Execute in smaller batches to avoid overwhelming the API
#     batch_size = 100  # Process 100 at a time
#     for i in range(0, len(tasks), batch_size):
#         batch_tasks = tasks[i:i + batch_size]
#         batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
#         
#         for result in batch_results:
#             if isinstance(result, Exception):
#                 logging.error(f"Batch task failed: {result}")
#                 continue
#             subtask_id, data = result
#             if data:
#                 results[subtask_id] = data
#         
#         # Brief pause between batches
#         if i + batch_size < len(tasks):
#             await asyncio.sleep(0.1)
#     
#     progress.complete()
#     logging.info(f"Async processing: fetched {len(results)} out of {len(subtask_ids)} subtasks")
#     return results

def process_subtasks_batch(subtask_ids):
    """Enhanced process subtasks with improved parallel processing, safety checks and monitoring"""
    if not subtask_ids:
        return {}
    
    logging.info(f"Processing {len(subtask_ids)} subtasks with enhanced batch processing...")
    progress = ProgressTracker(len(subtask_ids), "Fetching subtask details")
    results = {}
    failed_count = 0
    
    # Enhanced batch size for better API stability  
    batch_size = 30  # Reduced from 50 to prevent rate limits
    
    # Use ThreadPoolExecutor with optimized parallel processing (reduced to prevent rate limits)
    with ThreadPoolExecutor(max_workers=3) as executor:  # Reduced from 4 to prevent rate limits
        for i, batch in enumerate(process_in_batches(subtask_ids, batch_size)):
            # Submit all subtasks in batch for parallel processing
            future_to_id = {}
            batch_start_time = time.time()
            
            for subtask_id in batch:
                # Add safety check for valid subtask ID
                if not subtask_id or not str(subtask_id).strip():
                    progress.update(1)
                    failed_count += 1
                    continue
                    
                future = executor.submit(fetch_subtask_details, subtask_id)
                future_to_id[future] = subtask_id
            
            # Collect results as they complete with timeout safety
            for future in as_completed(future_to_id, timeout=45):  # Increased timeout
                subtask_id = future_to_id[future]
                try:
                    subtask_details = future.result(timeout=30)  # Individual timeout
                    if subtask_details:
                        results[subtask_id] = subtask_details
                    progress.update(1)
                except Exception as e:
                    logging.error(f"Failed to fetch subtask {subtask_id}: {str(e)}")
                    metrics.record_error("subtask_fetch_failed", subtask_id)
                    failed_count += 1
                    progress.update(1)
            
            batch_duration = time.time() - batch_start_time
            logging.debug(f"Batch {i+1} completed in {batch_duration:.1f}s")
            
            # Enhanced memory management every 15 batches (1500 subtasks)
            if (i + 1) % 15 == 0:
                if check_memory_usage():
                    logging.info("High memory usage detected - triggering cleanup")
                clear_caches_if_needed()
                
                # Report API health status
                health = rate_limiter.get_health_status()
                logging.info(f"API Health: {health['health_score']}%, Delay: {health['current_delay']:.1f}s, Avg Response: {health['avg_response_time']:.1f}s")
    
    progress.complete()
    success_rate = ((len(results) / len(subtask_ids)) * 100) if subtask_ids else 0
    logging.info(f"Successfully fetched {len(results)} out of {len(subtask_ids)} subtasks ({success_rate:.1f}% success rate, {failed_count} failures)")
    
    # Alert if success rate is too low
    if success_rate < 85:
        logging.warning(f"Low subtask fetch success rate: {success_rate:.1f}% - consider reducing concurrency or checking API health")
        metrics.record_error("low_success_rate", f"{success_rate:.1f}%")
    
    return results

# Async functions disabled for sync optimization
# async def async_fetch_task_details_batch(task_ids):
#     """Async batch fetch task details with high concurrency"""
#     if not task_ids:
#         return {}
#     
#     logging.info(f"Async fetching details for {len(task_ids)} tasks...")
#     semaphore = asyncio.Semaphore(10)  # Control concurrency
#     
#     async def fetch_task_detail(task_id):
#         try:
#             url = f"https://api.clickup.com/api/v2/task/{task_id}"
#             params = {"include_subtasks": "true"}
#             data = await make_async_api_request(url, params=params, semaphore=semaphore)
#             return task_id, data
#         except Exception as e:
#             logging.error(f"Failed to fetch async task {task_id}: {str(e)}")
#             return task_id, None
#     
#     # Execute all tasks concurrently
#     tasks = [fetch_task_detail(task_id) for task_id in task_ids]
#     results = await asyncio.gather(*tasks, return_exceptions=True)
#     
#     task_details_map = {}
#     for result in results:
#         if isinstance(result, Exception):
#             continue
#         task_id, data = result
#         if data:
#             task_details_map[task_id] = data
#     
#     logging.info(f"Async fetched {len(task_details_map)} task details")
#     return task_details_map

def insert_tasks(conn, rate_limiter: AdaptiveRateLimiter, metrics: SyncMetrics, validator: DataValidator):
    """Insert or update tasks and related data"""
    # Get lists from the database
    result = conn.execute(text("SELECT list_id FROM lists"))
    list_ids = [row[0] for row in result]
    total_tasks = 0
    total_lists = len(list_ids)
    
    # In REFETCH mode, clear existing data for this space only
    if mode == 'R':
        logging.info(f"REFETCH mode: Clearing existing data for space {SPACE_ID}...")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Get all list IDs for this space
                space_lists = conn.execute(text("""
                    SELECT list_id FROM lists 
                    WHERE folder_id IN (
                        SELECT folder_id FROM folders WHERE space_id = :space_id
                    )
                """), {"space_id": to_number(SPACE_ID)}).fetchall()
                list_ids_in_space = [row[0] for row in space_lists]
                
                if list_ids_in_space:
                    # Delete tasks and related data for lists in this space
                    conn.execute(text("""
                        DELETE FROM tasks 
                        WHERE list_id IN :list_ids
                    """), {"list_ids": tuple(list_ids_in_space)})
                    
                    # Related tables will be cleaned up automatically due to ON DELETE CASCADE
                else:
                    logging.info("No lists found in this space to clean up")
                break  # Success, exit retry loop
            except Exception as e:
                if "Lock wait timeout" in str(e) and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3  # 3, 6, 9 seconds (longer for complex operations)
                    logging.warning(f"Database lock timeout during data clearing (attempt {attempt + 1}/{max_retries}), retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to clear existing data after {max_retries} attempts: {str(e)}")
                    raise
    
    logging.info(f"Found {total_lists} lists in the database")
    if total_lists == 0:
        logging.warning("No lists found in the database. Make sure to populate the lists table first.")
        return
    
    # Initialize enhanced progress tracker for lists
    list_progress = EnhancedProgressTracker(total_lists, "Processing Lists", update_interval=45)
    
    for i, list_id in enumerate(list_ids, 1):
        # Add performance monitoring
        log_performance_metrics()
        
        logging.info(f"Processing list {i}/{total_lists} (ID: {list_id})")
        try:
            url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
            params = {}
            if mode == 'A' and last_run:
                params['date_updated_gt'] = last_run
            
            tasks_data = make_api_request(url, params)
            tasks = tasks_data.get("tasks", [])
            
            # Validate API response
            if not validator.validate_api_response(tasks_data, ["tasks"]):
                logging.error(f"Invalid API response for list {list_id}")
                continue
            
            # Filter valid tasks
            valid_tasks = []
            for task in tasks:
                if validator.validate_task_data(task):
                    valid_tasks.append(task)
            
            logging.info(f"Found {len(valid_tasks)} valid tasks out of {len(tasks)} total")
            
            # Collect all task IDs for intelligent batch processing
            task_ids = [task.get("id") for task in valid_tasks if task.get("id")]
            
            # Intelligent batching based on API health
            health_status = rate_limiter.get_health_status()
            
            # Use enhanced parallel method with improved safety and monitoring
            def fetch_task_details(task_id):
                """Fetch individual task details with error handling"""
                try:
                    return task_id, make_api_request(
                        f"https://api.clickup.com/api/v2/task/{task_id}",
                        {"include_subtasks": "true"}
                    )
                except Exception as e:
                    logging.error(f"Failed to fetch task details for {task_id}: {str(e)}")
                    return task_id, None
            
            task_details_map = {}
            
            if health_status['health_score'] < 50:
                # Process tasks sequentially when API health is poor
                logging.info(f"API health low ({health_status['health_score']:.0f}%), processing tasks sequentially")
                for task_id in task_ids:
                    try:
                        task_id_result, task_details = fetch_task_details(task_id)
                        if task_details:
                            task_details_map[task_id_result] = task_details
                        # Add small delay between requests when health is poor
                        time.sleep(0.8)
                    except Exception as e:
                        logging.error(f"Failed to fetch sequential task details for {task_id}: {str(e)}")
                        continue
            else:
                # Use parallel processing when API health is good
                with ThreadPoolExecutor(max_workers=4) as executor:  # Reduced from 10 to prevent rate limits
                    future_to_id = {executor.submit(fetch_task_details, tid): tid for tid in task_ids}
                    
                    completed_count = 0
                    for future in as_completed(future_to_id, timeout=120):  # Added overall timeout
                        task_id = future_to_id[future]
                        try:
                            task_id_result, task_details = future.result(timeout=45)  # Individual timeout
                            if task_details:
                                task_details_map[task_id_result] = task_details
                            completed_count += 1
                            
                            # Log progress every 100 completed tasks
                            if completed_count % 100 == 0:
                                logging.info(f"Task details progress: {completed_count}/{len(task_ids)} completed")
                                
                        except Exception as e:
                            logging.error(f"Failed to fetch task details for {task_id}: {str(e)}")
                            metrics.record_error("task_details_fetch_failed", task_id)
            
            # Collect all subtask IDs from all tasks for batch processing
            all_subtask_ids = set()
            for task_details in task_details_map.values():
                subtasks = task_details.get("subtasks", [])
                for subtask in subtasks:
                    subtask_id = subtask.get("id")
                    if subtask_id:
                        all_subtask_ids.add(subtask_id)
            
            # Use optimized sync processing for subtasks
            subtask_details_map = {}
            if all_subtask_ids:
                logging.info(f"Fetching details for {len(all_subtask_ids)} subtasks...")
                # Use optimized sync processing
                subtask_details_map = process_subtasks_batch(list(all_subtask_ids))
                
                # Memory management after subtask processing
                check_memory_usage()
            
            # Collect all subtask records for bulk insertion
            all_subtask_records = []
            
            # Process each task with pre-fetched data
            task_progress = ProgressTracker(len(valid_tasks), f"Processing tasks in list {list_id}")
            for task in valid_tasks:
                task_id = task.get("id")
                if not task_id or task_id not in task_details_map:
                    task_progress.update(1)
                    continue
                
                task_details = task_details_map[task_id]
                
                # Calculate subtask times first
                subtasks = task_details.get("subtasks", [])
                subtasks_time_estimate = 0
                subtasks_time_spent = 0
                subtask_records = []
                
                if subtasks:
                    for subtask in subtasks:
                        subtask_id = subtask.get("id")
                        if not subtask_id:
                            continue
                        
                        # Use pre-fetched subtask details
                        subtask_details = subtask_details_map.get(subtask_id, subtask)
                        
                        # Safely handle None values and convert milliseconds to minutes
                        try:
                            # Get raw time values from detailed subtask data
                            raw_estimate = subtask_details.get("time_estimate")
                            raw_spent = subtask_details.get("time_spent")
                            
                            # Convert to float first to handle string numbers
                            if isinstance(raw_estimate, str):
                                raw_estimate = float(raw_estimate)
                            if isinstance(raw_spent, str):
                                raw_spent = float(raw_spent)
                                
                            # Handle None or 0 values
                            time_estimate = int((raw_estimate or 0) / (1000 * 60))  # Convert ms to minutes
                            time_spent = int((raw_spent or 0) / (1000 * 60))  # Convert ms to minutes
                            
                            if time_estimate < 0:
                                logging.warning(f"Negative time estimate in subtask {subtask_id}, setting to 0")
                                time_estimate = 0
                            if time_spent < 0:
                                logging.warning(f"Negative time spent in subtask {subtask_id}, setting to 0")
                                time_spent = 0
                                
                        except (TypeError, ValueError) as e:
                            logging.warning(f"Invalid time values in subtask {subtask_id}: {str(e)}, defaulting to 0")
                            time_estimate = 0
                            time_spent = 0
                        
                        subtasks_time_estimate += time_estimate
                        subtasks_time_spent += time_spent
                        
                        subtask_tags = subtask_details.get("tags", [])
                        subtask_tag_names = [tag.get("name", "") for tag in subtask_tags if tag.get("name")]
                        
                        subtask_records.append({
                            "task_id": task_id,
                            "task_name": task_details.get("name"),
                            "subtask_id": subtask_id,
                            "subtask_name": subtask_details.get("name"),
                            "description": subtask_details.get("description"),
                            "time_estimate": time_estimate,
                            "time_spent": time_spent,
                            "status": subtask_details.get("status", {}).get("status") if isinstance(subtask_details.get("status"), dict) else subtask_details.get("status"),
                            "tags_names": ", ".join(subtask_tag_names) if subtask_tag_names else None
                        })
                
                # Extract task information
                list_id_from_task = to_number(task_details.get("list", {}).get("id"))
                locations = task_details.get("locations", [])
                location_ids = [loc["id"] for loc in locations]
                location_names = [loc["name"] for loc in locations]
                tags = task_details.get("tags", [])
                tag_names = [tag.get("name", "") for tag in tags if tag.get("name")]

                # Create task record first before handling time status
                task_record = {
                    "task_id": task_details.get("id"),
                    "list_id": list_id_from_task or to_number(list_id),
                    "custom_item_id": task_details.get("custom_item_id"),
                    "name": validator.sanitize_text_content(task_details.get("name"), 500),
                    "text_content": validator.sanitize_text_content(task_details.get("text_content")),
                    "description": validator.sanitize_text_content(task_details.get("description")),
                    "tags_names": validator.sanitize_text_content(", ".join(tag_names) if tag_names else None, 1000),
                    "points": task_details.get("points"),
                    "current_status": validator.sanitize_text_content(task_details.get("status", {}).get("status"), 255),
                    "time_estimate": validator.validate_time_value(task_details.get("time_estimate", 0)),
                    "time_spent": validator.validate_time_value(task_details.get("time_spent", 0)),
                    "date_created": ms_to_datetime(task_details.get("date_created")),
                    "date_updated": ms_to_datetime(task_details.get("date_updated")),
                    "date_closed": ms_to_datetime(task_details.get("date_closed")),
                    "date_done": ms_to_datetime(task_details.get("date_done")),
                    "time_track": validator.validate_time_value(task_details.get("time_spent", 0)),
                    "locations_ids": validator.sanitize_text_content(", ".join(location_ids), 1000),
                    "locations_names": validator.sanitize_text_content(", ".join(location_names), 1000),
                    "subtasks_time_estimate": subtasks_time_estimate,
                    "subtasks_time_spent": subtasks_time_spent
                }
                
                # Insert/Update task first
                try:
                    conn.execute(text("""
                        INSERT INTO tasks (
                            task_id, list_id, custom_item_id, name, text_content, description,
                            tags_names, points, current_status, time_estimate, time_spent,
                            date_created, date_updated, date_closed, date_done, time_track,
                            locations_ids, locations_names, subtasks_time_estimate, subtasks_time_spent
                        ) VALUES (
                            :task_id, :list_id, :custom_item_id, :name, :text_content, :description,
                            :tags_names, :points, :current_status, :time_estimate, :time_spent,
                            :date_created, :date_updated, :date_closed, :date_done, :time_track,
                            :locations_ids, :locations_names, :subtasks_time_estimate, :subtasks_time_spent
                        )
                        ON DUPLICATE KEY UPDATE
                            name = VALUES(name),
                            text_content = VALUES(text_content),
                            description = VALUES(description),
                            tags_names = VALUES(tags_names),
                            current_status = VALUES(current_status),
                            time_estimate = VALUES(time_estimate),
                            time_spent = VALUES(time_spent),
                            date_updated = VALUES(date_updated),
                            date_closed = VALUES(date_closed),
                            date_done = VALUES(date_done),
                            time_track = VALUES(time_track),
                            locations_ids = VALUES(locations_ids),
                            locations_names = VALUES(locations_names),
                            subtasks_time_estimate = VALUES(subtasks_time_estimate),
                            subtasks_time_spent = VALUES(subtasks_time_spent)
                    """), task_record)
                    
                    metrics.tasks_processed += 1
                    
                except Exception as e:
                    print(f"Error inserting task {task_id}: {e}")
                    metrics.record_error("task_insert", task_id)
                    continue

                # Now fetch and process time in status data with enhanced validation
                try:
                    # Check cache first for time_in_status to reduce API calls
                    cache_key = f"time_status_{task_id}"
                    if cache_key in api_request_cache:
                        time_in_status_data = api_request_cache[cache_key]
                    else:
                        time_in_status_data = make_api_request(f"https://api.clickup.com/api/v2/task/{task_id}/time_in_status")
                        if time_in_status_data and isinstance(time_in_status_data, dict):
                            api_request_cache[cache_key] = time_in_status_data
                    
                    # Enhanced validation for time_in_status response
                    if not time_in_status_data:
                        logging.debug(f"No time in status data returned for task {task_id} - skipping status processing")
                        metrics.record_error("no_status_data", task_id)
                    elif not isinstance(time_in_status_data, dict):
                        logging.warning(f"Time in status data is not a dictionary for task {task_id}: {type(time_in_status_data).__name__} - skipping status processing")
                        metrics.record_error("invalid_status_data_type", task_id)
                    elif not validator.validate_api_response(time_in_status_data):
                        logging.warning(f"Failed API response validation for time in status data for task {task_id} - skipping status processing")
                        metrics.record_error("invalid_status_data", task_id)
                    else:
                        # Only process time_in_status if validation passes
                        # Remove existing entries for this task
                        conn.execute(text("DELETE FROM task_time_in_status WHERE task_id = :task_id"), 
                                   {"task_id": task_id})
                        
                        # Process time in status data (converting milliseconds to minutes)
                        current_status = time_in_status_data.get("current_status", {})
                        status_history = time_in_status_data.get("status_history", [])
                        
                        # Convert millisecond timestamps in status history to minutes
                        for status in status_history:
                            if 'total_time' in status:
                                ms = status['total_time'].get('since', 0)
                                if ms:
                                    try:
                                        # Handle string timestamps
                                        if isinstance(ms, str):
                                            ms = int(float(ms))
                                        status['total_time']['since'] = int(ms / (1000 * 60))  # Convert ms to minutes
                                    except (ValueError, TypeError) as e:
                                        logging.warning(f"Could not convert status timestamp {ms} to minutes: {str(e)}")
                                        status['total_time']['since'] = None
                        
                        # Sort status history by orderindex to ensure correct chronological order
                        status_history.sort(key=lambda x: x.get("orderindex", 0))
                        
                        # Filter out the current status from history if it exists there
                        if current_status:
                            current_status_name = current_status.get("status")
                            status_history = [s for s in status_history if s.get("status") != current_status_name]
                        
                        # Create a list of all statuses including current status for processing
                        all_statuses = status_history + [current_status] if current_status else status_history
                        
                        # Process all statuses
                        for i, status_entry in enumerate(all_statuses):
                            try:
                                total_time_minutes = status_entry.get("total_time", {}).get("by_minute", 0)
                                since_timestamp = status_entry.get("total_time", {}).get("since")
                                
                                # Get the from_date of the next status to use as this status's to_date
                                next_status = all_statuses[i + 1] if i < len(all_statuses) - 1 else None
                                next_status_timestamp = next_status.get("total_time", {}).get("since") if next_status else None
                                
                                status_record = {
                                    "task_id": task_id,
                                    "task_name": validator.sanitize_text_content(task_details.get("name"), 65535),  # TEXT field can handle up to 65K characters
                                    "status": validator.sanitize_text_content(status_entry.get("status"), 255),
                                    "total_time": validator.validate_time_value(total_time_minutes),
                                    "from_date": ms_to_datetime(since_timestamp) if since_timestamp else None,
                                    "to_date": ms_to_datetime(next_status_timestamp) if next_status_timestamp else None
                                }
                                
                                # Insert status record without logging each one
                                try:
                                    conn.execute(text("""
                                        INSERT INTO task_time_in_status (
                                            task_id, task_name, status, total_time, from_date, to_date
                                        ) VALUES (
                                            :task_id, :task_name, :status, :total_time, :from_date, :to_date
                                        )
                                    """), status_record)
                                    
                                    metrics.status_records_processed += 1
                                except Exception as insert_error:
                                    # Log the actual task name length for debugging
                                    task_name_len = len(status_record.get("task_name", "")) if status_record.get("task_name") else 0
                                    logging.error(f"Failed to insert status record for task {task_id}: {str(insert_error)}")
                                    logging.error(f"Task name length: {task_name_len} characters")
                                    if task_name_len > 0:
                                        logging.error(f"Task name preview: {status_record.get('task_name', '')[:100]}...")
                                    metrics.record_error("status_insert", f"{task_id}_{status_entry.get('status')}")
                                    continue
                                
                            except Exception as e:
                                logging.error(f"Failed to process historical status {status_entry.get('status')} for task {task_id}: {str(e)}")
                                metrics.record_error("status_record", f"{task_id}_{status_entry.get('status')}")
                                continue
                            
                except Exception as e:
                    logging.warning(f"Failed to fetch time in status data for task {task_id}: {str(e)} - continuing with task processing")
                    metrics.record_error("status_fetch", task_id)
                    # Continue processing the task without time_in_status data
                
                # Process subtasks (collect for bulk insertion later)
                if subtask_records:
                    # Clear existing subtasks for this task
                    conn.execute(text("DELETE FROM subtasks WHERE task_id = :task_id"), {"task_id": task_id})
                    # Add to bulk collection
                    all_subtask_records.extend(subtask_records)

                # Process relations
                # Delete existing relations
                conn.execute(text("DELETE FROM relations WHERE task_id = :task_id"), {"task_id": task_id})
                
                # Insert new relations
                for linked_task in task_details.get("linked_tasks", []):
                    if linked_task.get("link_id"):
                        relation_record = {
                            "task_id": task_id,
                            "task_name": validator.sanitize_text_content(task_details.get("name"), 500),
                            "linked_id": linked_task.get("link_id"),
                            "linked_name": validator.sanitize_text_content(linked_task.get("task", {}).get("name", ""), 500)
                        }
                        conn.execute(text("""
                            INSERT INTO relations (task_id, task_name, linked_id, linked_name)
                            VALUES (:task_id, :task_name, :linked_id, :linked_name)
                        """), relation_record)
                
                # Process sprint associations
                # Delete existing sprint associations
                conn.execute(text("DELETE FROM sprints WHERE linked_task_id = :task_id"), {"task_id": task_id})
                
                # Insert new sprint associations
                for location in task_details.get("locations", []):
                    sprint_record = {
                        "sprint_id": location.get("id"),
                        "sprint_name": validator.sanitize_text_content(location.get("name"), 500),
                        "linked_task_id": task_id,
                        "task_name": validator.sanitize_text_content(task_details.get("name"), 500)
                    }
                    conn.execute(text("""
                        INSERT INTO sprints 
                            (sprint_id, sprint_name, linked_task_id, task_name)
                        VALUES 
                            (:sprint_id, :sprint_name, :linked_task_id, :task_name)
                    """), sprint_record)
                
                # Update task progress
                task_progress.update(1)
                
            # Complete task progress for this list
            task_progress.complete()
            
            # Bulk insert all subtasks for this list
            if all_subtask_records:
                logging.info(f"Bulk inserting {len(all_subtask_records)} subtasks for list {list_id}")
                bulk_insert_subtasks(conn, all_subtask_records)
                all_subtask_records.clear()  # Clear for next list
            
            # Update list progress
            list_progress.update(1)
                
            total_tasks += len(tasks)
        except Exception as e:
            logging.error(f"Error processing list {list_id}: {str(e)}")
            metrics.record_error("list_processing_failed", list_id)
            continue
        
        # Intelligent circuit breaker - pause processing if too many rate limits
        if rate_limiter.consecutive_rate_limits > 8:
            pause_duration = min(60 + (rate_limiter.consecutive_rate_limits * 5), 180)  # 60-180 seconds
            logging.warning(f"Circuit breaker activated! Too many rate limits ({rate_limiter.consecutive_rate_limits}). Pausing for {pause_duration} seconds...")
            time.sleep(pause_duration)
            # Reset rate limiter after pause
            rate_limiter.consecutive_rate_limits = max(0, rate_limiter.consecutive_rate_limits // 2)
            rate_limiter.api_health_score = min(100, rate_limiter.api_health_score + 20)
            logging.info("Circuit breaker pause completed. Resuming processing...")
        
        # Add intelligent delay between lists based on API health
        if i < total_lists:  # Don't sleep after the last list
            # Adaptive delay based on API health and performance
            health = rate_limiter.get_health_status()
            if health['health_score'] < 70:
                delay = 1.5  # Longer delay if API is unhealthy
            elif health['health_score'] < 85:
                delay = 0.5  # Medium delay
            else:
                delay = 0.2  # Short delay if API is healthy
            
            logging.debug(f"Inter-list delay: {delay}s (API health: {health['health_score']}%)")
            time.sleep(delay)
            
        # Enhanced memory management after each list with performance monitoring
        if check_memory_usage():
            logging.info("Memory cleanup triggered after list processing")
        if i % 8 == 0:  # Every 8 lists instead of 10, clear caches more frequently
            clear_caches_if_needed()
            
            # Log performance metrics every 8 lists
            health = rate_limiter.get_health_status()
            logging.info(f"Performance update - Lists: {i}/{total_lists}, API Health: {health['health_score']}%, Cache hits: {cache_hit_count}")
    
    # Complete list progress tracking
    list_progress.complete()
    
    logging.info(f"Processed {total_tasks} tasks")

def update_relation_names(conn):
    """Update relation names using task information"""
    try:
        logging.info("Updating relation names...")
        conn.execute(text("""
            UPDATE relations r
            JOIN tasks t ON r.linked_id = t.task_id
            SET r.linked_name = t.name
            WHERE r.linked_name = '' OR r.linked_name IS NULL
        """))
        logging.info("Relation names updated successfully")
    except Exception as e:
        logging.error(f"Failed to update relation names: {str(e)}")
        raise

def sync_time_in_status(task_id, conn):
    """Calculate cycle time based on first 'in progress' status and completion date"""
    try:
        # Get the first 'in progress' status record for this task
        first_in_progress = conn.execute(text("""
            SELECT from_date
            FROM task_time_in_status
            WHERE task_id = :task_id
            AND LOWER(status) = 'in progress'
            ORDER BY from_date ASC
            LIMIT 1
        """), {"task_id": task_id}).fetchone()

        # Get the task's completion date
        task_done_date = conn.execute(text("""
            SELECT date_done
            FROM tasks
            WHERE task_id = :task_id
        """), {"task_id": task_id}).fetchone()

        if first_in_progress and first_in_progress[0] and task_done_date and task_done_date[0]:
            # Calculate the difference in minutes
            time_diff = int((task_done_date[0] - first_in_progress[0]).total_seconds() / 60)
            return time_diff if time_diff > 0 else 0
        return 0
    except Exception as e:
        logging.error(f"Failed to calculate time in status for task {task_id}: {str(e)}")
        return 0

def calculate_us_cycle_time(conn):
    """Calculate user story cycle time based on time between first 'in progress' and completion"""
    logging.info("Calculating US cycle times...")
    try:
        # Get user stories with custom_item_id = '1001' that have a completion date
        user_stories = conn.execute(text("""
            SELECT DISTINCT task_id, current_status 
            FROM tasks 
            WHERE custom_item_id = '1001'
            AND date_done IS NOT NULL
        """)).fetchall()
        
        if not user_stories:
            logging.info("No completed user stories found to calculate cycle time")
            # Reset cycle time for tasks without completion date
            conn.execute(text("""
                UPDATE tasks 
                SET us_cycle_time = 0 
                WHERE custom_item_id = '1001'
                AND date_done IS NULL
            """))
            return
        
        updated_count = 0
        for task_id, status in user_stories:
            try:
                # Calculate cycle time based on first 'in progress' status and completion date
                cycle_time = sync_time_in_status(task_id, conn)
                
                # Update the task with the calculated cycle time
                result = conn.execute(
                    text("UPDATE tasks SET us_cycle_time = :tot WHERE task_id = :tid"),
                    {'tot': cycle_time, 'tid': task_id}
                )
                if result.rowcount > 0:
                    updated_count += 1
            except Exception as e:
                logging.error(f"Failed to calculate cycle time for task {task_id}: {str(e)}")
        
        # Reset cycle time for tasks without completion date
        conn.execute(text("""
            UPDATE tasks 
            SET us_cycle_time = 0 
            WHERE custom_item_id = '1001'
            AND date_done IS NULL
        """))
        
        logging.info(f"Successfully updated cycle time for {updated_count} out of {len(user_stories)} user stories")
    except Exception as e:
        logging.error(f"Failed to process user story cycle times: {str(e)}")
        raise
    
    logging.info("US cycle times calculated successfully")

# Main execution
if __name__ == "__main__":
    start_time = datetime.now()
    logging.info(f"Starting data sync at {start_time.strftime('%Y-%m-%d %H:%M:%S')} (Press Ctrl+C to terminate safely at any time)")
    
    # Initialize global objects
    rate_limiter = AdaptiveRateLimiter()
    metrics = SyncMetrics()
    validator = DataValidator()
    
    conn = None
    try:
        # Test database connectivity first with retry logic
        max_conn_retries = 3
        for attempt in range(max_conn_retries):
            try:
                conn = engine.connect()
                
                # Test connection with a simple query
                conn.execute(text("SELECT 1"))
                logging.info("Database connectivity test passed")
                break  # Success, exit retry loop
                
            except Exception as e:
                if attempt < max_conn_retries - 1:
                    wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                    logging.warning(f"Database connectivity test failed (attempt {attempt + 1}/{max_conn_retries}), retrying in {wait_time} seconds: {str(e)}")
                    if conn:
                        conn.close()
                        conn = None
                    time.sleep(wait_time)
                else:
                    logging.error(f"Database connectivity test failed after {max_conn_retries} attempts: {str(e)}")
                    raise
        
        # Set session variables for better lock handling
        conn.execute(text("SET SESSION innodb_lock_wait_timeout = 120"))
        conn.execute(text("SET SESSION lock_wait_timeout = 120"))
        logging.info("Database session configured for enhanced lock handling")
        
        # Create database structure
        create_tables(conn)
        logging.info("Tables created or verified")

        # Insert/update data
        insert_space(conn)
        insert_folders(conn)
        insert_lists(conn)
        insert_tasks(conn, rate_limiter, metrics, validator)
        
        # Post-processing
        update_relation_names(conn)
        calculate_us_cycle_time(conn)
        
        logging.info("All database operations completed successfully")
        
        # Save timestamp for incremental updates (in minutes since epoch)
        if mode == 'R':
            timestamp = int(datetime.now(timezone.utc).timestamp() / 60)
            try:
                with open('last_run.json', 'w') as f:
                    json.dump({'timestamp': timestamp}, f)
            except IOError as e:
                logging.error(f"Failed to save timestamp: {str(e)}")
            
        end_time = datetime.now()
        duration = end_time - start_time
        hours, remainder = divmod(duration.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logging.info(f"Data sync completed successfully in {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        
        # Log comprehensive final summary
        metrics.log_summary()
        
    except KeyboardInterrupt:
        logging.info("\nScript terminated by user")
        metrics.log_summary()
    except Exception as e:
        logging.error("General failure in DB operations", exc_info=True)
        metrics.record_error("general_failure", str(e))
        metrics.log_summary()
        sys.exit(1)
    finally:
        try:
            if conn and not conn.closed:
                conn.close()
                logging.info("Database connection closed successfully")
        except Exception as e:
            logging.error(f"Error during cleanup: {str(e)}")
        
        # Async cleanup disabled for sync version
        # try:
        #     loop = asyncio.new_event_loop()
        #     asyncio.set_event_loop(loop)
        #     loop.run_until_complete(close_async_session())
        #     loop.close()
        # except Exception as e:
        #     logging.error(f"Error cleaning up async session: {str(e)}")
        
        logging.info("Cleanup complete")
