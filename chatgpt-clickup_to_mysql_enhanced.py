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
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

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
            progress_percent >= (self.last_log_count + 15) or  # Every 15% instead of 10%
            (current_time - self.last_log_time) >= 45  # Every 45 seconds instead of 30
        )
        
        if should_log:
            self._log_progress(current_time, progress_percent)
            self.last_log_time = current_time
            self.last_log_count = int(progress_percent // 15) * 15  # Update to match 15% intervals
    
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

class DataValidator:
    """Validate and sanitize data from ClickUp API"""
    
    @staticmethod
    def validate_task_data(task_data: Dict[str, Any]) -> bool:
        """Validate essential task fields"""
        required_fields = ['id', 'name']
        
        for field in required_fields:
            if not task_data.get(field):
                logging.warning(f"Task missing required field: {field}")
                metrics.record_error("validation_error", task_data.get('id', 'unknown'))
                return False
        
        # Validate task ID format
        task_id = task_data.get('id')
        if not isinstance(task_id, str) or len(task_id) < 5:
            logging.warning(f"Invalid task ID format: {task_id}")
            metrics.record_error("invalid_task_id", task_id)
            return False
        
        return True
    
    @staticmethod
    def sanitize_text_content(text: Any, max_length: int = 65535) -> Optional[str]:
        """Sanitize text content for database storage"""
        if not text:
            return None
        
        # Convert to string if not already
        text = str(text)
        
        # Remove null bytes that can break MySQL
        text = text.replace('\x00', '')
        
        # Remove other problematic characters
        text = re.sub(r'[\x01-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', text)
        
        # Truncate if too long
        if len(text) > max_length:
            text = text[:max_length-3] + "..."
            logging.warning(f"Text truncated to {max_length} characters")
        
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
    """Adaptive rate limiter that adjusts delays based on API responses"""
    def __init__(self, base_delay=1.0, max_delay=30.0):
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.max_delay = max_delay
        self.consecutive_rate_limits = 0
        self.success_count = 0
        
    def on_rate_limit(self):
        """Called when a rate limit (429) is encountered"""
        self.consecutive_rate_limits += 1
        old_delay = self.current_delay
        # Increase delay by 50% with each rate limit, up to max
        self.current_delay = min(self.current_delay * 1.5, self.max_delay)
        logging.warning(f"Rate limit detected. Adjusting delay from {old_delay:.1f}s to {self.current_delay:.1f}s")
        self.success_count = 0  # Reset success counter
    
    def on_success(self):
        """Called when a request succeeds"""
        self.success_count += 1
        # Only reduce delay after several consecutive successes
        if self.success_count >= 5 and self.consecutive_rate_limits > 0:
            old_delay = self.current_delay
            # Gradually reduce delay back towards base
            self.current_delay = max(self.current_delay * 0.9, self.base_delay)
            if old_delay != self.current_delay:
                logging.info(f"API stable. Reducing delay from {old_delay:.1f}s to {self.current_delay:.1f}s")
            self.consecutive_rate_limits = max(0, self.consecutive_rate_limits - 1)
    
    def get_delay(self):
        """Get current delay time"""
        return self.current_delay

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
    if cache_info.currsize > 800:  # Clear when cache is 80% full (800/1000)
        logging.info(f"Clearing subtask cache (was {cache_info.currsize}/1000 items)")
        fetch_subtask_details.cache_clear()
        metrics.cache_clears += 1  # Track cache clears

def bulk_insert_subtasks(conn, subtask_records, batch_size=200):  # Increased from 100
    """Insert subtasks in bulk batches for better performance"""
    if not subtask_records:
        return
    
    total_inserted = 0
    for batch in process_in_batches(subtask_records, batch_size):
        try:
            # Sanitize data before insertion
            sanitized_batch = []
            for record in batch:
                sanitized_record = record.copy()
                sanitized_record['subtask_name'] = validator.sanitize_text_content(
                    record.get('subtask_name'), 255
                )
                sanitized_record['description'] = validator.sanitize_text_content(
                    record.get('description')
                )
                sanitized_record['tags_names'] = validator.sanitize_text_content(
                    record.get('tags_names'), 1000
                )
                sanitized_batch.append(sanitized_record)
            
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
            """), sanitized_batch)
            total_inserted += len(batch)
            metrics.bulk_inserts += 1
            metrics.db_operations += 1
        except Exception as e:
            logging.error(f"Failed to bulk insert subtask batch: {str(e)}")
            metrics.record_error("bulk_insert_failed", f"batch_size_{len(batch)}")
            # Fallback to individual inserts for this batch
            for record in batch:
                try:
                    # Sanitize individual record
                    sanitized_record = record.copy()
                    sanitized_record['subtask_name'] = validator.sanitize_text_content(
                        record.get('subtask_name'), 255
                    )
                    sanitized_record['description'] = validator.sanitize_text_content(
                        record.get('description')
                    )
                    sanitized_record['tags_names'] = validator.sanitize_text_content(
                        record.get('tags_names'), 1000
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
                    total_inserted += 1
                    metrics.individual_inserts += 1
                    metrics.db_operations += 1
                except Exception as e2:
                    logging.error(f"Failed to insert individual subtask {record.get('subtask_id')}: {str(e2)}")
                    metrics.record_error("individual_insert_failed", record.get('subtask_id', 'unknown'))
    
    if total_inserted > 0:
        logging.info(f"Successfully bulk inserted {total_inserted} subtasks")
        metrics.subtasks_processed += total_inserted

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

for name, value in [("CLICKUP_API_KEY", CLICKUP_API_KEY), ("TEAM_ID", TEAM_ID), ("SPACE_ID", SPACE_ID), ("DB_CONN_STRING", DB_CONN_STRING)]:
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

headers = {"Authorization": CLICKUP_API_KEY}

# Create a session for connection pooling and rate limiting
session = requests.Session()
session.headers.update(headers)

# Initialize adaptive rate limiter with more aggressive settings
rate_limiter = AdaptiveRateLimiter(base_delay=0.8, max_delay=25.0)  # Reduced from 1.0 and 30.0

def make_api_request(url, params=None):
    """Make rate-limited API request with adaptive retry logic and metrics tracking"""
    
    # Use adaptive delay instead of fixed delay
    time.sleep(rate_limiter.get_delay())
    
    request_start_time = time.time()
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params)
            request_duration = time.time() - request_start_time
            
            if response.status_code == 429:  # Rate limited
                rate_limiter.on_rate_limit()  # Update adaptive limiter
                metrics.record_rate_limit()  # Track rate limit
                wait_time = min(2 ** attempt * 3, 15)  # More aggressive backoff with max 15 seconds
                logging.warning(f"Rate limited, waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            rate_limiter.on_success()  # Mark successful request
            metrics.record_api_call(success=True, duration=request_duration)
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                metrics.record_api_call(success=False, duration=time.time() - request_start_time)
                metrics.record_error("api_request_failed", url)
                raise
            wait_time = min(2 ** attempt, 5)  # Max 5 seconds for other errors
            logging.warning(f"Request failed (attempt {attempt + 1}), retrying in {wait_time} seconds: {str(e)}")
            time.sleep(wait_time)
    
    return None

@lru_cache(maxsize=1000)  # Increased from 500 for better caching
def fetch_subtask_details(subtask_id):
    """Cache subtask details to avoid duplicate API calls"""
    try:
        return make_api_request(f"https://api.clickup.com/api/v2/task/{subtask_id}")
    except Exception as e:
        logging.warning(f"Failed to fetch cached subtask details for {subtask_id}: {str(e)}")
        return None

# Initialize DB engine with optimized connection pooling
try:
    engine = create_engine(
        DB_CONN_STRING,
        pool_size=5,          # Keep 5 connections in pool
        max_overflow=10,      # Allow up to 10 additional connections
        pool_pre_ping=True,   # Verify connections before use
        pool_recycle=3600,    # Recycle connections every hour
        echo=False            # Set to True for SQL debugging
    )
    logging.info("Database engine initialized with connection pooling")
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
            task_name VARCHAR(255),
            status VARCHAR(255),
            total_time INT,
            from_date DATETIME,
            to_date DATETIME,
            FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
        )
    """))

def insert_space(conn):
    """Insert or update space information"""
    logging.info(f"Fetching space information for space {SPACE_ID}")
    # In REFETCH mode, clear only this space
    if mode == 'R':
        logging.info(f"REFETCH mode: Clearing data for space {SPACE_ID}...")
        conn.execute(text("DELETE FROM spaces WHERE space_id = :space_id"), {"space_id": to_number(SPACE_ID)})
    
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
    """Insert or update folders"""
    logging.info(f"Fetching folders for space {SPACE_ID}")
    # In REFETCH mode, clear folders for this space only
    if mode == 'R':
        logging.info(f"REFETCH mode: Clearing folders data for space {SPACE_ID}...")
        conn.execute(text("DELETE FROM folders WHERE space_id = :space_id"), {"space_id": to_number(SPACE_ID)})
    
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
    """Insert or update lists"""
    # In REFETCH mode, clear lists for folders in this space only
    if mode == 'R':
        logging.info(f"REFETCH mode: Clearing lists data for space {SPACE_ID}...")
        conn.execute(text("""
            DELETE FROM lists 
            WHERE folder_id IN (
                SELECT folder_id FROM folders WHERE space_id = :space_id
            )
        """), {"space_id": to_number(SPACE_ID)})
    
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
    
    # Use ThreadPoolExecutor for parallel processing (reduced concurrency)
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_folder = {executor.submit(process_folder, folder_id): folder_id for folder_id in folder_ids}
        
        for future in as_completed(future_to_folder):
            list_records = future.result()
            
            # Bulk insert the list records
            for list_record in list_records:
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
            
            total += len(list_records)
    
    logging.info(f"Inserted/Updated {total} lists")

def process_subtasks_batch(subtask_ids):
    """Process subtasks in optimized batches with improved parallel processing"""
    if not subtask_ids:
        return {}
    
    logging.info(f"Processing {len(subtask_ids)} subtasks in optimized batches...")
    progress = ProgressTracker(len(subtask_ids), "Fetching subtask details")
    results = {}
    
    # Increased batch size for better performance
    batch_size = 50  # Increased from 25
    
    # Use ThreadPoolExecutor for parallel processing within batches
    with ThreadPoolExecutor(max_workers=3) as executor:  # Controlled parallelism
        for i, batch in enumerate(process_in_batches(subtask_ids, batch_size)):
            # Submit all subtasks in batch for parallel processing
            future_to_id = {}
            for subtask_id in batch:
                future = executor.submit(fetch_subtask_details, subtask_id)
                future_to_id[future] = subtask_id
            
            # Collect results as they complete
            for future in as_completed(future_to_id):
                subtask_id = future_to_id[future]
                try:
                    subtask_details = future.result(timeout=30)  # 30 second timeout
                    if subtask_details:
                        results[subtask_id] = subtask_details
                    progress.update(1)
                except Exception as e:
                    logging.error(f"Failed to fetch subtask {subtask_id}: {str(e)}")
                    metrics.record_error("subtask_fetch_failed", subtask_id)
                    progress.update(1)
            
            # Memory management every 200 subtasks instead of 100
            if (i + 1) % 8 == 0:  # Every 8 batches (400 subtasks)
                check_memory_usage()
                clear_caches_if_needed()
    
    progress.complete()
    logging.info(f"Successfully fetched {len(results)} out of {len(subtask_ids)} subtasks")
    return results

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
        except Exception as e:
            logging.error(f"Failed to clear existing data: {str(e)}")
            raise
    
    logging.info(f"Found {total_lists} lists in the database")
    if total_lists == 0:
        logging.warning("No lists found in the database. Make sure to populate the lists table first.")
        return
    
    # Initialize progress tracker for lists
    list_progress = ProgressTracker(total_lists, "Processing lists")
    
    for i, list_id in enumerate(list_ids, 1):
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
            
            # Collect all task IDs for batch processing
            task_ids = [task.get("id") for task in valid_tasks if task.get("id")]
            
            # Batch fetch task details
            def fetch_task_details(task_id):
                return task_id, make_api_request(
                    f"https://api.clickup.com/api/v2/task/{task_id}",
                    {"include_subtasks": "true"}
                )
            
            task_details_map = {}
            with ThreadPoolExecutor(max_workers=2) as executor:  # Increased from 1 to 2
                future_to_id = {executor.submit(fetch_task_details, tid): tid for tid in task_ids}
                
                for future in as_completed(future_to_id):
                    task_id, task_details = future.result()
                    if task_details:
                        task_details_map[task_id] = task_details
            
            # Collect all subtask IDs from all tasks for batch processing
            all_subtask_ids = set()
            for task_details in task_details_map.values():
                subtasks = task_details.get("subtasks", [])
                for subtask in subtasks:
                    subtask_id = subtask.get("id")
                    if subtask_id:
                        all_subtask_ids.add(subtask_id)
            
            # Batch fetch all subtask details
            subtask_details_map = {}
            if all_subtask_ids:
                logging.info(f"Fetching details for {len(all_subtask_ids)} subtasks...")
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

                # Now fetch and process time in status data
                try:
                    time_in_status_data = make_api_request(f"https://api.clickup.com/api/v2/task/{task_id}/time_in_status")
                    
                    if not validator.validate_api_response(time_in_status_data):
                        print(f"Invalid time in status data for task {task_id}")
                        metrics.record_error("invalid_status_data", task_id)
                        continue

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
                                "task_name": validator.sanitize_text_content(task_details.get("name"), 500),
                                "status": validator.sanitize_text_content(status_entry.get("status"), 255),
                                "total_time": validator.validate_time_value(total_time_minutes),
                                "from_date": ms_to_datetime(since_timestamp) if since_timestamp else None,
                                "to_date": ms_to_datetime(next_status_timestamp) if next_status_timestamp else None
                            }
                            
                            # Insert status record without logging each one
                            conn.execute(text("""
                                INSERT INTO task_time_in_status (
                                    task_id, task_name, status, total_time, from_date, to_date
                                ) VALUES (
                                    :task_id, :task_name, :status, :total_time, :from_date, :to_date
                                )
                            """), status_record)
                            
                            metrics.status_records_processed += 1
                            
                        except Exception as e:
                            logging.error(f"Failed to process historical status {status_entry.get('status')} for task {task_id}: {str(e)}")
                            metrics.record_error("status_record", f"{task_id}_{status_entry.get('status')}")
                            continue
                            
                except Exception as e:
                    logging.error(f"Failed to fetch time in status data for task {task_id}: {str(e)}")
                    metrics.record_error("status_fetch", task_id)
                    continue
                
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
            continue
        
        # Add a smaller delay between lists to reduce API pressure but improve speed
        if i < total_lists:  # Don't sleep after the last list
            time.sleep(1)  # Reduced from 2 to 1 second pause between lists
            
        # Memory management after each list - reduced frequency for better performance
        check_memory_usage()
        if i % 10 == 0:  # Every 10 lists instead of 5, clear caches
            clear_caches_if_needed()
    
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
        with engine.begin() as conn:
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
        except Exception as e:
            logging.error(f"Error during cleanup: {str(e)}")
        logging.info("Cleanup complete")
