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

# Initialize DB engine
try:
    engine = create_engine(DB_CONN_STRING)
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
    
    response = requests.get(f"https://api.clickup.com/api/v2/space/{SPACE_ID}", headers=headers)
    response.raise_for_status()
    space = response.json()
    
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
    
    response = requests.get(f"https://api.clickup.com/api/v2/space/{SPACE_ID}/folder", headers=headers)
    response.raise_for_status()
    folders = response.json().get("folders", [])
    
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
    
    for folder_id in folder_ids:
        try:
            logging.info(f"Fetching lists for folder {folder_id}")
            response = requests.get(f"https://api.clickup.com/api/v2/folder/{folder_id}/list", headers=headers)
            response.raise_for_status()
            lists = response.json().get("lists", [])
            
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
            total += len(lists)
        except Exception as e:
            logging.error(f"Failed to fetch/insert lists for folder {folder_id}: {e}")
            continue
    
    logging.info(f"Inserted/Updated {total} lists")

def insert_tasks(conn):
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
    
    for i, list_id in enumerate(list_ids, 1):
        logging.info(f"Processing list {i}/{total_lists} (ID: {list_id})")
        try:
            url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
            params = {}
            if mode == 'A' and last_run:
                params['date_updated_gt'] = last_run
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            tasks = response.json().get("tasks", [])
            
            for task in tasks:
                task_id = task.get("id")
                if not task_id:
                    logging.warning(f"Skipping task with null ID in list {list_id}")
                    continue
                
                # Fetch detailed task information
                task_details = requests.get(
                    f"https://api.clickup.com/api/v2/task/{task_id}",
                    params={"include_subtasks": "true"},
                    headers=headers
                ).json()
                
                # Calculate subtask times first
                subtasks = task_details.get("subtasks", [])
                subtasks_time_estimate = 0
                subtasks_time_spent = 0
                subtask_records = []
                
                if subtasks:
                    for subtask in subtasks:
                        # Safely handle None values and convert milliseconds to minutes
                        try:
                            # Get raw time values
                            raw_estimate = subtask.get("time_estimate")
                            raw_spent = subtask.get("time_spent")
                            
                            # Convert to float first to handle string numbers
                            if isinstance(raw_estimate, str):
                                raw_estimate = float(raw_estimate)
                            if isinstance(raw_spent, str):
                                raw_spent = float(raw_spent)
                                
                            # Handle None or 0 values
                            time_estimate = int((raw_estimate or 0) / (1000 * 60))  # Convert ms to minutes
                            time_spent = int((raw_spent or 0) / (1000 * 60))  # Convert ms to minutes
                            
                            if time_estimate < 0:
                                logging.warning(f"Negative time estimate in subtask {subtask.get('id')}, setting to 0")
                                time_estimate = 0
                            if time_spent < 0:
                                logging.warning(f"Negative time spent in subtask {subtask.get('id')}, setting to 0")
                                time_spent = 0
                                
                        except (TypeError, ValueError) as e:
                            logging.warning(f"Invalid time values in subtask {subtask.get('id')}: {str(e)}, defaulting to 0")
                            time_estimate = 0
                            time_spent = 0
                        
                        subtasks_time_estimate += time_estimate
                        subtasks_time_spent += time_spent
                        
                        subtask_tags = subtask.get("tags", [])
                        subtask_tag_names = [tag.get("name", "") for tag in subtask_tags if tag.get("name")]
                        
                        subtask_records.append({
                            "task_id": task_id,
                            "task_name": task_details.get("name"),
                            "subtask_id": subtask.get("id"),
                            "subtask_name": subtask.get("name"),
                            "time_estimate": time_estimate,
                            "time_spent": time_spent,
                            "status": subtask.get("status", {}).get("status"),
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
                    "name": task_details.get("name"),
                    "text_content": task_details.get("text_content"),
                    "description": task_details.get("description"),
                    "tags_names": ", ".join(tag_names) if tag_names else None,
                    "points": task_details.get("points"),
                    "current_status": task_details.get("status", {}).get("status"),
                    "time_estimate": int((task_details.get("time_estimate", 0) or 0) / (1000 * 60)),  # Convert ms to minutes
                    "time_spent": int((task_details.get("time_spent", 0) or 0) / (1000 * 60)),  # Convert ms to minutes
                    "date_created": ms_to_datetime(task_details.get("date_created")),
                    "date_updated": ms_to_datetime(task_details.get("date_updated")),
                    "date_closed": ms_to_datetime(task_details.get("date_closed")),
                    "date_done": ms_to_datetime(task_details.get("date_done")),
                    "time_track": int((task_details.get("time_spent", 0) or 0) / (1000 * 60)),  # Convert ms to minutes
                    "locations_ids": ", ".join(location_ids),
                    "locations_names": ", ".join(location_names),
                    "subtasks_time_estimate": subtasks_time_estimate,
                    "subtasks_time_spent": subtasks_time_spent
                }
                
                # Insert/Update task first
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

                # Now fetch and process time in status data
                try:
                    time_in_status_response = requests.get(
                        f"https://api.clickup.com/api/v2/task/{task_id}/time_in_status",
                        headers=headers
                    )
                    time_in_status_response.raise_for_status()
                    time_in_status_data = time_in_status_response.json()

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
                                "task_name": task_details.get("name"),
                                "status": status_entry.get("status"),
                                "total_time": total_time_minutes,
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
                        except Exception as e:
                            logging.error(f"Failed to process historical status {status_entry.get('status')} for task {task_id}: {str(e)}")
                            continue
                except Exception as e:
                    logging.error(f"Failed to fetch time in status data for task {task_id}: {str(e)}")
                    continue
                
                # Now process subtasks since task record exists
                if subtask_records:
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

                # Now that the task exists, we can insert subtasks
                if subtask_records:
                    # Clear existing subtasks
                    conn.execute(text("DELETE FROM subtasks WHERE task_id = :task_id"), {"task_id": task_id})
                    
                    # Insert new subtasks
                    for subtask_record in subtask_records:
                        try:
                            conn.execute(text("""
                                INSERT INTO subtasks (
                                    task_id, task_name, subtask_id, subtask_name,
                                    time_estimate, time_spent, status, tags_names
                                ) VALUES (
                                    :task_id, :task_name, :subtask_id, :subtask_name,
                                    :time_estimate, :time_spent, :status, :tags_names
                                )
                            """), subtask_record)
                        except Exception as e:
                            logging.error(f"Failed to insert subtask {subtask_record['subtask_id']} for task {task_id}: {str(e)}")
                            continue

                # Process relations
                # Delete existing relations
                conn.execute(text("DELETE FROM relations WHERE task_id = :task_id"), {"task_id": task_id})
                
                # Insert new relations
                for linked_task in task_details.get("linked_tasks", []):
                    if linked_task.get("link_id"):
                        relation_record = {
                            "task_id": task_id,
                            "task_name": task_details.get("name", ""),
                            "linked_id": linked_task["link_id"],
                            "linked_name": ""
                        }
                        conn.execute(text("""
                            INSERT INTO relations 
                                (task_id, task_name, linked_id, linked_name)
                            VALUES 
                                (:task_id, :task_name, :linked_id, :linked_name)
                        """), relation_record)
                
                # Process sprint associations
                # Delete existing sprint associations
                conn.execute(text("DELETE FROM sprints WHERE linked_task_id = :task_id"), {"task_id": task_id})
                
                # Insert new sprint associations
                for location in task_details.get("locations", []):
                    sprint_record = {
                        "sprint_id": location.get("id"),
                        "sprint_name": location.get("name"),
                        "linked_task_id": task_id,
                        "task_name": task_details.get("name", "")
                    }
                    conn.execute(text("""
                        INSERT INTO sprints 
                            (sprint_id, sprint_name, linked_task_id, task_name)
                        VALUES 
                            (:sprint_id, :sprint_name, :linked_task_id, :task_name)
                    """), sprint_record)
                
            total_tasks += len(tasks)
        except Exception as e:
            logging.error(f"Error processing list {list_id}: {str(e)}")
            continue
    
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
            insert_tasks(conn)
            
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
    except KeyboardInterrupt:
        logging.info("\nScript terminated by user")
    except Exception as e:
        logging.error("General failure in DB operations", exc_info=True)
        sys.exit(1)
    finally:
        try:
            if conn and not conn.closed:
                conn.close()
        except Exception as e:
            logging.error(f"Error during cleanup: {str(e)}")
        logging.info("Cleanup complete")
