
import os
from dotenv import load_dotenv
import requests
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import traceback
import signal
import sys

# Handle graceful shutdown
def signal_handler(signum, frame):
    logging.info("\nReceived termination signal. Cleaning up...")
    sys.exit(0)

# Register the signal handler for Ctrl+C (SIGINT)
signal.signal(signal.SIGINT, signal_handler)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper functions
def to_number(val):
    if val is None or val == '':
        return None
    try:
        return int(val)
    except Exception:
        try:
            return float(val)
        except:
            return None

def ms_to_datetime(ms):
    return datetime.fromtimestamp(int(ms) / 1000) if ms else None

def extract_comma_separated(values, key):
    return ", ".join([v.get(key, "") for v in values]) if isinstance(values, list) else None

# Load environment variables
load_dotenv()
CLICKUP_API_KEY = os.getenv("CLICKUP_API_KEY")
SPACE_ID = os.getenv("SPACE_ID")
DB_CONN_STRING = os.getenv("DB_CONN_STRING")

for name, value in [("CLICKUP_API_KEY", CLICKUP_API_KEY), ("SPACE_ID", SPACE_ID), ("DB_CONN_STRING", DB_CONN_STRING)]:
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

headers = {"Authorization": CLICKUP_API_KEY}

try:
    engine = create_engine(DB_CONN_STRING)
except Exception as e:
    logging.error("Failed to connect to DB", exc_info=True)
    exit(1)

def create_tables(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS lists (
            list_id BIGINT PRIMARY KEY,
            folder_id BIGINT,
            list_name VARCHAR(255),
            start_date DATETIME NULL,
            due_date DATETIME NULL,
            task_count INT,
            sprint_check BOOLEAN,
            sprint_name VARCHAR(255)
        )
    """))
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
            time_estimate BIGINT,
            time_spent BIGINT,
            date_created DATETIME,
            date_updated DATETIME,
            date_closed DATETIME,
            date_done DATETIME,
            time_track BIGINT,
            locations_ids TEXT,
            locations_names TEXT,
            FOREIGN KEY (list_id) REFERENCES lists(list_id) ON DELETE CASCADE
        )
    """))
    
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

def update_relation_names(conn):
    """Update the linked_name field in relations table using task names from tasks table"""
    logging.info("Updating relation names...")
    conn.execute(text("""
        UPDATE relations r
        JOIN tasks t ON r.linked_id = t.task_id
        SET r.linked_name = t.name
        WHERE r.linked_name = '' OR r.linked_name IS NULL
    """))
    logging.info("Relation names updated successfully")

def insert_tasks(conn):
    list_ids = [row[0] for row in conn.execute(text("SELECT list_id FROM lists"))]
    total_tasks = 0
    total_lists = len(list_ids)
    
    for i, list_id in enumerate(list_ids, 1):
        logging.info(f"Processing list {i}/{total_lists} (ID: {list_id})")
        try:
            url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            tasks = response.json().get("tasks", [])
            for task in tasks:
                task_id = task.get("id")
                if not task_id:
                    logging.warning(f"Skipping task with null ID in list {list_id}")
                    continue
                    
                task_details = requests.get(f"https://api.clickup.com/api/v2/task/{task_id}", headers=headers).json()
                
                # Get list ID from the task's list object
                list_id_from_task = to_number(task_details.get("list", {}).get("id"))
                
                # Extract locations
                locations = task_details.get("locations", [])
                location_ids = [loc["id"] for loc in locations]
                location_names = [loc["name"] for loc in locations]
                
                # Extract tags
                tags = task_details.get("tags", [])
                tag_names = [tag.get("name", "") for tag in tags if tag.get("name")]
                
                task_record = {
                    "task_id": task_details.get("id"),  # Using string ID directly
                    "list_id": list_id_from_task or to_number(list_id),  # Prefer list ID from task details
                    "custom_item_id": task_details.get("custom_item_id"),
                    "name": task_details.get("name"),
                    "text_content": task_details.get("text_content"),
                    "description": task_details.get("description"),
                    "tags_names": ", ".join(tag_names) if tag_names else None,
                    "points": task_details.get("points"),
                    "current_status": task_details.get("status", {}).get("status"),
                    "time_estimate": task_details.get("time_estimate"),
                    "time_spent": task_details.get("time_spent"),
                    "date_created": ms_to_datetime(task_details.get("date_created")),
                    "date_updated": ms_to_datetime(task_details.get("date_updated")),
                    "date_closed": ms_to_datetime(task_details.get("date_closed")),
                    "date_done": ms_to_datetime(task_details.get("date_done")),
                    "time_track": task_details.get("time_spent", 0),  # Using time_spent as time_track
                    "locations_ids": ", ".join(location_ids),
                    "locations_names": ", ".join(location_names)
                }
                # Insert task first
                # Insert the main task
                conn.execute(text("""
                    INSERT INTO tasks (
                        task_id, list_id, custom_item_id, name, text_content, description,
                        tags_names, points, current_status, time_estimate, time_spent,
                        date_created, date_updated, date_closed, date_done, time_track,
                        locations_ids, locations_names
                    ) VALUES (
                        :task_id, :list_id, :custom_item_id, :name, :text_content, :description,
                        :tags_names, :points, :current_status, :time_estimate, :time_spent,
                        :date_created, :date_updated, :date_closed, :date_done, :time_track,
                        :locations_ids, :locations_names
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
                        locations_names = VALUES(locations_names)
                """), task_record)

                # Insert task relations
                # First, delete existing relations for this task
                conn.execute(text("DELETE FROM relations WHERE task_id = :task_id"), {"task_id": task_id})
                
                # Insert new relations
                for linked_task in task_details.get("linked_tasks", []):
                    if linked_task.get("link_id"):
                        relation_record = {
                            "task_id": task_id,
                            "task_name": task_details.get("name", ""),
                            "linked_id": linked_task["link_id"],
                            "linked_name": ""  # We'll update this later when we process the linked task
                        }
                        conn.execute(text("""
                            INSERT INTO relations (task_id, task_name, linked_id, linked_name)
                            VALUES (:task_id, :task_name, :linked_id, :linked_name)
                        """), relation_record)
                
                # Handle sprint associations
                # First, delete existing sprint records for this task
                conn.execute(text("DELETE FROM sprints WHERE linked_task_id = :task_id"), {"task_id": task_id})
                
                # Insert sprint records
                for location in task_details.get("locations", []):
                    sprint_record = {
                        "sprint_id": location.get("id"),
                        "sprint_name": location.get("name"),
                        "linked_task_id": task_id,
                        "task_name": task_details.get("name", "")
                    }
                    conn.execute(text("""
                        INSERT INTO sprints (sprint_id, sprint_name, linked_task_id, task_name)
                        VALUES (:sprint_id, :sprint_name, :linked_task_id, :task_name)
                    """), sprint_record)
            total_tasks += len(tasks)
        except Exception as e:
            logging.error(f"Error inserting tasks for list {list_id}: {e}")
    logging.info(f"Inserted/Updated {total_tasks} tasks.")

# Main execution
if __name__ == "__main__":
    logging.info("Starting data sync (Press Ctrl+C to terminate safely at any time)")
    try:
        with engine.begin() as conn:
            create_tables(conn)
            insert_tasks(conn)
            update_relation_names(conn)
        logging.info("Data sync completed successfully")
    except KeyboardInterrupt:
        logging.info("\nScript terminated by user")
    except Exception as e:
        logging.error("General failure in DB operations", exc_info=True)
    finally:
        logging.info("Cleanup complete")
