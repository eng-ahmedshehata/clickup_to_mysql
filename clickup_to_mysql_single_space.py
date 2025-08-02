
import os
from dotenv import load_dotenv
import requests
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper functions
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

def ms_to_datetime(ms):
    return datetime.fromtimestamp(int(ms) / 1000) if ms else None

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

# Database setup
try:
    engine = create_engine(DB_CONN_STRING)
except Exception as e:
    logging.error("Failed to connect to DB", exc_info=True)
    exit(1)

def create_tables(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS spaces (
            space_id BIGINT PRIMARY KEY,
            space_name VARCHAR(255),
            private_status BOOLEAN,
            archived_status BOOLEAN
        )
    """))
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

def insert_space(conn):
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
    logging.info(f"Inserted/Updated {len(folders)} folders.")

def insert_lists(conn):
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
    logging.info(f"Inserted/Updated {total} lists.")

# Main execution
try:
    with engine.begin() as conn:
        create_tables(conn)
        logging.info("Tables created or already exist.")
        insert_space(conn)
        insert_folders(conn)
        insert_lists(conn)
except Exception as e:
    logging.error("General failure in DB operations", exc_info=True)
