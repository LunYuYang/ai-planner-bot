import sqlite3
from config import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()

    conn.execute("""
    CREATE TABLE IF NOT EXISTS reminder_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        event_time TEXT,
        message TEXT,
        keyword TEXT,
        canceled INTEGER DEFAULT 0,
        created_at TEXT
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS reminder_notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER,
        chat_id INTEGER,
        notify_time TEXT,
        notify_type TEXT,
        label TEXT,
        sent INTEGER DEFAULT 0,
        canceled INTEGER DEFAULT 0,
        created_at TEXT
    )
    """)

    conn.commit()
    conn.close()
