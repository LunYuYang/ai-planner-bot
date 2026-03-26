import sqlite3
from config import DB_PATH


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reminder_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        text TEXT,
        event_time TEXT,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reminder_notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER,
        notify_time TEXT,
        sent INTEGER DEFAULT 0
    )
    """)

    conn.commit()
    conn.close()
