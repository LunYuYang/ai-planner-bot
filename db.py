import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.getenv("DB_PATH", "planner.db")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reminder_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                message TEXT NOT NULL,
                keyword TEXT,
                canceled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                completed_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reminder_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                notify_at TEXT NOT NULL,
                offset_seconds INTEGER NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (event_id) REFERENCES reminder_events(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute("PRAGMA table_info(reminder_events)")
        event_columns = {row[1] for row in cur.fetchall()}
        if "completed_at" not in event_columns:
            cur.execute("ALTER TABLE reminder_events ADD COLUMN completed_at TEXT")

        cur.execute("PRAGMA table_info(reminder_notifications)")
        notification_columns = {row[1] for row in cur.fetchall()}
        if "sent" not in notification_columns:
            cur.execute(
                "ALTER TABLE reminder_notifications ADD COLUMN sent INTEGER NOT NULL DEFAULT 0"
            )

        conn.commit()
