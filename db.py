import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

DB_PATH = os.getenv("DB_PATH", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def _normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def _get_database_url() -> str:
    url = DATABASE_URL or DB_PATH
    url = _normalize_database_url(url)

    if not url:
        raise RuntimeError(
            "Missing DATABASE_URL (or DB_PATH). Please set your PostgreSQL connection string."
        )

    if not (
        url.startswith("postgresql://")
        or url.startswith("postgres://")
    ):
        raise RuntimeError(
            "Invalid database URL. PostgreSQL db.py requires DATABASE_URL like "
            "'postgresql://user:password@host:5432/dbname'"
        )

    return url


@contextmanager
def get_conn():
    conn = psycopg2.connect(_get_database_url(), cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS reminder_events (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    event_time TIMESTAMP NOT NULL,
                    message TEXT NOT NULL,
                    keyword TEXT,
                    canceled BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMP NULL
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS reminder_notifications (
                    id SERIAL PRIMARY KEY,
                    event_id INTEGER NOT NULL REFERENCES reminder_events(id) ON DELETE CASCADE,
                    notify_at TIMESTAMP NOT NULL,
                    offset_seconds INTEGER NOT NULL,
                    sent BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reminder_events_chat_time
                ON reminder_events (chat_id, event_time)
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reminder_events_canceled
                ON reminder_events (canceled)
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reminder_notifications_event_id
                ON reminder_notifications (event_id)
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reminder_notifications_notify_at
                ON reminder_notifications (notify_at)
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reminder_notifications_sent
                ON reminder_notifications (sent)
                """
            )

            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'reminder_events'
                """
            )
            event_columns = {row["column_name"] for row in cur.fetchall()}

            if "completed_at" not in event_columns:
                cur.execute(
                    "ALTER TABLE reminder_events ADD COLUMN completed_at TIMESTAMP NULL"
                )

            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'reminder_notifications'
                """
            )
            notification_columns = {row["column_name"] for row in cur.fetchall()}

            if "sent" not in notification_columns:
                cur.execute(
                    """
                    ALTER TABLE reminder_notifications
                    ADD COLUMN sent BOOLEAN NOT NULL DEFAULT FALSE
                    """
                )

        conn.commit()
