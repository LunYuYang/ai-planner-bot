import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL in environment variables.")


def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor
    )


def init_db():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS reminder_events (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT,
                event_time TIMESTAMPTZ,
                message TEXT,
                keyword TEXT,
                canceled INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS reminder_notifications (
                id SERIAL PRIMARY KEY,
                event_id INTEGER,
                chat_id BIGINT,
                notify_time TIMESTAMPTZ,
                notify_type TEXT,
                label TEXT,
                sent INTEGER DEFAULT 0,
                canceled INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ
            )
            """)

            cur.execute("""
            ALTER TABLE reminder_events
            ALTER COLUMN event_time TYPE TIMESTAMPTZ
            USING event_time AT TIME ZONE 'Asia/Taipei'
            """)

            cur.execute("""
            ALTER TABLE reminder_events
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'Asia/Taipei'
            """)

            cur.execute("""
            ALTER TABLE reminder_notifications
            ALTER COLUMN notify_time TYPE TIMESTAMPTZ
            USING notify_time AT TIME ZONE 'Asia/Taipei'
            """)

            cur.execute("""
            ALTER TABLE reminder_notifications
            ALTER COLUMN created_at TYPE TIMESTAMPTZ
            USING created_at AT TIME ZONE 'Asia/Taipei'
            """)

        conn.commit()
    finally:
        conn.close()
