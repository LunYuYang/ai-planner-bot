import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")

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
                event_time TIMESTAMP,
                message TEXT,
                keyword TEXT,
                canceled INTEGER DEFAULT 0,
                created_at TIMESTAMP
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS reminder_notifications (
                id SERIAL PRIMARY KEY,
                event_id INTEGER,
                chat_id BIGINT,
                notify_time TIMESTAMP,
                notify_type TEXT,
                label TEXT,
                sent INTEGER DEFAULT 0,
                canceled INTEGER DEFAULT 0,
                created_at TIMESTAMP
            )
            """)

        conn.commit()
    finally:
        conn.close()
