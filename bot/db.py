import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from config import DATABASE_URL, MAX_REPLIES_PER_ACTOR_PER_DAY

DDL = """
CREATE TABLE IF NOT EXISTS bot_tweet_log (
    id                  SERIAL PRIMARY KEY,
    tweet_id            VARCHAR(50)  UNIQUE NOT NULL,
    actor_handle        VARCHAR(100) NOT NULL,
    actor_db_name       VARCHAR(200) NOT NULL,
    trigger_type        VARCHAR(20)  NOT NULL DEFAULT 'tweet',
    platform            VARCHAR(20)  NOT NULL DEFAULT 'twitter',
    source_url          TEXT,
    draft_reply         TEXT,
    reply_tweet_id      VARCHAR(50),
    confidence_score    FLOAT,
    status              VARCHAR(20)  NOT NULL DEFAULT 'pending',
    telegram_message_id BIGINT,
    rejection_reason    VARCHAR(200),
    created_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
    posted_at           TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bot_tweet_log_status       ON bot_tweet_log(status);
CREATE INDEX IF NOT EXISTS idx_bot_tweet_log_actor_handle ON bot_tweet_log(actor_handle);
CREATE INDEX IF NOT EXISTS idx_bot_tweet_log_created_at   ON bot_tweet_log(created_at);
"""

# Incremental migrations for existing tables
MIGRATIONS = [
    "ALTER TABLE bot_tweet_log ADD COLUMN IF NOT EXISTS platform   VARCHAR(20) NOT NULL DEFAULT 'twitter'",
    "ALTER TABLE bot_tweet_log ADD COLUMN IF NOT EXISTS source_url TEXT",
]

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def run_migrations():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            for sql in MIGRATIONS:
                cur.execute(sql)
        conn.commit()
    print("[db] migrations applied")

def already_replied(tweet_id: str) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM bot_tweet_log WHERE tweet_id = %s AND status != 'dropped'",
                (tweet_id,)
            )
            return cur.fetchone() is not None

def actor_reply_count_today(handle: str) -> int:
    since = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) FROM bot_tweet_log
                   WHERE actor_handle = %s AND created_at >= %s
                   AND status IN ('pending','approved','posted')""",
                (handle, since)
            )
            return cur.fetchone()[0]

def last_post_time() -> datetime | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT posted_at FROM bot_tweet_log WHERE status = 'posted' ORDER BY posted_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            return row[0] if row else None

def insert_pending(tweet_id: str, actor_handle: str, actor_db_name: str,
                   draft_reply: str, confidence: float,
                   trigger_type: str = "tweet", platform: str = "twitter",
                   source_url: str = "") -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO bot_tweet_log
                   (tweet_id, actor_handle, actor_db_name, draft_reply, confidence_score,
                    trigger_type, platform, source_url)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (tweet_id, actor_handle, actor_db_name, draft_reply, confidence,
                 trigger_type, platform, source_url or "")
            )
            row_id = cur.fetchone()[0]
        conn.commit()
    return row_id

def set_telegram_message_id(row_id: int, telegram_message_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_tweet_log SET telegram_message_id = %s WHERE id = %s",
                (telegram_message_id, row_id)
            )
        conn.commit()

def get_pending_by_telegram_id(telegram_message_id: int) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM bot_tweet_log WHERE telegram_message_id = %s AND status = 'pending'",
                (telegram_message_id,)
            )
            return cur.fetchone()

def mark_approved(row_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_tweet_log SET status = 'approved' WHERE id = %s",
                (row_id,)
            )
        conn.commit()

def mark_posted(row_id: int, reply_tweet_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_tweet_log SET status = 'posted', reply_tweet_id = %s, posted_at = NOW() WHERE id = %s",
                (reply_tweet_id, row_id)
            )
        conn.commit()

def mark_rejected(row_id: int, reason: str = ""):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_tweet_log SET status = 'rejected', rejection_reason = %s WHERE id = %s",
                (reason, row_id)
            )
        conn.commit()

def mark_dropped(row_id: int, reason: str = ""):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_tweet_log SET status = 'dropped', rejection_reason = %s WHERE id = %s",
                (reason, row_id)
            )
        conn.commit()
