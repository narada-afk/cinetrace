import psycopg2
import psycopg2.extras
from datetime import date, datetime, timedelta
from config import DATABASE_URL, MAX_REPLIES_PER_ACTOR_PER_DAY

SCHEDULED_DDL = """
CREATE TABLE IF NOT EXISTS scheduled_tweets (
    id                  SERIAL PRIMARY KEY,
    scheduled_date      DATE         NOT NULL,
    slot_hour           SMALLINT     NOT NULL,
    actor_db_name       VARCHAR(200) NOT NULL,
    tweet_text          TEXT         NOT NULL,
    stat_key            VARCHAR(100),
    section             VARCHAR(50)  NOT NULL DEFAULT 'overview',
    status              VARCHAR(20)  NOT NULL DEFAULT 'pending',
    telegram_message_id BIGINT,
    tweet_id            VARCHAR(50),
    created_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
    posted_at           TIMESTAMP,
    UNIQUE (scheduled_date, slot_hour)
);
CREATE INDEX IF NOT EXISTS idx_sched_date_hour ON scheduled_tweets(scheduled_date, slot_hour);
CREATE INDEX IF NOT EXISTS idx_sched_status    ON scheduled_tweets(status);
"""

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
    "ALTER TABLE scheduled_tweets ADD COLUMN IF NOT EXISTS section VARCHAR(50) NOT NULL DEFAULT 'overview'",
]

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def run_migrations():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEDULED_DDL)
            cur.execute(DDL)
            for sql in MIGRATIONS:
                cur.execute(sql)
        conn.commit()
    print("[db] migrations applied")

# ── Scheduled tweets ──────────────────────────────────────────────────────────

def scheduled_slots_exist(scheduled_date: date) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM scheduled_tweets WHERE scheduled_date = %s LIMIT 1",
                (scheduled_date,)
            )
            return cur.fetchone() is not None

def insert_scheduled_tweet(scheduled_date: date, slot_hour: int,
                            actor_db_name: str, tweet_text: str,
                            stat_key: str, section: str = "overview") -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO scheduled_tweets
                   (scheduled_date, slot_hour, actor_db_name, tweet_text, stat_key, section)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (scheduled_date, slot_hour) DO UPDATE
                       SET actor_db_name = EXCLUDED.actor_db_name,
                           tweet_text    = EXCLUDED.tweet_text,
                           stat_key      = EXCLUDED.stat_key,
                           section       = EXCLUDED.section,
                           status        = 'pending'
                   RETURNING id""",
                (scheduled_date, slot_hour, actor_db_name, tweet_text, stat_key, section)
            )
            row_id = cur.fetchone()[0]
        conn.commit()
    return row_id

def set_scheduled_telegram_id(row_id: int, telegram_message_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scheduled_tweets SET telegram_message_id = %s WHERE id = %s",
                (telegram_message_id, row_id)
            )
        conn.commit()

def get_scheduled_tweet(scheduled_date: date, slot_hour: int) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM scheduled_tweets WHERE scheduled_date = %s AND slot_hour = %s",
                (scheduled_date, slot_hour)
            )
            return cur.fetchone()

def get_scheduled_by_telegram_id(telegram_message_id: int) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM scheduled_tweets WHERE telegram_message_id = %s AND status = 'pending'",
                (telegram_message_id,)
            )
            return cur.fetchone()

def mark_scheduled_approved(row_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scheduled_tweets SET status = 'approved' WHERE id = %s",
                (row_id,)
            )
        conn.commit()

def mark_scheduled_rejected(row_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scheduled_tweets SET status = 'rejected' WHERE id = %s",
                (row_id,)
            )
        conn.commit()

def mark_scheduled_posted(row_id: int, tweet_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scheduled_tweets SET status = 'posted', tweet_id = %s, posted_at = NOW() WHERE id = %s",
                (tweet_id, row_id)
            )
        conn.commit()

def get_used_fact_keys(days: int = 7) -> set[str]:
    since = datetime.utcnow() - timedelta(days=days)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stat_key FROM scheduled_tweets WHERE created_at >= %s AND stat_key IS NOT NULL",
                (since,)
            )
            return {row[0] for row in cur.fetchall()}

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

def last_post_time_for_actor(handle: str) -> datetime | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT posted_at FROM bot_tweet_log
                   WHERE actor_handle = %s AND status = 'posted'
                   ORDER BY posted_at DESC LIMIT 1""",
                (handle,)
            )
            row = cur.fetchone()
            return row[0] if row else None

def last_actor_activity_time(handle: str) -> datetime | None:
    """Most recent created_at across pending + approved + posted.

    Unlike last_post_time_for_actor() which only looks at confirmed posts,
    this catches in-flight items sitting in the Telegram review queue —
    preventing a second trigger from firing while a first is awaiting approval.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT MAX(created_at) FROM bot_tweet_log
                   WHERE actor_handle = %s
                   AND status IN ('pending', 'approved', 'posted')""",
                (handle,)
            )
            row = cur.fetchone()
            return row[0] if row else None

def total_reactive_count_this_week() -> int:
    """Count of reactive (non-scheduled) items queued or posted in the last 7 days.

    Used to enforce MAX_REACTIVE_REPLIES_PER_WEEK across all actors combined.
    Counts pending + approved + posted so items awaiting review are included.
    """
    since = datetime.utcnow() - timedelta(days=7)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) FROM bot_tweet_log
                   WHERE created_at >= %s
                   AND trigger_type IN ('tweet', 'signal', 'trend', 'reddit_post')
                   AND status IN ('pending', 'approved', 'posted')""",
                (since,)
            )
            return cur.fetchone()[0]

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
