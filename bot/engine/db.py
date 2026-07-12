"""
Engine-owned tables + repository functions.

Reuses the bot's psycopg2 connection (same DATABASE_URL / Postgres instance
as the backend, so discovery rules can query the cinema graph directly).

Tables:
  insights          — every discovered + ranked insight (payload = Insight JSON)
  content_items     — one rendering of an insight for one platform, lifecycle
                      new → approved/rejected → posted/failed
  insight_cooldowns — fingerprint → last time this fact was posted (dedup window)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Optional

import psycopg2
import psycopg2.extras

from config import DATABASE_URL
from engine.models import ContentItem, ContentStatus, Insight, Platform, RankedInsight
from engine.shared.logging import get_logger

log = get_logger("db")

_DDL = """
CREATE TABLE IF NOT EXISTS insights (
    id               BIGSERIAL PRIMARY KEY,
    rule             TEXT        NOT NULL,
    fingerprint      TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    score            NUMERIC,
    score_components JSONB,
    weights_version  TEXT,
    discovered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_insights_fingerprint
    ON insights (fingerprint, discovered_at DESC);
CREATE INDEX IF NOT EXISTS idx_insights_rule
    ON insights (rule, discovered_at DESC);

CREATE TABLE IF NOT EXISTS content_items (
    id                  BIGSERIAL PRIMARY KEY,
    insight_id          BIGINT REFERENCES insights(id),
    platform            TEXT        NOT NULL DEFAULT 'twitter',
    text                TEXT        NOT NULL,
    media_ref           TEXT,
    status              TEXT        NOT NULL DEFAULT 'new',
    scheduled_date      DATE,
    slot_hour           INT,
    telegram_message_id BIGINT,
    posted_id           TEXT,
    error               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (platform, scheduled_date, slot_hour)
);

CREATE TABLE IF NOT EXISTS insight_cooldowns (
    fingerprint    TEXT PRIMARY KEY,
    last_posted_at TIMESTAMPTZ NOT NULL
);
"""


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def run_engine_migrations() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
    log.info("engine migrations applied")


# ── Insights ──────────────────────────────────────────────────────────────────

def insert_insight(ranked: RankedInsight) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insights
                    (rule, fingerprint, payload, score, score_components, weights_version)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    ranked.insight.rule,
                    ranked.fingerprint,
                    ranked.insight.model_dump_json(),
                    ranked.score.total,
                    json.dumps(ranked.score.components),
                    ranked.score.weights_version,
                ),
            )
            return cur.fetchone()[0]


def get_insight(insight_id: int) -> Optional[Insight]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM insights WHERE id = %s", (insight_id,))
            row = cur.fetchone()
    return Insight.model_validate(row[0]) if row else None


def recent_fingerprint_counts(days: int = 365) -> dict[str, int]:
    """fingerprint → times discovered in the window (novelty feature input)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fingerprint, COUNT(*)
                FROM   insights
                WHERE  discovered_at > now() - make_interval(days => %s)
                GROUP  BY fingerprint
                """,
                (days,),
            )
            return dict(cur.fetchall())


# ── Cooldowns ─────────────────────────────────────────────────────────────────

def fingerprints_on_cooldown(days: int) -> set[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fingerprint FROM insight_cooldowns
                WHERE  last_posted_at > now() - make_interval(days => %s)
                """,
                (days,),
            )
            return {r[0] for r in cur.fetchall()}


def touch_cooldown(fingerprint: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insight_cooldowns (fingerprint, last_posted_at)
                VALUES (%s, now())
                ON CONFLICT (fingerprint) DO UPDATE SET last_posted_at = now()
                """,
                (fingerprint,),
            )


# ── Content items ─────────────────────────────────────────────────────────────

def insert_content_item(item: ContentItem,
                        scheduled_date: Optional[date] = None,
                        slot_hour: Optional[int] = None) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO content_items
                    (insight_id, platform, text, media_ref, status,
                     scheduled_date, slot_hour)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (platform, scheduled_date, slot_hour)
                DO UPDATE SET
                    insight_id = EXCLUDED.insight_id,
                    text       = EXCLUDED.text,
                    media_ref  = EXCLUDED.media_ref,
                    status     = 'new',
                    error      = NULL,
                    updated_at = now()
                RETURNING id
                """,
                (
                    item.insight_id, item.platform.value, item.text,
                    item.media_ref, item.status.value,
                    scheduled_date, slot_hour,
                ),
            )
            return cur.fetchone()[0]


def get_content_item(item_id: int) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM content_items WHERE id = %s", (item_id,))
            return cur.fetchone()


def get_slot_content(platform: Platform, scheduled_date: date,
                     slot_hour: int) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM content_items
                WHERE  platform = %s AND scheduled_date = %s AND slot_hour = %s
                """,
                (platform.value, scheduled_date, slot_hour),
            )
            return cur.fetchone()


def slot_content_exists(platform: Platform, scheduled_date: date) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM content_items
                WHERE  platform = %s AND scheduled_date = %s
                LIMIT  1
                """,
                (platform.value, scheduled_date),
            )
            return cur.fetchone() is not None


def _set_status(item_id: int, status: ContentStatus, **extra) -> None:
    sets = ["status = %s", "updated_at = now()"]
    vals: list[Any] = [status.value]
    for col, v in extra.items():
        sets.append(f"{col} = %s")
        vals.append(v)
    vals.append(item_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE content_items SET {', '.join(sets)} WHERE id = %s",
                vals,
            )


def mark_content_approved(item_id: int) -> None:
    _set_status(item_id, ContentStatus.APPROVED)


def mark_content_rejected(item_id: int) -> None:
    _set_status(item_id, ContentStatus.REJECTED)


def mark_content_posted(item_id: int, posted_id: str) -> None:
    _set_status(item_id, ContentStatus.POSTED, posted_id=posted_id)
    # Start the dedup cooldown for the underlying fact
    row = get_content_item(item_id)
    if row and row.get("insight_id"):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT fingerprint FROM insights WHERE id = %s",
                    (row["insight_id"],),
                )
                fp = cur.fetchone()
        if fp:
            touch_cooldown(fp[0])


def mark_content_failed(item_id: int, error: str) -> None:
    _set_status(item_id, ContentStatus.FAILED, error=error[:500])


def set_content_telegram_id(item_id: int, message_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE content_items SET telegram_message_id = %s, updated_at = now() WHERE id = %s",
                (message_id, item_id),
            )


def actors_used_recently(days: int = 1) -> set[int]:
    """Actor ids referenced by insights whose content was scheduled recently
    (batch-level diversity: max 1 insight per actor per day)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.payload
                FROM   content_items ci
                JOIN   insights i ON i.id = ci.insight_id
                WHERE  ci.created_at > now() - make_interval(days => %s)
                  AND  ci.status != 'rejected'
                """,
                (days,),
            )
            rows = cur.fetchall()
    ids: set[int] = set()
    for (payload,) in rows:
        for e in payload.get("entities", []):
            if e.get("kind") == "actor" and e.get("id"):
                ids.add(e["id"])
    return ids
