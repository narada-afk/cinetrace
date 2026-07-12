"""
Pydantic data contracts for the Insight Engine.

Insight is the single contract every generator consumes. It must contain
NO human-language prose — only entities, metrics and structured facts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class Platform(str, Enum):
    TWITTER   = "twitter"
    THREADS   = "threads"
    INSTAGRAM = "instagram"
    LINKEDIN  = "linkedin"
    YOUTUBE   = "youtube"
    NEWSLETTER = "newsletter"
    REDDIT    = "reddit"
    BLOG      = "blog"


class ContentStatus(str, Enum):
    NEW      = "new"
    APPROVED = "approved"
    REJECTED = "rejected"
    POSTED   = "posted"
    FAILED   = "failed"


class Entity(BaseModel):
    kind: Literal["actor", "director", "movie", "industry"]
    id: Optional[int] = None          # DB id when the entity exists in a table
    name: str
    slug: Optional[str] = None        # frontend slug, for stat-card screenshots


class Metric(BaseModel):
    key: str                          # e.g. "collab_count", "total_gross_cr"
    value: float | int
    unit: Optional[str] = None        # "films", "₹Cr", "%", "years"
    period: Optional[tuple[int, int]] = None   # (start_year, end_year)


class Insight(BaseModel):
    """A structured discovery. NO prose — a contract test enforces this."""
    rule: str                         # registered discovery-rule name
    entities: list[Entity]            # primary subject first
    metrics: list[Metric]             # primary metric first
    facts: dict[str, Any] = Field(default_factory=dict)  # structured extras only
    completeness: float = 1.0         # 0-1, fraction of optional fields populated
    discovered_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @property
    def primary_metric(self) -> Metric:
        return self.metrics[0]

    @property
    def primary_entity(self) -> Entity:
        return self.entities[0]

    def actor_ids(self) -> list[int]:
        return [e.id for e in self.entities if e.kind == "actor" and e.id]


class Score(BaseModel):
    total: float
    components: dict[str, float]      # novelty, surprise, popularity, visual_potential, recency, completeness
    weights_version: str


class RankedInsight(BaseModel):
    insight: Insight
    score: Score
    fingerprint: str
    db_id: Optional[int] = None       # set once persisted


class ContentItem(BaseModel):
    insight_id: int
    platform: Platform
    text: str
    media_ref: Optional[str] = None   # e.g. stat-card slug for screenshot
    status: ContentStatus = ContentStatus.NEW
    model: Optional[str] = None       # LLM model id used
    validated: bool = False
    db_id: Optional[int] = None
