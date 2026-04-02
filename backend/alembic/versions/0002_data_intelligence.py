"""Add data intelligence layer: is_valid, confidence scores, system_health, data_fix_log

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-02
"""

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── actor_movies: soft-delete flag ────────────────────────────────────────
    op.add_column("actor_movies",
        sa.Column("is_valid",       sa.Boolean(), nullable=False, server_default="true"))
    op.add_column("actor_movies",
        sa.Column("invalid_reason", sa.Text(),    nullable=True))

    # ── actors: confidence score + source metadata ────────────────────────────
    op.add_column("actors",
        sa.Column("data_confidence_score", sa.Float(), nullable=True))
    op.add_column("actors",
        sa.Column("data_source",           sa.String(32), nullable=True,
                  comment="Highest-trust source: tmdb | wikidata | derived"))

    # ── movies: confidence score + source metadata ────────────────────────────
    op.add_column("movies",
        sa.Column("data_confidence_score", sa.Float(), nullable=True))
    op.add_column("movies",
        sa.Column("data_source",           sa.String(32), nullable=True,
                  comment="tmdb | wikidata | derived"))

    # ── system_health: singleton row (id=1) ───────────────────────────────────
    op.create_table(
        "system_health",
        sa.Column("id",                    sa.Integer(), primary_key=True),
        sa.Column("data_confidence_score", sa.Float(),   nullable=True),
        sa.Column("avg_actor_score",       sa.Float(),   nullable=True),
        sa.Column("avg_movie_score",       sa.Float(),   nullable=True),
        sa.Column("collab_integrity",      sa.Float(),   nullable=True),
        sa.Column("ghost_collab_count",    sa.Integer(), nullable=True, server_default="0"),
        sa.Column("duplicate_count",       sa.Integer(), nullable=True, server_default="0"),
        sa.Column("invalid_link_count",    sa.Integer(), nullable=True, server_default="0"),
        sa.Column("total_actors",          sa.Integer(), nullable=True),
        sa.Column("total_movies",          sa.Integer(), nullable=True),
        sa.Column("total_collab_pairs",    sa.Integer(), nullable=True),
        sa.Column("validation_passed",     sa.Boolean(), nullable=True),
        sa.Column("sources_used",          sa.ARRAY(sa.Text()), nullable=True),
        sa.Column("last_scored_at",        sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_validated_at",     sa.DateTime(timezone=True), nullable=True),
    )

    # Seed the singleton row
    op.execute("INSERT INTO system_health (id) VALUES (1) ON CONFLICT DO NOTHING")

    # ── data_fix_log: full audit trail ────────────────────────────────────────
    op.create_table(
        "data_fix_log",
        sa.Column("id",           sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("action",       sa.String(64),  nullable=False),
        sa.Column("entity_type",  sa.String(32),  nullable=True),
        sa.Column("entity_id",    sa.Integer(),   nullable=True),
        sa.Column("entity_label", sa.Text(),      nullable=True),
        sa.Column("reason",       sa.Text(),      nullable=True),
        sa.Column("run_id",       sa.String(64),  nullable=True,
                  comment="Optional: UUID linking entries from one pipeline run"),
        sa.Column("created_at",   sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("idx_fix_log_entity",  "data_fix_log", ["entity_type", "entity_id"])
    op.create_index("idx_fix_log_created", "data_fix_log", ["created_at"])


def downgrade() -> None:
    op.drop_index("idx_fix_log_created", "data_fix_log")
    op.drop_index("idx_fix_log_entity",  "data_fix_log")
    op.drop_table("data_fix_log")
    op.drop_table("system_health")
    op.drop_column("movies", "data_source")
    op.drop_column("movies", "data_confidence_score")
    op.drop_column("actors", "data_source")
    op.drop_column("actors", "data_confidence_score")
    op.drop_column("actor_movies", "invalid_reason")
    op.drop_column("actor_movies", "is_valid")
