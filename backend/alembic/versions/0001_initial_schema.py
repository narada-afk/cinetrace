"""initial schema — full table set as of sprint 24

Revision ID: 0001
Revises:
Create Date: 2026-03-31

NOTE FOR EXISTING INSTALLATIONS
--------------------------------
The database already has all these tables.  Mark this migration as applied
without re-running it:

    alembic stamp head

Only run `alembic upgrade head` on a FRESH database.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── actors ────────────────────────────────────────────────────────────────
    op.create_table(
        "actors",
        sa.Column("id",               sa.Integer(),    primary_key=True),
        sa.Column("name",             sa.String(),     nullable=False, unique=True),
        sa.Column("industry",         sa.String(),     nullable=False),
        sa.Column("debut_year",       sa.Integer(),    nullable=True),
        sa.Column("tmdb_person_id",   sa.Integer(),    nullable=True, unique=True),
        sa.Column("is_primary_actor", sa.Boolean(),    nullable=False, server_default="false"),
        sa.Column("actor_tier",       sa.String(),     nullable=True),
        sa.Column("gender",           sa.String(1),    nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_actors_id", "actors", ["id"])

    # ── movies ────────────────────────────────────────────────────────────────
    op.create_table(
        "movies",
        sa.Column("id",                 sa.Integer(), primary_key=True),
        sa.Column("title",              sa.String(),  nullable=False),
        sa.Column("release_year",       sa.Integer(), nullable=False),
        sa.Column("imdb_rating",        sa.Float(),   nullable=True),
        sa.Column("box_office",         sa.Float(),   nullable=True),
        sa.Column("industry",           sa.String(),  nullable=False),
        sa.Column("director",           sa.String(),  nullable=True),
        sa.Column("poster_url",         sa.String(),  nullable=True),
        sa.Column("backdrop_url",       sa.String(),  nullable=True),
        sa.Column("production_company", sa.String(),  nullable=True),
        sa.Column("runtime",            sa.Integer(), nullable=True),
        sa.Column("language",           sa.String(),  nullable=True),
        sa.Column("tmdb_id",            sa.Integer(), nullable=True, unique=True),
        sa.Column("vote_average",       sa.Float(),   nullable=True),
        sa.Column("popularity",         sa.Float(),   nullable=True),
        sa.Column("is_documentary",     sa.Boolean(), nullable=True, server_default="false"),
    )
    op.create_index("ix_movies_id", "movies", ["id"])

    # ── cast ──────────────────────────────────────────────────────────────────
    op.create_table(
        "cast",
        sa.Column("id",       sa.Integer(), primary_key=True),
        sa.Column("actor_id", sa.Integer(), sa.ForeignKey("actors.id"), nullable=False),
        sa.Column("movie_id", sa.Integer(), sa.ForeignKey("movies.id"), nullable=False),
        sa.Column("role_type", sa.String(), nullable=True),
    )
    op.create_index("ix_cast_id", "cast", ["id"])

    # ── directors ─────────────────────────────────────────────────────────────
    op.create_table(
        "directors",
        sa.Column("id",   sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(),  nullable=False, unique=True),
    )
    op.create_index("ix_directors_id",   "directors", ["id"])
    op.create_index("ix_directors_name", "directors", ["name"])

    # ── movie_directors ───────────────────────────────────────────────────────
    op.create_table(
        "movie_directors",
        sa.Column("movie_id",    sa.Integer(), sa.ForeignKey("movies.id"),    primary_key=True, nullable=False),
        sa.Column("director_id", sa.Integer(), sa.ForeignKey("directors.id"), primary_key=True, nullable=False),
    )

    # ── actor_registry ────────────────────────────────────────────────────────
    op.create_table(
        "actor_registry",
        sa.Column("id",          sa.Integer(), primary_key=True),
        sa.Column("name",        sa.String(),  nullable=False),
        sa.Column("wikidata_id", sa.String(),  nullable=False, unique=True),
        sa.Column("industry",    sa.String(),  nullable=False),
    )
    op.create_index("ix_actor_registry_id",          "actor_registry", ["id"])
    op.create_index("ix_actor_registry_wikidata_id", "actor_registry", ["wikidata_id"])

    # ── pipeline_runs ─────────────────────────────────────────────────────────
    op.create_table(
        "pipeline_runs",
        sa.Column("id",       sa.Integer(),     primary_key=True),
        sa.Column("run_type", sa.String(100),   nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status",      sa.String(20), nullable=False, server_default="running"),
        sa.Column("details",     sa.Text(),     nullable=True),
    )
    op.create_index("ix_pipeline_runs_id", "pipeline_runs", ["id"])

    # ── actor_stats ───────────────────────────────────────────────────────────
    op.create_table(
        "actor_stats",
        sa.Column("actor_id",        sa.Integer(), primary_key=True),
        sa.Column("film_count",      sa.Integer(), nullable=False, server_default="0"),
        sa.Column("first_film_year", sa.Integer(), nullable=True),
        sa.Column("last_film_year",  sa.Integer(), nullable=True),
        sa.Column("avg_runtime",     sa.Float(),   nullable=True),
    )

    # ── actor_collaborations ──────────────────────────────────────────────────
    op.create_table(
        "actor_collaborations",
        sa.Column("actor1_id",           sa.Integer(), primary_key=True, nullable=False),
        sa.Column("actor2_id",           sa.Integer(), primary_key=True, nullable=False),
        sa.Column("collaboration_count", sa.Integer(), nullable=False, server_default="0"),
    )

    # ── actor_director_stats ──────────────────────────────────────────────────
    op.create_table(
        "actor_director_stats",
        sa.Column("actor_id",   sa.Integer(), primary_key=True, nullable=False),
        sa.Column("director",   sa.String(),  primary_key=True, nullable=False),
        sa.Column("film_count", sa.Integer(), nullable=False, server_default="0"),
    )

    # ── actor_production_stats ────────────────────────────────────────────────
    op.create_table(
        "actor_production_stats",
        sa.Column("actor_id",           sa.Integer(), primary_key=True, nullable=False),
        sa.Column("production_company", sa.String(),  primary_key=True, nullable=False),
        sa.Column("film_count",         sa.Integer(), nullable=False, server_default="0"),
    )

    # ── actor_movies ──────────────────────────────────────────────────────────
    op.create_table(
        "actor_movies",
        sa.Column("actor_id",       sa.Integer(),    sa.ForeignKey("actors.id"), primary_key=True, nullable=False),
        sa.Column("movie_id",       sa.Integer(),    sa.ForeignKey("movies.id"), primary_key=True, nullable=False),
        sa.Column("character_name", sa.String(),     nullable=True),
        sa.Column("billing_order",  sa.Integer(),    nullable=True),
        sa.Column("role_type",      sa.String(16),   nullable=False, server_default="supporting"),
    )


def downgrade() -> None:
    op.drop_table("actor_movies")
    op.drop_table("actor_production_stats")
    op.drop_table("actor_director_stats")
    op.drop_table("actor_collaborations")
    op.drop_table("actor_stats")
    op.drop_table("pipeline_runs")
    op.drop_table("actor_registry")
    op.drop_table("movie_directors")
    op.drop_table("directors")
    op.drop_table("cast")
    op.drop_table("movies")
    op.drop_table("actors")
