# models.py
# Defines the database tables as Python classes using SQLAlchemy.
# Each class = one table in PostgreSQL.
#
# Schema (Sprint 4):
#
#   actor_registry  (seed catalog — Wikidata QIDs for bulk ingestion)
#   pipeline_runs   (audit log — tracks every ingestion/enrichment run)
#
#   actors  ──<  cast  >──  movies  ──<  movie_directors  >──  directors
#
# The "cast" table is the actor↔movie join table (many-to-many).
# The "movie_directors" table is the movie↔director join table (many-to-many),
# replacing the legacy movies.director TEXT column which is kept for backward
# compatibility but should not be used for new analytics queries.

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Float, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base


# ---------------------------------------------------------------------------
# Actor
# ---------------------------------------------------------------------------

class Actor(Base):
    """
    Represents a South Indian film actor.
    One actor can appear in many movies (through the Cast table).
    """
    __tablename__ = "actors"

    id               = Column(Integer, primary_key=True, index=True)
    name             = Column(String,  unique=True, nullable=False)   # e.g. "Allu Arjun"
    industry         = Column(String,  nullable=False)                 # e.g. "Telugu", "Tamil"
    debut_year       = Column(Integer, nullable=True)                  # e.g. 2003

    # Sprint 8 — TMDB identity + primary/supporting flag
    tmdb_person_id   = Column(Integer, nullable=True, unique=True)    # TMDB person ID
    is_primary_actor = Column(Boolean, nullable=False, default=False)  # True for the 13 seeded actors
    created_at       = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationship: Actor → Cast → Movie  (Wikidata-sourced)
    cast_entries = relationship("Cast", back_populates="actor")

    # Relationship: Actor → ActorMovie  (TMDB-sourced, Sprint 8)
    actor_movie_entries = relationship("ActorMovie", back_populates="actor")


# ---------------------------------------------------------------------------
# Movie
# ---------------------------------------------------------------------------

class Movie(Base):
    """
    Represents a South Indian film.
    One movie can have many actors (Cast) and many directors (MovieDirector).

    Legacy column `director` (TEXT) is kept for backward compatibility with
    existing API endpoints and seed data.  New code must use the normalized
    `movie_director_entries` / Director model instead.
    """
    __tablename__ = "movies"

    id                 = Column(Integer, primary_key=True, index=True)
    title              = Column(String,  nullable=False)           # e.g. "Pushpa"
    release_year       = Column(Integer, nullable=False)           # e.g. 2021
    imdb_rating        = Column(Float,   nullable=True)            # e.g. 7.6
    box_office         = Column(Float,   nullable=True)            # in crores (INR)
    industry           = Column(String,  nullable=False)           # e.g. "Telugu"

    # ------------------------------------------------------------------
    # Legacy TEXT column — kept for backward compatibility.
    # DO NOT remove until all API consumers use the normalized tables.
    # ------------------------------------------------------------------
    director           = Column(String,  nullable=True)            # e.g. "Sukumar" (denormalized)

    # Rich-media fields (populated by TMDB / Wikipedia clients)
    poster_url         = Column(String,  nullable=True)            # TMDB poster image URL
    backdrop_url       = Column(String,  nullable=True)            # TMDB backdrop image URL
    production_company = Column(String,  nullable=True)            # e.g. "Mythri Movie Makers"
    runtime            = Column(Integer, nullable=True)            # duration in minutes
    language           = Column(String,  nullable=True)            # e.g. "Telugu", "Tamil"

    # TMDB enrichment fields (Sprint 7 — populated by enrich_tmdb_movies.py)
    # NULL means this movie has not been looked up on TMDB yet.
    # tmdb_id is the "enriched" sentinel: skip row if tmdb_id IS NOT NULL.
    tmdb_id            = Column(Integer, nullable=True, unique=True)  # TMDB movie ID
    vote_average       = Column(Float,   nullable=True)               # TMDB vote avg (0–10)
    popularity         = Column(Float,   nullable=True)               # TMDB popularity score

    # Relationship: Movie → Cast → Actor  (Wikidata-sourced)
    cast_entries = relationship("Cast", back_populates="movie")

    # Relationship: Movie → ActorMovie  (TMDB-sourced, Sprint 8)
    actor_movie_entries = relationship("ActorMovie", back_populates="movie")

    # Relationship: Movie → MovieDirector → Director  (normalized, Sprint 2)
    # Navigate via:  movie.movie_director_entries[n].director
    # or use the convenience property below.
    movie_director_entries = relationship(
        "MovieDirector",
        back_populates="movie",
        cascade="all, delete-orphan",   # removing a movie cleans up its join rows
    )

    @property
    def director_names(self) -> list[str]:
        """
        Convenience property: returns a list of director name strings.
        Avoids exposing the join-table internals to calling code.

        Example:
            movie.director_names  →  ["Sukumar"]
        """
        return [entry.director.name for entry in self.movie_director_entries]


# ---------------------------------------------------------------------------
# Cast  (actor ↔ movie join table)
# ---------------------------------------------------------------------------

class Cast(Base):
    """
    Join table linking actors to movies.
    Also stores what role the actor played (e.g. "Lead", "Supporting").
    """
    __tablename__ = "cast"

    id        = Column(Integer, primary_key=True, index=True)
    actor_id  = Column(Integer, ForeignKey("actors.id"),  nullable=False)
    movie_id  = Column(Integer, ForeignKey("movies.id"),  nullable=False)
    role_type = Column(String,  nullable=True)             # e.g. "Lead", "Supporting"

    # Back-references so we can navigate: cast.actor  /  cast.movie
    actor = relationship("Actor", back_populates="cast_entries")
    movie = relationship("Movie", back_populates="cast_entries")


# ---------------------------------------------------------------------------
# Director  (Sprint 2 — normalized)
# ---------------------------------------------------------------------------

class Director(Base):
    """
    Represents a film director.

    Normalized entity extracted from the legacy movies.director TEXT column.
    One director can be linked to many movies via the MovieDirector join table,
    enabling proper actor-director collaboration analytics.
    """
    __tablename__ = "directors"

    id   = Column(Integer, primary_key=True, index=True)
    name = Column(String,  unique=True, nullable=False, index=True)  # e.g. "Sukumar"

    # Relationship: Director → MovieDirector → Movie
    # Navigate via:  director.movie_director_entries[n].movie
    movie_director_entries = relationship(
        "MovieDirector",
        back_populates="director",
        cascade="all, delete-orphan",
    )

    @property
    def movie_titles(self) -> list[str]:
        """
        Convenience property: returns a list of movie titles this director worked on.

        Example:
            director.movie_titles  →  ["Pushpa: The Rise", "Rangasthalam"]
        """
        return [entry.movie.title for entry in self.movie_director_entries]


# ---------------------------------------------------------------------------
# MovieDirector  (movie ↔ director join table, Sprint 2 — normalized)
# ---------------------------------------------------------------------------

class MovieDirector(Base):
    """
    Join table linking movies to directors (many-to-many).

    Uses a composite primary key (movie_id, director_id) so the same
    director cannot be linked to the same movie twice — the uniqueness
    constraint is enforced at the database level without a separate index.

    Future extension: add a `role` column (e.g. "Director", "Co-Director")
    without any schema migration to existing columns.
    """
    __tablename__ = "movie_directors"

    movie_id    = Column(Integer, ForeignKey("movies.id"),    primary_key=True, nullable=False)
    director_id = Column(Integer, ForeignKey("directors.id"), primary_key=True, nullable=False)

    # Back-references for bidirectional navigation
    movie    = relationship("Movie",    back_populates="movie_director_entries")
    director = relationship("Director", back_populates="movie_director_entries")


# ---------------------------------------------------------------------------
# ActorRegistry  (Sprint 3 — QID-based ingestion seed catalog)
# ---------------------------------------------------------------------------

class ActorRegistry(Base):
    """
    Catalog of South Indian actors whose filmographies are ingested from
    Wikidata via QID-based SPARQL queries.

    Why a separate table from `actors`?
      - `actors` is populated by the ingestion pipeline and reflects what is
        already *in* the database.
      - `actor_registry` is the *instruction set* — it tells the pipeline
        *which* actors to ingest and their canonical Wikidata identifiers.
      - Keeping them separate means you can add an actor to the registry
        before ingesting, and re-run ingestion without confusion.

    wikidata_id uniqueness:
      Each QID maps to exactly one real-world person on Wikidata, so
      `wikidata_id` carries a UNIQUE constraint.  The `name` column is for
      human-readable display only and is not required to be unique (though
      in practice it will be).
    """
    __tablename__ = "actor_registry"

    id          = Column(Integer, primary_key=True, index=True)
    name        = Column(String,  nullable=False)                        # e.g. "Allu Arjun"
    wikidata_id = Column(String,  unique=True, nullable=False, index=True)  # e.g. "Q352416"
    industry    = Column(String,  nullable=False)                        # e.g. "Telugu", "Tamil"


# ---------------------------------------------------------------------------
# PipelineRun  (Sprint 4 — audit log for data pipeline executions)
# ---------------------------------------------------------------------------

class PipelineRun(Base):
    """
    Audit log entry for a single execution of a data pipeline.

    One row is created when a pipeline starts (status='running') and updated
    to 'success' or 'failed' when it finishes.  The details column stores a
    JSON blob with per-run statistics.

    Run types:
      - "wikidata_ingestion"    — ingest_all_actors.py
      - "wikipedia_enrichment"  — enrich_movies.py

    Requires migration: backend/migrations/sprint4_pipeline_runs.sql
    """
    __tablename__ = "pipeline_runs"

    id          = Column(Integer,  primary_key=True, index=True)
    run_type    = Column(String(100), nullable=False)          # e.g. "wikidata_ingestion"
    started_at  = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    finished_at = Column(DateTime(timezone=True), nullable=True)   # NULL while running
    status      = Column(String(20), nullable=False, default="running")
    # "running" → "success" | "failed"

    details     = Column(Text, nullable=True)
    # JSON string: e.g. '{"actors": 13, "inserted": 2, "skipped": 757}'


# ---------------------------------------------------------------------------
# ActorStats  (Sprint 5 — precomputed career summary per actor)
# ---------------------------------------------------------------------------

class ActorStats(Base):
    """
    Precomputed career statistics for one actor.

    Populated (and refreshed) exclusively by build_analytics_tables.py.
    Powers actor profile pages and career-span analytics.

    Columns
    -------
    actor_id        : FK to actors.id (plain INT — no FK constraint for fast TRUNCATE)
    film_count      : total distinct films the actor appears in
    first_film_year : earliest release_year > 0 (sentinel 0 excluded)
    last_film_year  : latest  release_year > 0 (sentinel 0 excluded)
    avg_runtime     : average runtime in minutes (NULL if no enriched movies exist)

    Requires migration: backend/migrations/sprint5_analytics_tables.sql
    """
    __tablename__ = "actor_stats"

    actor_id        = Column(Integer, primary_key=True)
    film_count      = Column(Integer, nullable=False, default=0)
    first_film_year = Column(Integer, nullable=True)
    last_film_year  = Column(Integer, nullable=True)
    avg_runtime     = Column(Float,   nullable=True)


# ---------------------------------------------------------------------------
# ActorCollaboration  (Sprint 5 — co-occurrence counts between actor pairs)
# ---------------------------------------------------------------------------

class ActorCollaboration(Base):
    """
    How many films two actors have appeared in together.

    Both directions (A→B) and (B→A) are stored with the same count so
    dashboard queries can use a simple ``WHERE actor1_id = ?`` without OR.

    Populated exclusively by build_analytics_tables.py.
    Powers "actors who worked together" features.

    Requires migration: backend/migrations/sprint5_analytics_tables.sql
    """
    __tablename__ = "actor_collaborations"

    actor1_id           = Column(Integer, primary_key=True, nullable=False)
    actor2_id           = Column(Integer, primary_key=True, nullable=False)
    collaboration_count = Column(Integer, nullable=False, default=0)


# ---------------------------------------------------------------------------
# ActorDirectorStat  (Sprint 5 — actor × director film counts)
# ---------------------------------------------------------------------------

class ActorDirectorStat(Base):
    """
    How many films an actor has made with a particular director.

    Sourced from the legacy movies.director TEXT column.
    Powers "Prabhas worked with Rajamouli X times" queries.

    Populated exclusively by build_analytics_tables.py.

    Requires migration: backend/migrations/sprint5_analytics_tables.sql
    """
    __tablename__ = "actor_director_stats"

    actor_id   = Column(Integer, primary_key=True, nullable=False)
    director   = Column(String,  primary_key=True, nullable=False)
    film_count = Column(Integer, nullable=False, default=0)


# ---------------------------------------------------------------------------
# ActorProductionStat  (Sprint 5 — actor × production company film counts)
# ---------------------------------------------------------------------------

class ActorProductionStat(Base):
    """
    How many films an actor has made under a particular production company.

    Sourced from movies.production_company (populated by enrich_movies.py).
    Powers "Vijay worked with Sun Pictures X times" queries.

    Populated exclusively by build_analytics_tables.py.

    Requires migration: backend/migrations/sprint5_analytics_tables.sql
    """
    __tablename__ = "actor_production_stats"

    actor_id           = Column(Integer, primary_key=True, nullable=False)
    production_company = Column(String,  primary_key=True, nullable=False)
    film_count         = Column(Integer, nullable=False, default=0)


# ---------------------------------------------------------------------------
# ActorMovie  (Sprint 8 — TMDB-sourced actor ↔ movie relationship table)
# ---------------------------------------------------------------------------

class ActorMovie(Base):
    """
    TMDB-sourced join table linking actors (both primary and supporting)
    to movies, populated by ingest_supporting_actors.py.

    This table is intentionally separate from the existing `cast` table
    (which stores Wikidata-sourced relationships) so neither pipeline
    interferes with the other.

    Columns
    -------
    actor_id       : FK to actors.id
    movie_id       : FK to movies.id
    character_name : character the actor played (from TMDB credits)
    billing_order  : 0-based cast billing position from TMDB
    role_type      : 'primary'   — one of the 13 seeded primary actors
                     'supporting' — newly discovered supporting actor

    Composite PK (actor_id, movie_id) enforces one row per actor-film pair
    and enables ON CONFLICT DO NOTHING for idempotent inserts.

    Requires migration: backend/migrations/sprint8_supporting_actor_schema.sql
    """
    __tablename__ = "actor_movies"

    actor_id       = Column(Integer, ForeignKey("actors.id"), primary_key=True, nullable=False)
    movie_id       = Column(Integer, ForeignKey("movies.id"), primary_key=True, nullable=False)
    character_name = Column(String,  nullable=True)
    billing_order  = Column(Integer, nullable=True)
    role_type      = Column(String(16), nullable=False, default="supporting")
    # role_type CHECK constraint is enforced at DB level by the migration

    # Back-references
    actor = relationship("Actor", back_populates="actor_movie_entries")
    movie = relationship("Movie", back_populates="actor_movie_entries")
