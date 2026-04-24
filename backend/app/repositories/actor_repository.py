"""
repositories/actor_repository.py
=================================
Pure database queries for the actors domain.

Rules:
  - Only SQL / ORM queries here. No business logic.
  - Methods take (db, ...) and return raw ORM objects or Row tuples.
  - No Pydantic model construction — that's the router's job.

Ready for a future service layer:
    router → service → actor_repo.method(db, ...)
"""

from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, text, case, union, select, or_

from app import models


class ActorRepository:
    """All database read operations for actors. Stateless singleton."""

    # ── List / lookup ──────────────────────────────────────────────────────────

    def get_all(
        self,
        db: Session,
        primary_only: bool = False,
        gender: Optional[str] = None,
    ) -> list[models.Actor]:
        """Return actors with optional filters."""
        q = db.query(models.Actor)
        if primary_only:
            q = q.filter(models.Actor.is_primary_actor == True)  # noqa: E712
        if gender is not None:
            q = q.filter(models.Actor.gender == gender)
        return q.all()

    def get_by_id(self, db: Session, actor_id: int) -> Optional[models.Actor]:
        """Single actor by primary key, or None."""
        return db.query(models.Actor).filter(models.Actor.id == actor_id).first()

    def get_by_name(self, db: Session, name: str) -> Optional[models.Actor]:
        """
        Case-insensitive exact name lookup.
        Uses functional index idx_actors_name_lower.
        """
        return (
            db.query(models.Actor)
            .filter(func.lower(models.Actor.name) == name.lower())
            .first()
        )

    # ── Search ─────────────────────────────────────────────────────────────────

    def search(
        self,
        db: Session,
        q: str,
        lead_only: bool = False,
        limit: int = 20,
    ) -> list:
        """
        Partial-match search using the pg_trgm GIN index (idx_actors_name_trgm).

        ILIKE '%q%' triggers a Bitmap Index Scan on the trigram index — ~0.4 ms
        vs ~17 ms for the old sequential ILIKE scan (~39× faster).

        Ranking: exact match (0) → starts-with (1) → contains (2).
        Returns (id, name, industry) tuples — lightweight for autocomplete.
        """
        q_lower = q.lower().strip()

        rank = case(
            (func.lower(models.Actor.name) == q_lower, 0),
            (func.lower(models.Actor.name).like(f"{q_lower}%"), 1),
            else_=2,
        )

        query = (
            db.query(models.Actor.id, models.Actor.name, models.Actor.industry)
            .filter(models.Actor.name.ilike(f"%{q}%"))
        )
        if lead_only:
            query = query.filter(models.Actor.is_primary_actor == True)  # noqa: E712
        return (
            query
            .order_by(rank, models.Actor.is_primary_actor.desc(), models.Actor.name)
            .limit(limit)
            .all()
        )

    # ── Profile ────────────────────────────────────────────────────────────────

    def get_with_stats(
        self,
        db: Session,
        actor_id: int,
    ) -> Optional[tuple[models.Actor, Optional[models.ActorStats]]]:
        """
        Return (Actor, ActorStats | None).
        Stats come from the precomputed actor_stats table — O(1) lookup.
        Returns None when actor doesn't exist.
        """
        actor = self.get_by_id(db, actor_id)
        if not actor:
            return None
        stats = (
            db.query(models.ActorStats)
            .filter(models.ActorStats.actor_id == actor_id)
            .first()
        )
        return actor, stats

    def get_with_stats_by_name(
        self,
        db: Session,
        name: str,
    ) -> Optional[tuple[models.Actor, models.ActorStats]]:
        """
        For /compare. Returns (Actor, ActorStats) or None if either is missing.
        Both rows required — returns None if analytics tables haven't been built.
        """
        actor = self.get_by_name(db, name)
        if not actor:
            return None
        stats = (
            db.query(models.ActorStats)
            .filter(models.ActorStats.actor_id == actor.id)
            .first()
        )
        return (actor, stats) if stats else None

    # ── Filmography ────────────────────────────────────────────────────────────

    # Character names that indicate a non-acting appearance (narrator, voice, cameo etc.)
    _NON_ACTING_PATTERNS = [
        'narrator', 'narration', '(voice)', 'voice)',
        'himself', 'herself', 'cameo', 'self -',
    ]

    def _is_non_acting(self, character_name: str | None) -> bool:
        if not character_name:
            return False
        low = character_name.lower()
        return any(p in low for p in self._NON_ACTING_PATTERNS)

    def get_movies(self, db: Session, actor_id: int) -> list[models.Movie]:
        """
        All movies for an actor, newest first.
        Unions both ingestion pipelines:
          • cast        → Wikidata-sourced (original actors)
          • actor_movies → TMDB-sourced (supporting + Malayalam expansion)
        Excludes non-acting roles (narrator, voice-only, cameo, himself/herself).

        Cameo guard: for the 13 original seed actors (is_primary_actor=True), TMDB
        sometimes picks up guest appearances in other actors' films and stores them in
        actor_movies with role_type='supporting'.  Those entries are excluded by only
        accepting role_type='primary' from actor_movies for primary actors — their real
        filmography already lives in the cast (Wikidata) table so nothing legit is lost.
        Non-primary actors are unaffected (all their films are in actor_movies as
        role_type='supporting', which is correct for them).
        """
        # Check once whether this is a seed primary actor
        is_primary: bool = (
            db.query(models.Actor.is_primary_actor)
            .filter(models.Actor.id == actor_id)
            .scalar()
            or False
        )

        cast_ids = select(models.Cast.movie_id).where(
            models.Cast.actor_id == actor_id
        )

        # Exclude narrator / voice / cameo / himself roles from TMDB pipeline
        non_acting = [f"%{p}%" for p in self._NON_ACTING_PATTERNS]
        tmdb_ids = select(models.ActorMovie.movie_id).where(
            models.ActorMovie.actor_id == actor_id,
            ~or_(*[
                func.lower(models.ActorMovie.character_name).like(p)
                for p in non_acting
            ])
        )
        # For seed primary actors, drop supporting-tagged entries (cameos in other films)
        if is_primary:
            tmdb_ids = tmdb_ids.where(models.ActorMovie.role_type == 'primary')

        all_ids = union(cast_ids, tmdb_ids).scalar_subquery()

        return (
            db.query(models.Movie)
            .filter(models.Movie.id.in_(all_ids))
            .order_by(models.Movie.release_year.desc())
            .all()
        )

    # ── Relationships ──────────────────────────────────────────────────────────

    def get_collaborators(self, db: Session, actor_id: int) -> list:
        """
        Top co-stars ordered by collaboration count.
        Reads precomputed actor_collaborations table — O(1) per actor.
        Returns (name, collaboration_count, actor_id) tuples.
        """
        return (
            db.query(
                models.Actor.name,
                models.ActorCollaboration.collaboration_count,
                models.Actor.id,
            )
            .join(
                models.ActorCollaboration,
                models.ActorCollaboration.actor2_id == models.Actor.id,
            )
            .filter(models.ActorCollaboration.actor1_id == actor_id)
            .order_by(models.ActorCollaboration.collaboration_count.desc())
            .all()
        )

    def get_lead_collaborators(self, db: Session, actor_id: int) -> list:
        """
        Co-stars who appeared in a PRIMARY role in the same films as this actor
        (also in a primary role). Excludes supporting/background appearances.
        Returns (name, film_count) tuples ordered by film_count DESC.
        """
        result = db.execute(
            text("""
                SELECT a.name, COUNT(DISTINCT am2.movie_id) AS film_count
                FROM actor_movies am1
                JOIN actor_movies am2
                  ON  am2.movie_id  = am1.movie_id
                  AND am2.actor_id != am1.actor_id
                  AND am2.role_type = 'primary'
                JOIN actors a ON a.id = am2.actor_id
                WHERE am1.actor_id  = :actor_id
                  AND am1.role_type = 'primary'
                GROUP BY a.name
                ORDER BY film_count DESC
            """),
            {"actor_id": actor_id},
        )
        return result.fetchall()

    def get_heroine_collaborators(
        self, db: Session, actor_id: int, max_billing: int = 4
    ) -> list:
        """
        Female co-stars who appeared as the lead actress (heroine) in the same
        films as this actor, identified via TMDB billing_order.

        Why billing_order instead of role_type:
          TMDB ingestion assigns role_type='primary' only to the original 13 seed
          actors, so heroines discovered via TMDB credits all get role_type='supporting'
          regardless of their actual on-screen importance.  billing_order is a more
          reliable signal: heroines are typically billed at position 1–3, while
          supporting character actresses appear at position 10+.

        A female actor qualifies if:
          • billing_order IS NOT NULL AND billing_order <= max_billing (default 4)
          • OR billing_order IS NULL but actor_tier = 'primary' (fallback for films
            without TMDB billing data that have the actress in the primary seed set)

        Returns (actor_id, name, film_count) tuples ordered by film_count DESC.
        """
        result = db.execute(
            text("""
                SELECT a.id AS actor_id, a.name,
                       COUNT(DISTINCT am2.movie_id) AS film_count
                FROM actor_movies am1
                JOIN actor_movies am2
                  ON  am2.movie_id  = am1.movie_id
                  AND am2.actor_id != am1.actor_id
                JOIN actors a ON a.id = am2.actor_id
                WHERE am1.actor_id = :actor_id
                  AND a.gender = 'F'
                  AND (
                    (am2.billing_order IS NOT NULL AND am2.billing_order <= :max_billing)
                    OR (am2.billing_order IS NULL AND a.actor_tier = 'primary')
                  )
                GROUP BY a.id, a.name
                ORDER BY film_count DESC
            """),
            {"actor_id": actor_id, "max_billing": max_billing},
        )
        return result.fetchall()

    def get_directors(self, db: Session, actor_id: int) -> list[models.ActorDirectorStat]:
        """Directors an actor has worked with, ordered by film count."""
        return (
            db.query(models.ActorDirectorStat)
            .filter(models.ActorDirectorStat.actor_id == actor_id)
            .order_by(models.ActorDirectorStat.film_count.desc())
            .all()
        )

    def get_production_companies(
        self, db: Session, actor_id: int
    ) -> list[models.ActorProductionStat]:
        """Production companies an actor has worked with, ordered by film count."""
        return (
            db.query(models.ActorProductionStat)
            .filter(models.ActorProductionStat.actor_id == actor_id)
            .order_by(models.ActorProductionStat.film_count.desc())
            .all()
        )

    def get_shared_films(
        self, db: Session, actor1_id: int, actor2_id: int
    ) -> list:
        """
        Movies both actors appeared in a lead/significant role, newest first.
        Applies the same cameo guard as get_movies(): for is_primary_actor=TRUE
        actors, actor_movies entries are restricted to role_type='primary' so
        that guest appearances (e.g. Mohanlal in Jailer) don't show as shared
        films. Non-primary actors are unaffected.
        """
        return db.execute(text("""
            WITH actor_credits AS (
                -- Wikidata (cast table) — always included, curated
                SELECT actor_id, movie_id FROM "cast"
                UNION
                -- TMDB pipeline — exclude cameos for primary seed actors
                SELECT am.actor_id, am.movie_id
                FROM   actor_movies am
                JOIN   actors a ON a.id = am.actor_id
                WHERE  a.is_primary_actor = FALSE OR am.role_type = 'primary'
            )
            SELECT
                m.title,
                m.release_year,
                m.director,
                m.poster_url,
                m.vote_average,
                m.popularity,
                am1.character_name  AS actor1_character,
                am1.role_type       AS actor1_role,
                am2.character_name  AS actor2_character,
                am2.role_type       AS actor2_role
            FROM   actor_credits ac1
            JOIN   actor_credits ac2 ON  ac2.movie_id = ac1.movie_id
                                     AND ac2.actor_id  = :a2
            JOIN   movies m  ON m.id = ac1.movie_id
            LEFT JOIN actor_movies am1 ON am1.movie_id = m.id AND am1.actor_id = :a1
            LEFT JOIN actor_movies am2 ON am2.movie_id = m.id AND am2.actor_id = :a2
            WHERE  ac1.actor_id = :a1
            ORDER  BY m.release_year DESC
        """), {"a1": actor1_id, "a2": actor2_id}).fetchall()

    # ── Health counts ──────────────────────────────────────────────────────────

    def get_counts(self, db: Session) -> tuple[int, int]:
        """(actor_count, movie_count) for the health endpoint."""
        actors = db.query(func.count(models.Actor.id)).scalar() or 0
        movies = db.query(func.count(models.Movie.id)).scalar() or 0
        return actors, movies


# Module-level singleton — import and use directly.
actor_repo = ActorRepository()
