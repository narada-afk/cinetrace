"""
services/graph_service.py
==========================
In-memory collaboration graph for BFS (actor connections) and
Brandes betweenness centrality (gravity center).

Design
------
• The adjacency list is built ONCE at startup from the DB and stored in memory.
  All graph traversal (BFS, Brandes) is then pure Python — zero DB calls.
• Actor names and metadata are pre-loaded at build time as well.
• A simple result cache (dict + TTL) stores expensive results between requests.
• Manual rebuild via graph_service.rebuild(db) — call after running
  build_analytics_tables.py on new data.

Usage
-----
    # In main.py lifespan:
    from app.services.graph_service import graph_service
    graph_service.build(db)

    # In a router:
    result = graph_service.find_connection(actor1_id, actor2_id)
    top    = graph_service.get_gravity_center(db, limit=25)
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict, deque
from typing import Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import settings
from app.database import SessionLocal

logger = logging.getLogger(__name__)


# ── Simple in-memory TTL cache ────────────────────────────────────────────────
# Thread-safe enough for single-process uvicorn (GIL protects dict ops).
# Swap to Redis: replace get/set/clear with redis.get/setex/flushdb.

class _Cache:
    """Minimal TTL cache. ~25 lines. Redis-compatible interface."""

    def __init__(self, maxsize: int = 500):
        self._store: dict[str, tuple[Any, float]] = {}
        self._max = maxsize

    def get(self, key: str) -> Any:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, exp = entry
        if time.monotonic() > exp:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl: int = 300) -> None:
        if len(self._store) >= self._max:
            oldest = min(self._store, key=lambda k: self._store[k][1])
            del self._store[oldest]
        self._store[key] = (value, time.monotonic() + ttl)

    def clear(self) -> None:
        self._store.clear()


# ── Graph service ─────────────────────────────────────────────────────────────

class GraphService:
    """
    Manages two in-memory graphs:

    _full_graph:
        dict[actor_id, dict[neighbor_id, (movie_id, movie_title)]]
        Full collaboration graph — used by BFS (connection finder).
        Covers all actors in both cast (Wikidata) and actor_movies (TMDB).

    _primary_graph:
        dict[actor_id, set[neighbor_id]]
        Primary-actor-only subgraph — used by Brandes centrality.
        Small (≤85 nodes) so the O(V·E) algorithm completes instantly.

    _actor_names:
        dict[actor_id, str]
        Pre-loaded actor name lookup — avoids DB call after BFS.
    """

    def __init__(self) -> None:
        self._full_graph:    dict[int, dict[int, tuple[int, str, str | None, int | None]]] = {}
        self._primary_graph: dict[int, set[int]]                   = {}
        self._actor_names:   dict[int, str]                        = {}
        self._ready: bool = False
        self._cache = _Cache(maxsize=settings.GRAPH_CACHE_MAXSIZE)
        self._built_version: str | None = None   # version active when graph was last built
        self._rebuild_lock = threading.Lock()    # prevents concurrent version-triggered rebuilds

    # ── Build ─────────────────────────────────────────────────────────────────

    def build(self, db: Session) -> None:
        """
        Load both graphs and actor names from DB.
        Called ONCE at app startup via the lifespan context manager.
        No periodic background refresh — call rebuild() after new ingestion.
        """
        # ── Full graph (for BFS) ──────────────────────────────────────────────
        # DISTINCT ON picks the most popular movie per actor pair.
        # Union of cast (Wikidata) + actor_movies (TMDB) ensures all pipelines
        # are represented — e.g. Rajinikanth's 1970s-80s Tamil films.
        rows = db.execute(text("""
            WITH all_credits AS (
                SELECT actor_id, movie_id FROM "cast"
                UNION
                SELECT actor_id, movie_id FROM actor_movies
            )
            SELECT DISTINCT ON (ac1.actor_id, ac2.actor_id)
                ac1.actor_id,
                ac2.actor_id,
                m.id          AS movie_id,
                m.title       AS movie_title,
                m.poster_url  AS poster_url,
                m.tmdb_id     AS tmdb_id
            FROM all_credits ac1
            JOIN all_credits ac2
              ON ac1.movie_id = ac2.movie_id
             AND ac1.actor_id < ac2.actor_id
            JOIN movies m ON m.id = ac1.movie_id
            WHERE m.is_documentary = FALSE
            ORDER BY ac1.actor_id, ac2.actor_id,
                     m.popularity DESC NULLS LAST
        """)).fetchall()

        full: dict[int, dict[int, tuple[int, str, str | None, int | None]]] = defaultdict(dict)
        for a, b, mid, title, poster_url, tmdb_id in rows:
            full[a][b] = (mid, title or "Unknown", poster_url, tmdb_id)
            full[b][a] = (mid, title or "Unknown", poster_url, tmdb_id)
        self._full_graph = dict(full)

        # ── Primary graph (for Brandes) ───────────────────────────────────────
        primary_rows = db.execute(text("""
            WITH all_credits AS (
                SELECT actor_id, movie_id FROM "cast"
                UNION
                SELECT actor_id, movie_id FROM actor_movies
            )
            SELECT DISTINCT ac1.actor_id, ac2.actor_id
            FROM all_credits ac1
            JOIN all_credits ac2
              ON ac1.movie_id = ac2.movie_id
             AND ac1.actor_id < ac2.actor_id
            JOIN actors a1 ON a1.id = ac1.actor_id AND a1.actor_tier = 'primary'
            JOIN actors a2 ON a2.id = ac2.actor_id AND a2.actor_tier = 'primary'
        """)).fetchall()

        primary: dict[int, set[int]] = defaultdict(set)
        for a, b in primary_rows:
            primary[a].add(b)
            primary[b].add(a)
        self._primary_graph = dict(primary)

        # ── Actor names (for path output — no DB call after BFS) ─────────────
        name_rows = db.execute(text("SELECT id, name FROM actors")).fetchall()
        self._actor_names = {r[0]: r[1] for r in name_rows}

        self._cache.clear()   # Invalidate stale result cache after rebuild
        self._built_version = settings.GRAPH_VERSION
        self._ready = True

        nodes   = len(self._full_graph)
        edges   = sum(len(v) for v in self._full_graph.values()) // 2
        primary_n = len(self._primary_graph)
        logger.info("graph built: %d actors, %d edges (%d primary)", nodes, edges, primary_n)

    def rebuild(self, db: Session) -> None:
        """
        Optional manual refresh.
        Call after running `python -m data_pipeline.build_analytics_tables`
        to reflect newly ingested data.
        """
        logger.info("rebuilding graph...")
        self.build(db)

    def ensure_current(self) -> None:
        """
        Called on every request (from middleware) to detect version drift.

        The hot path is a single string comparison — effectively free.
        A DB session is only opened when a mismatch is detected (rare).
        A threading.Lock prevents concurrent rebuilds when multiple requests
        arrive simultaneously during the version gap.

        Workflow to propagate a graph update to all Gunicorn workers:
          1. Ingest new data.
          2. Bump GRAPH_VERSION in your .env (e.g. "1" → "2").
          3. Restart the backend (docker compose up -d backend).
             Each worker rebuilds at startup and stores the new version.
          4. If a worker's startup build failed, ensure_current() catches it
             on the next real request and retries.
        """
        if self._built_version == settings.GRAPH_VERSION:
            return  # hot path — nothing to do

        # Only one thread should rebuild; others wait and then re-check.
        with self._rebuild_lock:
            if self._built_version == settings.GRAPH_VERSION:
                return  # another thread already rebuilt while we waited

            logger.info(
                "graph version mismatch (built=%s, target=%s) — rebuilding",
                self._built_version, settings.GRAPH_VERSION,
            )
            db = SessionLocal()
            try:
                self.build(db)
            except Exception as exc:
                logger.error("version-triggered graph rebuild failed: %s", exc)
            finally:
                db.close()

    # ── Introspection ─────────────────────────────────────────────────────────

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def node_count(self) -> int:
        return len(self._full_graph)

    @property
    def edge_count(self) -> int:
        return sum(len(v) for v in self._full_graph.values()) // 2

    # ── BFS: actor connection finder ──────────────────────────────────────────

    def find_connection(
        self,
        actor1_id: int,
        actor2_id: int,
        max_depth: int = 6,
    ) -> dict:
        """
        Bidirectional BFS — shortest collaboration path between two actors.
        Searches from both ends simultaneously; stops when the two frontiers
        meet. For a graph with branching factor b≈50 and typical depth d≈2-3,
        this visits O(b^(d/2)) ≈ 700 nodes instead of O(b^d) ≈ 125 000 nodes
        for unidirectional BFS — roughly 100-200× fewer nodes on the first call.

        Purely in-memory — no DB calls during traversal.
        Result is cached per unique actor pair.

        Returns
        -------
        {
            "found": bool,
            "depth": int,          # hops (0 = same actor)
            "path":  [...],        # [{"id": int, "name": str}, ...]
            "connections": [...]   # [{"movie_id": int, "movie_title": str}, ...]
        }
        """
        if not self._ready:
            return {"found": False, "depth": -1, "path": [], "connections": [],
                    "error": "Graph not loaded. Backend may still be starting up."}

        # Same actor — trivial
        if actor1_id == actor2_id:
            return {
                "found": True,
                "depth": 0,
                "path": [{"id": actor1_id,
                          "name": self._actor_names.get(actor1_id, "?")}],
                "connections": [],
            }

        # Check result cache (key is order-independent)
        cache_key = f"conn:{min(actor1_id, actor2_id)}:{max(actor1_id, actor2_id)}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        # Actor not in graph → not connected
        if actor1_id not in self._full_graph or actor2_id not in self._full_graph:
            result = {"found": False, "depth": -1, "path": [], "connections": []}
            self._cache.set(cache_key, result, ttl=settings.GRAPH_RESULT_TTL)
            return result

        # ── Bidirectional BFS ─────────────────────────────────────────────────
        # fwd_prev[n] = (parent_id, movie_id, title, poster_url, tmdb_id)
        # fwd_prev[actor1_id] = None  (source sentinel)
        fwd_prev: dict[int, tuple | None] = {actor1_id: None}
        bwd_prev: dict[int, tuple | None] = {actor2_id: None}

        fwd_frontier: set[int] = {actor1_id}
        bwd_frontier: set[int] = {actor2_id}

        meeting_node: int | None = None

        for _ in range(max_depth):
            if not fwd_frontier or not bwd_frontier:
                break

            # Expand whichever frontier is smaller (minimises total work)
            if len(fwd_frontier) <= len(bwd_frontier):
                next_fwd: set[int] = set()
                for node in fwd_frontier:
                    for neighbor, edge in self._full_graph.get(node, {}).items():
                        if neighbor not in fwd_prev:
                            fwd_prev[neighbor] = (node, *edge)
                            next_fwd.add(neighbor)
                fwd_frontier = next_fwd
                overlap = fwd_frontier & set(bwd_prev)
                if overlap:
                    meeting_node = min(overlap)   # deterministic tie-break
                    break
            else:
                next_bwd: set[int] = set()
                for node in bwd_frontier:
                    for neighbor, edge in self._full_graph.get(node, {}).items():
                        if neighbor not in bwd_prev:
                            bwd_prev[neighbor] = (node, *edge)
                            next_bwd.add(neighbor)
                bwd_frontier = next_bwd
                overlap = set(fwd_prev) & bwd_frontier
                if overlap:
                    meeting_node = min(overlap)
                    break

        if meeting_node is None:
            result = {"found": False, "depth": -1, "path": [], "connections": []}
            self._cache.set(cache_key, result, ttl=settings.GRAPH_RESULT_TTL)
            return result

        # ── Reconstruct path ──────────────────────────────────────────────────
        # Forward half: actor1_id → ... → meeting_node
        fwd_ids: list[int] = []
        node: int | None = meeting_node
        while node is not None:
            fwd_ids.append(node)
            entry = fwd_prev.get(node)
            node = entry[0] if entry is not None else None
        fwd_ids.reverse()   # now [actor1_id, ..., meeting_node]

        # Backward half: one step past meeting_node → ... → actor2_id
        bwd_ids: list[int] = []
        node = meeting_node
        entry = bwd_prev.get(node)
        while entry is not None:
            node = entry[0]
            bwd_ids.append(node)
            entry = bwd_prev.get(node)

        path_ids = fwd_ids + bwd_ids   # [actor1, ..., meeting, ..., actor2]

        # Build connections from consecutive path nodes using the stored graph
        connections: list[dict] = []
        for i in range(len(path_ids) - 1):
            a, b = path_ids[i], path_ids[i + 1]
            edge = self._full_graph.get(a, {}).get(b)
            if edge:
                connections.append({
                    "movie_id":    edge[0],
                    "movie_title": edge[1],
                    "poster_url":  edge[2],
                    "tmdb_id":     edge[3],
                })

        result = {
            "found":       True,
            "depth":       len(path_ids) - 1,
            "path":        [{"id": aid, "name": self._actor_names.get(aid, "?")}
                            for aid in path_ids],
            "connections": connections,
        }
        self._cache.set(cache_key, result, ttl=settings.GRAPH_RESULT_TTL)
        return result

    # ── Brandes betweenness centrality ────────────────────────────────────────

    def get_gravity_center(self, db: Session, limit: int = 25) -> list[dict]:
        """
        Betweenness centrality ranking on the primary-actor subgraph.

        The graph is already in memory (_primary_graph, ≤85 nodes).
        Only the final metadata lookup (film counts) hits the DB.
        Result is cached to avoid re-running Brandes on repeated requests.

        Parameters
        ----------
        db    : DB session — used only for the final metadata fetch.
        limit : Number of top actors to return.
        """
        cache_key = f"gravity:{limit}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        graph = self._primary_graph
        V     = list(graph.keys())

        if not V:
            return []

        # ── Brandes algorithm (exact betweenness centrality) ──────────────────
        centrality: dict[int, float] = {v: 0.0 for v in V}

        for s in V:
            stack: list[int]        = []
            pred:  dict[int, list]  = {v: [] for v in V}
            sigma = dict.fromkeys(V, 0.0);  sigma[s] = 1.0
            dist  = dict.fromkeys(V, -1);   dist[s]  = 0
            q: deque = deque([s])

            while q:
                v = q.popleft()
                stack.append(v)
                for w in graph.get(v, set()):
                    if dist[w] < 0:
                        q.append(w)
                        dist[w] = dist[v] + 1
                    if dist[w] == dist[v] + 1:
                        sigma[w] += sigma[v]
                        pred[w].append(v)

            delta = dict.fromkeys(V, 0.0)
            while stack:
                w = stack.pop()
                for v in pred[w]:
                    delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
                if w != s:
                    centrality[w] += delta[w]

        # Normalize to [0, 1]
        n    = len(V)
        norm = (n - 1) * (n - 2) if n > 2 else 1
        for v in centrality:
            centrality[v] /= norm

        top_ids = sorted(centrality, key=lambda v: centrality[v], reverse=True)[:limit]

        # ── Fetch film + costar counts (one DB call, after algorithm) ─────────
        counts = db.execute(text("""
            WITH all_credits AS (
                SELECT actor_id, movie_id FROM "cast"
                UNION
                SELECT actor_id, movie_id FROM actor_movies
            )
            SELECT ac.actor_id,
                   COUNT(DISTINCT ac.movie_id)   AS film_count,
                   COUNT(DISTINCT ac2.actor_id)  AS costar_count
            FROM all_credits ac
            JOIN all_credits ac2
              ON ac2.movie_id = ac.movie_id AND ac2.actor_id != ac.actor_id
            WHERE ac.actor_id = ANY(:ids)
            GROUP BY ac.actor_id
        """), {"ids": top_ids}).fetchall()
        cnt_map = {r[0]: (r[1], r[2]) for r in counts}

        result = [
            {
                "id":           aid,
                "name":         self._actor_names.get(aid, "?"),
                "industry":     "Unknown",   # enriched below if available
                "centrality":   round(centrality[aid], 6),
                "film_count":   cnt_map.get(aid, (0, 0))[0],
                "costar_count": cnt_map.get(aid, (0, 0))[1],
            }
            for aid in top_ids
        ]

        # Enrich industry from actor names map isn't enough — fetch from DB
        industry_rows = db.execute(
            text("SELECT id, industry FROM actors WHERE id = ANY(:ids)"),
            {"ids": top_ids},
        ).fetchall()
        industry_map = {r[0]: r[1] for r in industry_rows}
        for entry in result:
            entry["industry"] = industry_map.get(entry["id"]) or "Unknown"

        self._cache.set(cache_key, result, ttl=settings.GRAVITY_RESULT_TTL)
        return result


# ── Module-level singleton ────────────────────────────────────────────────────
# Shared across all requests. Built once during app startup.
# Import and use directly:
#
#     from app.services.graph_service import graph_service
#     graph_service.find_connection(actor1_id, actor2_id)

graph_service = GraphService()
