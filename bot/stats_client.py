import httpx
from config import CINETRACE_API_URL, CINETRACE_BASE_URL

_client = httpx.AsyncClient(base_url=CINETRACE_API_URL, timeout=15)

async def find_actor(db_name: str) -> dict | None:
    r = await _client.get("/actors/search", params={"q": db_name})
    if r.status_code != 200:
        return None
    results = r.json()
    if not results:
        return None
    name_lower = db_name.lower()
    for actor in results:
        if actor.get("name", "").lower() == name_lower:
            return actor
    return results[0]

async def get_actor_stats(actor_id: int) -> dict | None:
    r = await _client.get(f"/actors/{actor_id}")
    return r.json() if r.status_code == 200 else None

async def get_actor_movies(actor_id: int) -> list:
    r = await _client.get(f"/actors/{actor_id}/movies")
    return r.json() if r.status_code == 200 else []

async def get_actor_collaborators(actor_id: int) -> list:
    r = await _client.get(f"/actors/{actor_id}/collaborators")
    return r.json() if r.status_code == 200 else []

async def get_actor_directors(actor_id: int) -> list:
    r = await _client.get(f"/actors/{actor_id}/directors")
    return r.json() if r.status_code == 200 else []

async def get_full_profile(db_name: str) -> dict | None:
    actor = await find_actor(db_name)
    if not actor:
        return None

    actor_id = actor["id"]
    stats, movies, collaborators, directors = await asyncio.gather(
        get_actor_stats(actor_id),
        get_actor_movies(actor_id),
        get_actor_collaborators(actor_id),
        get_actor_directors(actor_id),
    )

    return {
        "actor":        actor,
        "stats":        stats,
        "movies":       movies,
        "collaborators": collaborators,
        "directors":    directors,
        "profile_url":  f"{CINETRACE_BASE_URL}/actors/{actor.get('slug', db_name.lower().replace(' ', ''))}",
    }

import asyncio
