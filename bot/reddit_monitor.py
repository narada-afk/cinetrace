import asyncio
import re
import asyncpraw
from actors import ACTORS
from config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD

SUBREDDITS     = ["tollywood", "kollywood", "MalayalamMovies", "KannadaMovies"]
USER_AGENT     = f"CineTrace Stats Bot v1.0 by /u/{REDDIT_USERNAME}"

_seen_posts: set[str] = set()

def detect_actor_in_text(text: str) -> dict | None:
    text_lower = text.lower()
    for actor in ACTORS:
        if re.search(rf"\b{re.escape(actor['name'].lower())}\b", text_lower):
            return actor
    return None

def subreddit_of(submission) -> str:
    return str(submission.subreddit).lower()

async def monitor_subreddits(process_fn):
    if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD]):
        print("[reddit] credentials not set — monitor disabled")
        return

    reddit = asyncpraw.Reddit(
        client_id     = REDDIT_CLIENT_ID,
        client_secret = REDDIT_CLIENT_SECRET,
        username      = REDDIT_USERNAME,
        password      = REDDIT_PASSWORD,
        user_agent    = USER_AGENT,
    )

    sub_str   = "+".join(SUBREDDITS)
    subreddit = await reddit.subreddit(sub_str)
    print(f"[reddit] monitoring r/{sub_str}")

    try:
        async for submission in subreddit.stream.submissions(skip_existing=True):
            if submission.id in _seen_posts:
                continue
            _seen_posts.add(submission.id)

            text  = f"{submission.title} {submission.selftext or ''}"
            actor = detect_actor_in_text(text)
            if actor:
                sub_name = subreddit_of(submission)
                print(f"[reddit] r/{sub_name} — post about {actor['name']}: {submission.title[:60]}")
                asyncio.create_task(process_fn(submission, actor))
    except Exception as e:
        print(f"[reddit] stream error: {e}")
    finally:
        await reddit.close()
