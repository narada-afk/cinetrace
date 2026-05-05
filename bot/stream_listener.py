import asyncio
import tweepy
from actors import BY_HANDLE, ALL_HANDLES
from config import TWITTER_BEARER_TOKEN

class ActorStreamListener(tweepy.AsyncStreamingClient):
    def __init__(self, process_fn, *args, **kwargs):
        super().__init__(TWITTER_BEARER_TOKEN, *args, **kwargs)
        self._process = process_fn

    async def on_tweet(self, tweet):
        author_id = str(tweet.author_id)
        # look up actor by user id (resolved at startup)
        actor = _USER_ID_MAP.get(author_id)
        if not actor:
            return
        asyncio.create_task(self._process(tweet, actor))

    async def on_errors(self, errors):
        print(f"[stream] errors: {errors}")

    async def on_disconnect(self):
        print("[stream] disconnected — reconnecting in 10s...")
        await asyncio.sleep(10)

_USER_ID_MAP: dict[str, dict] = {}

async def resolve_user_ids(client: tweepy.AsyncClient) -> dict[str, dict]:
    resolved = {}
    batch_size = 100
    handles = ALL_HANDLES[:]
    for i in range(0, len(handles), batch_size):
        batch = handles[i:i + batch_size]
        usernames = ",".join(batch)
        resp = await client.get_users(usernames=usernames, user_fields=["id"])
        if resp.data:
            for user in resp.data:
                handle_lower = user.username.lower()
                actor = BY_HANDLE.get(handle_lower)
                if actor:
                    resolved[str(user.id)] = actor
                    print(f"[stream] resolved @{user.username} → id {user.id}")
    return resolved

async def setup_stream_rules(stream: ActorStreamListener, user_ids: list[str]):
    existing = await stream.get_rules()
    if existing.data:
        ids = [r.id for r in existing.data]
        await stream.delete_rules(ids)
        print(f"[stream] deleted {len(ids)} old rules")

    # X filtered stream supports up to 25 rules on Basic
    # We pack up to 25 user IDs per rule using OR
    chunk_size = 25
    chunks = [user_ids[i:i+chunk_size] for i in range(0, len(user_ids), chunk_size)]
    for chunk in chunks:
        rule = " OR ".join(f"from:{uid}" for uid in chunk)
        await stream.add_rules(tweepy.StreamRule(rule))
    print(f"[stream] added {len(chunks)} stream rules for {len(user_ids)} actors")

async def start_stream(process_fn) -> ActorStreamListener:
    client = tweepy.AsyncClient(bearer_token=TWITTER_BEARER_TOKEN)

    global _USER_ID_MAP
    _USER_ID_MAP = await resolve_user_ids(client)
    user_ids = list(_USER_ID_MAP.keys())

    stream = ActorStreamListener(process_fn)
    await setup_stream_rules(stream, user_ids)

    asyncio.create_task(
        stream.filter(tweet_fields=["author_id", "text", "created_at"])
    )
    print(f"[stream] listening to {len(user_ids)} actors")
    return stream
