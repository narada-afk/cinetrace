import asyncio
import threading
import tweepy
from actors import BY_HANDLE, ALL_HANDLES
from config import TWITTER_BEARER_TOKEN

_loop: asyncio.AbstractEventLoop | None = None
_USER_ID_MAP: dict[str, dict] = {}

class ActorStreamListener(tweepy.StreamingClient):
    def __init__(self, process_fn, *args, **kwargs):
        super().__init__(TWITTER_BEARER_TOKEN, *args, **kwargs)
        self._process = process_fn

    def on_tweet(self, tweet):
        actor = _USER_ID_MAP.get(str(tweet.author_id))
        if not actor or not _loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._process(tweet, actor), _loop
        )

    def on_errors(self, errors):
        print(f"[stream] errors: {errors}")

    def on_disconnect(self):
        print("[stream] disconnected")

def resolve_user_ids() -> dict[str, dict]:
    client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN)
    resolved = {}
    batch_size = 100
    handles = ALL_HANDLES[:]
    for i in range(0, len(handles), batch_size):
        batch = handles[i:i + batch_size]
        try:
            resp = client.get_users(usernames=batch, user_fields=["id"])
            if resp.data:
                for user in resp.data:
                    actor = BY_HANDLE.get(user.username.lower())
                    if actor:
                        resolved[str(user.id)] = actor
                        print(f"[stream] resolved @{user.username} → {user.id}")
        except Exception as e:
            print(f"[stream] resolve error: {e}")
    return resolved

def setup_stream_rules(stream: ActorStreamListener, user_ids: list[str]):
    existing = stream.get_rules()
    if existing.data:
        ids = [r.id for r in existing.data]
        stream.delete_rules(ids)
        print(f"[stream] deleted {len(ids)} old rules")

    chunk_size = 25
    for i in range(0, len(user_ids), chunk_size):
        chunk = user_ids[i:i + chunk_size]
        rule = " OR ".join(f"from:{uid}" for uid in chunk)
        stream.add_rules(tweepy.StreamRule(rule))
    print(f"[stream] rules set for {len(user_ids)} actors")

async def start_stream(process_fn):
    global _loop, _USER_ID_MAP
    _loop = asyncio.get_running_loop()

    _USER_ID_MAP = await _loop.run_in_executor(None, resolve_user_ids)
    user_ids = list(_USER_ID_MAP.keys())

    stream = ActorStreamListener(process_fn)
    setup_stream_rules(stream, user_ids)

    thread = threading.Thread(
        target=stream.filter,
        kwargs={"tweet_fields": ["author_id", "text", "created_at"]},
        daemon=True,
    )
    thread.start()
    print(f"[stream] listening to {len(user_ids)} actors in background thread")
    return stream
