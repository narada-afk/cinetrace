import asyncio
import threading
import time
import tweepy
from actors import BY_HANDLE, ALL_HANDLES, SIGNALS_BY_HANDLE, ALL_SIGNAL_HANDLES
from config import TWITTER_BEARER_TOKEN

_loop: asyncio.AbstractEventLoop | None = None
_ACTOR_ID_MAP:  dict[str, dict] = {}   # user_id → actor dict
_SIGNAL_ID_MAP: dict[str, dict] = {}   # user_id → signal account dict

_BACKOFF_BASE    = 60    # seconds
_BACKOFF_MAX     = 900   # 15 minutes cap
_ALERT_THRESHOLD = 5     # alert after this many consecutive 429s

# Circuit-breaker signal: set True when a resolve attempt hits a
# credit/quota/access failure (HTTP 402/403 or "credits" in the error). The
# watchdog reads credits_depleted() to throttle restarts instead of hammering
# get_users every 5 minutes and bleeding API requests. Reset at the start of
# each resolve_user_ids() attempt so it always reflects the latest try.
_credits_depleted = False


def credits_depleted() -> bool:
    return _credits_depleted

class ActorStreamListener(tweepy.StreamingClient):
    def __init__(self, actor_fn, signal_fn, *args, **kwargs):
        super().__init__(TWITTER_BEARER_TOKEN, *args, **kwargs)
        self._on_actor       = actor_fn
        self._on_signal      = signal_fn
        self._consecutive_429 = 0

    def on_tweet(self, tweet):
        if not _loop:
            return
        uid = str(tweet.author_id)
        actor = _ACTOR_ID_MAP.get(uid)
        if actor:
            asyncio.run_coroutine_threadsafe(
                self._on_actor(tweet, actor), _loop
            )
            return
        signal = _SIGNAL_ID_MAP.get(uid)
        if signal:
            asyncio.run_coroutine_threadsafe(
                self._on_signal(tweet, signal), _loop
            )

    def on_errors(self, errors):
        print(f"[stream] errors: {errors}")

    def on_disconnect(self):
        print("[stream] disconnected")

    def on_request_error(self, status_code):
        if status_code == 429:
            self._consecutive_429 += 1
            backoff = min(_BACKOFF_BASE * (2 ** (self._consecutive_429 - 1)), _BACKOFF_MAX)
            print(f"[stream] HTTP 429 — backoff {backoff}s (consecutive: {self._consecutive_429})")
            if self._consecutive_429 == _ALERT_THRESHOLD and _loop:
                import telegram_handler
                asyncio.run_coroutine_threadsafe(
                    telegram_handler.send_alert(
                        f"Stream rate-limited for {self._consecutive_429} consecutive requests. "
                        f"Twitter API quota may be exhausted."
                    ), _loop
                )
            time.sleep(backoff)
        else:
            self._consecutive_429 = 0
            print(f"[stream] HTTP {status_code} — backing off 60s")
            time.sleep(60)
        return True

def _resolve_batch(client, handles: list[str], lookup: dict, label: str) -> dict[str, dict]:
    resolved = {}
    for i in range(0, len(handles), 100):
        batch = handles[i:i + 100]
        try:
            resp = client.get_users(usernames=batch, user_fields=["id"])
            if resp.data:
                for user in resp.data:
                    entry = lookup.get(user.username.lower())
                    if entry:
                        resolved[str(user.id)] = entry
                        print(f"[stream] resolved [{label}] @{user.username} → {user.id}")
        except Exception as e:
            global _credits_depleted
            code = getattr(getattr(e, "response", None), "status_code", None)
            if code in (402, 403) or "credits" in str(e).lower() or "402" in str(e):
                _credits_depleted = True
            print(f"[stream] resolve error ({label}): {e}")
    return resolved

def resolve_user_ids() -> tuple[dict[str, dict], dict[str, dict]]:
    global _credits_depleted
    _credits_depleted = False   # reflect only this attempt
    client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN)
    actors  = _resolve_batch(client, ALL_HANDLES,        BY_HANDLE,        "actor")
    signals = _resolve_batch(client, ALL_SIGNAL_HANDLES, SIGNALS_BY_HANDLE, "signal")
    return actors, signals

def setup_stream_rules(stream: ActorStreamListener, all_user_ids: list[str]):
    existing = stream.get_rules()
    if existing.data:
        ids = [r.id for r in existing.data]
        stream.delete_rules(ids)
        print(f"[stream] deleted {len(ids)} old rules")

    chunk_size = 25
    for i in range(0, len(all_user_ids), chunk_size):
        chunk = all_user_ids[i:i + chunk_size]
        rule = " OR ".join(f"from:{uid}" for uid in chunk)
        stream.add_rules(tweepy.StreamRule(rule))
    print(f"[stream] rules set for {len(all_user_ids)} accounts (actors + signals)")

async def start_stream(actor_fn, signal_fn):
    global _loop, _ACTOR_ID_MAP, _SIGNAL_ID_MAP
    _loop = asyncio.get_running_loop()

    try:
        _ACTOR_ID_MAP, _SIGNAL_ID_MAP = await _loop.run_in_executor(None, resolve_user_ids)
        all_ids = list(_ACTOR_ID_MAP.keys()) + list(_SIGNAL_ID_MAP.keys())

        # Nothing resolved → do NOT call setup_stream_rules or start the thread
        # (both would fire more failing API calls). Returning None tells the
        # watchdog the start failed so it can back off instead of hammering.
        if not all_ids:
            reason = "API credits depleted" if _credits_depleted else "no user IDs resolved"
            print(f"[stream] DISABLED — {reason}; skipping rule setup and stream start.")
            print("[stream] Scheduled tweets (broadcaster) will still run normally.")
            return None

        stream = ActorStreamListener(actor_fn, signal_fn)
        setup_stream_rules(stream, all_ids)

        # name MUST contain "stream" — the main.py watchdog decides the stream
        # is alive by looking for a thread whose name contains "stream"/"tweepy".
        # An unnamed thread is "Thread-N (filter)" (Py3.11), which matches
        # neither, so the watchdog would think a healthy stream is dead and
        # restart-loop every 5 min — the original cause of the API-request bleed.
        thread = threading.Thread(
            target=stream.filter,
            kwargs={"tweet_fields": ["author_id", "text", "created_at"]},
            daemon=True,
            name="tweepy-stream",
        )
        thread.start()
        print(f"[stream] listening: {len(_ACTOR_ID_MAP)} actors + {len(_SIGNAL_ID_MAP)} signals")
        return stream
    except Exception as e:
        print(f"[stream] DISABLED — could not start: {e}")
        print("[stream] Scheduled tweets (broadcaster) will still run normally.")
        print("[stream] Reactive tweets require Twitter Basic tier ($100/month) for Filtered Stream access.")
        return None
