import asyncio
import tweepy
from actors import ACTORS
from config import TWITTER_BEARER_TOKEN, TWITTER_API_KEY, TWITTER_API_SECRET, \
                   TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TREND_POLL_INTERVAL_SECONDS

_seen_trends: set[str] = set()

WOEID_INDIA = 23424848  # Yahoo Where On Earth ID for India

def _build_client() -> tweepy.API:
    auth = tweepy.OAuth1UserHandler(
        TWITTER_API_KEY, TWITTER_API_SECRET,
        TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET,
    )
    return tweepy.API(auth)

def _matches_actor(trend_name: str) -> dict | None:
    name_lower = trend_name.lower()
    for actor in ACTORS:
        if actor["name"].lower() in name_lower or \
           actor["handle"].lower() in name_lower:
            return actor
    return None

async def poll_trends(process_trend_fn):
    client = _build_client()
    print(f"[trends] poller started — interval {TREND_POLL_INTERVAL_SECONDS}s")

    while True:
        try:
            # v1.1 trends endpoint via tweepy.API
            trends_response = client.trends_place(WOEID_INDIA)
            if trends_response and trends_response[0]:
                trends = trends_response[0].get("trends", [])
                for trend in trends:
                    name       = trend.get("name", "")
                    tweet_vol  = trend.get("tweet_volume") or 0
                    trend_key  = name.lower()

                    if trend_key in _seen_trends:
                        continue

                    actor = _matches_actor(name)
                    if actor:
                        _seen_trends.add(trend_key)
                        print(f"[trends] matched actor trend: {name} (vol: {tweet_vol})")
                        asyncio.create_task(process_trend_fn(name, actor, tweet_vol))

        except Exception as e:
            print(f"[trends] poll error: {e}")

        await asyncio.sleep(TREND_POLL_INTERVAL_SECONDS)
