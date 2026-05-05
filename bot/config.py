import os
from dotenv import load_dotenv

load_dotenv("/opt/cinetrace/.env")

TWITTER_API_KEY             = os.environ["TWITTER_API_KEY"]
TWITTER_API_SECRET          = os.environ["TWITTER_API_SECRET"]
TWITTER_BEARER_TOKEN        = os.environ["TWITTER_BEARER_TOKEN"]
TWITTER_ACCESS_TOKEN        = os.environ["TWITTER_ACCESS_TOKEN"]
TWITTER_ACCESS_TOKEN_SECRET = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]

ANTHROPIC_API_KEY   = os.environ["ANTHROPIC_API_KEY"]
TELEGRAM_BOT_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID    = int(os.environ["TELEGRAM_CHAT_ID"])

DATABASE_URL        = os.environ["DATABASE_URL"]
CINETRACE_BASE_URL  = os.getenv("CINETRACE_BASE_URL", "https://cinetrace.in")
CINETRACE_API_URL   = "http://backend:8000"

# Bot behaviour
MAX_REPLIES_PER_ACTOR_PER_DAY = 3
MIN_HOURS_BETWEEN_POSTS       = 2
CONFIDENCE_AUTO_THRESHOLD     = 101   # 101 = all go to Telegram (100% review mode)
TREND_POLL_INTERVAL_SECONDS   = 1800  # 30 minutes
