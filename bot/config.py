import os
from pathlib import Path
from dotenv import load_dotenv

# Load from production path first, fall back to repo root .env for local dev
_prod_env = Path("/opt/cinetrace/.env")
_local_env = Path(__file__).parent.parent / ".env"
load_dotenv(_prod_env if _prod_env.exists() else _local_env)

TWITTER_API_KEY             = os.environ["TWITTER_API_KEY"]
TWITTER_API_SECRET          = os.environ["TWITTER_API_SECRET"]
TWITTER_BEARER_TOKEN        = os.environ["TWITTER_BEARER_TOKEN"]
TWITTER_ACCESS_TOKEN        = os.environ["TWITTER_ACCESS_TOKEN"]
TWITTER_ACCESS_TOKEN_SECRET = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]

ANTHROPIC_API_KEY   = os.environ["ANTHROPIC_API_KEY"]
TELEGRAM_BOT_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID    = int(os.environ["TELEGRAM_CHAT_ID"])

DATABASE_URL        = os.environ["DATABASE_URL"]
CINETRACE_BASE_URL    = os.getenv("CINETRACE_BASE_URL", "https://cinetrace.in")
# Internal URL used by Playwright inside Docker — avoids Cloudflare/SSL overhead.
# Falls back to the public URL so local dev works without setting this.
CINETRACE_SCREENSHOT_URL = os.getenv("CINETRACE_SCREENSHOT_URL", CINETRACE_BASE_URL)
CINETRACE_API_URL   = "http://backend:8000"

# Reddit (optional — monitor disabled if not set)
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USERNAME      = os.getenv("REDDIT_USERNAME", "")
REDDIT_PASSWORD      = os.getenv("REDDIT_PASSWORD", "")

# Bot behaviour
MAX_REPLIES_PER_ACTOR_PER_DAY = 3
MIN_HOURS_BETWEEN_POSTS       = 2
CONFIDENCE_AUTO_THRESHOLD     = 101   # 101 = all go to Telegram (100% review mode)
TREND_POLL_INTERVAL_SECONDS   = 1800  # 30 minutes
