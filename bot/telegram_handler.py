import asyncio
import io
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import Application, CallbackQueryHandler, ContextTypes
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
import db

REJECT_REASONS = [
    ("Too generic",    "generic"),
    ("Wrong tone",     "tone"),
    ("Weak stat",      "weak_stat"),
    ("Sensitive topic","sensitive"),
    ("Other",          "other"),
]

_app: Application | None = None
_post_callback          = None   # injected from main.py — posts to Twitter
_reddit_post_callback   = None   # injected from main.py — posts to Reddit
_reddit_format_callback = None   # injected from main.py — generates Reddit draft from Twitter row

def set_post_callback(fn):
    global _post_callback
    _post_callback = fn

def set_reddit_post_callback(fn):
    global _reddit_post_callback
    _reddit_post_callback = fn

def set_reddit_format_callback(fn):
    global _reddit_format_callback
    _reddit_format_callback = fn

# ── Shared header builder ─────────────────────────────────────────────────────

def _build_header(actor_name: str, handle: str, trigger: str, trigger_context: str,
                  engage_reason: str, stat_angle: str, confidence: float,
                  original_tweet: str, reply_text: str, platform: str = "twitter") -> str:
    icon = "🐦" if platform == "twitter" else "🟠"
    lines = [f"{icon} *Draft {'Reply' if platform == 'twitter' else 'Reddit Comment'}*"]
    lines.append(f"Actor: *{actor_name}* (@{handle})")

    if trigger == "signal" and trigger_context:
        lines.append(f"Trigger: signal — `{trigger_context[:80]}`")
    elif trigger == "trend":
        lines.append(f"Trigger: `trending topic`")
    elif trigger == "reddit_post":
        lines.append(f"Trigger: `Reddit post`")
    else:
        lines.append(f"Trigger: `direct tweet`")

    if engage_reason:
        lines.append(f"Why engage: _{engage_reason}_")
    if stat_angle:
        lines.append(f"Stat angle: `{stat_angle}`")
    lines.append(f"Confidence: `{confidence:.0f}%`")

    if original_tweet:
        truncated = original_tweet[:200] + ("…" if len(original_tweet) > 200 else "")
        label = "Reddit post" if trigger == "reddit_post" else "Their tweet"
        lines.append(f"\n*{label}:*\n_{truncated}_")

    lines.append(f"\n*Draft:*\n```\n{reply_text}\n```")
    return "\n".join(lines)

# ── Twitter review ────────────────────────────────────────────────────────────

async def send_for_review(row_id: int, actor_name: str, handle: str,
                           reply_text: str, confidence: float,
                           trigger: str, screenshot: bytes | None,
                           original_tweet: str = "",
                           engage_reason: str = "",
                           stat_angle: str = "",
                           trigger_context: str = "") -> int | None:
    bot    = Bot(token=TELEGRAM_BOT_TOKEN)
    header = _build_header(actor_name, handle, trigger, trigger_context,
                           engage_reason, stat_angle, confidence,
                           original_tweet, reply_text, platform="twitter")

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"approve:{row_id}"),
        InlineKeyboardButton("❌ Reject",  callback_data=f"reject:{row_id}"),
    ]])

    try:
        if screenshot:
            msg = await bot.send_photo(
                chat_id=TELEGRAM_CHAT_ID,
                photo=io.BytesIO(screenshot),
                caption=header,
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
        else:
            msg = await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=header,
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
        return msg.message_id
    except Exception as e:
        print(f"[telegram] send failed: {e}")
        return None

# ── Reddit review ─────────────────────────────────────────────────────────────

async def send_reddit_for_review(row_id: int, actor_name: str, handle: str,
                                  comment_text: str, confidence: float,
                                  subreddit: str, post_title: str,
                                  post_url: str) -> int | None:
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    header = (
        f"🟠 *Reddit Draft* — r/{subreddit}\n"
        f"Actor: *{actor_name}* (@{handle})\n"
        f"Confidence: `{confidence:.0f}%`\n\n"
        f"*Post:* _{post_title[:120]}_\n\n"
        f"*Comment draft:*\n```\n{comment_text[:800]}\n```"
    )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Post to Reddit", callback_data=f"reddit_approve:{row_id}"),
        InlineKeyboardButton("❌ Skip",           callback_data=f"reddit_skip:{row_id}"),
    ]])

    try:
        msg = await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=header,
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        return msg.message_id
    except Exception as e:
        print(f"[telegram] reddit send failed: {e}")
        return None

# ── Callback handlers ─────────────────────────────────────────────────────────

async def _handle_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    row_id = int(query.data.split(":")[1])

    row = db.get_pending_by_telegram_id(query.message.message_id)
    if not row:
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(TELEGRAM_CHAT_ID, f"⚠️ Row {row_id} not found or already actioned.")
        return

    db.mark_approved(row_id)
    await query.edit_message_reply_markup(reply_markup=None)
    await context.bot.send_message(TELEGRAM_CHAT_ID, "✅ Approved — posting to Twitter now...")

    if _post_callback:
        asyncio.create_task(_post_callback(row))

    # Offer Reddit formatting
    reddit_keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🟠 Format for Reddit", callback_data=f"reddit_format:{row_id}"),
        InlineKeyboardButton("Skip",                 callback_data=f"reddit_format_skip:{row_id}"),
    ]])
    await context.bot.send_message(
        TELEGRAM_CHAT_ID,
        f"Also post this to Reddit?",
        reply_markup=reddit_keyboard,
    )

async def _handle_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    row_id = int(query.data.split(":")[1])

    reason_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(label, callback_data=f"reason:{row_id}:{key}")]
        for label, key in REJECT_REASONS
    ])
    await query.edit_message_reply_markup(reply_markup=reason_keyboard)

async def _handle_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, row_id_str, reason = query.data.split(":", 2)
    row_id = int(row_id_str)

    db.mark_rejected(row_id, reason)
    await query.edit_message_reply_markup(reply_markup=None)
    await context.bot.send_message(TELEGRAM_CHAT_ID, f"❌ Rejected ({reason})")

async def _handle_reddit_format(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    row_id = int(query.data.split(":")[1])

    await query.edit_message_reply_markup(reply_markup=None)
    await context.bot.send_message(TELEGRAM_CHAT_ID, "🟠 Generating Reddit draft...")

    if _reddit_format_callback:
        asyncio.create_task(_reddit_format_callback(row_id))

async def _handle_reddit_format_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_reply_markup(reply_markup=None)

async def _handle_reddit_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    row_id = int(query.data.split(":")[1])

    row = db.get_pending_by_telegram_id(query.message.message_id)
    if not row:
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(TELEGRAM_CHAT_ID, f"⚠️ Row {row_id} not found.")
        return

    db.mark_approved(row_id)
    await query.edit_message_reply_markup(reply_markup=None)
    await context.bot.send_message(TELEGRAM_CHAT_ID, "🟠 Posting to Reddit...")

    if _reddit_post_callback:
        asyncio.create_task(_reddit_post_callback(row))

async def _handle_reddit_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    row_id = int(query.data.split(":")[1])

    db.mark_rejected(row_id, "reddit_skipped")
    await query.edit_message_reply_markup(reply_markup=None)
    await context.bot.send_message(TELEGRAM_CHAT_ID, "⏭ Reddit post skipped")

# ── App builder ───────────────────────────────────────────────────────────────

def build_app() -> Application:
    global _app
    _app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    _app.add_handler(CallbackQueryHandler(_handle_approve,           pattern=r"^approve:"))
    _app.add_handler(CallbackQueryHandler(_handle_reject,            pattern=r"^reject:"))
    _app.add_handler(CallbackQueryHandler(_handle_reason,            pattern=r"^reason:"))
    _app.add_handler(CallbackQueryHandler(_handle_reddit_format,     pattern=r"^reddit_format:\d+$"))
    _app.add_handler(CallbackQueryHandler(_handle_reddit_format_skip,pattern=r"^reddit_format_skip:"))
    _app.add_handler(CallbackQueryHandler(_handle_reddit_approve,    pattern=r"^reddit_approve:"))
    _app.add_handler(CallbackQueryHandler(_handle_reddit_skip,       pattern=r"^reddit_skip:"))

    return _app
