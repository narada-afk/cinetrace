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
_post_callback = None  # injected from main.py

def set_post_callback(fn):
    global _post_callback
    _post_callback = fn

async def send_for_review(row_id: int, actor_name: str, handle: str,
                           reply_text: str, confidence: float,
                           trigger: str, screenshot: bytes | None,
                           original_tweet: str = "",
                           engage_reason: str = "",
                           stat_angle: str = "",
                           trigger_context: str = "") -> int | None:
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    # Build context block
    lines = [f"📋 *Draft Reply*"]
    lines.append(f"Actor: *{actor_name}* (@{handle})")

    if trigger == "signal" and trigger_context:
        lines.append(f"Trigger: signal — `{trigger_context[:80]}`")
    elif trigger == "trend":
        lines.append(f"Trigger: `trending topic`")
    else:
        lines.append(f"Trigger: `direct tweet`")

    if engage_reason:
        lines.append(f"Why engage: _{engage_reason}_")
    if stat_angle:
        lines.append(f"Stat angle: `{stat_angle}`")
    lines.append(f"Confidence: `{confidence:.0f}%`")

    if original_tweet:
        truncated = original_tweet[:200] + ("…" if len(original_tweet) > 200 else "")
        lines.append(f"\n*Their tweet:*\n_{truncated}_")

    lines.append(f"\n*Draft:*\n```\n{reply_text}\n```")

    header = "\n".join(lines)

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"approve:{row_id}"),
            InlineKeyboardButton("❌ Reject",  callback_data=f"reject:{row_id}"),
        ]
    ])

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

async def _handle_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    row_id = int(query.data.split(":")[1])

    row = db.get_pending_by_telegram_id(query.message.message_id)
    if not row:
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(TELEGRAM_CHAT_ID, f"⚠️ Row {row_id} not found or already actioned.")
        return

    db.mark_approved(row_id)
    await query.edit_message_reply_markup(reply_markup=None)
    await context.bot.send_message(TELEGRAM_CHAT_ID, f"✅ Approved — posting now...")

    if _post_callback:
        asyncio.create_task(_post_callback(row))

async def _handle_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
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

def build_app() -> Application:
    global _app
    _app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )
    _app.add_handler(CallbackQueryHandler(_handle_approve, pattern=r"^approve:"))
    _app.add_handler(CallbackQueryHandler(_handle_reject,  pattern=r"^reject:"))
    _app.add_handler(CallbackQueryHandler(_handle_reason,  pattern=r"^reason:"))
    return _app
