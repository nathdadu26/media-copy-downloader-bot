import os
import re
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from aiohttp import web
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChatForwardsRestrictedError
from telethon.tl.types import (
    MessageMediaDocument,
    DocumentAttributeVideo,
    DocumentAttributeAnimated
)
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler
)

# ═══════════════════════════════════════════════
#                  LOGGING SETUP
# ═══════════════════════════════════════════════
os.makedirs("logs", exist_ok=True)

log_formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler = RotatingFileHandler(
    "logs/bot.log", maxBytes=5 * 1024 * 1024,
    backupCount=3, encoding="utf-8"
)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger("TGBot")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════
#                  LOAD ENV
# ═══════════════════════════════════════════════
load_dotenv()
API_ID         = int(os.getenv("API_ID"))
API_HASH       = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
BOT_TOKEN      = os.getenv("BOT_TOKEN")
OWNER_ID       = int(os.getenv("OWNER_ID"))
TARGET_CHANNEL = int(os.getenv("TARGET_CHANNEL"))
MONGO_URI      = os.getenv("MONGO_URI")
MONGO_DB       = os.getenv("MONGO_DB", "tgbot")
PORT           = int(os.getenv("PORT", 8000))

# ═══════════════════════════════════════════════
#                  CONSTANTS
# ═══════════════════════════════════════════════
GAP_SECONDS = 60
LINK_REGEX  = r"https://t.me/(c/)?([\w\d_]+)/(\d+)"

# ═══════════════════════════════════════════════
#              CONVERSATION STATES
# ═══════════════════════════════════════════════
WAIT_FIRST_LINK, WAIT_LAST_LINK = range(2)

# ═══════════════════════════════════════════════
#              GLOBAL STATE
# ═══════════════════════════════════════════════
is_running     = False
current_msg_id = None

# ═══════════════════════════════════════════════
#         MEDIA TYPE FILTER
# ═══════════════════════════════════════════════
def is_video_or_document(msg) -> bool:
    """
    True  → Video ya Document   → Process karo
    False → Image, GIF, Audio,
            Sticker, ya kuch aur → Skip karo
    """
    # Sirf MessageMediaDocument allow hai
    # (Photos, Polls, Geo etc. yahan nahi aate)
    if not msg.media or not isinstance(msg.media, MessageMediaDocument):
        return False

    doc   = msg.media.document
    attrs = doc.attributes

    # Animated GIF → skip
    if any(isinstance(a, DocumentAttributeAnimated) for a in attrs):
        return False

    # Video → process
    if any(isinstance(a, DocumentAttributeVideo) for a in attrs):
        return True

    # Document (application/*, text/*, etc.) → process
    mime = doc.mime_type or ""
    if mime.startswith("application/") or mime.startswith("text/"):
        return True

    # Baaki sab (audio/*, image/*, sticker etc.) → skip
    return False

# ═══════════════════════════════════════════════
#         SAFE TELEGRAM SEND/EDIT HELPERS
# ═══════════════════════════════════════════════
async def safe_edit(msg, text, retries=3, parse_mode="Markdown"):
    for attempt in range(retries):
        try:
            await msg.edit_text(text, parse_mode=parse_mode)
            return
        except Exception as e:
            if "Message is not modified" in str(e):
                return
            logger.warning(f"safe_edit attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
    logger.error("safe_edit: all retries failed, giving up")

async def safe_send(update, text, retries=3, parse_mode="Markdown"):
    for attempt in range(retries):
        try:
            await update.message.reply_text(text, parse_mode=parse_mode)
            return
        except Exception as e:
            logger.warning(f"safe_send attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
    logger.error("safe_send: all retries failed, giving up")

# ═══════════════════════════════════════════════
#                  CLIENTS
# ═══════════════════════════════════════════════
userbot = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

mongo_client = AsyncIOMotorClient(MONGO_URI)
db           = mongo_client[MONGO_DB]
col_task     = db["task"]

# ═══════════════════════════════════════════════
#            HEALTH CHECK SERVER (Koyeb)
# ═══════════════════════════════════════════════
async def health_handler(request):
    return web.Response(
        text='{"status": "ok", "bot": "running"}',
        content_type="application/json"
    )

async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"✅ Health check server started on port {PORT}")

# ═══════════════════════════════════════════════
#              MONGODB HELPERS
# ═══════════════════════════════════════════════
async def db_get_task():
    return await col_task.find_one({"_id": "current"})

async def db_save_task(data: dict):
    await col_task.update_one({"_id": "current"}, {"$set": data}, upsert=True)

async def db_clear_task():
    await col_task.delete_one({"_id": "current"})
    logger.info("MongoDB: task cleared")

# ═══════════════════════════════════════════════
#              USERBOT LOGIN
# ═══════════════════════════════════════════════
async def start_userbot():
    logger.info("UserBot connecting via session string...")
    await userbot.connect()
    if not await userbot.is_user_authorized():
        logger.error("❌ SESSION_STRING invalid ya expire ho gayi!")
        raise SystemExit("SESSION_STRING kaam nahi kar rahi.")
    me = await userbot.get_me()
    logger.info(f"UserBot logged in as: {me.first_name} (@{me.username}) [ID: {me.id}]")
    try:
        entity = await userbot.get_entity(TARGET_CHANNEL)
        logger.info(f"TARGET_CHANNEL OK: {entity.title} ✅")
    except Exception as e:
        logger.error(f"❌ TARGET_CHANNEL error: {e}")
        raise SystemExit("TARGET_CHANNEL invalid. Bot band ho raha hai.")

# ═══════════════════════════════════════════════
#              LINK PARSER
# ═══════════════════════════════════════════════
def parse_link(link: str):
    match = re.search(LINK_REGEX, link)
    if not match:
        return None, None
    is_private = match.group(1)
    chat       = match.group(2)
    msg_id     = int(match.group(3))
    chat_id    = int("-100" + chat) if is_private else chat
    return chat_id, msg_id

# ═══════════════════════════════════════════════
#              BUSY STATUS MESSAGE
# ═══════════════════════════════════════════════
async def send_busy_message(update: Update):
    task = await db_get_task()
    if not task:
        await update.message.reply_text("⚠️ Bot busy hai lekin task info nahi mili.")
        return
    remaining = task["last_id"] - task["current_id"] + 1
    done      = task["current_id"] - task["first_id"]
    total     = task["last_id"] - task["first_id"] + 1
    await update.message.reply_text(
        f"🚫 *Bot abhi busy hai!*\n\n"
        f"📺 *Channel:* `{task.get('chat_title', 'Unknown')}`\n"
        f"🆔 *Channel ID:* `{task.get('chat_id')}`\n"
        f"📊 *Total IDs:* `{total}`\n"
        f"✅ *Done:* `{done}` | 🔲 *Remaining:* `{remaining}`\n"
        f"⚙️ *Abhi:* ID `{current_msg_id}` copy ho raha hai",
        parse_mode="Markdown"
    )

# ═══════════════════════════════════════════════
#           BOT COMMAND HANDLERS
# ═══════════════════════════════════════════════
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    logger.info(f"/start by {update.effective_user.id}")
    task = await db_get_task()
    task_text = ""
    if task:
        remaining = task["last_id"] - task["current_id"] + 1
        done      = task["current_id"] - task["first_id"]
        total     = task["last_id"] - task["first_id"] + 1
        task_text = (
            f"\n\n📌 *Active Task:*\n"
            f"📺 Channel: `{task.get('chat_title', 'Unknown')}`\n"
            f"🔲 Remaining: `{remaining}/{total}`\n"
            f"✅ Done: `{done}/{total}`"
        )
    await update.message.reply_text(
        "👋 *Telegram Media Bot*\n\n"
        "📌 *Commands:*\n"
        "/copy\\_all — Range copy karo\n"
        "/status — Current task status\n"
        "/cancel — Conversation cancel karo\n\n"
        f"⚙️ Gap: `{GAP_SECONDS}s`" + task_text,
        parse_mode="Markdown"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    task = await db_get_task()
    if not task:
        await update.message.reply_text("✅ Koi active task nahi hai.")
        return
    remaining = task["last_id"] - task["current_id"] + 1
    done      = task["current_id"] - task["first_id"]
    total     = task["last_id"] - task["first_id"] + 1
    percent   = (done / total * 100) if total > 0 else 0
    bar       = "█" * int(percent / 5) + "░" * (20 - int(percent / 5))
    status_icon = "🟢 Running" if is_running else "⏸ Paused"
    await update.message.reply_text(
        f"📊 *Task Status*\n\n"
        f"`{bar}` {percent:.1f}%\n\n"
        f"📺 *Channel:* `{task.get('chat_title', 'Unknown')}`\n"
        f"🆔 *Channel ID:* `{task.get('chat_id')}`\n"
        f"📌 *Range:* `{task['first_id']}` → `{task['last_id']}`\n"
        f"✅ *Done:* `{done}/{total}`\n"
        f"🔲 *Remaining:* `{remaining}`\n\n"
        f"📋 Copied: `{task['copied']}` | ⏭ Skipped: `{task['skipped']}` | ❌ Failed: `{task['failed']}`\n\n"
        f"⚙️ *Status:* {status_icon}",
        parse_mode="Markdown"
    )

async def cmd_copy_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    if is_running:
        await send_busy_message(update)
        return
    task = await db_get_task()
    if task:
        remaining = task["last_id"] - task["current_id"] + 1
        done      = task["current_id"] - task["first_id"]
        total     = task["last_id"] - task["first_id"] + 1
        await update.message.reply_text(
            f"⚠️ *Pichla task abhi complete nahi hua!*\n\n"
            f"📺 Channel: `{task.get('chat_title', 'Unknown')}`\n"
            f"🆔 Channel ID: `{task.get('chat_id')}`\n"
            f"📊 Total IDs: `{total}`\n"
            f"✅ Done: `{done}` | 🔲 Remaining: `{remaining}`\n\n"
            f"_Pehle pichla channel complete karo._\n"
            f"/status se status dekho",
            parse_mode="Markdown"
        )
        return
    logger.info(f"/copy_all started by {update.effective_user.id}")
    await update.message.reply_text(
        "📋 *Copy All Mode*\n\nPehle message ka link bhejo:",
        parse_mode="Markdown"
    )
    return WAIT_FIRST_LINK

async def receive_first_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    link = update.message.text.strip()
    chat_id, msg_id = parse_link(link)
    if not chat_id:
        await update.message.reply_text("❌ Invalid link, dobara bhejo:")
        return WAIT_FIRST_LINK
    context.user_data["dl_chat_id"]  = chat_id
    context.user_data["dl_first_id"] = msg_id
    logger.info(f"First link — chat: {chat_id}, msg_id: {msg_id}")
    await update.message.reply_text(
        f"✅ First ID: `{msg_id}`\n\nAb last message ka link bhejo:",
        parse_mode="Markdown"
    )
    return WAIT_LAST_LINK

async def receive_last_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    link = update.message.text.strip()
    chat_id, last_id = parse_link(link)
    if not chat_id:
        await update.message.reply_text("❌ Invalid link, dobara bhejo:")
        return WAIT_LAST_LINK
    first_id       = context.user_data.get("dl_first_id")
    stored_chat_id = context.user_data.get("dl_chat_id")
    if chat_id != stored_chat_id:
        await update.message.reply_text("❌ Dono links same channel ke hone chahiye!")
        return ConversationHandler.END
    if last_id < first_id:
        await update.message.reply_text("❌ Last ID, First ID se chota nahi ho sakta!")
        return ConversationHandler.END
    try:
        entity     = await userbot.get_entity(chat_id)
        chat_title = entity.title
    except Exception:
        chat_title = str(chat_id)
    total = last_id - first_id + 1
    logger.info(f"Range set — {chat_title}: {first_id}→{last_id}, total: {total}")
    await db_save_task({
        "chat_id": chat_id, "chat_title": chat_title,
        "first_id": first_id, "last_id": last_id,
        "current_id": first_id,
        "copied": 0, "skipped": 0, "failed": 0,
        "status": "running"
    })
    progress_msg = await update.message.reply_text(
        f"🚀 *Copy All Started*\n\n"
        f"📺 *Channel:* `{chat_title}`\n"
        f"📌 *Range:* `{first_id}` → `{last_id}`\n"
        f"📊 *Total IDs:* `{total}`\n\n"
        f"⏳ Shuru ho raha hai...",
        parse_mode="Markdown"
    )
    asyncio.create_task(process_range(update, progress_msg))
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    logger.info(f"/cancel by {update.effective_user.id}")
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END

# ═══════════════════════════════════════════════
#              RANGE PROCESSOR
# ═══════════════════════════════════════════════
async def process_range(update: Update, progress_msg):
    global is_running, current_msg_id

    task = await db_get_task()
    if not task:
        logger.error("process_range: no task in DB")
        return

    is_running = True
    chat_id    = task["chat_id"]
    first_id   = task["first_id"]
    last_id    = task["last_id"]
    start_from = task["current_id"]
    copied     = task["copied"]
    skipped    = task["skipped"]
    failed     = task["failed"]
    total      = last_id - first_id + 1
    start_time = datetime.now()

    logger.info(f"process_range — {task['chat_title']}: {start_from}→{last_id}")

    def build_progress_text(cur_id):
        done    = cur_id - first_id
        percent = (done / total * 100) if total > 0 else 0
        bar     = "█" * int(percent / 5) + "░" * (20 - int(percent / 5))
        elapsed = max((datetime.now() - start_time).seconds, 1)
        eta     = int((elapsed / done) * (total - done)) if done > 0 else 0
        eta_str = f"{eta // 60}m {eta % 60}s" if eta > 0 else "calculating..."
        return (
            f"📊 *Progress — {task['chat_title']}*\n\n"
            f"`{bar}` {percent:.1f}%\n\n"
            f"✅ Done: `{done}/{total}`\n"
            f"📌 Current ID: `{cur_id}`\n"
            f"⏱ ETA: `{eta_str}`\n\n"
            f"📋 Copied: `{copied}` | ⏭ Skipped: `{skipped}` | ❌ Failed: `{failed}`"
        )

    for msg_id in range(start_from, last_id + 1):
        current_msg_id = msg_id

        await db_save_task({
            "current_id": msg_id,
            "copied": copied, "skipped": skipped, "failed": failed,
            "status": "running"
        })

        try:
            msg = await userbot.get_messages(chat_id, ids=msg_id)

            # ── Media type filter ──────────────────────────
            if not msg or not is_video_or_document(msg):
                skipped += 1
                logger.debug(f"MSG ID {msg_id} — skipped (not video/document)")
            # ── Restricted channel check ───────────────────
            else:
                chat_entity   = await userbot.get_entity(chat_id)
                is_restricted = getattr(chat_entity, "noforwards", False) or msg.noforwards

                if is_restricted:
                    skipped += 1
                    logger.info(f"MSG ID {msg_id} — skipped (restricted channel)")
                else:
                    try:
                        # Caption same rahti hai, "Forwarded from" tag nahi lagta
                        await userbot.send_message(TARGET_CHANNEL, msg)
                        copied += 1
                        logger.info(f"MSG ID {msg_id} — copied ✅")
                        await asyncio.sleep(GAP_SECONDS)
                    except ChatForwardsRestrictedError:
                        skipped += 1
                        logger.warning(f"MSG ID {msg_id} — skipped (forwards restricted)")

        except FloodWaitError as e:
            logger.warning(f"FloodWait at MSG ID {msg_id} — {e.seconds}s")
            await safe_send(update, f"⏳ FloodWait: `{e.seconds}s` wait ho raha hai...")
            await asyncio.sleep(e.seconds)
            msg_id -= 1
            continue

        except Exception as e:
            failed += 1
            logger.error(f"MSG ID {msg_id} — error: {e}", exc_info=True)

        # Update progress every 10 IDs
        if (msg_id - first_id + 1) % 10 == 0:
            try:
                await safe_edit(progress_msg, build_progress_text(msg_id + 1))
            except Exception:
                pass

    # Complete
    elapsed     = (datetime.now() - start_time).seconds
    elapsed_str = f"{elapsed // 60}m {elapsed % 60}s"
    await db_clear_task()
    is_running     = False
    current_msg_id = None
    logger.info(
        f"process_range COMPLETE — copied: {copied}, "
        f"skipped: {skipped}, failed: {failed}, time: {elapsed_str}"
    )
    await safe_edit(progress_msg,
        f"🎉 *Task Complete!*\n\n"
        f"📺 *Channel:* `{task['chat_title']}`\n"
        f"📊 *Total checked:* `{total}`\n\n"
        f"📋 Copied: `{copied}`\n"
        f"⏭ Skipped: `{skipped}`\n"
        f"❌ Failed: `{failed}`\n"
        f"⏱ Time: `{elapsed_str}`"
    )

# ═══════════════════════════════════════════════
#              GLOBAL ERROR HANDLER
# ═══════════════════════════════════════════════
async def error_handler(update, context):
    err = context.error
    logger.error(f"Global error: {err}", exc_info=context.error)
    from telegram.error import NetworkError, TimedOut
    if isinstance(err, (NetworkError, TimedOut)):
        logger.warning(f"Transient network error (ignored): {err}")
        return
    try:
        if update and update.message:
            await safe_send(update, f"⚠️ Unexpected error: {err}")
    except Exception:
        pass

# ═══════════════════════════════════════════════
#                    MAIN
# ═══════════════════════════════════════════════
def main():
    logger.info("=" * 55)
    logger.info("Bot starting...")
    logger.info("=" * 55)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(start_userbot())
    loop.run_until_complete(start_health_server())

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("status", cmd_status))

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("copy_all", cmd_copy_all)],
        states={
            WAIT_FIRST_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_first_link)],
            WAIT_LAST_LINK:  [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_last_link)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_error_handler(error_handler)

    logger.info("✅ Bot polling started")
    app.run_polling()

if __name__ == "__main__":
    main()
