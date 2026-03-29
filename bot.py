import os
import re
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError, ChatForwardsRestrictedError
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

# ═══════════════════════════════════════════════
#                  LOAD ENV
# ═══════════════════════════════════════════════
load_dotenv()
API_ID         = int(os.getenv("API_ID"))
API_HASH       = os.getenv("API_HASH")
BOT_TOKEN      = os.getenv("BOT_TOKEN")
OWNER_ID       = int(os.getenv("OWNER_ID"))
TARGET_CHANNEL = int(os.getenv("TARGET_CHANNEL"))
PHONE_NUMBER   = os.getenv("PHONE_NUMBER")
MONGO_URI      = os.getenv("MONGO_URI")          # mongodb+srv://...
MONGO_DB       = os.getenv("MONGO_DB", "tgbot")  # database name

# ═══════════════════════════════════════════════
#                  CONSTANTS
# ═══════════════════════════════════════════════
DAILY_LIMIT  = 150   # max videos per day
GAP_SECONDS  = 60    # 1 minute gap between each copy/download
LINK_REGEX   = r"https://t.me/(c/)?([\w\d_]+)/(\d+)"

# ═══════════════════════════════════════════════
#              CONVERSATION STATES
# ═══════════════════════════════════════════════
WAIT_FIRST_LINK, WAIT_LAST_LINK = range(2)

# ═══════════════════════════════════════════════
#              GLOBAL STATE
# ═══════════════════════════════════════════════
is_running     = False   # is a task currently running?
current_op     = None    # "copy" or "download"
current_msg_id = None    # which message id is being processed right now

# ═══════════════════════════════════════════════
#                  CLIENTS
# ═══════════════════════════════════════════════
userbot = TelegramClient("userbot_session", API_ID, API_HASH)

mongo_client = AsyncIOMotorClient(MONGO_URI)
db           = mongo_client[MONGO_DB]
col_task     = db["task"]      # current task document
col_daily    = db["daily"]     # daily count tracker

# ═══════════════════════════════════════════════
#              MONGODB HELPERS
# ═══════════════════════════════════════════════

async def db_get_task():
    """Return current task document or None."""
    return await col_task.find_one({"_id": "current"})

async def db_save_task(data: dict):
    await col_task.update_one(
        {"_id": "current"},
        {"$set": data},
        upsert=True
    )

async def db_clear_task():
    await col_task.delete_one({"_id": "current"})
    logger.info("MongoDB: task cleared")

async def db_get_daily_count() -> int:
    """Return today's processed count."""
    today = date.today().isoformat()
    doc = await col_daily.find_one({"_id": "daily"})
    if doc and doc.get("date") == today:
        return doc.get("count", 0)
    # New day — reset
    await col_daily.update_one(
        {"_id": "daily"},
        {"$set": {"date": today, "count": 0}},
        upsert=True
    )
    logger.info(f"MongoDB: new day {today}, daily count reset")
    return 0

async def db_increment_daily():
    today = date.today().isoformat()
    await col_daily.update_one(
        {"_id": "daily"},
        {"$set": {"date": today}, "$inc": {"count": 1}},
        upsert=True
    )

# ═══════════════════════════════════════════════
#              USERBOT LOGIN
# ═══════════════════════════════════════════════
async def start_userbot():
    logger.info("UserBot connecting...")
    await userbot.connect()
    if not await userbot.is_user_authorized():
        logger.info("Session nahi mili, phone se login ho raha hai...")
        await userbot.send_code_request(PHONE_NUMBER)
        code = input("📩 OTP enter karo (Telegram se aaya): ").strip()
        try:
            await userbot.sign_in(PHONE_NUMBER, code)
        except SessionPasswordNeededError:
            password = input("🔐 2FA password enter karo: ").strip()
            await userbot.sign_in(password=password)
    me = await userbot.get_me()
    logger.info(f"UserBot logged in as: {me.first_name} (@{me.username}) [ID: {me.id}]")

    # TARGET_CHANNEL validation
    logger.info(f"TARGET_CHANNEL validating: {TARGET_CHANNEL}")
    try:
        entity = await userbot.get_entity(TARGET_CHANNEL)
        logger.info(f"TARGET_CHANNEL OK: {entity.title} ✅")
    except Exception as e:
        logger.error(
            f"❌ TARGET_CHANNEL error: {e}\n"
            f"   1. .env me TARGET_CHANNEL=-100XXXXXXXXXX format use karo\n"
            f"   2. Userbot ko channel ka admin banao (Post Messages permission)"
        )
        raise SystemExit("TARGET_CHANNEL invalid. Bot band ho raha hai.")

# ═══════════════════════════════════════════════
#              LINK PARSER
# ═══════════════════════════════════════════════
def parse_link(link: str):
    match = re.search(LINK_REGEX, link)
    if not match:
        return None, None
    is_private = match.group(1)
    chat = match.group(2)
    msg_id = int(match.group(3))
    chat_id = int("-100" + chat) if is_private else chat
    return chat_id, msg_id

# ═══════════════════════════════════════════════
#         DOWNLOAD + UPLOAD HELPER
# ═══════════════════════════════════════════════
async def download_and_upload(msg) -> str:
    if not msg.media:
        logger.warning(f"MSG ID {msg.id} — media nahi mila, skip")
        return "no_media"
    logger.info(f"MSG ID {msg.id} — downloading...")
    file = await msg.download_media()
    logger.info(f"MSG ID {msg.id} — downloaded: {file} — uploading...")
    await userbot.send_file(TARGET_CHANNEL, file, caption=None)
    if file and os.path.exists(file):
        os.remove(file)
    logger.info(f"MSG ID {msg.id} — uploaded & local file deleted ✅")
    return "downloaded"

# ═══════════════════════════════════════════════
#         SINGLE MESSAGE PROCESS (/download)
# ═══════════════════════════════════════════════
async def process_single_message(chat_id, msg_id) -> str:
    logger.info(f"Single process — chat: {chat_id}, msg_id: {msg_id}")
    try:
        msg = await userbot.get_messages(chat_id, ids=msg_id)
        if not msg:
            return "❌ Message not found"

        chat_entity = await userbot.get_entity(chat_id)
        is_restricted = getattr(chat_entity, "noforwards", False) or msg.noforwards

        if is_restricted:
            result = await download_and_upload(msg)
            if result == "no_media":
                return "❌ Restricted but no media found"
            return "✅ Downloaded & uploaded (restricted)"

        try:
            if msg.media:
                await userbot.send_file(TARGET_CHANNEL, msg.media, caption=None)
                logger.info(f"MSG ID {msg_id} — copied via reference ✅")
            else:
                await userbot.send_message(TARGET_CHANNEL, msg.text or "")
            await asyncio.sleep(GAP_SECONDS)
            return "✅ Copied via reference"
        except ChatForwardsRestrictedError:
            logger.warning(f"MSG ID {msg_id} — fallback to download")
            result = await download_and_upload(msg)
            return "✅ Downloaded & uploaded (fallback)" if result == "downloaded" else "❌ Fallback: no media"

    except FloodWaitError as e:
        logger.warning(f"FloodWait: {e.seconds}s")
        await asyncio.sleep(e.seconds)
        return f"⏳ FloodWait {e.seconds}s, retry karo"
    except Exception as e:
        logger.error(f"MSG ID {msg_id} — error: {e}", exc_info=True)
        return f"❌ Error: {e}"

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
    daily_cnt = await db_get_daily_count()

    op_emoji = "📋 Copy" if current_op == "copy" else "📥 Download"

    await update.message.reply_text(
        f"🚫 *Bot abhi busy hai!*\n\n"
        f"📺 *Channel:* `{task.get('chat_title', 'Unknown')}`\n"
        f"🆔 *Channel ID:* `{task.get('chat_id')}`\n"
        f"📊 *Total IDs:* `{total}`\n"
        f"✅ *Done:* `{done}` | 🔲 *Remaining:* `{remaining}`\n"
        f"⚙️ *Abhi:* {op_emoji} ho raha hai ID `{current_msg_id}`\n\n"
        f"📅 *Aaj ka count:* `{daily_cnt}/{DAILY_LIMIT}`\n\n"
        f"_Naya task tab de sakte ho jab remaining ≤ {NEW_TASK_THRESHOLD} ho_",
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
    daily_cnt = await db_get_daily_count()

    task_text = ""
    if task:
        remaining = task["last_id"] - task["current_id"] + 1
        done = task["current_id"] - task["first_id"]
        total = task["last_id"] - task["first_id"] + 1
        task_text = (
            f"\n\n📌 *Active Task:*\n"
            f"📺 Channel: `{task.get('chat_title', 'Unknown')}`\n"
            f"🔲 Remaining: `{remaining}/{total}`\n"
            f"📅 Aaj done: `{daily_cnt}/{DAILY_LIMIT}`\n"
            f"/resume se continue karo"
        )

    await update.message.reply_text(
        "👋 *Telegram Media Bot*\n\n"
        "📌 *Commands:*\n"
        "/download `<link>` — Single message\n"
        "/download\\_all — Range copy/download\n"
        "/status — Current task status\n"
        "/resume — Ruka hua task resume karo\n"
        "/cancel — Conversation cancel karo\n\n"
        f"⚙️ Daily limit: `{DAILY_LIMIT}` | Gap: `{GAP_SECONDS}s`" + task_text,
        parse_mode="Markdown"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    task = await db_get_task()
    if not task:
        await update.message.reply_text("✅ Koi active task nahi hai.")
        return
    daily_cnt = await db_get_daily_count()
    remaining = task["last_id"] - task["current_id"] + 1
    done = task["current_id"] - task["first_id"]
    total = task["last_id"] - task["first_id"] + 1
    percent = (done / total * 100) if total > 0 else 0
    bar = "█" * int(percent / 5) + "░" * (20 - int(percent / 5))
    status_icon = "🟢 Running" if is_running else "⏸ Paused"
    await update.message.reply_text(
        f"📊 *Task Status*\n\n"
        f"`{bar}` {percent:.1f}%\n\n"
        f"📺 *Channel:* `{task.get('chat_title', 'Unknown')}`\n"
        f"🆔 *Channel ID:* `{task.get('chat_id')}`\n"
        f"📌 *Range:* `{task['first_id']}` → `{task['last_id']}`\n"
        f"✅ *Done:* `{done}/{total}`\n"
        f"🔲 *Remaining:* `{remaining}`\n\n"
        f"📋 Copied: `{task['copied']}` | 📥 Downloaded: `{task['downloaded']}`\n"
        f"⏭ Skipped: `{task['skipped']}` | ❌ Failed: `{task['failed']}`\n\n"
        f"📅 *Aaj done:* `{daily_cnt}/{DAILY_LIMIT}`\n"
        f"⚙️ *Status:* {status_icon}",
        parse_mode="Markdown"
    )

async def cmd_download(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    if is_running:
        await send_busy_message(update)
        return
    if not context.args:
        await update.message.reply_text("❌ Link do:\n`/download https://t.me/c/xxx/123`", parse_mode="Markdown")
        return
    link = context.args[0].strip()
    chat_id, msg_id = parse_link(link)
    if not chat_id:
        await update.message.reply_text("❌ Invalid link")
        return
    logger.info(f"/download — link: {link}")
    await update.message.reply_text("⏳ Processing...")
    result = await process_single_message(chat_id, msg_id)
    logger.info(f"/download — result: {result}")
    await update.message.reply_text(result)

async def cmd_download_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return

    # Block if a task is currently running
    if is_running:
        await send_busy_message(update)
        return

    # Block if a task exists but is not yet complete (paused due to daily limit)
    task = await db_get_task()
    if task:
        remaining = task["last_id"] - task["current_id"] + 1
        done = task["current_id"] - task["first_id"]
        total = task["last_id"] - task["first_id"] + 1
        await update.message.reply_text(
            f"⚠️ *Pichla task abhi complete nahi hua!*\n\n"
            f"📺 Channel: `{task.get('chat_title', 'Unknown')}`\n"
            f"🆔 Channel ID: `{task.get('chat_id')}`\n"
            f"📊 Total IDs: `{total}`\n"
            f"✅ Done: `{done}` | 🔲 Remaining: `{remaining}`\n\n"
            f"_Pehle pichla channel complete karo._\n"
            f"/resume se continue karo | /status se status dekho",
            parse_mode="Markdown"
        )
        return

    logger.info(f"/download_all started by {update.effective_user.id}")
    await update.message.reply_text(
        "📥 *Download All Mode*\n\nPehle message ka link bhejo:",
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
    context.user_data["dl_chat_id"] = chat_id
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

    # Get channel title
    try:
        entity = await userbot.get_entity(chat_id)
        chat_title = entity.title
    except Exception:
        chat_title = str(chat_id)

    total = last_id - first_id + 1
    logger.info(f"Range set — {chat_title}: {first_id}→{last_id}, total: {total}")

    # Save task to MongoDB
    await db_save_task({
        "chat_id":    chat_id,
        "chat_title": chat_title,
        "first_id":   first_id,
        "last_id":    last_id,
        "current_id": first_id,
        "copied":     0,
        "downloaded": 0,
        "skipped":    0,
        "failed":     0,
        "status":     "running"
    })

    daily_cnt = await db_get_daily_count()
    today_can = DAILY_LIMIT - daily_cnt

    progress_msg = await update.message.reply_text(
        f"🚀 *Download All Started*\n\n"
        f"📺 *Channel:* `{chat_title}`\n"
        f"📌 *Range:* `{first_id}` → `{last_id}`\n"
        f"📊 *Total IDs:* `{total}`\n"
        f"📅 *Aaj process hoga:* `{min(today_can, total)}/{DAILY_LIMIT}`\n\n"
        f"⏳ Shuru ho raha hai...",
        parse_mode="Markdown"
    )
    asyncio.create_task(process_range(update, progress_msg))
    return ConversationHandler.END

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    if is_running:
        await send_busy_message(update)
        return
    task = await db_get_task()
    if not task:
        await update.message.reply_text("❌ Koi saved task nahi mila.")
        return

    daily_cnt = await db_get_daily_count()
    if daily_cnt >= DAILY_LIMIT:
        await update.message.reply_text(
            f"📅 *Aaj ka limit ({DAILY_LIMIT}) poora ho gaya!*\n\n"
            f"Kal automatically resume hoga.\n"
            f"/status se progress dekh sakte ho.",
            parse_mode="Markdown"
        )
        return

    remaining = task["last_id"] - task["current_id"] + 1
    done = task["current_id"] - task["first_id"]
    total = task["last_id"] - task["first_id"] + 1
    logger.info(f"/resume — {task['chat_title']}, from ID: {task['current_id']}, done: {done}/{total}")

    progress_msg = await update.message.reply_text(
        f"▶️ *Resuming Task*\n\n"
        f"📺 *Channel:* `{task['chat_title']}`\n"
        f"📌 *Range:* `{task['first_id']}` → `{task['last_id']}`\n"
        f"⏩ *Resume from:* `{task['current_id']}`\n"
        f"✅ *Already done:* `{done}/{total}`\n"
        f"🔲 *Remaining:* `{remaining}`\n\n"
        f"⏳ Processing...",
        parse_mode="Markdown"
    )
    asyncio.create_task(process_range(update, progress_msg))

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    logger.info(f"/cancel by {update.effective_user.id}")
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END

# ═══════════════════════════════════════════════
#              RANGE PROCESSOR (core)
# ═══════════════════════════════════════════════
async def process_range(update: Update, progress_msg):
    global is_running, current_op, current_msg_id

    task = await db_get_task()
    if not task:
        logger.error("process_range: no task found in DB")
        return

    is_running = True
    chat_id    = task["chat_id"]
    first_id   = task["first_id"]
    last_id    = task["last_id"]
    start_from = task["current_id"]
    copied     = task["copied"]
    downloaded = task["downloaded"]
    skipped    = task["skipped"]
    failed     = task["failed"]
    total      = last_id - first_id + 1
    start_time = datetime.now()

    logger.info(f"process_range — {task['chat_title']}: {start_from}→{last_id}")

    def build_progress_text(cur_id, daily_cnt):
        done = cur_id - first_id
        percent = (done / total * 100) if total > 0 else 0
        bar = "█" * int(percent / 5) + "░" * (20 - int(percent / 5))
        elapsed = max((datetime.now() - start_time).seconds, 1)
        eta = int((elapsed / done) * (total - done)) if done > 0 else 0
        eta_str = f"{eta // 60}m {eta % 60}s" if eta > 0 else "calculating..."
        return (
            f"📊 *Progress — {task['chat_title']}*\n\n"
            f"`{bar}` {percent:.1f}%\n\n"
            f"✅ Done: `{done}/{total}`\n"
            f"📌 Current ID: `{cur_id}`\n"
            f"⏱ ETA: `{eta_str}`\n\n"
            f"📋 Copied: `{copied}` | 📥 Downloaded: `{downloaded}`\n"
            f"⏭ Skipped: `{skipped}` | ❌ Failed: `{failed}`\n\n"
            f"📅 Aaj: `{daily_cnt}/{DAILY_LIMIT}`"
        )

    for msg_id in range(start_from, last_id + 1):
        current_msg_id = msg_id

        # ── Daily limit check ──
        daily_cnt = await db_get_daily_count()
        if daily_cnt >= DAILY_LIMIT:
            is_running = False
            current_op = None
            logger.info(f"Daily limit {DAILY_LIMIT} reached at ID {msg_id}, pausing")
            await db_save_task({
                "current_id": msg_id,
                "copied": copied, "downloaded": downloaded,
                "skipped": skipped, "failed": failed,
                "status": "paused"
            })
            await progress_msg.edit_text(
                f"📅 *Aaj ka limit ({DAILY_LIMIT}) poora!*\n\n"
                f"⏸ Task paused at ID: `{msg_id}`\n"
                f"📺 Channel: `{task['chat_title']}`\n"
                f"🔲 Remaining: `{last_id - msg_id + 1}` IDs\n\n"
                f"📋 Copied: `{copied}` | 📥 Downloaded: `{downloaded}`\n\n"
                f"_Kal /resume bhejo ya bot restart karne par auto-resume hoga_",
                parse_mode="Markdown"
            )
            return

        # ── Save progress ──
        await db_save_task({
            "current_id": msg_id,
            "copied": copied, "downloaded": downloaded,
            "skipped": skipped, "failed": failed,
            "status": "running"
        })

        try:
            msg = await userbot.get_messages(chat_id, ids=msg_id)

            if not msg or not msg.media:
                skipped += 1
                logger.debug(f"MSG ID {msg_id} — skipped (no media)")
            else:
                chat_entity = await userbot.get_entity(chat_id)
                is_restricted = getattr(chat_entity, "noforwards", False) or msg.noforwards

                if is_restricted:
                    current_op = "download"
                    logger.info(f"MSG ID {msg_id} — restricted, downloading...")
                    result = await download_and_upload(msg)
                    await db_increment_daily()
                    if result == "downloaded":
                        downloaded += 1
                    else:
                        skipped += 1
                else:
                    try:
                        current_op = "copy"
                        await userbot.send_file(TARGET_CHANNEL, msg.media, caption=None)
                        await db_increment_daily()
                        copied += 1
                        logger.info(f"MSG ID {msg_id} — copied via reference ✅")
                        await asyncio.sleep(GAP_SECONDS)
                    except ChatForwardsRestrictedError:
                        current_op = "download"
                        logger.warning(f"MSG ID {msg_id} — fallback download")
                        result = await download_and_upload(msg)
                        await db_increment_daily()
                        if result == "downloaded":
                            downloaded += 1
                        else:
                            skipped += 1

        except FloodWaitError as e:
            logger.warning(f"FloodWait at MSG ID {msg_id} — {e.seconds}s")
            await update.message.reply_text(
                f"⏳ FloodWait: `{e.seconds}s` wait...", parse_mode="Markdown"
            )
            await asyncio.sleep(e.seconds)
            # retry same ID
            msg_id -= 1
            continue

        except Exception as e:
            failed += 1
            logger.error(f"MSG ID {msg_id} — error: {e}", exc_info=True)

        # ── Update progress message every 10 IDs ──
        done_so_far = msg_id - first_id + 1
        if done_so_far % 10 == 0:
            daily_cnt = await db_get_daily_count()
            try:
                await progress_msg.edit_text(
                    build_progress_text(msg_id + 1, daily_cnt),
                    parse_mode="Markdown"
                )
            except Exception:
                pass

    # ── All done ──
    elapsed = (datetime.now() - start_time).seconds
    elapsed_str = f"{elapsed // 60}m {elapsed % 60}s"
    await db_clear_task()
    is_running = False
    current_op = None
    current_msg_id = None

    logger.info(
        f"process_range COMPLETE — copied: {copied}, downloaded: {downloaded}, "
        f"skipped: {skipped}, failed: {failed}, time: {elapsed_str}"
    )

    await progress_msg.edit_text(
        f"🎉 *Task Complete!*\n\n"
        f"📺 *Channel:* `{task['chat_title']}`\n"
        f"📊 *Total checked:* `{total}`\n\n"
        f"📋 Copied: `{copied}`\n"
        f"📥 Downloaded: `{downloaded}`\n"
        f"⏭ Skipped: `{skipped}`\n"
        f"❌ Failed: `{failed}`\n"
        f"⏱ Time: `{elapsed_str}`",
        parse_mode="Markdown"
    )

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

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",   start))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("download", cmd_download))
    app.add_handler(CommandHandler("resume",  cmd_resume))

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("download_all", cmd_download_all)],
        states={
            WAIT_FIRST_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_first_link)],
            WAIT_LAST_LINK:  [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_last_link)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(conv_handler)

    logger.info("✅ Bot polling started")
    app.run_polling()

if __name__ == "__main__":
    main()
