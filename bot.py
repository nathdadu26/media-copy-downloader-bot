import os
import re
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import aiohttp
from aiohttp import web
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChatForwardsRestrictedError
from telegram import Update
from telegram.error import Conflict, NetworkError, TimedOut
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

# Apne aap ko ping karne ke liye apna public health-check URL (Koyeb/Railway
# waala public URL, jaise "https://your-app.koyeb.app/health"). Agar set
# nahi kiya to self-ping automatically disable rahega.
SELF_PING_URL      = os.getenv("SELF_PING_URL", "").strip()
SELF_PING_INTERVAL = int(os.getenv("SELF_PING_INTERVAL", 20 * 60))  # seconds, default 20 min

# ═══════════════════════════════════════════════
#                  CONSTANTS
# ═══════════════════════════════════════════════
GAP_SECONDS = 10
LINK_REGEX  = r"https://t.me/(c/)?([\w\d_]+)/(\d+)"

# Sirf itne se bade files copy honge (MB mein). Railway variable se override
# kar sakte ho (MIN_FILE_SIZE_MB), warna default 10 MB rahega.
MIN_FILE_SIZE_MB    = float(os.getenv("MIN_FILE_SIZE_MB", 10))
MIN_FILE_SIZE_BYTES = int(MIN_FILE_SIZE_MB * 1024 * 1024)

# ═══════════════════════════════════════════════
#              CONVERSATION STATES
# ═══════════════════════════════════════════════
WAIT_FIRST_LINK, WAIT_LAST_LINK = range(2)

# ═══════════════════════════════════════════════
#              GLOBAL STATE
# ═══════════════════════════════════════════════
is_running     = False
current_msg_id = None
copy_task      = None   # holds the asyncio.Task running process_range
ping_task      = None   # holds the asyncio.Task running self_ping_loop
manual_cancel  = False  # True sirf jab /cancel command se cancel hua ho

# ═══════════════════════════════════════════════
#         FILE SIZE FILTER
# ═══════════════════════════════════════════════
def is_size_ok(msg) -> bool:
    """
    True  → Koi bhi media (image, video, document, zip, gif, audio...)
            jiska size MIN_FILE_SIZE_MB se bada ho → Process karo
    False → Media nahi hai, ya size chota hai        → Skip karo

    Type ki koi restriction nahi — sirf size check hota hai.
    """
    if not msg.media:
        return False

    try:
        size = msg.file.size if msg.file else None
    except Exception:
        size = None

    if not size:
        return False

    return size > MIN_FILE_SIZE_BYTES

# ═══════════════════════════════════════════════
#         MARKDOWN ESCAPE HELPER
# ═══════════════════════════════════════════════
def escape_md(text) -> str:
    """
    Legacy Markdown (parse_mode='Markdown') sirf kuch characters ko
    special maanta hai: _ * ` [
    Agar chat_title ya koi bhi dynamic text (jo Telegram se aata hai)
    inme se koi character rakhta hai to parser crash ho jata hai
    ("Can't parse entities"). Isliye bhejne se pehle unhe escape karo.
    """
    if not text:
        return text
    text = str(text)
    for ch in ("\\", "_", "*", "`", "["):
        text = text.replace(ch, "\\" + ch)
    return text

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

async def notify(bot, chat_id, text, retries=3, parse_mode="Markdown"):
    """
    safe_send jaisa hi hai, lekin kisi command 'update' object ke bina
    kaam karta hai — sirf bot + chat_id chahiye. Startup par auto-resume
    jaise cases mein use hota hai jahan koi live update maujood nahi hota.
    """
    for attempt in range(retries):
        try:
            return await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        except Exception as e:
            logger.warning(f"notify attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
    logger.error("notify: all retries failed, giving up")
    return None

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
#         AUTO SELF-PING (instance ko sleep se bachane ke liye)
# ═══════════════════════════════════════════════
async def self_ping_loop():
    """
    Har SELF_PING_INTERVAL seconds (default 20 min) mein apne khud ke
    health-check URL ko HTTP GET request bhejta hai. Kai hosting platforms
    (Koyeb/Render/Railway free tier) service ko "idle" samajh kar sula
    dete hain agar koi real external traffic na aaye — ye periodic ping
    us traffic ka kaam karta hai aur instance ko jaga rakhta hai.

    SELF_PING_URL env variable mein apna public health-check URL daalo,
    jaise: https://your-app-name.koyeb.app/health
    Agar ye set nahi hai to self-ping automatically skip ho jayega.
    """
    if not SELF_PING_URL:
        logger.info("ℹ️ SELF_PING_URL set nahi hai — self-ping disabled.")
        return

    logger.info(f"🔁 Self-ping enabled — har {SELF_PING_INTERVAL}s mein {SELF_PING_URL} ping hoga")
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(SELF_PING_INTERVAL)
            try:
                async with session.get(
                    SELF_PING_URL, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    logger.info(f"🔁 Self-ping OK — status {resp.status}")
            except Exception as e:
                logger.warning(f"🔁 Self-ping failed: {e}")

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
        f"📺 *Channel:* `{escape_md(task.get('chat_title', 'Unknown'))}`\n"
        f"🆔 *Channel ID:* `{task.get('chat_id')}`\n"
        f"📊 *Total IDs:* `{total}`\n"
        f"✅ *Done:* `{done}` | 🔲 *Remaining:* `{remaining}`\n"
        f"⚙️ *Abhi:* ID `{current_msg_id}` copy ho raha hai\n\n"
        f"🛑 Rokne ke liye /cancel bhejo",
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
            f"📺 Channel: `{escape_md(task.get('chat_title', 'Unknown'))}`\n"
            f"🔲 Remaining: `{remaining}/{total}`\n"
            f"✅ Done: `{done}/{total}`"
        )
    await update.message.reply_text(
        "👋 *Telegram Media Bot*\n\n"
        "📌 *Commands:*\n"
        "/copy\\_all — Range copy karo\n"
        "/status — Current task status\n"
        "/cancel — Chal raha task ya conversation cancel karo\n\n"
        f"⚙️ Gap: `{GAP_SECONDS}s` | Min size: `{MIN_FILE_SIZE_MB}MB`" + task_text,
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
        f"📺 *Channel:* `{escape_md(task.get('chat_title', 'Unknown'))}`\n"
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
            f"📺 Channel: `{escape_md(task.get('chat_title', 'Unknown'))}`\n"
            f"🆔 Channel ID: `{task.get('chat_id')}`\n"
            f"📊 Total IDs: `{total}`\n"
            f"✅ Done: `{done}` | 🔲 Remaining: `{remaining}`\n\n"
            f"_Pehle pichla channel complete karo ya /cancel bhejo._\n"
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
    global copy_task
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
        f"📺 *Channel:* `{escape_md(chat_title)}`\n"
        f"📌 *Range:* `{first_id}` → `{last_id}`\n"
        f"📊 *Total IDs:* `{total}`\n"
        f"📦 *Min Size:* `{MIN_FILE_SIZE_MB}MB`\n\n"
        f"⏳ Shuru ho raha hai...\n"
        f"🛑 Rokne ke liye /cancel bhejo",
        parse_mode="Markdown"
    )
    copy_task = asyncio.create_task(process_range(progress_msg))
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global copy_task, is_running, current_msg_id, manual_cancel
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    logger.info(f"/cancel by {update.effective_user.id}")

    # Case 1: Background copy task memory mein actively chal raha hai
    if copy_task and not copy_task.done():
        manual_cancel = True
        copy_task.cancel()
        try:
            await copy_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"cancel: error while awaiting cancelled task: {e}")

        is_running     = False
        current_msg_id = None
        copy_task      = None
        await db_clear_task()
        await update.message.reply_text(
            "🛑 *Copy process cancel kar diya gaya.*\nTask clear ho gaya, ab naya /copy\\_all shuru kar sakte ho.",
            parse_mode="Markdown"
        )
        context.user_data.clear()
        return ConversationHandler.END

    # Case 2: Memory mein koi task nahi (bot restart/redeploy hua tha),
    # lekin MongoDB mein purana task abhi bhi pada hai → use bhi clear karo
    stale_task = await db_get_task()
    if stale_task:
        await db_clear_task()
        is_running     = False
        current_msg_id = None
        copy_task      = None
        await update.message.reply_text(
            f"🛑 *Purana/stale task DB se clear kar diya gaya.*\n\n"
            f"📺 Channel: `{escape_md(stale_task.get('chat_title', 'Unknown'))}`\n"
            f"✅ Ab naya /copy\\_all shuru kar sakte ho.",
            parse_mode="Markdown"
        )
        context.user_data.clear()
        return ConversationHandler.END

    # Case 3: Koi task hi nahi hai, sirf link-input conversation cancel ho rahi hai
    await update.message.reply_text("❌ Cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════
#         ALBUM / GROUPED CAPTION RESOLVER
# ═══════════════════════════════════════════════
async def resolve_caption(chat_id, msg):
    """
    Telegram albums (grouped media) mein sirf ek hi message
    (usually pehla) caption rakhta hai — baaki sab ka msg.text
    empty hota hai. Agar current msg ka apna text/caption nahi hai
    lekin ye kisi group ka hissa hai, to group ke andar dhoondh kar
    uska original caption nikaalo.
    """
    if msg.text:
        return msg.text

    if not getattr(msg, "grouped_id", None):
        return msg.text or ""

    # Album ke andar caption dhoondhne ke liye aas paas ke IDs check karo
    lo = max(msg.id - 9, 1)
    hi = msg.id + 9
    try:
        nearby = await userbot.get_messages(chat_id, ids=list(range(lo, hi + 1)))
    except Exception as e:
        logger.warning(f"resolve_caption: nearby fetch failed for MSG ID {msg.id}: {e}")
        return msg.text or ""

    for m in nearby:
        if m and getattr(m, "grouped_id", None) == msg.grouped_id and m.text:
            return m.text

    return msg.text or ""

# ═══════════════════════════════════════════════
#              RANGE PROCESSOR
# ═══════════════════════════════════════════════
async def process_range(progress_msg):
    global is_running, current_msg_id, manual_cancel

    bot = progress_msg.get_bot()
    notify_chat_id = progress_msg.chat_id

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
            f"📊 *Progress — {escape_md(task['chat_title'])}*\n\n"
            f"`{bar}` {percent:.1f}%\n\n"
            f"✅ Done: `{done}/{total}`\n"
            f"📌 Current ID: `{cur_id}`\n"
            f"⏱ ETA: `{eta_str}`\n\n"
            f"📋 Copied: `{copied}` | ⏭ Skipped: `{skipped}` | ❌ Failed: `{failed}`\n\n"
            f"🛑 Rokne ke liye /cancel bhejo"
        )

    try:
        for msg_id in range(start_from, last_id + 1):
            current_msg_id = msg_id

            await db_save_task({
                "current_id": msg_id,
                "copied": copied, "skipped": skipped, "failed": failed,
                "status": "running"
            })

            try:
                msg = await userbot.get_messages(chat_id, ids=msg_id)

                # ── File size filter ──────────────────────────
                if not msg or not is_size_ok(msg):
                    skipped += 1
                    logger.debug(
                        f"MSG ID {msg_id} — skipped "
                        f"(no media ya size <= {MIN_FILE_SIZE_MB}MB)"
                    )
                # ── Restricted channel check ───────────────────
                else:
                    chat_entity   = await userbot.get_entity(chat_id)
                    is_restricted = getattr(chat_entity, "noforwards", False) or msg.noforwards

                    if is_restricted:
                        skipped += 1
                        logger.info(f"MSG ID {msg_id} — skipped (restricted channel)")
                    else:
                        try:
                            # ✅ Original caption jaisi hai waisi hi bhejo (album ka bhi handle hota hai)
                            caption = await resolve_caption(chat_id, msg)
                            await userbot.send_file(TARGET_CHANNEL, msg.media, caption=caption)
                            copied += 1
                            logger.info(f"MSG ID {msg_id} — copied (original caption) ✅")
                            await asyncio.sleep(GAP_SECONDS)
                        except ChatForwardsRestrictedError:
                            skipped += 1
                            logger.warning(f"MSG ID {msg_id} — skipped (forwards restricted)")

            except FloodWaitError as e:
                logger.warning(f"FloodWait at MSG ID {msg_id} — {e.seconds}s")
                await notify(bot, notify_chat_id, f"⏳ FloodWait: `{e.seconds}s` wait ho raha hai...")
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
        logger.info(
            f"process_range COMPLETE — copied: {copied}, "
            f"skipped: {skipped}, failed: {failed}, time: {elapsed_str}"
        )
        await safe_edit(progress_msg,
            f"🎉 *Task Complete!*\n\n"
            f"📺 *Channel:* `{escape_md(task['chat_title'])}`\n"
            f"📊 *Total checked:* `{total}`\n\n"
            f"📋 Copied: `{copied}`\n"
            f"⏭ Skipped: `{skipped}`\n"
            f"❌ Failed: `{failed}`\n"
            f"⏱ Time: `{elapsed_str}`"
        )

    except asyncio.CancelledError:
        if manual_cancel:
            # /cancel command se yahan pahuchte hain — DB ko cancel() function khud clear karega
            logger.info(f"process_range CANCELLED by user at MSG ID {current_msg_id}")
            try:
                await safe_edit(progress_msg,
                    f"🛑 *Task Cancelled (by user)*\n\n"
                    f"📺 *Channel:* `{escape_md(task['chat_title'])}`\n"
                    f"📌 *Ruka hua ID:* `{current_msg_id}`\n\n"
                    f"📋 Copied: `{copied}` | ⏭ Skipped: `{skipped}` | ❌ Failed: `{failed}`"
                )
            except Exception:
                pass
            manual_cancel = False
        else:
            # Bot restart/redeploy/shutdown ki wajah se hua — task DB mein
            # save hai, agla start hote hi khud-b-khud resume ho jayega
            logger.info(
                f"process_range interrupted (shutdown/redeploy) at MSG ID "
                f"{current_msg_id} — DB mein safe hai, auto-resume hoga"
            )
            try:
                await safe_edit(progress_msg,
                    f"🔄 *Bot restart ho raha hai — task pause hua*\n\n"
                    f"📺 *Channel:* `{escape_md(task['chat_title'])}`\n"
                    f"📌 *Ruka hua ID:* `{current_msg_id}`\n\n"
                    f"📋 Copied: `{copied}` | ⏭ Skipped: `{skipped}` | ❌ Failed: `{failed}`\n\n"
                    f"✅ Restart complete hote hi khud resume ho jayega."
                )
            except Exception:
                pass
        raise

    finally:
        is_running = False

# ═══════════════════════════════════════════════
#         DUPLICATE INSTANCE TAKEOVER
# ═══════════════════════════════════════════════
async def force_takeover_polling():
    """
    Telegram Bot API mein ek waqt par sirf EK hi getUpdates (long-polling)
    session allowed hota hai. Agar koi purana bot instance (jaise pichla
    deployment jo abhi tak fully band nahi hua) already polling kar raha
    hai, to hum khud ek raw getUpdates call bhej kar uska session turant
    "kick" kar dete hain — Telegram hamesha sabse latest call ko priority
    deta hai, isliye purana instance turant Conflict error khaake ruk
    jaata hai aur humari polling free ho jaati hai.

    Note: Agar 2 instances GENUINELY hamesha saath chal rahe hain (jaise
    galti se bot do jagah deploy ho gaya), to ye sirf temporary fix hai —
    dono baar-baar ek dusre ko kick karte rahenge. Asli fix wahan sirf
    ek hi jagah deploy rakhna hai.
    """
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json={"offset": -1, "timeout": 0},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                logger.info(f"🔧 Takeover request bheja gaya — ok={data.get('ok')}")
    except Exception as e:
        logger.warning(f"🔧 Takeover request fail hua: {e}")
    # Purane instance ko apna conflict handle karne ke liye thoda time do
    await asyncio.sleep(2)

# ═══════════════════════════════════════════════
#              GLOBAL ERROR HANDLER
# ═══════════════════════════════════════════════
async def error_handler(update, context):
    err = context.error
    logger.error(f"Global error: {err}", exc_info=context.error)

    if isinstance(err, Conflict):
        logger.warning(
            "⚠️ Conflict: Dusra bot instance (purana deployment) abhi bhi "
            "polling kar raha hai. Takeover ki koshish kar rahe hain..."
        )
        asyncio.create_task(force_takeover_polling())
        return

    if isinstance(err, (NetworkError, TimedOut)):
        logger.warning(f"Transient network error (ignored): {err}")
        return
    try:
        if update and update.message:
            await safe_send(update, f"⚠️ Unexpected error: {escape_md(err)}")
    except Exception:
        pass

# ═══════════════════════════════════════════════
#         AUTO-RESUME (bot restart/sleep ke baad)
# ═══════════════════════════════════════════════
async def post_init(app):
    """
    Application start hone ke turant baad chalta hai (same event loop mein).
    Agar restart/sleep/redeploy se pehle koi task adhoora reh gaya tha
    (DB mein abhi bhi maujood hai), to use khud-b-khud resume karo —
    owner ko dobara /copy_all bharne ki zaroorat nahi.
    """
    global copy_task, is_running, ping_task

    # Self-ping loop shuru karo (agar SELF_PING_URL set hai)
    ping_task = asyncio.create_task(self_ping_loop())

    task = await db_get_task()
    if not task:
        return
    logger.info(
        f"Auto-resume: pending task mila — {task.get('chat_title')} "
        f"@ ID {task.get('current_id')} → {task.get('last_id')}"
    )
    progress_msg = await notify(
        app.bot, OWNER_ID,
        f"🔄 *Bot restart hua hai — task auto-resume ho raha hai*\n\n"
        f"📺 *Channel:* `{escape_md(task.get('chat_title', 'Unknown'))}`\n"
        f"📌 *Ruka hua ID:* `{task.get('current_id')}` → `{task.get('last_id')}`\n\n"
        f"🛑 Rokne ke liye /cancel bhejo"
    )
    if progress_msg:
        copy_task = asyncio.create_task(process_range(progress_msg))

async def post_shutdown(app):
    """
    Application band hone se pehle chalta hai (loop close hone se pehle).
    Chal rahe copy_task ko cleanly cancel karo aur userbot ko disconnect
    karo — isse restart/sleep ke waqt 'Task was destroyed' jaisi
    tracebacks nahi aati aur agli baar reconnect bhi saaf hota hai.
    """
    global copy_task, ping_task
    if copy_task and not copy_task.done():
        copy_task.cancel()
        try:
            await copy_task
        except Exception:
            pass
    if ping_task and not ping_task.done():
        ping_task.cancel()
        try:
            await ping_task
        except Exception:
            pass
    try:
        await userbot.disconnect()
    except Exception:
        pass
    logger.info("Graceful shutdown complete.")

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

    # Agar koi purana bot instance (pichla deployment) abhi bhi polling
    # kar raha hai, to use pehle "kick" kar do taaki naya instance
    # bina Conflict error ke turant polling shuru kar sake.
    logger.info("🔧 Purana instance (agar chal raha ho) takeover kar rahe hain...")
    loop.run_until_complete(force_takeover_polling())

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("status", cmd_status))

    # /cancel ko sabse pehle add karo (group 0) taaki ye hamesha
    # sabse pehle check ho, chahe conversation active ho ya na ho.
    app.add_handler(CommandHandler("cancel", cancel))

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
    app.add_error_handler(error_handler)

    logger.info("✅ Bot polling started")
    app.run_polling()

if __name__ == "__main__":
    main()
