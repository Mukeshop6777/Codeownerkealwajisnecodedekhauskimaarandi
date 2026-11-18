#!/usr/bin/env python3
# app.py — NOVA · Sites + Proxies focused build
#
#   • /validatesites   upload .txt of URLs → tested with 3 CCs → clean sites added to global pool
#   • /listsites       view all global sites
#   • /rmsite <id>     remove a site by id
#   • /cleansites      re-validate global pool, drop dead ones
#   • /addpro          add one proxy inline
#   • /uploadproxies   upload .txt of proxies → tested → added to pool
#   • /clearproxies    wipe your personal pools
#   • /cleanshpro      test your Shopify pool, keep only live
#   • /msh             Shopify mass check (uses global sites + your proxies)
#   • /addmysite       add a personal Shopify site
#   • /mysites         list your personal sites
#   • /start           menu

import os, re, sys, json, time, html, random, threading, traceback, queue, socket, ssl, uuid, functools, hashlib
import requests, telebot, urllib3
from telebot import types
from telebot.apihelper import ApiTelegramException
from datetime import datetime, timedelta
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from pymongo.server_api import ServerApi

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
socket.setdefaulttimeout(30)

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
# CORE IMPORTS FROM EXISTING MODULES
# ═══════════════════════════════════════════════════════════════════
from gates import check_shopify_api, process_shopify_api_response
from complete_handler import (
    setup_complete_handler,
    get_bin_info,
    get_active_proxies,
    add_proxy_to_user,
    add_global_proxy,
    get_user_proxies,
    save_user_proxies,
    test_proxy_for_shopify,
    test_proxy_for_razorpay,
    normalize_proxy,
    _quick_alive,
    process_shopify_mass_check,
)

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════
BOT_TOKEN    = "8663538819:AAGO37R2Nua2K_T1KZzBLyeYvEgfs_CwB6Q"
OWNER_ID     = [5963548505, 5547897619]
DARKS_ID     = 5963548505
FREE_GROUP_ID = -1003926435438
REQUIRED_CHATS = ["@Nova_bot_update", "-1003926435438"]

SITES_FILE         = "sites.json"
PROXIES_FILE       = "proxies.json"
USERS_FILE         = "users.json"
GROUPS_FILE        = "groups.json"
USER_PROXIES_FILE  = "user_proxies.json"
USER_SITES_FILE    = "user_sites.json"
STATS_FILE         = "stats.json"
SETTINGS_FILE      = "settings.json"

# 3 CCs used to validate each Shopify site
SITE_VALIDATION_CCS = [
    "5242430428405662|03|28|323",
    "4517650667292635|08|2028|261",
    "4242424242424242|12|2028|123",
]

bot = telebot.TeleBot(BOT_TOKEN, num_threads=30)

# ═══════════════════════════════════════════════════════════════════
# MONGO + JSON HELPERS
# ═══════════════════════════════════════════════════════════════════
uri = "mongodb+srv://rahll_db_user:k0RRXUHqnXMgsCJx@cluster0.aom0bxs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
try:
    _mc = MongoClient(uri, server_api=ServerApi('1'), maxPoolSize=10,
                      connectTimeoutMS=30000, socketTimeoutMS=120000,
                      serverSelectionTimeoutMS=30000)
    _mc.admin.command('ping')
    db = _mc['nova_bot_db']
    logger.info("✅ MongoDB connected")
except Exception as e:
    logger.warning(f"❌ MongoDB failed: {e}")
    db = None


def _coll(file_path):
    return file_path.replace('.json', '').replace('data/', '').replace('/', '_')


def load_json(file_path, default):
    if db is not None:
        try:
            doc = db[_coll(file_path)].find_one({"_id": "main_data"})
            if doc and 'data' in doc:
                data = doc['data']
                if file_path == SITES_FILE:
                    if isinstance(data, list):
                        data = {"sites": data}
                    data.setdefault('sites', [])
                elif file_path == PROXIES_FILE:
                    if isinstance(data, list):
                        data = {"shopify": data, "razorpay": []}
                    data.setdefault('shopify', [])
                    data.setdefault('razorpay', [])
                return data
        except Exception as e:
            logger.warning(f"Mongo load {file_path}: {e}")
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if file_path == SITES_FILE:
                if isinstance(data, list):
                    data = {"sites": data}
                data.setdefault('sites', [])
            elif file_path == PROXIES_FILE:
                if isinstance(data, list):
                    data = {"shopify": data, "razorpay": []}
                data.setdefault('shopify', [])
                data.setdefault('razorpay', [])
            return data
    except Exception:
        pass
    return default


def save_json(file_path, data):
    if db is not None:
        try:
            db[_coll(file_path)].update_one(
                {"_id": "main_data"},
                {"$set": {"data": data, "updated_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as e:
            logger.warning(f"Mongo save {file_path}: {e}")
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Local save {file_path}: {e}")


# ═══════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════
sites_data        = load_json(SITES_FILE, {"sites": []})
proxies_data      = load_json(PROXIES_FILE, {"shopify": [], "razorpay": []})
users_data        = load_json(USERS_FILE, {})
groups_data       = load_json(GROUPS_FILE, {})
user_proxies_data = load_json(USER_PROXIES_FILE, {})
user_sites_data   = load_json(USER_SITES_FILE, {})
stats_data        = load_json(STATS_FILE, {})
settings_data     = load_json(SETTINGS_FILE, {"price_filter": None})

# migrate + assign ids
_changed_sites = False
_next_sid = 1
for s in sites_data.get('sites', []):
    if 'id' not in s:
        s['id'] = _next_sid
        _changed_sites = True
    if s['id'] >= _next_sid:
        _next_sid = s['id'] + 1
if _changed_sites:
    save_json(SITES_FILE, sites_data)

_changed_up = False
for uid, entry in user_proxies_data.items():
    if isinstance(entry, list):
        user_proxies_data[uid] = {"shopify": entry, "razorpay": []}
        _changed_up = True
    else:
        entry.setdefault('shopify', [])
        entry.setdefault('razorpay', [])
if _changed_up:
    save_json(USER_PROXIES_FILE, user_proxies_data)


# ═══════════════════════════════════════════════════════════════════
# RATE LIMITER + SAFE SEND
# ═══════════════════════════════════════════════════════════════════
class TelegramRateLimiter:
    def __init__(self, global_interval=0.05, private_interval=1.05, group_interval=3.0):
        self.q = queue.Queue()
        self.g = global_interval
        self.p = private_interval
        self.gr = group_interval
        self._last_g = 0.0
        self._last_c = {}
        self._lock = threading.Lock()
        threading.Thread(target=self._worker, daemon=True).start()

    def _chat(self, fn_name, args, kwargs):
        cid = kwargs.get('chat_id')
        ct = kwargs.get('chat_type', 'private')
        if cid is None and args:
            a0 = args[0]
            if hasattr(a0, 'chat'):
                cid = getattr(a0.chat, 'id', None)
                ct = getattr(a0.chat, 'type', ct)
            elif hasattr(a0, 'message') and hasattr(a0.message, 'chat'):
                cid = getattr(a0.message.chat, 'id', None)
                ct = getattr(a0.message.chat, 'type', ct)
            elif isinstance(a0, int):
                cid = a0
        if cid is None and fn_name == 'edit_message_text' and len(args) >= 2 and isinstance(args[1], int):
            cid = args[1]
        return cid, ct

    def _wait(self, cid, ct):
        per = self.gr if ct in ('group', 'supergroup') else self.p
        while True:
            with self._lock:
                now = time.time()
                gw = self.g - (now - self._last_g)
                cw = per - (now - self._last_c.get(cid, 0)) if cid is not None else 0
                w = max(gw, cw, 0)
            if w <= 0:
                with self._lock:
                    now = time.time()
                    self._last_g = now
                    if cid is not None:
                        self._last_c[cid] = now
                return
            time.sleep(min(w, 0.25))

    def _worker(self):
        while True:
            try:
                item = self.q.get(timeout=1.0)
            except queue.Empty:
                continue
            fn, args, kwargs, fut = item
            name = getattr(fn, '__name__', '')
            cid, ct = self._chat(name, args, kwargs)
            self._wait(cid, ct)
            res = None
            err = None
            for attempt in range(5):
                try:
                    res = fn(*args, **kwargs)
                    err = None
                    break
                except ApiTelegramException as e:
                    err = e
                    if e.error_code == 429:
                        ra = (e.result_json or {}).get('parameters', {}).get('retry_after', 3)
                        time.sleep(min(ra, 10))
                    else:
                        break
                except Exception as e:
                    err = e
                    break
            if fut is not None:
                fut['r'] = res
                fut['e'] = err
                fut['ev'].set()
            try:
                self.q.task_done()
            except Exception:
                pass

    def call(self, fn, *args, **kwargs):
        fut = {'ev': threading.Event(), 'r': None, 'e': None}
        self.q.put((fn, args, kwargs, fut))
        fut['ev'].wait(timeout=120)
        return fut['r']


_limiter = TelegramRateLimiter()

def safe_send(fn, *args, **kwargs):
    return _limiter.call(fn, *args, **kwargs)


# ═══════════════════════════════════════════════════════════════════
# SAFE HANDLER WRAPPER
# ═══════════════════════════════════════════════════════════════════
def _safe(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.error(f"Handler {func.__name__} crashed: {traceback.format_exc()}")
            try:
                if args and hasattr(args[0], 'id') and hasattr(args[0], 'message'):
                    bot.answer_callback_query(args[0].id, text="Error, retry.")
            except Exception:
                pass
            return None
    return wrapper


_orig_mh = bot.message_handler
_orig_cb = bot.callback_query_handler

def _wrap_dec(dec):
    def wrapper(*a, **kw):
        def deco(fn):
            return dec(*a, **kw)(_safe(fn))
        return deco
    return wrapper

bot.message_handler = _wrap_dec(_orig_mh)
bot.callback_query_handler = _wrap_dec(_orig_cb)


# ═══════════════════════════════════════════════════════════════════
# AUTH + FORCE SUB
# ═══════════════════════════════════════════════════════════════════
def is_owner(uid):
    return uid in OWNER_ID

def is_user_allowed(uid):
    if uid in OWNER_ID:
        return True
    u = users_data.get(str(uid))
    if not u:
        return False
    exp = u.get('expiry') or u.get('expiry_date')
    if not exp:
        return False
    try:
        return datetime.fromisoformat(exp) > datetime.now()
    except Exception:
        return False


def is_subscribed(uid):
    if not REQUIRED_CHATS:
        return True
    for chat_id in REQUIRED_CHATS:
        try:
            m = bot.get_chat_member(chat_id, uid)
            if m.status not in ('creator', 'administrator', 'member'):
                return False
        except Exception:
            return False
    return True


def _sub_gate(message):
    """Returns True if allowed, sends gate message if not."""
    uid = message.from_user.id
    if uid in OWNER_ID:
        return True
    if is_subscribed(uid):
        return True
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📢 Join Channel", url="https://t.me/Nova_bot_update"))
    kb.add(types.InlineKeyboardButton("👥 Join Group", url="https://t.me/+NxKPx890ExkwYjQ0"))
    kb.add(types.InlineKeyboardButton("🔄 I've Joined Both", callback_data="check_subscription"))
    safe_send(bot.reply_to, message,
        "<b>🔒 ACCESS LOCKED</b>\n\n"
        "Join both our channel and group to unlock the bot.\n\n"
        "📢 <b>Channel</b> · @Nova_bot_update\n"
        "👥 <b>Group</b> · <a href='https://t.me/+NxKPx890ExkwYjQ0'>tap to join</a>",
        parse_mode='HTML', reply_markup=kb)
    return False


# ═══════════════════════════════════════════════════════════════════
# SITE HELPERS
# ═══════════════════════════════════════════════════════════════════
_SITE_REJECT = (
    'CAPTCHA', '429', 'RATE LIMIT', 'RATE_LIMITED', 'TOO MANY REQUESTS',
    '503', '502', 'CONNECTION REFUSED', 'TIMEOUT', 'CONNECT TUNNEL FAILED',
    'PROXY ERROR', 'UNREACHABLE', 'API ERROR', 'PRODUCT FETCH FAILED',
    'NO PRODUCT', 'SITE NOT FOUND', 'NOT A SHOPIFY', 'SYSTEM ERROR',
    'CART PERMALINK', 'SHOP NOT FOUND', 'INVALID SHOP', 'MISSING SHOP',
    'EMPTY CART', 'LINE_ITEMS_ARE_EMPTY',
)


def test_shopify_site(site_url, proxy=None, proxy_pool=None):
    """
    Test a Shopify site with up to 3 CCs. Valid if ANY of them returns a real
    gateway response (decline/approve). Rejects captcha/429/dead-site errors.
    """
    for cc in SITE_VALIDATION_CCS:
        try:
            api_resp = check_shopify_api(
                site_url, cc, proxy,
                all_proxies=proxy_pool,
                all_sites=[site_url],
                max_attempts=1,
            )
            if not api_resp or not isinstance(api_resp, dict):
                continue
            response_text, status, gateway = process_shopify_api_response(api_resp, '0.00')
            resp_up = response_text.upper()
            if any(k in resp_up for k in _SITE_REJECT):
                continue
            if status in ('APPROVED', 'DECLINED', 'EXPIRED'):
                return True, response_text[:80], gateway or 'Shopify Payments'
        except Exception as e:
            logger.debug(f"site test {site_url} cc err: {e}")
            continue
    return False, "no valid gateway response", None


def parse_site_urls(text):
    urls = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        m = re.search(r'https?://[^\s/]+', line)
        if m:
            urls.append(m.group(0).rstrip('/'))
    seen, out = set(), []
    for u in urls:
        k = u.lower()
        if k not in seen:
            seen.add(k)
            out.append(u)
    return out


def get_next_site_id():
    if not sites_data.get('sites'):
        return 1
    return max((s.get('id', 0) for s in sites_data['sites']), default=0) + 1


def add_site_to_global(url, gateway="Shopify Payments"):
    global sites_data
    existing = {s['url'].lower().rstrip('/') for s in sites_data.get('sites', [])}
    if url.lower().rstrip('/') in existing:
        return False
    sites_data['sites'].append({
        "url": url,
        "name": url.replace('https://', '').replace('http://', '').split('/')[0],
        "price": "0.00",
        "gateway": gateway,
        "id": get_next_site_id(),
    })
    save_json(SITES_FILE, sites_data)
    return True


def get_user_sites(uid):
    return user_sites_data.get(str(uid), [])


def save_user_site(uid, entry):
    user_sites_data.setdefault(str(uid), []).append(entry)
    save_json(USER_SITES_FILE, user_sites_data)


# ═══════════════════════════════════════════════════════════════════
# PROXY HELPERS
# ═══════════════════════════════════════════════════════════════════
def read_user_proxies(uid):
    raw = user_proxies_data.get(str(uid), {"shopify": [], "razorpay": []})
    if isinstance(raw, list):
        return {"shopify": raw, "razorpay": []}
    raw.setdefault('shopify', [])
    raw.setdefault('razorpay', [])
    return raw


def save_user_proxies_local(uid, data):
    user_proxies_data[str(uid)] = data
    save_json(USER_PROXIES_FILE, user_proxies_data)


def parse_proxy_lines(text):
    proxies = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split()
        gate = "shopify"
        if len(parts) >= 2 and parts[-1].lower() in ('shopify', 'razorpay'):
            gate = parts[-1].lower()
            line = " ".join(parts[:-1])
        norm = normalize_proxy(line)
        if norm:
            proxies.append((norm, gate))
    seen, out = set(), []
    for p, g in proxies:
        k = (p, g)
        if k not in seen:
            seen.add(k)
            out.append((p, g))
    return out


# ═══════════════════════════════════════════════════════════════════
# SESSION FLAGS
# ═══════════════════════════════════════════════════════════════════
user_sessions = {}


def _set_await(uid, key):
    user_sessions.setdefault(uid, {})
    for k in list(user_sessions[uid].keys()):
        if k.startswith('awaiting_'):
            user_sessions[uid].pop(k, None)
    user_sessions[uid][key] = True
    user_sessions[uid][key + '_ts'] = time.time()


def _pop_await(uid, key):
    user_sessions.get(uid, {}).pop(key, None)
    user_sessions.get(uid, {}).pop(key + '_ts', None)


def _check_await(uid):
    s = user_sessions.get(uid, {})
    best = None
    best_ts = 0
    for k, v in s.items():
        if k.startswith('awaiting_') and not k.endswith('_ts') and v:
            ts = s.get(k + '_ts', 0)
            if ts >= best_ts:
                best, best_ts = k, ts
    return best


# ═══════════════════════════════════════════════════════════════════
# DOCUMENT UPLOAD ROUTER
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(content_types=['document'])
def on_document(message):
    uid = message.from_user.id
    if not _sub_gate(message):
        return

    if not is_user_allowed(uid) and not is_owner(uid):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return

    flag = _check_await(uid)
    if not flag:
        return

    _pop_await(uid, flag)

    fname = (message.document.file_name or '').lower()
    if not fname.endswith('.txt'):
        safe_send(bot.reply_to, message, "❌ Please send a .txt file.")
        return

    try:
        info = bot.get_file(message.document.file_id)
        raw = bot.download_file(info.file_path).decode('utf-8', errors='ignore')
    except Exception as e:
        safe_send(bot.reply_to, message, f"❌ Download failed: {e}")
        return

    if flag == 'awaiting_site_file':
        _run_site_validation(message, raw)
    elif flag == 'awaiting_proxy_file':
        _run_proxy_upload(message, raw)
    elif flag == 'awaiting_cards_file':
        _run_msh(message, raw)


# ═══════════════════════════════════════════════════════════════════
# SITE VALIDATION FLOW
# ═══════════════════════════════════════════════════════════════════
def _run_site_validation(message, raw):
    uid = message.from_user.id
    urls = parse_site_urls(raw)
    if not urls:
        safe_send(bot.reply_to, message, "❌ No URLs found in file.")
        return

    proxy_pool = []
    try:
        proxy_pool = get_active_proxies(uid, "shopify")
    except Exception:
        pass

    status = safe_send(bot.send_message, message.chat.id,
        f"🧪 Testing <b>{len(urls)}</b> sites with 3 CCs each…\n"
        f"🔌 Proxy pool: <b>{len(proxy_pool)}</b>",
        parse_mode='HTML')
    if not status:
        return

    tested = 0
    live = []
    dead = 0
    lock = threading.Lock()
    last_upd = time.time()

    def _worker(u):
        proxy = random.choice(proxy_pool) if proxy_pool else None
        return u, test_shopify_site(u, proxy=proxy, proxy_pool=proxy_pool)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_worker, u): u for u in urls}
        for fut in as_completed(futures):
            tested += 1
            try:
                url, (ok, resp, gw) = fut.result(timeout=120)
            except Exception:
                ok, url, gw = False, futures[fut], None
            with lock:
                if ok:
                    live.append((url, gw or 'Shopify Payments'))
                else:
                    dead += 1
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧪 Testing: <b>{tested}/{len(urls)}</b>\n"
                        f"✅ Live: <b>{len(live)}</b>  ❌ Dead: <b>{dead}</b>",
                        message.chat.id, status.message_id, parse_mode='HTML')
                    last_upd = now
                except Exception:
                    pass

    added = 0
    skipped = 0
    for url, gw in live:
        if add_site_to_global(url, gw):
            added += 1
        else:
            skipped += 1

    safe_send(bot.edit_message_text,
        f"<b>✅ SITE VALIDATION COMPLETE</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🧪 Tested: <b>{tested}</b>\n"
        f"✅ Live: <b>{len(live)}</b>\n"
        f"❌ Dead: <b>{dead}</b>\n"
        f"➕ Added to global: <b>{added}</b>\n"
        f"⏭️ Skipped (dupes): <b>{skipped}</b>\n"
        f"🌐 Total global sites: <b>{len(sites_data.get('sites', []))}</b>",
        message.chat.id, status.message_id, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# PROXY UPLOAD FLOW
# ═══════════════════════════════════════════════════════════════════
def _run_proxy_upload(message, raw):
    uid = message.from_user.id
    parsed = parse_proxy_lines(raw)
    if not parsed:
        safe_send(bot.reply_to, message, "❌ No valid proxies found.")
        return

    status = safe_send(bot.send_message, message.chat.id,
        f"🧪 Testing <b>{len(parsed)}</b> proxies…", parse_mode='HTML')
    if not status:
        return

    tested = 0
    live = []
    lock = threading.Lock()
    last_upd = time.time()

    def _worker(item):
        proxy, gate = item
        if gate == 'razorpay':
            ok = test_proxy_for_razorpay(proxy)
        else:
            ok = test_proxy_for_shopify(proxy)
        return proxy, gate, ok

    with ThreadPoolExecutor(max_workers=50) as ex:
        futures = [ex.submit(_worker, p) for p in parsed]
        for fut in as_completed(futures):
            tested += 1
            try:
                proxy, gate, ok = fut.result(timeout=90)
            except Exception:
                continue
            if ok:
                with lock:
                    live.append((proxy, gate))
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧪 Testing: <b>{tested}/{len(parsed)}</b>\n"
                        f"✅ Live: <b>{len(live)}</b>",
                        message.chat.id, status.message_id, parse_mode='HTML')
                    last_upd = now
                except Exception:
                    pass

    data = read_user_proxies(uid)
    for proxy, gate in live:
        if proxy not in data[gate]:
            data[gate].append(proxy)
    save_user_proxies_local(uid, data)

    sh = len(data['shopify'])
    rz = len(data['razorpay'])

    safe_send(bot.edit_message_text,
        f"<b>✅ PROXY UPLOAD COMPLETE</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🧪 Tested: <b>{tested}</b>\n"
        f"✅ Live: <b>{len(live)}</b>\n"
        f"🛍️ Shopify pool: <b>{sh}</b>\n"
        f"💎 Razorpay pool: <b>{rz}</b>",
        message.chat.id, status.message_id, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# MSH — SHOPIFY MASS CHECK FLOW
# ═══════════════════════════════════════════════════════════════════
def _run_msh(message, raw):
    uid = message.from_user.id

    ccs = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        m = re.search(r'(\d{13,19})[|:/\s](\d{1,2})[|:/\s](\d{2,4})[|:/\s](\d{3,4})', line)
        if m:
            cc, mm, yyyy, cvv = m.groups()
            if len(yyyy) == 2:
                yyyy = "20" + yyyy
            mm = mm.zfill(2)
            ccs.append(f"{cc}|{mm}|{yyyy}|{cvv}")

    if not ccs:
        safe_send(bot.reply_to, message, "❌ No valid cards found.")
        return

    sites = sites_data.get('sites', [])
    if not sites:
        safe_send(bot.reply_to, message, "📭 No global sites to check against.")
        return

    proxies = get_active_proxies(uid, 'shopify') or []

    start = bot.reply_to(message, f"📦 Starting mass check on {len(ccs)} cards…")
    try:
        process_shopify_mass_check(
            bot, message, start, ccs, sites, proxies,
            uid, load_json, save_json, USERS_FILE,
            hit_pref="both", dm_target_user_id=None,
        )
    except Exception as e:
        logger.error(f"msh error: {traceback.format_exc()}")
        safe_send(bot.send_message, message.chat.id, f"❌ Mass check error: {e}")


# ═══════════════════════════════════════════════════════════════════
# START MENU
# ═══════════════════════════════════════════════════════════════════
def cbtn(text, data=None, url=None, style=None):
    b = {"text": text}
    if data is not None: b["callback_data"] = data
    if url is not None: b["url"] = url
    if style in ("primary", "success", "danger"): b["style"] = style
    return b

def ckb(*rows):
    return json.dumps({"inline_keyboard": [list(r) for r in rows]})


def _start_text(uid, name):
    tier = "owner" if uid in OWNER_ID else ("premium" if is_user_allowed(uid) else "free")
    tier_line = {"owner": "👑 Owner", "premium": "💎 Premium", "free": "🆓 Free"}[tier]
    sh = len(sites_data.get('sites', []))
    up = read_user_proxies(uid)
    return (
        f"<b>⚡ N O V A · C H E C K E R</b>\n"
        f"<i>sites. proxies. clean.</i>\n\n"
        f"👋 <b>{html.escape(name or 'User')}</b>\n"
        f"{tier_line}\n\n"
        f"🌐 Global sites: <b>{sh}</b>\n"
        f"🛡️ Your Shopify proxies: <b>{len(up['shopify'])}</b>\n"
        f"💎 Your Razorpay proxies: <b>{len(up['razorpay'])}</b>\n\n"
        f"<i>⚡ NOVA · <a href='tg://user?id={DARKS_ID}'>Unknownop</a></i>"
    )


def _start_kb(uid):
    rows = [
        [cbtn("🌐 Site Manager",   "menu_sites",  style="success"),
         cbtn("🛡️ Proxy Manager", "menu_proxy",  style="primary")],
        [cbtn("📦 Shopify Mass",   "menu_msh")],
    ]
    if is_owner(uid):
        rows.append([cbtn("👑 Owner Panel", "own_root", style="danger")])
    return ckb(*rows)


@bot.message_handler(commands=['start'])
def cmd_start(message):
    if not _sub_gate(message):
        return
    safe_send(bot.send_message, message.chat.id,
        _start_text(message.from_user.id, message.from_user.first_name),
        parse_mode='HTML', reply_markup=_start_kb(message.from_user.id))


@bot.callback_query_handler(func=lambda c: c.data == "back_to_start")
def cb_back_start(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text(_start_text(call.from_user.id, call.from_user.first_name),
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML', reply_markup=_start_kb(call.from_user.id))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "check_subscription")
def cb_check_sub(call):
    if is_subscribed(call.from_user.id) or call.from_user.id in OWNER_ID:
        bot.answer_callback_query(call.id, "✅ Access granted!")
        try:
            bot.edit_message_text(_start_text(call.from_user.id, call.from_user.first_name),
                chat_id=call.message.chat.id, message_id=call.message.message_id,
                parse_mode='HTML', reply_markup=_start_kb(call.from_user.id))
        except Exception:
            pass
    else:
        bot.answer_callback_query(call.id, "❌ Not yet — join both.", show_alert=True)


# ═══════════════════════════════════════════════════════════════════
# SITE MANAGER MENU
# ═══════════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "menu_sites")
def cb_menu_sites(call):
    bot.answer_callback_query(call.id)
    sh = len(sites_data.get('sites', []))
    mine = len(get_user_sites(call.from_user.id))
    rows = [
        [cbtn("🌐 Global Sites",   "site_list_global", style="success"),
         cbtn("🏠 My Sites",       "site_list_mine",   style="success")],
    ]
    if is_owner(call.from_user.id):
        rows.append([cbtn("🧪 Validate New Sites", "site_validate_prompt", style="danger")])
        rows.append([cbtn("🧹 Re-validate Global Pool", "site_clean_global", style="danger")])
    rows.append([cbtn("🔙 Back", "back_to_start", style="danger")])

    text = (
        f"<b>🌐 S I T E  ·  M A N A G E R</b>\n\n"
        f"<blockquote>Global: <b>{sh}</b> sites\n"
        f"Personal: <b>{mine}</b> sites</blockquote>\n\n"
        "🌐 <b>Global</b> — shared pool, everyone uses it\n"
        "🏠 <b>Mine</b> — only you use them\n\n"
    )
    if is_owner(call.from_user.id):
        text += "🧪 <b>Validate</b> — upload URL list, tested with 3 CCs\n"
        text += "🧹 <b>Re-validate</b> — retest the whole pool, drop dead\n\n"
    text += "<i>Tap a pool to view.</i>"

    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML',
            reply_markup=ckb(*rows))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "site_list_global")
def cb_site_list_global(call):
    bot.answer_callback_query(call.id)
    sites = sites_data.get('sites', [])
    if not sites:
        body = "<i>Empty.</i>"
    else:
        lines = [f"#{s.get('id', '?')}  {html.escape(s.get('url', ''))}" for s in sites[:40]]
        body = "\n".join(lines)
        if len(sites) > 40:
            body += f"\n<i>… and {len(sites) - 40} more</i>"

    text = f"<b>🌐 GLOBAL SITES ({len(sites)})</b>\n\n<pre>{body}</pre>"
    kb = ckb(
        [cbtn("📋 Full List", "site_full_list", style="primary")],
        [cbtn("🔙 Back", "menu_sites", style="danger")],
    )
    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML', reply_markup=kb)
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "site_list_mine")
def cb_site_list_mine(call):
    bot.answer_callback_query(call.id)
    mine = get_user_sites(call.from_user.id)
    if not mine:
        body = "<i>You haven't added any sites yet.</i>"
    else:
        body = "\n".join(f"#{s.get('id', '?')}  {html.escape(s.get('url', ''))}" for s in mine)

    text = (
        f"<b>🏠 MY SITES ({len(mine)})</b>\n\n<pre>{body}</pre>\n\n"
        f"Add with: <code>/addmysite https://yoursite.com</code>"
    )
    kb = ckb(
        [cbtn("➕ Add Site", "site_add_prompt", style="success")],
        [cbtn("🔙 Back", "menu_sites", style="danger")],
    )
    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML', reply_markup=kb)
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "site_full_list")
def cb_site_full_list(call):
    bot.answer_callback_query(call.id)
    sites = sites_data.get('sites', [])
    if not sites:
        bot.send_message(call.message.chat.id, "📭 No sites.")
        return
    text = "\n".join(f"{i}. [#{s.get('id','?')}] {s.get('url','')}"
                     for i, s in enumerate(sites, 1))
    fname = f"sites_{call.from_user.id}.txt"
    with open(fname, 'w', encoding='utf-8') as f:
        f.write(text)
    with open(fname, 'rb') as f:
        bot.send_document(call.message.chat.id, f, caption=f"🌐 {len(sites)} sites")
    try:
        os.remove(fname)
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "site_validate_prompt")
def cb_site_validate_prompt(call):
    if not is_owner(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Owner only.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    _set_await(call.from_user.id, 'awaiting_site_file')
    try:
        bot.edit_message_text(
            "<b>🧪 VALIDATE NEW SITES</b>\n\n"
            "Send a <b>.txt</b> file — one URL per line.\n\n"
            "Each site is tested with <b>3 CCs</b>. Only sites that "
            "return a real gateway response get added to the global pool.",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Cancel", "menu_sites", style="danger")]))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "site_clean_global")
def cb_site_clean_global(call):
    global sites_data
    if not is_owner(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Owner only.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    sites = sites_data.get('sites', [])
    if not sites:
        bot.send_message(call.message.chat.id, "📭 No sites to clean.")
        return

    status = bot.send_message(call.message.chat.id,
        f"🧹 Re-testing <b>{len(sites)}</b> sites…", parse_mode='HTML')
    if not status:
        return

    uid = call.from_user.id
    proxy_pool = []
    try:
        proxy_pool = get_active_proxies(uid, "shopify")
    except Exception:
        pass

    tested = 0
    keep = []
    drop = 0
    lock = threading.Lock()
    last_upd = time.time()

    def _w(s):
        proxy = random.choice(proxy_pool) if proxy_pool else None
        return s, test_shopify_site(s['url'], proxy=proxy, proxy_pool=proxy_pool)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(_w, s) for s in sites]
        for fut in as_completed(futures):
            tested += 1
            try:
                s, (ok, _r, _g) = fut.result(timeout=120)
            except Exception:
                continue
            with lock:
                if ok:
                    keep.append(s)
                else:
                    drop += 1
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧹 Re-testing: <b>{tested}/{len(sites)}</b>\n"
                        f"✅ Keep: <b>{len(keep)}</b>  🗑️ Drop: <b>{drop}</b>",
                        call.message.chat.id, status.message_id, parse_mode='HTML')
                    last_upd = now
                except Exception:
                    pass

    sites_data['sites'] = keep
    save_json(SITES_FILE, sites_data)

    safe_send(bot.edit_message_text,
        f"<b>🧹 GLOBAL POOL CLEANED</b>\n"
        f"✅ Kept: <b>{len(keep)}</b>\n"
        f"🗑️ Dropped: <b>{drop}</b>",
        call.message.chat.id, status.message_id, parse_mode='HTML')


@bot.callback_query_handler(func=lambda c: c.data == "site_add_prompt")
def cb_site_add_prompt(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text(
            "<b>➕ ADD PERSONAL SITE</b>\n\n"
            "Send it in the chat:\n"
            "<code>/addmysite https://yoursite.com</code>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Back", "menu_sites", style="danger")]))
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# PROXY MANAGER
# ═══════════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "menu_proxy")
def cb_menu_proxy(call):
    bot.answer_callback_query(call.id)
    up = read_user_proxies(call.from_user.id)
    text = (
        f"<b>🛡️ P R O X Y · M A N A G E R</b>\n\n"
        f"<blockquote>Shopify pool: <b>{len(up['shopify'])}</b>\n"
        f"Razorpay pool: <b>{len(up['razorpay'])}</b></blockquote>\n\n"
        "🛍️ <b>Shopify</b> — used by <code>/sh</code>, <code>/msh</code>\n"
        "💎 <b>Razorpay</b> — used by <code>/rz</code>, <code>/mrz</code>"
    )
    kb = ckb(
        [cbtn("➕ Add Proxy",   "proxy_add_prompt",    style="success"),
         cbtn("📂 Upload .txt", "proxy_upload_prompt", style="success")],
        [cbtn("📋 View Proxies", "proxy_view")],
        [cbtn("🧹 Clean Dead",   "proxy_clean",         style="primary")],
        [cbtn("🗑️ Clear All",    "clearproxies_prompt", style="danger")],
        [cbtn("🔙 Back",         "back_to_start",       style="danger")],
    )
    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML', reply_markup=kb)
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "proxy_add_prompt")
def cb_proxy_add_prompt(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text(
            "<b>➕ ADD A PROXY</b>\n\n"
            "Send in chat:\n"
            "<code>/addpro ip:port</code>\n"
            "<code>/addpro ip:port:user:pass</code>\n\n"
            "<i>Optional gate suffix:</i>\n"
            "<code>/addpro ip:port:user:pass shopify</code>\n"
            "<code>/addpro ip:port:user:pass razorpay</code>\n"
            "<i>Default: shopify</i>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Back", "menu_proxy", style="danger")]))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "proxy_upload_prompt")
def cb_proxy_upload_prompt(call):
    bot.answer_callback_query(call.id)
    _set_await(call.from_user.id, 'awaiting_proxy_file')
    try:
        bot.edit_message_text(
            "<b>📂 UPLOAD PROXIES</b>\n\n"
            "Send a <b>.txt</b> file, one proxy per line:\n"
            "<code>ip:port</code>\n"
            "<code>ip:port:user:pass</code>\n\n"
            "<i>Optional gate suffix per line:</i> <code> shopify</code> / <code> razorpay</code>\n"
            "<i>I'll test them and save only the live ones.</i>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Cancel", "menu_proxy", style="danger")]))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "proxy_view")
def cb_proxy_view(call):
    bot.answer_callback_query(call.id)
    up = read_user_proxies(call.from_user.id)

    def _fmt(lst, cap=15):
        if not lst:
            return "  <i>empty</i>"
        return "\n".join(f"  <code>{p}</code>" for p in lst[:cap]) + (
            f"\n  <i>… and {len(lst) - cap} more</i>" if len(lst) > cap else "")

    text = (
        f"<b>🌐 YOUR PROXIES</b>\n\n"
        f"🛍️ <b>Shopify</b> · {len(up['shopify'])}\n<pre>{_fmt(up['shopify'])}</pre>\n\n"
        f"💎 <b>Razorpay</b> · {len(up['razorpay'])}\n<pre>{_fmt(up['razorpay'])}</pre>"
    )
    kb = ckb(
        [cbtn("🧹 Clean Dead", "proxy_clean",         style="primary")],
        [cbtn("🗑️ Clear All",  "clearproxies_prompt", style="danger")],
        [cbtn("🔙 Back",       "menu_proxy",          style="danger")],
    )
    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML', reply_markup=kb)
    except Exception:
        bot.send_message(call.message.chat.id, text, parse_mode='HTML', reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data == "proxy_clean")
def cb_proxy_clean(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    up = read_user_proxies(uid)
    all_p = list(dict.fromkeys(up['shopify'] + up['razorpay']))
    if not all_p:
        try:
            bot.edit_message_text("📭 No proxies to clean.",
                chat_id=call.message.chat.id, message_id=call.message.message_id)
        except Exception:
            pass
        return

    status = bot.send_message(call.message.chat.id,
        f"🧹 Testing <b>{len(all_p)}</b> proxies…", parse_mode='HTML')
    if not status:
        return

    tested = 0
    live = []
    lock = threading.Lock()
    last_upd = time.time()

    def _w(p):
        return p, test_proxy_for_shopify(p)

    with ThreadPoolExecutor(max_workers=50) as ex:
        futures = [ex.submit(_w, p) for p in all_p]
        for fut in as_completed(futures):
            tested += 1
            try:
                p, ok = fut.result(timeout=90)
            except Exception:
                continue
            if ok:
                with lock:
                    live.append(p)
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧹 Testing: <b>{tested}/{len(all_p)}</b>\n"
                        f"✅ Live: <b>{len(live)}</b>",
                        call.message.chat.id, status.message_id, parse_mode='HTML')
                    last_upd = now
                except Exception:
                    pass

    up['shopify'] = live
    up['razorpay'] = []
    save_user_proxies_local(uid, up)

    safe_send(bot.edit_message_text,
        f"<b>✅ POOL CLEANED</b>\n"
        f"✅ Live: <b>{len(live)}</b> / {len(all_p)}",
        call.message.chat.id, status.message_id, parse_mode='HTML')


@bot.callback_query_handler(func=lambda c: c.data == "clearproxies_prompt")
def cb_clearproxies_prompt(call):
    bot.answer_callback_query(call.id)
    kb = ckb(
        [cbtn("⚠️ Yes, wipe both pools", "clearproxies_confirm", style="danger")],
        [cbtn("❌ Cancel", "menu_proxy", style="success")],
    )
    try:
        bot.edit_message_text(
            "<b>⚠️ CONFIRM WIPE</b>\n\n"
            "This deletes every personal proxy you added. Cannot be undone.",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML', reply_markup=kb)
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "clearproxies_confirm")
def cb_clearproxies_confirm(call):
    bot.answer_callback_query(call.id, "✅ Cleared.")
    uid = str(call.from_user.id)
    user_proxies_data[uid] = {"shopify": [], "razorpay": []}
    save_json(USER_PROXIES_FILE, user_proxies_data)
    try:
        bot.edit_message_text(
            "<b>✅ PROXIES CLEARED</b>\n\nBoth pools are empty.",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Proxy Menu", "menu_proxy", style="success")]))
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# MASS SHOPIFY CHECK — info panel
# ═══════════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "menu_msh")
def cb_menu_msh(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text(
            "<b>📦 SHOPIFY MASS CHECK</b>\n\n"
            "<blockquote>Upload a <b>.txt</b> of cards → one card per line →\n"
            "bot checks against global sites + your proxies.</blockquote>\n\n"
            "Command: <code>/msh</code>\n"
            "Format: <code>CC|MM|YYYY|CVV</code>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Back", "back_to_start", style="danger")]))
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# OWNER PANEL
# ═══════════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "own_root")
def cb_own_root(call):
    if not is_owner(call.from_user.id):
        bot.answer_callback_query(call.id, "🚫 Owners only.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    kb = ckb(
        [cbtn("🌐 Sites",         "menu_sites",           style="success"),
         cbtn("🛡️ Proxies",       "menu_proxy",           style="success")],
        [cbtn("🧪 Validate Sites", "site_validate_prompt", style="danger"),
         cbtn("🧹 Clean Pool",    "site_clean_global",    style="danger")],
        [cbtn("🔙 Back",          "back_to_start",        style="danger")],
    )
    try:
        bot.edit_message_text(
            f"<b>👑 OWNER PANEL</b>\n\n"
            f"🌐 Total global sites: <b>{len(sites_data.get('sites', []))}</b>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML', reply_markup=kb)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# TEXT COMMANDS
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['validatesites'])
def cmd_validatesites(message):
    if not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Owner only.")
        return
    _set_await(message.from_user.id, 'awaiting_site_file')
    safe_send(bot.reply_to, message,
        "<b>🧪 VALIDATE SITES</b>\n\n"
        "Send a <b>.txt</b> file — one URL per line.\n"
        "Each site tested with 3 CCs. Only valid sites added.",
        parse_mode='HTML')


@bot.message_handler(commands=['listsites', 'listsite'])
def cmd_listsites(message):
    sites = sites_data.get('sites', [])
    if not sites:
        safe_send(bot.reply_to, message, "📭 No sites in the global pool.")
        return
    lines = "\n".join(f"{i}. [#{s.get('id','?')}] {s.get('url','')}"
                      for i, s in enumerate(sites, 1))
    header = f"<b>🌐 GLOBAL SITES ({len(sites)})</b>\n\n"
    if len(header) + len(lines) < 3800:
        safe_send(bot.reply_to, message, header + f"<pre>{html.escape(lines)}</pre>",
                  parse_mode='HTML')
    else:
        fname = f"sites_{message.chat.id}.txt"
        with open(fname, 'w', encoding='utf-8') as f:
            f.write(lines)
        with open(fname, 'rb') as f:
            bot.send_document(message.chat.id, f, caption=f"🌐 {len(sites)} sites")
        try:
            os.remove(fname)
        except Exception:
            pass


@bot.message_handler(commands=['rmsite'])
def cmd_rmsite(message):
    global sites_data
    if not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Owner only.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(bot.reply_to, message, "Usage: /rmsite <id>", parse_mode='HTML')
        return
    try:
        sid = int(parts[1].strip().lstrip('#'))
    except ValueError:
        safe_send(bot.reply_to, message, "❌ Invalid id.")
        return
    before = len(sites_data['sites'])
    sites_data['sites'] = [s for s in sites_data['sites'] if s.get('id') != sid]
    if len(sites_data['sites']) == before:
        safe_send(bot.reply_to, message, f"❌ No site with id #{sid}.")
        return
    save_json(SITES_FILE, sites_data)
    safe_send(bot.reply_to, message,
        f"✅ Removed site #{sid}. Remaining: {len(sites_data['sites'])}")


@bot.message_handler(commands=['addmysite'])
def cmd_addmysite(message):
    if not _sub_gate(message):
        return
    if not is_user_allowed(message.from_user.id) and not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(bot.reply_to, message, "Usage: /addmysite <url>", parse_mode='HTML')
        return
    url = parts[1].strip().rstrip('/')
    if not url.startswith('https://'):
        safe_send(bot.reply_to, message, "❌ URL must start with https://")
        return
    uid = message.from_user.id
    mine = get_user_sites(uid)
    nid = max((s.get('id', 0) for s in mine), default=0) + 1
    save_user_site(uid, {
        "url": url,
        "name": url.replace('https://', '').split('/')[0],
        "price": "0.00",
        "gateway": "Shopify Payments",
        "id": nid,
    })
    safe_send(bot.reply_to, message,
        f"✅ Added personal site:\n<code>{html.escape(url)}</code>",
        parse_mode='HTML')


@bot.message_handler(commands=['mysites'])
def cmd_mysites(message):
    mine = get_user_sites(message.from_user.id)
    if not mine:
        safe_send(bot.reply_to, message,
            "📭 No personal sites.\nAdd with /addmysite <url>", parse_mode='HTML')
        return
    lines = "\n".join(f"{i}. [#{s.get('id','?')}] {s.get('url','')}"
                      for i, s in enumerate(mine, 1))
    safe_send(bot.reply_to, message,
        f"<b>🏠 MY SITES ({len(mine)})</b>\n\n<pre>{html.escape(lines)}</pre>",
        parse_mode='HTML')


@bot.message_handler(commands=['addpro'])
def cmd_addpro(message):
    if not _sub_gate(message):
        return
    if not is_user_allowed(message.from_user.id) and not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(bot.reply_to, message,
            "Usage: <code>/addpro ip:port:user:pass [shopify|razorpay]</code>",
            parse_mode='HTML')
        return

    raw = parts[1].strip()
    gate = "shopify"
    tokens = raw.split()
    if len(tokens) >= 2 and tokens[-1].lower() in ('shopify', 'razorpay'):
        gate = tokens[-1].lower()
        raw = " ".join(tokens[:-1])

    norm = normalize_proxy(raw)
    if not norm:
        safe_send(bot.reply_to, message, "❌ Invalid proxy format.")
        return

    if not _quick_alive(norm, timeout=8):
        safe_send(bot.reply_to, message,
            f"❌ Proxy dead: <code>{html.escape(norm)}</code>", parse_mode='HTML')
        return

    uid = message.from_user.id
    up = read_user_proxies(uid)
    if norm in up[gate]:
        safe_send(bot.reply_to, message,
            f"⚠️ Already in <b>{gate}</b> pool:\n<code>{html.escape(norm)}</code>",
            parse_mode='HTML')
        return
    up[gate].append(norm)
    save_user_proxies_local(uid, up)
    safe_send(bot.reply_to, message,
        f"✅ Added to <b>{gate}</b> pool:\n<code>{html.escape(norm)}</code>\n\n"
        f"Pool size: <b>{len(up[gate])}</b>",
        parse_mode='HTML')


@bot.message_handler(commands=['uploadproxies'])
def cmd_uploadproxies(message):
    if not _sub_gate(message):
        return
    if not is_user_allowed(message.from_user.id) and not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return
    _set_await(message.from_user.id, 'awaiting_proxy_file')
    safe_send(bot.reply_to, message,
        "<b>📂 Send your proxy .txt now.</b>\n\nOne per line:\n"
        "<code>ip:port</code>\n<code>ip:port:user:pass</code>\n"
        "<i>Optional gate suffix: <code> shopify</code> or <code> razorpay</code></i>",
        parse_mode='HTML')


@bot.message_handler(commands=['clearproxies'])
def cmd_clearproxies(message):
    if not _sub_gate(message):
        return
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("⚠️ Yes, wipe both pools", callback_data="clearproxies_confirm"))
    kb.add(types.InlineKeyboardButton("❌ Cancel", callback_data="menu_proxy"))
    safe_send(bot.reply_to, message,
        "<b>⚠️ CONFIRM</b>\n\nThis deletes every personal proxy you added.",
        parse_mode='HTML', reply_markup=kb)


@bot.message_handler(commands=['cleanshpro'])
def cmd_cleanshpro(message):
    if not _sub_gate(message):
        return
    if not is_user_allowed(message.from_user.id) and not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return
    uid = message.from_user.id
    up = read_user_proxies(uid)
    all_p = up['shopify']
    if not all_p:
        safe_send(bot.reply_to, message, "📭 No proxies to test.")
        return

    status = bot.reply_to(message, f"🧹 Testing {len(all_p)} proxies…")
    tested = 0
    live = []
    last_upd = time.time()

    def _w(p):
        return p, test_proxy_for_shopify(p)

    with ThreadPoolExecutor(max_workers=50) as ex:
        futures = [ex.submit(_w, p) for p in all_p]
        for fut in as_completed(futures):
            tested += 1
            try:
                p, ok = fut.result(timeout=90)
            except Exception:
                continue
            if ok:
                live.append(p)
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧹 Testing: {tested}/{len(all_p)}\n✅ Live: {len(live)}",
                        message.chat.id, status.message_id)
                    last_upd = now
                except Exception:
                    pass

    up['shopify'] = live
    save_user_proxies_local(uid, up)
    safe_send(bot.edit_message_text,
        f"✅ Cleaned: {len(live)}/{len(all_p)} live",
        message.chat.id, status.message_id)


# ═══════════════════════════════════════════════════════════════════
# MSH — Shopify mass check entry
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['msh'])
def cmd_msh(message):
    if not _sub_gate(message):
        return
    if not is_user_allowed(message.from_user.id) and not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return
    _set_await(message.from_user.id, 'awaiting_cards_file')
    safe_send(bot.reply_to, message,
        "<b>📦 SHOPIFY MASS CHECK</b>\n\n"
        "Send a <b>.txt</b> of cards, one per line:\n"
        "<code>CC|MM|YYYY|CVV</code>\n\n"
        "Uses global sites + your Shopify proxies.",
        parse_mode='HTML')


@bot.message_handler(commands=['stop'])
def cmd_stop(message):
    safe_send(bot.reply_to, message, "⏸️ Stop requested.")


# ═══════════════════════════════════════════════════════════════════
# COMPLETE HANDLER SETUP (optional — enables premium/etc from complete_handler)
# ═══════════════════════════════════════════════════════════════════
try:
    setup_complete_handler(
        bot=bot,
        site_filter_func=lambda: sites_data.get('sites', []),
        proxies_data=proxies_data,
        check_site_func=check_shopify_api,
        is_valid_response_func=lambda r: True,
        process_response_func=process_shopify_api_response,
        update_stats_func=lambda *a, **k: None,
        save_json_func_param=save_json,
        load_json_func_param=load_json,
        is_user_allowed_func=is_user_allowed,
        users_data_ref=users_data,
        users_file_param=USERS_FILE,
        force_subscribe_decorator=None,
        user_sessions=user_sessions,
    )
    logger.info("✅ complete_handler wired")
except Exception as e:
    logger.warning(f"complete_handler setup skipped: {e}")


# ═══════════════════════════════════════════════════════════════════
# POLL
# ═══════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    logger.info("🚀 Bot starting…")
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=25)
        except Exception as e:
            logger.error(f"Poll crash: {e}")
            time.sleep(5)
