#!/usr/bin/env python3
# app.py — NOVA · Sites + Proxies focused build (clean rewrite)

import os, re, sys, json, time, html, random, threading, traceback, queue, socket, functools
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

# ═══════════════════════════════════════════════════════════════════
# CORE IMPORTS
# ═══════════════════════════════════════════════════════════════════
from gates import check_shopify_api, process_shopify_api_response
from complete_handler import (
    get_active_proxies,
    test_proxy_for_shopify,
    test_proxy_for_razorpay,
    normalize_proxy,
    _quick_alive,
    process_shopify_mass_check,
)

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════
BOT_TOKEN = "8533961422:AAGzOHNJEIeM7bsqb-qOW-fgErwgJ2jPV0U"
OWNER_ID  = [5963548505, 5547897619]
DARKS_ID  = 5963548505
FREE_GROUP_ID  = -1003926435438
REQUIRED_CHATS = ["@Nova_bot_update", "-1003926435438"]

SITES_FILE        = "sites.json"
PROXIES_FILE      = "proxies.json"
USERS_FILE        = "users.json"
GROUPS_FILE       = "groups.json"
USER_PROXIES_FILE = "user_proxies.json"
USER_SITES_FILE   = "user_sites.json"
STATS_FILE        = "stats.json"
SETTINGS_FILE     = "settings.json"

SITE_VALIDATION_CCS = [
    "5242430428405662|03|28|323",
    "4517650667292635|08|2028|261",
    "4242424242424242|12|2028|123",
]

bot = telebot.TeleBot(BOT_TOKEN, num_threads=30)

# ═══════════════════════════════════════════════════════════════════
# MONGO + JSON HELPERS
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
    logger.warning(f"❌ MongoDB failed: {e}")
    db = None


def _coll(fp):
    return fp.replace('.json', '').replace('data/', '').replace('/', '_')


def load_json(fp, default):
    if db is not None:
        try:
            doc = db[_coll(fp)].find_one({"_id": "main_data"})
            if doc and 'data' in doc:
                data = doc['data']
                if fp == SITES_FILE:
                    if isinstance(data, list): data = {"sites": data}
                    data.setdefault('sites', [])
                elif fp == PROXIES_FILE:
                    if isinstance(data, list): data = {"shopify": data, "razorpay": []}
                    data.setdefault('shopify', []); data.setdefault('razorpay', [])
                return data
        except Exception as e:
            logger.warning(f"Mongo load {fp}: {e}")
    try:
        if os.path.exists(fp):
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if fp == SITES_FILE:
                if isinstance(data, list): data = {"sites": data}
                data.setdefault('sites', [])
            elif fp == PROXIES_FILE:
                if isinstance(data, list): data = {"shopify": data, "razorpay": []}
                data.setdefault('shopify', []); data.setdefault('razorpay', [])
            return data
    except Exception:
        pass
    return default


def save_json(fp, data):
    if db is not None:
        try:
            db[_coll(fp)].update_one(
                {"_id": "main_data"},
                {"$set": {"data": data, "updated_at": datetime.utcnow().isoformat()}},
                upsert=True
            )
        except Exception as e:
            logger.warning(f"Mongo save {fp}: {e}")
    try:
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Local save {fp}: {e}")


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

# migrate + id assign
_need = False
_nid = 1
for s in sites_data.get('sites', []):
    if 'id' not in s:
        s['id'] = _nid
        _need = True
    if s['id'] >= _nid:
        _nid = s['id'] + 1
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
# SAFE SEND (rate-limited)
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
# ACK HELPER (never throws)
# ═══════════════════════════════════════════════════════════════════
def _ack(call, text=None, show_alert=False):
    try:
        if text is not None:
            bot.answer_callback_query(call.id, text=text, show_alert=show_alert)
        else:
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.warning(f"ack failed: {e}")


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
    uid = message.from_user.id
    if uid in OWNER_ID:
        return True
    if is_subscribed(uid):
        return True
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📢 Join Channel", url="https://t.me/Nova_bot_update"))
    kb.add(types.InlineKeyboardButton("👥 Join Group", url="https://t.me/+NxKPx890ExkwYjQ0"))
    kb.add(types.InlineKeyboardButton("🔄 I've Verified", callback_data="check_subscription"))
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
    for cc in SITE_VALIDATION_CCS:
        try:
            api_resp = check_shopify_api(site_url, cc, proxy,
                all_proxies=proxy_pool, all_sites=[site_url], max_attempts=1)
            if not api_resp or not isinstance(api_resp, dict):
                continue
            response_text, status, gateway = process_shopify_api_response(api_resp, '0.00')
            up = response_text.upper()
            if any(k in up for k in _SITE_REJECT):
                continue
            if status in ('APPROVED', 'DECLINED', 'EXPIRED'):
                return True, response_text[:80], gateway or 'Shopify Payments'
        except Exception:
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
            seen.add(k); out.append(u)
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
    raw.setdefault('shopify', []); raw.setdefault('razorpay', [])
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
    best, best_ts = None, 0
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
# SITE VALIDATION
# ═══════════════════════════════════════════════════════════════════
def _run_site_validation(message, raw):
    uid = message.from_user.id
    urls = parse_site_urls(raw)
    if not urls:
        safe_send(bot.reply_to, message, "❌ No URLs found.")
        return
    proxy_pool = []
    try:
        proxy_pool = get_active_proxies(uid, "shopify")
    except Exception:
        pass

    status = safe_send(bot.send_message, message.chat.id,
        f"🧪 Testing <b>{len(urls)}</b> sites with 3 CCs each…\n"
        f"🔌 Proxies: <b>{len(proxy_pool)}</b>",
        parse_mode='HTML')
    if not status:
        return

    tested, live, dead = 0, [], 0
    lock = threading.Lock()
    last_upd = time.time()

    def _w(u):
        proxy = random.choice(proxy_pool) if proxy_pool else None
        return u, test_shopify_site(u, proxy=proxy, proxy_pool=proxy_pool)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_w, u): u for u in urls}
        for fut in as_completed(futs):
            tested += 1
            try:
                url, (ok, _r, gw) = fut.result(timeout=120)
            except Exception:
                ok, url, gw = False, futs[fut], None
            with lock:
                if ok: live.append((url, gw or 'Shopify Payments'))
                else: dead += 1
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧪 {tested}/{len(urls)} · ✅ {len(live)} · ❌ {dead}",
                        message.chat.id, status.message_id)
                    last_upd = now
                except Exception:
                    pass

    added = sum(1 for u, g in live if add_site_to_global(u, g))
    skipped = len(live) - added

    safe_send(bot.edit_message_text,
        f"<b>✅ VALIDATION DONE</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Tested: {tested}\n"
        f"Live: {len(live)}\n"
        f"Dead: {dead}\n"
        f"Added: {added}\n"
        f"Skipped dupes: {skipped}\n"
        f"Total global: {len(sites_data.get('sites', []))}",
        message.chat.id, status.message_id, parse_mode='HTML')


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
        f"🧪 Testing <b>{len(parsed)}</b> proxies…", parse_mode='HTML')
    if not status:
        return

    tested, live = 0, []
    lock = threading.Lock()
    last_upd = time.time()

    def _w(item):
        p, g = item
        return p, g, (test_proxy_for_razorpay(p) if g == 'razorpay' else test_proxy_for_shopify(p))

    with ThreadPoolExecutor(max_workers=50) as ex:
        futs = [ex.submit(_w, p) for p in parsed]
        for fut in as_completed(futs):
            tested += 1
            try:
                p, g, ok = fut.result(timeout=90)
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
    for p, g in live:
        if p not in data[g]:
            data[g].append(p)
    save_user_proxies_local(uid, data)

    safe_send(bot.edit_message_text,
        f"<b>✅ PROXY UPLOAD DONE</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Tested: {tested}\n"
        f"Live: {len(live)}\n"
        f"Shopify pool: {len(data['shopify'])}\n"
        f"Razorpay pool: {len(data['razorpay'])}",
        message.chat.id, status.message_id, parse_mode='HTML')


# ═══════════════════════════════════════════════════════════════════
# MSH FLOW
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
        safe_send(bot.reply_to, message, "📭 No global sites.")
        return
    proxies = get_active_proxies(uid, 'shopify') or []
    start = bot.reply_to(message, f"📦 Starting on {len(ccs)} cards…")
    try:
        process_shopify_mass_check(
            bot, message, start, ccs, sites, proxies,
            uid, load_json, save_json, USERS_FILE,
            hit_pref="both", dm_target_user_id=None,
        )
    except Exception as e:
        logger.error(f"msh: {traceback.format_exc()}")
        safe_send(bot.send_message, message.chat.id, f"❌ Error: {e}")


# ═══════════════════════════════════════════════════════════════════
# KEYBOARD BUILDERS
# ═══════════════════════════════════════════════════════════════════
def cbtn(text, data=None, url=None, style=None):
    b = {"text": text}
    if data is not None: b["callback_data"] = data
    if url is not None: b["url"] = url
    if style in ("primary", "success", "danger"): b["style"] = style
    return b

def ckb(*rows):
    return json.dumps({"inline_keyboard": [list(r) for r in rows]})


# ═══════════════════════════════════════════════════════════════════
# START
# ═══════════════════════════════════════════════════════════════════
def _start_text(uid, name):
    tier = "owner" if uid in OWNER_ID else ("premium" if is_user_allowed(uid) else "free")
    tline = {"owner": "👑 Owner", "premium": "💎 Premium", "free": "🆓 Free"}[tier]
    sh = len(sites_data.get('sites', []))
    up = read_user_proxies(uid)
    return (
        f"<b>⚡ N O V A · C H E C K E R</b>\n"
        f"<i>sites. proxies. clean.</i>\n\n"
        f"👋 <b>{html.escape(name or 'User')}</b>\n"
        f"{tline}\n\n"
        f"🌐 Global sites: <b>{sh}</b>\n"
        f"🛡️ Shopify proxies: <b>{len(up['shopify'])}</b>\n"
        f"💎 Razorpay proxies: <b>{len(up['razorpay'])}</b>\n\n"
        f"<i>⚡ NOVA · <a href='tg://user?id={DARKS_ID}'>Unknownop</a></i>"
    )


def _start_kb(uid):
    rows = [
        [cbtn("🌐 Site Manager",   "menu_sites", style="success"),
         cbtn("🛡️ Proxy Manager", "menu_proxy", style="primary")],
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
    _ack(call)
    try:
        bot.edit_message_text(
            _start_text(call.from_user.id, call.from_user.first_name),
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML', reply_markup=_start_kb(call.from_user.id))
    except Exception as e:
        logger.warning(f"back_to_start edit: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "check_subscription")
def cb_check_sub(call):
    if is_subscribed(call.from_user.id) or call.from_user.id in OWNER_ID:
        _ack(call, "✅ Access granted!")
        try:
            bot.edit_message_text(
                _start_text(call.from_user.id, call.from_user.first_name),
                chat_id=call.message.chat.id, message_id=call.message.message_id,
                parse_mode='HTML', reply_markup=_start_kb(call.from_user.id))
        except Exception:
            pass
    else:
        _ack(call, "❌ Not yet — join both.", show_alert=True)


# ═══════════════════════════════════════════════════════════════════
# SITE MANAGER
# ═══════════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "menu_sites")
def cb_menu_sites(call):
    _ack(call)
    sh = len(sites_data.get('sites', []))
    mine = len(get_user_sites(call.from_user.id))
    rows = [
        [cbtn("🌐 Global Sites", "site_list_global", style="success"),
         cbtn("🏠 My Sites",     "site_list_mine",   style="success")],
    ]
    if is_owner(call.from_user.id):
        rows.append([cbtn("🧪 Validate New", "site_validate_prompt", style="danger")])
        rows.append([cbtn("🧹 Re-validate Pool", "site_clean_global", style="danger")])
    rows.append([cbtn("🔙 Back", "back_to_start", style="danger")])

    text = (
        f"<b>🌐 SITE MANAGER</b>\n\n"
        f"Global: <b>{sh}</b> sites\n"
        f"Personal: <b>{mine}</b> sites\n\n"
        "🌐 <b>Global</b> — shared pool\n"
        "🏠 <b>Mine</b> — your own\n\n"
    )
    if is_owner(call.from_user.id):
        text += "🧪 <b>Validate</b> — add URLs (3-CC test)\n"
        text += "🧹 <b>Re-validate</b> — retest pool"
    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML',
            reply_markup=ckb(*rows))
    except Exception as e:
        logger.warning(f"menu_sites edit: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "site_list_global")
def cb_site_list_global(call):
    _ack(call)
    sites = sites_data.get('sites', [])
    if not sites:
        body = "<i>Empty.</i>"
    else:
        lines = []
        for s in sites[:40]:
            sid = s.get('id', '?')
            u = html.escape(s.get('url', ''))
            lines.append(f"#{sid}  {u}")
        body = "\n".join(lines)
        if len(sites) > 40:
            body += f"\n… and {len(sites)-40} more"
    text = f"<b>🌐 GLOBAL ({len(sites)})</b>\n\n{body}"
    kb = ckb(
        [cbtn("📋 Full List", "site_full_list", style="primary")],
        [cbtn("🔙 Back", "menu_sites", style="danger")],
    )
    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML', reply_markup=kb)
    except Exception as e:
        logger.warning(f"site_list_global edit: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "site_list_mine")
def cb_site_list_mine(call):
    _ack(call)
    mine = get_user_sites(call.from_user.id)
    if not mine:
        body = "<i>None yet.</i>"
    else:
        lines = []
        for s in mine:
            sid = s.get('id', '?')
            u = html.escape(s.get('url', ''))
            lines.append(f"#{sid}  {u}")
        body = "\n".join(lines)
    text = (
        f"<b>🏠 MY SITES ({len(mine)})</b>\n\n{body}\n\n"
        f"Add: <code>/addmysite https://site.com</code>"
    )
    kb = ckb(
        [cbtn("➕ Add Site", "site_add_prompt", style="success")],
        [cbtn("🔙 Back", "menu_sites", style="danger")],
    )
    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML', reply_markup=kb)
    except Exception as e:
        logger.warning(f"site_list_mine edit: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "site_full_list")
def cb_site_full_list(call):
    _ack(call)
    sites = sites_data.get('sites', [])
    if not sites:
        bot.send_message(call.message.chat.id, "📭 No sites.")
        return
    text = "\n".join(f"{i}. [#{s.get('id','?')}] {s.get('url','')}"
                     for i, s in enumerate(sites, 1))
    fname = f"sites_{call.from_user.id}.txt"
    try:
        with open(fname, 'w', encoding='utf-8') as f:
            f.write(text)
        with open(fname, 'rb') as f:
            bot.send_document(call.message.chat.id, f, caption=f"🌐 {len(sites)} sites")
        os.remove(fname)
    except Exception as e:
        logger.warning(f"site_full_list: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "site_validate_prompt")
def cb_site_validate_prompt(call):
    if not is_owner(call.from_user.id):
        _ack(call, "🚫 Owner only.", show_alert=True)
        return
    _ack(call)
    _set_await(call.from_user.id, 'awaiting_site_file')
    try:
        bot.edit_message_text(
            "<b>🧪 VALIDATE NEW SITES</b>\n\n"
            "Send a <b>.txt</b> file — one URL per line.\n\n"
            "Each site tested with <b>3 CCs</b>. Only valid ones added.",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Cancel", "menu_sites", style="danger")]))
    except Exception as e:
        logger.warning(f"site_validate_prompt: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "site_clean_global")
def cb_site_clean_global(call):
    global sites_data
    if not is_owner(call.from_user.id):
        _ack(call, "🚫 Owner only.", show_alert=True)
        return
    _ack(call)
    sites = sites_data.get('sites', [])
    if not sites:
        bot.send_message(call.message.chat.id, "📭 No sites.")
        return
    status = bot.send_message(call.message.chat.id,
        f"🧹 Re-testing <b>{len(sites)}</b> sites…", parse_mode='HTML')
    if not status:
        return
    proxy_pool = []
    try:
        proxy_pool = get_active_proxies(call.from_user.id, "shopify")
    except Exception:
        pass
    tested, keep, drop = 0, [], 0
    lock = threading.Lock()
    last_upd = time.time()

    def _w(s):
        proxy = random.choice(proxy_pool) if proxy_pool else None
        return s, test_shopify_site(s['url'], proxy=proxy, proxy_pool=proxy_pool)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(_w, s) for s in sites]
        for fut in as_completed(futs):
            tested += 1
            try:
                s, (ok, _r, _g) = fut.result(timeout=120)
            except Exception:
                continue
            with lock:
                if ok: keep.append(s)
                else: drop += 1
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧹 {tested}/{len(sites)} · ✅ {len(keep)} · 🗑️ {drop}",
                        call.message.chat.id, status.message_id)
                    last_upd = now
                except Exception:
                    pass

    sites_data['sites'] = keep
    save_json(SITES_FILE, sites_data)

    safe_send(bot.edit_message_text,
        f"<b>🧹 CLEANED</b>\n\n✅ Kept: {len(keep)}\n🗑️ Dropped: {drop}",
        call.message.chat.id, status.message_id, parse_mode='HTML')


@bot.callback_query_handler(func=lambda c: c.data == "site_add_prompt")
def cb_site_add_prompt(call):
    _ack(call)
    try:
        bot.edit_message_text(
            "<b>➕ ADD PERSONAL SITE</b>\n\n"
            "Type in chat:\n"
            "<code>/addmysite https://site.com</code>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Back", "menu_sites", style="danger")]))
    except Exception as e:
        logger.warning(f"site_add_prompt: {e}")


# ═══════════════════════════════════════════════════════════════════
# PROXY MANAGER
# ═══════════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "menu_proxy")
def cb_menu_proxy(call):
    _ack(call)
    up = read_user_proxies(call.from_user.id)
    text = (
        f"<b>🛡️ PROXY MANAGER</b>\n\n"
        f"Shopify pool: <b>{len(up['shopify'])}</b>\n"
        f"Razorpay pool: <b>{len(up['razorpay'])}</b>\n\n"
        "🛍️ Shopify — used by /sh, /msh\n"
        "💎 Razorpay — used by /rz, /mrz"
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
    except Exception as e:
        logger.warning(f"menu_proxy edit: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "proxy_add_prompt")
def cb_proxy_add_prompt(call):
    _ack(call)
    try:
        bot.edit_message_text(
            "<b>➕ ADD A PROXY</b>\n\n"
            "Type in chat:\n"
            "<code>/addpro ip:port</code>\n"
            "<code>/addpro ip:port:user:pass</code>\n\n"
            "Optional gate suffix:\n"
            "<code>/addpro ip:port:user:pass shopify</code>\n"
            "<code>/addpro ip:port:user:pass razorpay</code>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Back", "menu_proxy", style="danger")]))
    except Exception as e:
        logger.warning(f"proxy_add_prompt: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "proxy_upload_prompt")
def cb_proxy_upload_prompt(call):
    _ack(call)
    _set_await(call.from_user.id, 'awaiting_proxy_file')
    try:
        bot.edit_message_text(
            "<b>📂 UPLOAD PROXIES</b>\n\n"
            "Send a <b>.txt</b> file — one per line:\n"
            "<code>ip:port</code>\n"
            "<code>ip:port:user:pass</code>\n\n"
            "Optional per line: <code> shopify</code> or <code> razorpay</code>\n"
            "Only live proxies saved.",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Cancel", "menu_proxy", style="danger")]))
    except Exception as e:
        logger.warning(f"proxy_upload_prompt: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "proxy_view")
def cb_proxy_view(call):
    _ack(call)
    up = read_user_proxies(call.from_user.id)

    def fmt(lst, cap=15):
        if not lst:
            return "  (empty)"
        out = "\n".join(f"  {p}" for p in lst[:cap])
        if len(lst) > cap:
            out += f"\n  … and {len(lst)-cap} more"
        return out

    text = (
        f"<b>🌐 YOUR PROXIES</b>\n\n"
        f"🛍️ <b>Shopify</b> · {len(up['shopify'])}\n{fmt(up['shopify'])}\n\n"
        f"💎 <b>Razorpay</b> · {len(up['razorpay'])}\n{fmt(up['razorpay'])}"
    )
    kb = ckb(
        [cbtn("🧹 Clean Dead", "proxy_clean",         style="primary")],
        [cbtn("🗑️ Clear All",  "clearproxies_prompt", style="danger")],
        [cbtn("🔙 Back",       "menu_proxy",          style="danger")],
    )
    try:
        bot.edit_message_text(text, chat_id=call.message.chat.id,
            message_id=call.message.message_id, parse_mode='HTML', reply_markup=kb)
    except Exception as e:
        logger.warning(f"proxy_view edit: {e}")
        try:
            bot.send_message(call.message.chat.id, text, parse_mode='HTML', reply_markup=kb)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda c: c.data == "proxy_clean")
def cb_proxy_clean(call):
    _ack(call)
    uid = call.from_user.id
    up = read_user_proxies(uid)
    all_p = list(dict.fromkeys(up['shopify'] + up['razorpay']))
    if not all_p:
        try:
            bot.edit_message_text("📭 No proxies.",
                chat_id=call.message.chat.id, message_id=call.message.message_id)
        except Exception:
            pass
        return
    status = bot.send_message(call.message.chat.id,
        f"🧹 Testing <b>{len(all_p)}</b> proxies…", parse_mode='HTML')
    if not status:
        return
    tested, live = 0, []
    lock = threading.Lock()
    last_upd = time.time()

    def _w(p):
        return p, test_proxy_for_shopify(p)

    with ThreadPoolExecutor(max_workers=50) as ex:
        futs = [ex.submit(_w, p) for p in all_p]
        for fut in as_completed(futs):
            tested += 1
            try:
                p, ok = fut.result(timeout=90)
            except Exception:
                continue
            if ok:
                with lock: live.append(p)
            now = time.time()
            if now - last_upd >= 3:
                try:
                    safe_send(bot.edit_message_text,
                        f"🧹 {tested}/{len(all_p)} · ✅ {len(live)}",
                        call.message.chat.id, status.message_id)
                    last_upd = now
                except Exception:
                    pass

    up['shopify'] = live
    up['razorpay'] = []
    save_user_proxies_local(uid, up)

    safe_send(bot.edit_message_text,
        f"<b>✅ CLEANED</b>\n\nLive: {len(live)} / {len(all_p)}",
        call.message.chat.id, status.message_id, parse_mode='HTML')


@bot.callback_query_handler(func=lambda c: c.data == "clearproxies_prompt")
def cb_clearproxies_prompt(call):
    _ack(call)
    kb = ckb(
        [cbtn("⚠️ Yes, wipe both", "clearproxies_confirm", style="danger")],
        [cbtn("❌ Cancel", "menu_proxy", style="success")],
    )
    try:
        bot.edit_message_text(
            "<b>⚠️ CONFIRM WIPE</b>\n\n"
            "This deletes ALL your proxies. Cannot be undone.",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML', reply_markup=kb)
    except Exception as e:
        logger.warning(f"clearproxies_prompt: {e}")


@bot.callback_query_handler(func=lambda c: c.data == "clearproxies_confirm")
def cb_clearproxies_confirm(call):
    _ack(call, "✅ Cleared.")
    uid = str(call.from_user.id)
    user_proxies_data[uid] = {"shopify": [], "razorpay": []}
    save_json(USER_PROXIES_FILE, user_proxies_data)
    try:
        bot.edit_message_text(
            "<b>✅ PROXIES CLEARED</b>\n\nBoth pools empty.",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Proxy Menu", "menu_proxy", style="success")]))
    except Exception as e:
        logger.warning(f"clearproxies_confirm: {e}")


# ═══════════════════════════════════════════════════════════════════
# MSH INFO
# ═══════════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "menu_msh")
def cb_menu_msh(call):
    _ack(call)
    try:
        bot.edit_message_text(
            "<b>📦 SHOPIFY MASS CHECK</b>\n\n"
            "Upload a <b>.txt</b> of cards → one per line →\n"
            "bot checks against global sites + your proxies.\n\n"
            "Command: <code>/msh</code>\n"
            "Format: <code>CC|MM|YYYY|CVV</code>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML',
            reply_markup=ckb([cbtn("🔙 Back", "back_to_start", style="danger")]))
    except Exception as e:
        logger.warning(f"menu_msh: {e}")


# ═══════════════════════════════════════════════════════════════════
# OWNER PANEL
# ═══════════════════════════════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "own_root")
def cb_own_root(call):
    if not is_owner(call.from_user.id):
        _ack(call, "🚫 Owners only.", show_alert=True)
        return
    _ack(call)
    kb = ckb(
        [cbtn("🌐 Sites",          "menu_sites",           style="success"),
         cbtn("🛡️ Proxies",        "menu_proxy",           style="success")],
        [cbtn("🧪 Validate Sites", "site_validate_prompt", style="danger"),
         cbtn("🧹 Clean Pool",     "site_clean_global",    style="danger")],
        [cbtn("🔙 Back",           "back_to_start",        style="danger")],
    )
    try:
        bot.edit_message_text(
            f"<b>👑 OWNER PANEL</b>\n\n"
            f"🌐 Global sites: <b>{len(sites_data.get('sites', []))}</b>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            parse_mode='HTML', reply_markup=kb)
    except Exception as e:
        logger.warning(f"own_root: {e}")


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
        "Each site tested with 3 CCs.",
        parse_mode='HTML')


@bot.message_handler(commands=['listsites', 'listsite'])
def cmd_listsites(message):
    sites = sites_data.get('sites', [])
    if not sites:
        safe_send(bot.reply_to, message, "📭 No sites.")
        return
    lines = "\n".join(f"{i}. [#{s.get('id','?')}] {s.get('url','')}"
                      for i, s in enumerate(sites, 1))
    if len(lines) < 3500:
        safe_send(bot.reply_to, message,
            f"<b>🌐 GLOBAL SITES ({len(sites)})</b>\n\n{html.escape(lines)}",
            parse_mode='HTML')
    else:
        fname = f"sites_{message.chat.id}.txt"
        try:
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(lines)
            with open(fname, 'rb') as f:
                bot.send_document(message.chat.id, f, caption=f"🌐 {len(sites)} sites")
            os.remove(fname)
        except Exception as e:
            logger.warning(f"listsites: {e}")


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
        safe_send(bot.reply_to, message, f"❌ No site #{sid}.")
        return
    save_json(SITES_FILE, sites_data)
    safe_send(bot.reply_to, message, f"✅ Removed #{sid}. Remaining: {len(sites_data['sites'])}")


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
        f"✅ Added: <code>{html.escape(url)}</code>", parse_mode='HTML')


@bot.message_handler(commands=['mysites'])
def cmd_mysites(message):
    mine = get_user_sites(message.from_user.id)
    if not mine:
        safe_send(bot.reply_to, message,
            "📭 No personal sites.\nAdd: /addmysite <url>", parse_mode='HTML')
        return
    lines = "\n".join(f"{i}. [#{s.get('id','?')}] {s.get('url','')}"
                      for i, s in enumerate(mine, 1))
    safe_send(bot.reply_to, message,
        f"<b>🏠 MY SITES ({len(mine)})</b>\n\n{html.escape(lines)}",
        parse_mode='HTML')


@bot.message_handler(commands=['addpro', 'addproxy', 'addproxies'])
def cmd_addpro(message):
    if not _sub_gate(message):
        return
    if not is_user_allowed(message.from_user.id) and not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        # No proxy given → start file upload flow
        _set_await(message.from_user.id, 'awaiting_proxy_file')
        safe_send(bot.reply_to, message,
            "<b>📂 ADD PROXIES</b>\n\n"
            "Send a <b>.txt</b> file — one per line:\n"
            "<code>ip:port</code>\n"
            "<code>ip:port:user:pass</code>\n\n"
            "Or inline:\n"
            "<code>/addpro ip:port:user:pass</code>",
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
            f"⚠️ Already in {gate}:\n<code>{html.escape(norm)}</code>",
            parse_mode='HTML')
        return
    up[gate].append(norm)
    save_user_proxies_local(uid, up)
    safe_send(bot.reply_to, message,
        f"✅ Added to <b>{gate}</b>:\n<code>{html.escape(norm)}</code>\n\n"
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
        "<b>📂 Send proxy .txt now.</b>\n\nOne per line:\n"
        "<code>ip:port</code>\n<code>ip:port:user:pass</code>\n"
        "Optional: <code> shopify</code> or <code> razorpay</code>",
        parse_mode='HTML')


@bot.message_handler(commands=['clearproxies'])
def cmd_clearproxies(message):
    if not _sub_gate(message):
        return
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("⚠️ Yes, wipe both", callback_data="clearproxies_confirm"))
    kb.add(types.InlineKeyboardButton("❌ Cancel", callback_data="menu_proxy"))
    safe_send(bot.reply_to, message,
        "<b>⚠️ CONFIRM</b>\n\nDeletes ALL proxies.",
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
        safe_send(bot.reply_to, message, "📭 No proxies.")
        return
    status = bot.reply_to(message, f"🧹 Testing {len(all_p)}…")
    tested, live = 0, []
    last_upd = time.time()

    def _w(p):
        return p, test_proxy_for_shopify(p)

    with ThreadPoolExecutor(max_workers=50) as ex:
        futs = [ex.submit(_w, p) for p in all_p]
        for fut in as_completed(futs):
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
                        f"🧹 {tested}/{len(all_p)} · ✅ {len(live)}",
                        message.chat.id, status.message_id)
                    last_upd = now
                except Exception:
                    pass
    up['shopify'] = live
    save_user_proxies_local(uid, up)
    safe_send(bot.edit_message_text,
        f"✅ Cleaned: {len(live)}/{len(all_p)} live",
        message.chat.id, status.message_id)


@bot.message_handler(commands=['msh'])
def cmd_msh(message):
    if not _sub_gate(message):
        return
    if not is_user_allowed(message.from_user.id) and not is_owner(message.from_user.id):
        safe_send(bot.reply_to, message, "🚫 Premium required.")
        return
    _set_await(message.from_user.id, 'awaiting_cards_file')
    safe_send(bot.reply_to, message,
        "<b>📦 SHOPIFY MASS</b>\n\n"
        "Send a <b>.txt</b> of cards (one per line):\n"
        "<code>CC|MM|YYYY|CVV</code>",
        parse_mode='HTML')


@bot.message_handler(commands=['stop'])
def cmd_stop(message):
    safe_send(bot.reply_to, message, "⏸️ Stop requested.")


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
