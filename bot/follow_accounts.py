"""
follow_accounts.py
──────────────────
Follows all accounts in actors.py (ACTORS + SIGNAL_ACCOUNTS) from the bot's
Twitter account using OAuth 1.0a. Safe to re-run — already-followed accounts
are skipped silently.

Usage:
    python follow_accounts.py            # follow everyone in actors.py
    python follow_accounts.py --dry-run  # just print who would be followed
"""

import sys
import time
import urllib.request
import urllib.parse
import urllib.error
import ssl
import json
import hmac
import hashlib
import base64
import os
import secrets
from datetime import datetime, timezone

# ── Credentials ───────────────────────────────────────────────────────────────
API_KEY             = "0nCu7K5n3tivpb3dKesZWLlTa"
API_SECRET          = "NeZAVMNXcwY8Klt7k3LEwxTnG8dI2o71jctsJfwxooOngscerv"
ACCESS_TOKEN        = "1753282899263266816-56vR9C0kbC6wozan5580XXa51ApdX9"
ACCESS_TOKEN_SECRET = "6gpt4462aDEtWkYT58YtfIYZwdoHaoJDjvPuMrih5t08Q"
BEARER_TOKEN        = urllib.parse.unquote(
    "AAAAAAAAAAAAAAAAAAAAAMmx9QEAAAAAoApYsZULN5uPaX5fUo7s2JLnjwQ%3DFyAbOIACiOTCZkaRS37HToLtG2sE96BBPG0bXhd66mJ0yOuKUR"
)
BOT_USER_ID         = "1753282899263266816"

DRY_RUN = "--dry-run" in sys.argv

# SSL context (macOS cert workaround)
_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE

# ── OAuth 1.0a helper ─────────────────────────────────────────────────────────

def _percent_encode(s: str) -> str:
    return urllib.parse.quote(str(s), safe="")

def _oauth_header(method: str, url: str, params: dict) -> str:
    oauth_params = {
        "oauth_consumer_key":     API_KEY,
        "oauth_nonce":            secrets.token_hex(16),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp":        str(int(datetime.now(timezone.utc).timestamp())),
        "oauth_token":            ACCESS_TOKEN,
        "oauth_version":          "1.0",
    }
    all_params = {**params, **oauth_params}
    sorted_params = "&".join(
        f"{_percent_encode(k)}={_percent_encode(v)}"
        for k, v in sorted(all_params.items())
    )
    base_string = "&".join([
        _percent_encode(method.upper()),
        _percent_encode(url),
        _percent_encode(sorted_params),
    ])
    signing_key = f"{_percent_encode(API_SECRET)}&{_percent_encode(ACCESS_TOKEN_SECRET)}"
    signature = base64.b64encode(
        hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha1).digest()
    ).decode()
    oauth_params["oauth_signature"] = signature
    header_parts = ", ".join(
        f'{_percent_encode(k)}="{_percent_encode(v)}"'
        for k, v in sorted(oauth_params.items())
    )
    return f"OAuth {header_parts}"

# ── Twitter API helpers ───────────────────────────────────────────────────────

def resolve_handles(handles: list[str]) -> dict[str, str]:
    """Returns {handle_lower: user_id}"""
    resolved = {}
    for i in range(0, len(handles), 100):
        batch = handles[i:i + 100]
        ids_param = ",".join(batch)
        url = f"https://api.twitter.com/2/users/by?usernames={ids_param}"
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {BEARER_TOKEN}"}
        )
        try:
            with urllib.request.urlopen(req, context=_ctx) as resp:
                data = json.loads(resp.read())
            for u in data.get("data", []):
                resolved[u["username"].lower()] = u["id"]
            for err in data.get("errors", []):
                print(f"  [resolve] skipping @{err.get('value','?')}: {err.get('detail','unknown')}")
        except Exception as e:
            print(f"  [resolve] batch error: {e}")
        time.sleep(0.5)
    return resolved

def follow_user(target_id: str) -> str:
    """Returns 'followed', 'already', or 'error:<msg>'"""
    url = f"https://api.twitter.com/2/users/{BOT_USER_ID}/following"
    body = json.dumps({"target_user_id": target_id}).encode()
    auth = _oauth_header("POST", url, {})
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization":  auth,
            "Content-Type":   "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, context=_ctx) as resp:
            data = json.loads(resp.read())
        if data.get("data", {}).get("following"):
            return "followed"
        if data.get("data", {}).get("pending_follow"):
            return "pending"
        return f"unexpected:{data}"
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        if e.code == 403 and "already" in body_text.lower():
            return "already"
        return f"error:{e.code} {body_text[:120]}"
    except Exception as e:
        return f"error:{e}"

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import importlib.util, os as _os
    _os.environ.setdefault("DATABASE_URL", "x")
    _os.environ.setdefault("TELEGRAM_BOT_TOKEN", "x")
    _os.environ.setdefault("TELEGRAM_CHAT_ID", "x")

    spec = importlib.util.spec_from_file_location(
        "actors",
        os.path.join(os.path.dirname(__file__), "actors.py")
    )
    actors_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(actors_mod)

    all_handles = actors_mod.ALL_HANDLES + actors_mod.ALL_SIGNAL_HANDLES
    unique_handles = list(dict.fromkeys(h.lower() for h in all_handles))

    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Total unique handles to follow: {len(unique_handles)}")

    if DRY_RUN:
        for h in unique_handles:
            print(f"  would follow @{h}")
        return

    print("Resolving handles → user IDs …")
    id_map = resolve_handles(unique_handles)
    print(f"Resolved {len(id_map)}/{len(unique_handles)} handles\n")

    followed = already = pending = errors = 0
    for handle in unique_handles:
        uid = id_map.get(handle)
        if not uid:
            print(f"  @{handle:<35} ⚠ not resolved (suspended/private?)")
            errors += 1
            continue

        result = follow_user(uid)
        if result == "followed":
            print(f"  @{handle:<35} ✅ followed")
            followed += 1
        elif result == "already":
            print(f"  @{handle:<35} ✓  already following")
            already += 1
        elif result == "pending":
            print(f"  @{handle:<35} ⏳ follow request sent (protected account)")
            pending += 1
        else:
            print(f"  @{handle:<35} ❌ {result}")
            errors += 1

        time.sleep(1.2)   # stay well under rate limits

    print(f"\n── Done ──────────────────────────────────────────")
    print(f"  New follows:      {followed}")
    print(f"  Already followed: {already}")
    print(f"  Pending:          {pending}")
    print(f"  Errors/skipped:   {errors}")

if __name__ == "__main__":
    main()
