import hashlib
import hmac
import os
import time
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.responses import FileResponse
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "h-obs-500318")
DATASET    = os.environ.get("BQ_DATASET", "switchbot_analytics")
TABLE      = os.environ.get("BQ_TABLE",   "switchbot_raw_readings")
COOKIE     = "sb_session"

app = FastAPI(docs_url=None, redoc_url=None)
bq  = bigquery.Client(project=PROJECT_ID)

# ── Password (Secret Manager → env var fallback) ───────────────────────────
def _load_password() -> str:
    try:
        from google.cloud import secretmanager
        sm   = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/dashboard-password/versions/latest"
        resp = sm.access_secret_version(request={"name": name})
        return resp.payload.data.decode("utf-8").strip()
    except Exception:
        pw = os.environ.get("DASHBOARD_PASSWORD", "")
        if not pw:
            raise RuntimeError(
                "Configure 'dashboard-password' in Secret Manager or set DASHBOARD_PASSWORD env var"
            )
        return pw

_PASSWORD: str = _load_password()

# ── Rate limiting (brute-force protection on /login) ───────────────────────
_LOGIN_ATTEMPTS: dict[str, list[float]] = defaultdict(list)
_RL_WINDOW = 300   # 5-minute sliding window
_RL_MAX    = 5     # max attempts per window per IP

def _client_ip(request: Request) -> str:
    # Cloud Run sets X-Forwarded-For; fall back to direct connection
    forwarded = request.headers.get("x-forwarded-for", "")
    return forwarded.split(",")[0].strip() or request.client.host or "unknown"

def _is_rate_limited(ip: str) -> bool:
    now = time.monotonic()
    window = [t for t in _LOGIN_ATTEMPTS[ip] if now - t < _RL_WINDOW]
    _LOGIN_ATTEMPTS[ip] = window
    if len(window) >= _RL_MAX:
        return True
    _LOGIN_ATTEMPTS[ip].append(now)
    return False

def _session_token() -> str:
    """Deterministic token derived from password — stable across instances/restarts."""
    return hashlib.sha256(f"sb-dashboard:{_PASSWORD}".encode()).hexdigest()

def _is_auth(request: Request) -> bool:
    cookie = request.cookies.get(COOKIE, "")
    return hmac.compare_digest(cookie, _session_token())

# ── Auth guard ─────────────────────────────────────────────────────────────
@app.middleware("http")
async def auth_guard(request: Request, call_next):
    if request.url.path in {"/login", "/healthz"}:
        return await call_next(request)
    if not _is_auth(request):
        if request.url.path.startswith("/api/"):
            return JSONResponse({"error": "unauthenticated"}, status_code=401)
        return RedirectResponse("/login", status_code=303)
    return await call_next(request)

# ── Login routes ───────────────────────────────────────────────────────────
_LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>SwitchBot Monitor · Login</title>
  <style>
    body{{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:40px;width:100%;max-width:360px}}
    h1{{font-size:18px;font-weight:600;margin-bottom:8px}}
    p{{font-size:13px;color:#6e7681;margin-bottom:28px}}
    label{{display:block;font-size:12px;color:#8b949e;margin-bottom:6px}}
    input{{width:100%;padding:9px 12px;background:#0d1117;border:1px solid #30363d;border-radius:8px;
           color:#e6edf3;font-size:14px;outline:none;box-sizing:border-box}}
    input:focus{{border-color:#58a6ff}}
    button{{margin-top:16px;width:100%;padding:10px;background:#238636;border:none;border-radius:8px;
            color:#fff;font-size:14px;font-weight:500;cursor:pointer}}
    button:hover{{background:#2ea043}}
    .err{{margin-top:14px;font-size:13px;color:#f85149;text-align:center}}
  </style>
</head>
<body>
  <div class="card">
    <h1>SwitchBot Monitor</h1>
    <p>Enter your password to continue.</p>
    <form method="post" action="/login">
      <label for="pw">Password</label>
      <input id="pw" type="password" name="password" autofocus required/>
      <button type="submit">Sign in</button>
      {error}
    </form>
  </div>
</body>
</html>"""


@app.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    if _is_auth(request):
        return RedirectResponse("/", status_code=303)
    return _LOGIN_PAGE.format(error="")


@app.post("/login")
async def login_post(request: Request, password: str = Form(...)):
    ip = _client_ip(request)
    if _is_rate_limited(ip):
        return HTMLResponse(
            _LOGIN_PAGE.format(error='<p class="err">Too many attempts. Try again in 5 minutes.</p>'),
            status_code=429,
        )
    provided = hashlib.sha256(password.encode()).hexdigest()
    expected = hashlib.sha256(_PASSWORD.encode()).hexdigest()
    if hmac.compare_digest(provided, expected):
        resp = RedirectResponse("/", status_code=303)
        resp.set_cookie(
            COOKIE, _session_token(),
            httponly=True, secure=True, samesite="lax",
            max_age=60 * 60 * 24 * 30,  # 30 days
        )
        return resp
    return HTMLResponse(
        _LOGIN_PAGE.format(error='<p class="err">Incorrect password.</p>'),
        status_code=401,
    )


@app.get("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie(COOKIE)
    return resp


# ── BigQuery API ───────────────────────────────────────────────────────────
_QUERY = f"""
SELECT
  ingestion_timestamp,
  device_name,
  device_type,
  temperature,
  humidity
FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND (temperature IS NOT NULL OR humidity IS NOT NULL)
ORDER BY ingestion_timestamp
"""


@app.get("/api/readings")
async def readings():
    rows = list(bq.query(_QUERY).result())
    return JSONResponse({
        "readings": [
            {
                "timestamp":   row.ingestion_timestamp.isoformat(),
                "device_name": row.device_name or "Unknown",
                "device_type": row.device_type or "",
                "temperature": row.temperature,
                "humidity":    row.humidity,
            }
            for row in rows
        ]
    })


@app.get("/healthz")
async def health():
    return {"status": "ok"}


# ── Dashboard UI ──────────────────────────────────────────────────────────
_INDEX = Path(__file__).parent / "static" / "index.html"

@app.get("/")
async def index():
    return FileResponse(str(_INDEX))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
