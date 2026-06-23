#!/usr/bin/env python3
"""
Cloud Function (Gen 2) for ingesting SwitchBot Hygrometer data via v1.1 API.
Polls devices and statuses, saves raw enriched readings as partitioned JSONL to GCS.
Designed for low cost, fully managed, rock-solid scheduled execution via Cloud Scheduler.

Environment variables (set at deploy):
- GCS_BUCKET: Name of the target GCS bucket (e.g. my-switchbot-raw-archive)
- SWITCHBOT_SECRET_NAME: Name of Secret Manager secret containing JSON {"token": "...", "secret": "..."}
"""

import os
import json
import time
import hashlib
import hmac
import base64
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import functions_framework
import requests
from google.cloud import secretmanager, storage

# Configure structured logging for Cloud Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Config
DEFAULT_TIMEOUT = 15  # seconds per API call
MAX_RETRIES = 2
RETRY_BACKOFF = 1.5  # seconds


def get_switchbot_credentials(project_id: str, secret_name: str) -> Dict[str, str]:
    """Fetch token and secret from Secret Manager. Expects JSON payload: {"token": "...", "secret": "..."}"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8").strip()
        creds = json.loads(payload)
        if not isinstance(creds, dict) or "token" not in creds or "secret" not in creds:
            raise ValueError("Secret must be JSON with 'token' and 'secret' keys")
        logger.info("Successfully retrieved SwitchBot credentials from Secret Manager")
        return creds
    except Exception as e:
        logger.error(f"Failed to access secret {secret_name}: {e}")
        raise


def generate_sign_and_t(token: str, secret: str) -> tuple[str, str]:
    """Generate fresh signature and millisecond timestamp for v1.1 auth (must be done per request)."""
    t = int(round(time.time() * 1000))
    string_to_sign = f"{token}{t}"
    string_to_sign_bytes = string_to_sign.encode("utf-8")
    secret_bytes = secret.encode("utf-8")
    sign = base64.b64encode(
        hmac.new(secret_bytes, msg=string_to_sign_bytes, digestmod=hashlib.sha256).digest()
    ).decode("utf-8")
    return sign, str(t)


def call_switchbot_api(
    url: str,
    token: str,
    secret: str,
    method: str = "GET",
    json_data: Optional[Dict] = None,
    timeout: int = DEFAULT_TIMEOUT
) -> Dict[str, Any]:
    """Make authenticated call to SwitchBot v1.1 API with retries for transient errors."""
    headers_base = {
        "Content-Type": "application/json",
        "nonce": ""
    }

    last_exception = None
    for attempt in range(MAX_RETRIES + 1):
        sign, t = generate_sign_and_t(token, secret)
        headers = {
            **headers_base,
            "Authorization": token,
            "sign": sign,
            "t": t,
        }
        try:
            if method.upper() == "GET":
                resp = requests.get(url, headers=headers, timeout=timeout)
            else:
                resp = requests.post(url, headers=headers, json=json_data, timeout=timeout)

            if resp.status_code == 429:
                # Rate limit - back off longer
                wait = RETRY_BACKOFF * (2 ** attempt) + 5
                logger.warning(f"Rate limited (429) on {url}, attempt {attempt+1}, waiting {wait}s")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            logger.debug(f"API call to {url} succeeded with statusCode={data.get('statusCode')}")
            return data

        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning(f"Transient error calling {url} (attempt {attempt+1}): {e}. Retrying in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"Failed to call {url} after {MAX_RETRIES+1} attempts: {e}")
                raise

    raise last_exception or RuntimeError("API call failed")


def fetch_all_device_statuses(token: str, secret: str) -> List[Dict[str, Any]]:
    """Fetch list of all devices, then status for each. Returns enriched list of readings."""
    devices_url = "https://api.switch-bot.com/v1.1/devices"
    devices_resp = call_switchbot_api(devices_url, token, secret)

    if devices_resp.get("statusCode") != 100:
        logger.error(f"Get devices failed: {devices_resp.get('message')} (code {devices_resp.get('statusCode')})")
        raise RuntimeError(f"Device list fetch failed: {devices_resp}")

    device_list: List[Dict] = devices_resp.get("body", {}).get("deviceList", [])
    logger.info(f"Discovered {len(device_list)} physical devices in account")

    readings: List[Dict[str, Any]] = []
    ingestion_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    run_id = f"run_{int(time.time() * 1000)}"

    for dev in device_list:
        dev_id: str = dev.get("deviceId", "")
        if not dev_id:
            continue

        dev_name = dev.get("deviceName", "unknown")
        dev_type = dev.get("deviceType", "unknown")
        hub_id = dev.get("hubDeviceId")

        status_url = f"https://api.switch-bot.com/v1.1/devices/{dev_id}/status"
        try:
            status_resp = call_switchbot_api(status_url, token, secret)
            status_code = status_resp.get("statusCode")
            body: Dict = status_resp.get("body", {}) or {}

            record = {
                "ingestion_timestamp": ingestion_ts,
                "run_id": run_id,
                "device_id": dev_id,
                "device_name": dev_name,
                "device_type": dev_type,
                "hub_device_id": hub_id,
                "temperature": body.get("temperature"),
                "humidity": body.get("humidity"),
                "battery": body.get("battery"),
                "raw_body": body,
                "api_status_code": status_code,
                "api_message": status_resp.get("message"),
            }

            if status_code == 100:
                logger.info(f"✓ {dev_name} ({dev_type}): T={body.get('temperature')}°C H={body.get('humidity')}%")
            else:
                logger.warning(f"⚠ Status error for {dev_name} ({dev_id}): code={status_code} msg={status_resp.get('message')}")

            readings.append(record)

        except Exception as exc:
            logger.exception(f"Failed to fetch status for device {dev_id} ({dev_name})")
            readings.append({
                "ingestion_timestamp": ingestion_ts,
                "run_id": run_id,
                "device_id": dev_id,
                "device_name": dev_name,
                "device_type": dev_type,
                "hub_device_id": hub_id,
                "api_status_code": None,
                "error": str(exc),
                "api_message": "exception during status fetch"
            })

    return readings


def save_readings_to_gcs_jsonl(
    bucket_name: str,
    readings: List[Dict[str, Any]],
    ingestion_ts: str,
    run_id: str
) -> str:
    """Save list of readings as a single newline-delimited JSON (JSONL) file in date/hour partitioned path."""
    if not readings:
        logger.info("No readings to save.")
        return ""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Parse for Hive-style partitioning (easy for BigQuery external tables / Spark)
    dt = datetime.fromisoformat(ingestion_ts.replace("Z", "+00:00"))
    date_partition = dt.strftime("%Y-%m-%d")
    hour_partition = dt.strftime("%H")
    file_timestamp = dt.strftime("%Y%m%dT%H%M%S")

    blob_name = (
        f"switchbot_raw/"
        f"dt={date_partition}/"
        f"hh={hour_partition}/"
        f"switchbot_{file_timestamp}_{run_id}.jsonl"
    )

    blob = bucket.blob(blob_name)

    # JSON Lines format (one JSON object per line)
    lines = [json.dumps(r, ensure_ascii=False, default=str) for r in readings]
    content = "\n".join(lines) + "\n"

    blob.upload_from_string(
        content,
        content_type="application/x-ndjson",
        # Add metadata for traceability
        metadata={
            "source": "switchbot-api-v1.1",
            "run_id": run_id,
            "ingestion_timestamp": ingestion_ts,
            "num_readings": str(len(readings))
        }
    )

    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    logger.info(f"Saved {len(readings)} readings → {gcs_uri}")
    return gcs_uri


@functions_framework.http
def ingest_switchbot_data(request) -> tuple[Dict[str, Any], int]:
    """
    Main entrypoint for Cloud Function (HTTP trigger).
    Called by Cloud Scheduler on cron schedule.
    Returns JSON + HTTP status for Scheduler retry logic.
    """
    start_time = time.time()
    logger.info("=== Starting SwitchBot data ingestion run ===")

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    if not project_id:
        logger.error("GOOGLE_CLOUD_PROJECT env var not set")
        return {"status": "error", "message": "Missing project ID"}, 500

    bucket_name = os.environ.get("GCS_BUCKET")
    if not bucket_name:
        logger.error("GCS_BUCKET env var not set")
        return {"status": "error", "message": "Missing GCS_BUCKET"}, 500

    secret_name = os.environ.get("SWITCHBOT_SECRET_NAME", "switchbot-credentials")

    try:
        creds = get_switchbot_credentials(project_id, secret_name)
        token = creds["token"]
        secret = creds["secret"]

        readings = fetch_all_device_statuses(token, secret)

        gcs_uri = ""
        if readings:
            ingestion_ts = readings[0]["ingestion_timestamp"] if readings else datetime.now(timezone.utc).isoformat()
            gcs_uri = save_readings_to_gcs_jsonl(bucket_name, readings, ingestion_ts, readings[0]["run_id"] if readings else "no-run")

        duration = time.time() - start_time
        logger.info(f"=== Ingestion complete in {duration:.2f}s. Readings: {len(readings)}. GCS: {gcs_uri or 'none'} ===")

        return {
            "status": "success",
            "readings_collected": len(readings),
            "gcs_uri": gcs_uri,
            "duration_seconds": round(duration, 2),
            "run_id": readings[0]["run_id"] if readings else None
        }, 200

    except Exception as exc:
        logger.exception("Ingestion run failed")
        duration = time.time() - start_time
        return {
            "status": "error",
            "message": str(exc),
            "duration_seconds": round(duration, 2)
        }, 500  # 5xx triggers Scheduler retry if configured


if __name__ == "__main__":
    # For local testing (requires GOOGLE_APPLICATION_CREDENTIALS + env vars)
    print("Local test mode. Set env vars and run with functions-framework or directly.")
    # Example: python switchbot_ingest.py  (but http decorator needs framework)
    pass
