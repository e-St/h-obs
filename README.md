# SwitchBot Hygrometer Data Ingestion Layer on Google Cloud

**Goal**: Rock-solid, low-cost (< $1-2/month in practice), fully managed serverless pipeline to poll SwitchBot API v1.1 every 5 minutes for 20+ hygrometers/thermo sensors. Raw data saved as partitioned **JSONL** in Cloud Storage for 10+ year retention and easy querying via BigQuery external tables. Supports aggregates, visualization in Looker Studio, and alerting via Cloud Monitoring.

## Architecture (Managed + Low Cost)
- **Trigger**: Cloud Scheduler (cron `*/5 * * * *`) → HTTP call to Cloud Function (authenticated)
- **Compute**: Cloud Functions (2nd gen, Python) — pay-per-use, scales to zero, negligible cost for ~9k invocations/month
- **Secrets**: Secret Manager (token + secret)
- **Raw Storage (Long-term)**: Cloud Storage (multi/dual-region bucket) with lifecycle rules (Standard → Nearline → Coldline). JSONL files partitioned by `dt=YYYY-MM-DD/hh=HH/` for Hive-style access. Durability 11 9's. Cost: pennies/year even after 10 years.
- **Query Layer (Aggregates)**: BigQuery **external table** over the GCS JSONL (no data duplication, zero BQ storage cost for raw). Partition pruning on date/hour. Write simple SQL for daily/hourly averages, min/max, trends per device.
- **Visualization & Dashboards**: Looker Studio (free, managed) — direct BigQuery connector, beautiful time-series charts, gauges, filters by device/date. Schedule email/PDF reports.
- **Alerts**: 
  - Built-in: Threshold checks in the function (configurable via env vars) → structured JSON logs → Cloud Monitoring **log-based alerting policy** → email/Slack/PagerDuty notification.
  - For aggregate alerts (e.g. daily avg humidity low): Add a second scheduled Cloud Function or use BigQuery scheduled queries + similar logging.
- **Why this stack?** Maximally managed (no servers, no ops), extremely low cost, serverless, easy to extend (add more devices/sensors automatically discovered), future-proof raw archive + queryable.

**Total estimated cost**: <$0.50–2/month for 20 devices @ 5min interval (mostly Scheduler + tiny CF + GCS + BQ metadata). Fits easily under $10.

## Prerequisites
- Google Cloud Project with billing enabled
- SwitchBot account with 20+ hygrometers (Meter, Meter Plus, WoIOSensor / Indoor-Outdoor Thermo-Hygrometer, etc.) and **Cloud Service enabled** + Hub connected
- `gcloud` CLI authenticated (`gcloud auth login && gcloud config set project YOUR_PROJECT`)
- (Optional but recommended) Terraform or just follow gcloud commands below

## Step 1: Get SwitchBot API Credentials (v1.1)
1. Open SwitchBot mobile app
2. Profile (bottom right) → Preferences → tap "About" or version number ~10 times to unlock Developer Options
3. Go to Developer Options
4. Copy **API token** and **Secret key** (both needed for v1.1 signed requests)
5. Keep them safe — never commit to code

## Step 2: Create Resources (gcloud or Console)
Run these in Cloud Shell or your terminal.

```bash
# Set your project
echo 'export PROJECT_ID="h-obs-500318"' >> ~/.bashrc
export PROJECT_ID="h-obs-500318"

gcloud config set project $PROJECT_ID


# Enable required APIs (one-time)
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com

# Create service account for the function (least privilege)
gcloud iam service-accounts create switchbot-ingest-sa \
  --display-name="SwitchBot Data Ingestion SA" --project "$PROJECT_ID"

# Grant minimal roles
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:switchbot-ingest-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:switchbot-ingest-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectCreator"

# (Optional) If you later add BQ native loads: roles/bigquery.dataEditor
```

### Create Secret (recommended: one JSON secret)
```bash
echo '{"token":"YOUR_SWITCHBOT_TOKEN_HERE","secret":"YOUR_SWITCHBOT_SECRET_HERE"}' | \
  gcloud secrets create sb-creds --data-file=-

# Grant access to the SA
gcloud secrets add-iam-policy-binding sb-creds \
  --member="serviceAccount:switchbot-ingest-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding h-obs-500318 \
 --member=serviceAccount:558626352676-compute@developer.gserviceaccount.com \
 --role=roles/cloudbuild.builds.builder 
```


### Create GCS Bucket (multi-region for durability, or dual-region e.g. nam4/eur4)
```bash
# Example: Multi-region US (change to EU or specific dual-region as needed)
gsutil mb -p $PROJECT_ID -l EU -b on gs://h-obs-sws101-raw-eu

# Enable uniform bucket-level access (recommended)
gsutil uniformbucketlevelaccess set on gs://h-obs-sws101-raw-eu

# Lifecycle policy: move to cheaper storage over time (edit lifecycle.json as needed)
cat > /tmp/lifecycle.json << 'EOF'
{
  "rule": [
    {
      "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
      "condition": {"age": 90}
    },
    {
      "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
      "condition": {"age": 365}
    }
  ]
}
EOF
gsutil lifecycle set /tmp/lifecycle.json gs://h-obs-sws101-raw-eu

# Bucket retention policy for compliance / prevent early deletion (e.g. 10 years)
gsutil retention set 10y gs://h-obs-sws101-raw-eu
```


## Step 3: Deploy the Cloud Function
The code auto-discovers all devices and saves status for everything (hygrometers + other devices = complete raw history).

```bash
cd switchbot-ingestion/   # where switchbot_ingest.py and requirements.txt are

gcloud functions deploy ingest-switchbot-data \
  --gen2 \
  --runtime python312 \
  --region europe-west3 \
  --source . \
  --entry-point ingest_switchbot_data \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account switchbot-ingest-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --set-env-vars "GCS_BUCKET=h-obs-sws101-raw-eu,SWITCHBOT_SECRET_NAME=sb-creds,GOOGLE_CLOUD_PROJECT=$PROJECT_ID" \
  --set-env-vars "ALERT_MIN_HUMIDITY=25,ALERT_MAX_TEMP=38,ALERT_MIN_TEMP=10" \
  --memory 256MB \
  --timeout 60s \
  --max-instances 1
```

**Test locally first (optional)**: Use `functions-framework --target ingest_switchbot_data` after setting env vars (but needs real creds + internet).

After deploy, note the **HTTPS trigger URL** from output (or `gcloud functions describe ...`).

## Step 4: Create Cloud Scheduler Job (the cron)
```bash
gcloud scheduler jobs create http ingest-switchbot-cron \
  --location europe-west3 \
  --schedule "*/5 * * * *" \
  --uri "https://europe-west3-h-obs-500318.cloudfunctions.net/ingest-switchbot-data" \
  --http-method POST \
  --oidc-service-account-email switchbot-ingest-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --oidc-token-audience "https://europe-west3-h-obs-500318.cloudfunctions.net/ingest-switchbot-data" \
  --attempt-deadline 60s \
  --max-retry-attempts 3
```

This runs every 5 minutes. Change schedule if you prefer (e.g. `*/10 * * * *` for ~half the API calls).

**Monitor**: In Cloud Console → Cloud Scheduler or Cloud Functions → Logs.

## Step 5: BigQuery External Table (for easy querying & aggregates)
No data copy — queries the JSONL directly from GCS (partitioned & pruned automatically).
```
bq --location=EU mk --dataset h-obs-500318:switchbot_analytics

echo '{}' | gsutil cp - gs://h-obs-sws101-raw-eu/switchbot_raw/dt=2026-06-23/hh=00/placeholder.jsonl


# 2. Create the external table (save to a file to avoid quoting hell)
cat > /tmp/create_ext_table.sql << 'EOF'
CREATE OR REPLACE EXTERNAL TABLE `h-obs-500318.switchbot_analytics.switchbot_raw_readings`
(
  ingestion_timestamp TIMESTAMP,
  run_id              STRING,
  device_id           STRING,
  device_name         STRING,
  device_type         STRING,
  hub_device_id       STRING,
  temperature         FLOAT64,
  humidity            INT64,
  battery             INT64,
  raw_body            STRING,
  api_status_code     INT64,
  api_message         STRING,
  error               STRING
)
WITH PARTITION COLUMNS (
  dt DATE,
  hh INT64
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://h-obs-sws101-raw-eu/switchbot_raw/*'],
  hive_partition_uri_prefix = 'gs://h-obs-sws101-raw-eu/switchbot_raw',
  max_bad_records = 0
);
EOF

bq query \
  --location=EU \
  --nouse_legacy_sql \
  --project_id=h-obs-500318 \
  "$(cat /tmp/create_ext_table.sql)"

```


**Test**:
```sql
SELECT 
  dt,
  device_name,
  device_type,
  AVG(temperature) as avg_temp_c,
  MIN(humidity) as min_humidity,
  COUNT(*) as readings
FROM `h-obs-500318.switchbot_analytics.switchbot_raw_readings`
WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND temperature IS NOT NULL
GROUP BY dt, device_name, device_type
ORDER BY dt DESC, device_name;
```

**Example useful views** (create once):
```sql
-- Latest reading per device (great for dashboards)
CREATE OR REPLACE VIEW `h-obs-500318.switchbot_analytics.latest_readings` AS
SELECT * EXCEPT(rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY ingestion_timestamp DESC) as rn
  FROM `your_project.switchbot_analytics.switchbot_raw_readings`
)
WHERE rn = 1;

-- Daily aggregates
CREATE OR REPLACE VIEW `h-obs-500318.switchbot_analytics.daily_aggregates` AS
SELECT
  dt,
  device_id,
  device_name,
  device_type,
  AVG(temperature) as avg_temp,
  MIN(temperature) as min_temp,
  MAX(temperature) as max_temp,
  AVG(humidity) as avg_humidity,
  MIN(humidity) as min_humidity,
  MAX(humidity) as max_humidity,
  COUNT(*) as reading_count
FROM `your_project.switchbot_analytics.switchbot_raw_readings`
WHERE temperature IS NOT NULL OR humidity IS NOT NULL
GROUP BY dt, device_id, device_name, device_type;
```

## Step 6: Visualization in Looker Studio (Free & Easy)
1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Create → Data Source → BigQuery → select your project/dataset → the external table or the views above
3. Create new Report
4. Add charts:
   - Time series line chart: Date vs avg_temp / humidity, breakdown by device_name
   - Scorecard / Gauge for current values (use `latest_readings` view)
   - Table with filters
   - Heatmap or bar for daily aggregates
5. Add date range control, device filter (dropdown)
6. Style beautifully (colors for temp/humidity)
7. Share with team or schedule email delivery of the dashboard (PDF or link)

**Pro tip**: Use the `daily_aggregates` or `latest_readings` views for faster dashboards.

## Step 7: Alerts (Thresholds + Monitoring)
The function already logs structured warnings when thresholds are breached (see `check_and_log_alerts`).

### Create Log-based Alerting Policy
1. Cloud Console → Monitoring → Alerting → Create Policy
2. Condition: "Log match" or create a logs-based metric first (Metrics → Logs-based metric → Counter on `jsonPayload.event="threshold_breach"`)
3. Or directly: Resource type = Cloud Function, log filter like:
   ```
   resource.type="cloud_function"
   jsonPayload.event="threshold_breach"
   ```
4. Set threshold: e.g. > 0 breaches in 5 minutes
5. Notification channels: Add Email, Slack, or PagerDuty (free tier available)
6. Name it "SwitchBot Sensor Threshold Breach"

You will get notified (email etc.) almost immediately when e.g. humidity drops below your `ALERT_MIN_HUMIDITY`.

For **aggregate-based alerts** (e.g. "daily min humidity < 20% for any device"):
- Create a second Cloud Function (similar pattern) scheduled daily that runs a BigQuery query on the view and logs `threshold_breach` if condition met.
- Or use BigQuery scheduled queries to write alerts to a monitoring table and alert on that.

## Maintenance & Tips
- **Change polling frequency**: Update Scheduler schedule + consider rate limit (10,000 calls/day). 5 min = safe for 20–50 devices.
- **Add/remove devices**: Automatic (code discovers all physical devices every run).
- **Schema evolution**: New fields from SwitchBot appear in `raw_body` and are extracted if you extend the code. JSONL is flexible.
- **Reprocess old data**: All raw in GCS forever. Reload to new BQ table or query external anytime.
- **Cost control**: Monitor with Billing reports or `gcloud billing budgets create`. Set alerts at $5.
- **Security**: Token/secret never in code or logs. Function has least-privilege SA. Bucket uniform access.
- **High availability**: Multi-region bucket + Cloud Functions multi-region possible. Scheduler is global.
- **Webhook alternative** (future improvement): SwitchBot supports webhooks for real-time push on Meter devices. Can replace polling with a Cloud Run service receiving webhooks → even lower cost/API usage.
- **Scaling**: Works for hundreds of devices (still cheap). For 1000s consider Pub/Sub + Dataflow, but overkill here.

## Files in this package
- `switchbot_ingest.py` — Production-ready Cloud Function code (with retries, structured logging, alerts, full raw preservation)
- `requirements.txt` — Minimal deps

## Next Steps / Roadmap for your full data layer
1. This raw ingestion (done)
2. Add daily/ hourly materialized aggregate tables via BigQuery scheduled queries (or dbt / Dataform)
3. Advanced viz & self-serve analytics in Looker Studio or connected BI tool
4. (Optional) Stream recent data to a fast dashboard DB or Pub/Sub for real-time
5. Long-term: Consider switching hot path to Parquet in GCS + native BQ table for even better query perf (easy migration)

This gives you a **production-grade, future-proof, low-maintenance data ingestion layer** that will still be accessible and queryable in 2036+.

Questions or customizations (e.g. more sensors, different regions, Terraform version)? Let me know!

**You are now ready to deploy.** Copy the files, fill in your values, and run the gcloud commands. The whole thing takes ~30-60 minutes to set up end-to-end.
