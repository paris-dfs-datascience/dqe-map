# Running the Backend Pipeline

## Option A: Cloud Run Job (Production)

### Build and Deploy

```bash
cd dqe-map-backend

# Build container image and deploy Cloud Run Job
gcloud builds submit --config cloudbuild.yaml --project manifest-altar-490719-j7
```

This builds the Docker image, pushes to GCR, and creates/updates the Cloud Run Job with:

| Setting | Value |
|---------|-------|
| **Image** | `gcr.io/manifest-altar-490719-j7/dqe-battle-card-pipeline:latest` |
| **Tasks** | 4 (parallel shards) |
| **Memory** | 8Gi per task |
| **CPU** | 4 cores per task |
| **Timeout** | 24 hours |
| **Default env** | `MAX_WORKERS=3`, `MAX_ROWS=0` |
| **Secrets** | Mounted from Google Secret Manager |

### Execute the Job

From Cloud Console (easiest): **Cloud Run → Jobs → dqe-battle-card-job → Execute**.

Or via CLI:

```bash
gcloud run jobs execute dqe-battle-card-job \
  --project=manifest-altar-490719-j7 \
  --region=us-central1
```

Re-running is cheap for Step 3 alone: Steps 1 and 2 are skipped if their checkpoint flags are set. To force a full re-run (e.g., you uploaded a new EY CSV or rotated HubSpot data), delete the checkpoint first:

```bash
gsutil rm gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json
```

Then Execute the job as usual. On a completely cold bucket (no enriched CSV yet), run with `--tasks=1` the first time so only task 0 runs — tasks 1-3 don't poll and will fail if their inputs aren't yet in GCS.

### Run a Smoke Test First

Before a full 13k run, always do a small test:

```bash
gcloud run jobs update dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1 \
  --update-env-vars=MAX_ROWS=500

gcloud run jobs execute dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1
```

Verify the outputs:

```bash
gsutil cat gs://csv-battle-cards-dqe/EY-file/last_run.txt
```

Reset to full run:

```bash
gcloud run jobs update dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1 \
  --update-env-vars=MAX_ROWS=0
```

### Scale Workers Up or Down

Default is `MAX_WORKERS=3` (per task × 4 tasks = 12 concurrent rows). If Gemini starts throwing heavy 429s, drop it to 2 or 1. If you have quota headroom and want to push throughput, try 4-6:

```bash
gcloud run jobs update dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1 \
  --update-env-vars=MAX_WORKERS=2
```

### Monitor

```bash
# View live logs
gcloud logging tail 'resource.type="cloud_run_job" AND resource.labels.job_name="dqe-battle-card-job"' \
  --project=manifest-altar-490719-j7

# List recent executions
gcloud run jobs executions list --job=dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1 --limit=5
```

Or in the Console: **Cloud Run → Jobs → dqe-battle-card-job → Executions → (current) → Logs**.

## Option B: Local Development

### Setup

```bash
cd dqe-map-backend

# Create venv (Debian 12+ requires this)
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# GCP auth (for GCS + Vertex AI)
gcloud auth application-default login --project=manifest-altar-490719-j7

# Pull secrets
export PROJECT_ID=manifest-altar-490719-j7
export GCS_BUCKET=csv-battle-cards-dqe
export GOOGLE_MAPS_API_KEY=$(gcloud secrets versions access latest --secret=google-maps-api-key)
export CONNECTBASE_TENANT_API_KEY=$(gcloud secrets versions access latest --secret=connectbase-tenant-api-key)
export CONNECTBASE_NETWORK_API_KEY=$(gcloud secrets versions access latest --secret=connectbase-network-api-key)
export HUBSPOT_ACCESS_TOKEN=$(gcloud secrets versions access latest --secret=hubspot-access-token)
export MAX_WORKERS=4
export MAX_ROWS=10  # 0 = all rows
```

### Run Full Pipeline

```bash
python battlecard_generator.py
```

When running locally:
- `CLOUD_RUN_TASK_INDEX` defaults to 0, `CLOUD_RUN_TASK_COUNT` defaults to 1 — single process handles all steps sequentially
- Output goes to a single shard; merge still runs (merges 1 shard → the final JSON)

### Run Individual Steps

```bash
python tenant_enrichment.py    # Step 1
python hubspot_update.py       # Step 2
# Steps 3+4 only run via battlecard_generator.py
```

## Pipeline Runtime Estimates

| Step | 500 rows | 13k rows |
|------|---------|----------|
| Tenant Enrichment (ConnectBase) | 5-10 min | 30-60 min |
| HubSpot Refresh | 2-5 min | 2-5 min |
| Step 3 (per-row LLM + match, 12 concurrent at default `MAX_WORKERS=3`) | 15-35 min | 5-9 hours |
| Shard Merge | 1-2 min | 1-2 min |
| **Total** | **~25-50 min** | **~6-10 hours** |

Actual runtime varies with Gemini RPM throttling. If you see heavy 429 retries, drop `MAX_WORKERS`. If you have headroom, raise it.

## Reset Outputs Manually

To force a full re-run from Step 1, delete the checkpoint. You can also clear derived outputs to start completely cold:

```bash
# Force Steps 1 + 2 to re-run
gsutil rm gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json

# Optional: also clear derived outputs (rarely needed — they're overwritten)
gsutil rm gs://csv-battle-cards-dqe/enriched-data/tenants_enriched.csv
gsutil rm gs://csv-battle-cards-dqe/hubspot-data/hubspot_companies.json
gsutil rm gs://csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json
gsutil rm 'gs://csv-battle-cards-dqe/csv-battle-cards/dqe_prospects_shard_*.json'
```

If you clear the derived outputs, run the next Execute with `--tasks=1` so only task 0 regenerates the shared inputs; tasks 1-3 don't wait for them.

## Docker (Local Build)

```bash
cd dqe-map-backend

docker build -t dqe-pipeline .

docker run \
  -e PROJECT_ID=manifest-altar-490719-j7 \
  -e GCS_BUCKET=csv-battle-cards-dqe \
  -e GOOGLE_MAPS_API_KEY=<key> \
  -e CONNECTBASE_TENANT_API_KEY=<key> \
  -e CONNECTBASE_NETWORK_API_KEY=<key> \
  -e HUBSPOT_ACCESS_TOKEN=<token> \
  -e MAX_ROWS=10 \
  -v $HOME/.config/gcloud:/root/.config/gcloud \
  dqe-pipeline
```
