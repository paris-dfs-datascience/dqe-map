# Environment Variables & Secrets

## Frontend (.env file)

Create a `.env` file in the `dqe-map/` root directory. These are baked into the build at compile time.

```bash
# Google Maps (client-side, should be restricted by domain in GCP console)
REACT_APP_GOOGLE_MAPS_API_KEY=<your_key>

# Firebase (all client-side public keys, safe to expose)
REACT_APP_FIREBASE_API_KEY=<your_key>
REACT_APP_FIREBASE_AUTH_DOMAIN=manifest-altar-490719-j7.firebaseapp.com
REACT_APP_FIREBASE_PROJECT_ID=manifest-altar-490719-j7
REACT_APP_FIREBASE_STORAGE_BUCKET=manifest-altar-490719-j7.firebasestorage.app
REACT_APP_FIREBASE_MESSAGING_SENDER_ID=77431910624
REACT_APP_FIREBASE_APP_ID=<your_key>
REACT_APP_FIREBASE_MEASUREMENT_ID=<your_key>
```

**Notes:**
- All variables must start with `REACT_APP_` to be injected by Create React App
- These are compiled into the JavaScript bundle — they are client-side only
- The Google Maps API key should have HTTP referrer restrictions set in GCP console
- No `.env.example` exists — this file must be manually created

## Backend (Cloud Run Job)

### Environment Variables

Set directly on the Cloud Run Job:

| Variable | Value | Purpose |
|----------|-------|---------|
| `PROJECT_ID` | `manifest-altar-490719-j7` | GCP project ID |
| `GCS_BUCKET` | `csv-battle-cards-dqe` | Google Cloud Storage bucket |
| `MAX_WORKERS` | `3` | Parallel threads per task for Step 3 (per-row LLM + matching). Also reused by Step 1 enrichment via `ENRICHMENT_MAX_WORKERS` (default 2) |
| `MAX_ROWS` | `0` | Row limit (0 = process all rows). Set to e.g. `500` for a smoke test |

To force Steps 1 and 2 to re-run (e.g. after uploading a new EY CSV), delete `gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json` before Execute — there is no env-var flag for this.

### Auto-Set by Cloud Run

| Variable | Purpose |
|----------|---------|
| `CLOUD_RUN_TASK_INDEX` | Which task this is (0, 1, 2, 3) |
| `CLOUD_RUN_TASK_COUNT` | Total number of tasks (4) |

### Secrets (Google Secret Manager)

These are mounted as environment variables from Secret Manager:

| Variable | Secret Manager Key | Purpose |
|----------|-------------------|---------|
| `GOOGLE_MAPS_API_KEY` | `google-maps-api-key` | Google Geocoding API |
| `CONNECTBASE_TENANT_API_KEY` | `connectbase-tenant-api-key` | ConnectBase Tenant + OnNet APIs |
| `CONNECTBASE_NETWORK_API_KEY` | `connectbase-network-api-key` | ConnectBase Network Intelligence API |
| `HUBSPOT_ACCESS_TOKEN` | `hubspot-access-token` | HubSpot CRM API bearer token |

### Updating Secrets

```bash
# Update a secret value
echo -n "new_value" | gcloud secrets versions add SECRET_NAME \
  --data-file=- \
  --project=manifest-altar-490719-j7

# Example: rotate HubSpot token
echo -n "pat-na1-xxxxx" | gcloud secrets versions add hubspot-access-token \
  --data-file=- \
  --project=manifest-altar-490719-j7
```

After updating secrets, re-deploy the Cloud Run Job to pick up the latest versions:

```bash
gcloud builds submit --config cloudbuild.yaml --project manifest-altar-490719-j7
```

### Local Development

When running the pipeline locally, set secrets as environment variables:

```bash
export GOOGLE_MAPS_API_KEY=<your_key>
export CONNECTBASE_TENANT_API_KEY=<your_key>
export CONNECTBASE_NETWORK_API_KEY=<your_key>
export HUBSPOT_ACCESS_TOKEN=<your_token>
```

You also need GCP authentication for GCS and Gemini access:

```bash
gcloud auth application-default login --project=manifest-altar-490719-j7
```

### Vertex AI IAM

The Cloud Run Job's service account needs the **Vertex AI User** role (`roles/aiplatform.user`) for the Gemini `generate_content` calls in Step 3 and the matcher confirmation calls.

```bash
gcloud projects add-iam-policy-binding manifest-altar-490719-j7 \
  --member=serviceAccount:battle-card-sa@manifest-altar-490719-j7.iam.gserviceaccount.com \
  --role=roles/aiplatform.user
```
