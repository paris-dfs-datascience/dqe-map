# DQE ICP Map Tool — Overview

## What Is This

The DQE ICP Map Tool is a sales intelligence application that consolidates data from multiple platforms (EY prospect files, ConnectBase, HubSpot CRM, NetSuite, Google AI) into an interactive map with scored battle cards for each prospect.

Sales reps use it to identify high-value prospects in their territory, view AI-generated selling points and pain points, and check CRM status — all without switching between systems.

## Key Info

| | |
|---|---|
| **GCP Project** | `manifest-altar-490719-j7` |
| **Region** | `us-central1` |
| **Repository** | `https://github.com/paris-dfs-datascience/dqe-map.git` |
| **Frontend URL** | `https://manifest-altar-490719-j7.firebaseapp.com` |
| **GCS Bucket** | `gs://csv-battle-cards-dqe/` |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    FRONTEND (React)                      │
│              Firebase Hosting + Firestore                 │
│                                                           │
│  User → Google Map → Battle Card InfoWindows              │
│         ↑                                                 │
│         │ Fetches JSON on load                            │
│         │                                                 │
├─────────┼─────────────────────────────────────────────────┤
│         │          GOOGLE CLOUD STORAGE                   │
│         │                                                 │
│         └── gs://csv-battle-cards-dqe/                    │
│             ├── csv-battle-cards/dqe_prospects.json  ◄────┤
│             ├── sales-map.geojson (fiber routes)          │
│             ├── enriched-data/tenants_enriched.csv        │
│             ├── hubspot-data/hubspot_companies.json       │
│             └── netsuite/netsuite_data_mar3.csv           │
│                          ▲                                │
├──────────────────────────┼────────────────────────────────┤
│                          │                                │
│              BACKEND PIPELINE (Python)                    │
│              Cloud Run Job — 4 parallel tasks             │
│                                                           │
│  EY CSV → Step 1: Enrich (ConnectBase) ─┐                 │
│       → Step 2: Refresh HubSpot  ───────┘ task 0 only     │
│       → Step 3: Per-row, 4 tasks × 3 workers parallel:    │
│         Geocode, Gemini LLM (w/ google_search grounding), │
│         HubSpot match, NetSuite match                     │
│       → Step 4: Merge shards → Battle Card JSON → GCS     │
└───────────────────────────────────────────────────────────┘
```

The frontend and backend are **completely independent**. The backend produces a JSON file in GCS. The frontend reads that JSON file on page load. There is no live API connection between them.

**Runtime:** With 12 concurrent rows (4 tasks × 3 workers), 500 rows ≈ 15-40 min, 13k rows ≈ 6-10 hours. Tune via `MAX_WORKERS`. See the [Backend Pipeline](./02-backend-pipeline.md) doc for details.

**Runs are checkpoint-gated.** Steps 1 (enrichment) and 2 (HubSpot refresh) skip themselves when `gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json` shows them complete. Step 3 always runs against whatever `enriched-data/tenants_enriched.csv` and `hubspot-data/hubspot_companies.json` are currently in the bucket. To force a full re-run after uploading a new EY CSV, delete the checkpoint blob first — see [Running the Backend Pipeline](./05-running-backend.md).

## Two Deployable Systems

| System | Language | Deployed To | How to Deploy |
|--------|----------|-------------|---------------|
| **Frontend** | React/TypeScript | Firebase Hosting | `npm run build && npx firebase deploy --only hosting` |
| **Backend** | Python 3.11 | Cloud Run Jobs | `gcloud builds submit --config cloudbuild.yaml` |

See individual docs for full details:

- [Frontend](./01-frontend.md) — React app, component structure, features
- [Backend Pipeline](./02-backend-pipeline.md) — Python pipeline, 4-step processing
- [Data Flow](./03-data-flow.md) — End-to-end data path from CSV to map
- [Deploying the Frontend](./04-deploying-frontend.md) — Step-by-step deploy guide
- [Running the Backend Pipeline](./05-running-backend.md) — Cloud Run and local execution
- [Environment Variables & Secrets](./06-environment-variables.md) — All keys and config
- [GCS Bucket Structure](./07-gcs-bucket.md) — What lives where in storage
- [Firestore Collections](./08-firestore.md) — Session and event tracking schemas
- [External APIs](./09-external-apis.md) — Every API, endpoint, and auth method
- [Troubleshooting](./10-troubleshooting.md) — Common issues and fixes
