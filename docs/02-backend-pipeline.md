# Backend Pipeline

## Tech Stack

- Python 3.11
- Google Cloud Run Jobs (4 parallel tasks)
- Gemini 2.5 Flash on Vertex AI (online `generate_content` per row)
- Google Search grounding tool (enabled per LLM request)
- Google Maps Geocoding API
- ConnectBase APIs (Tenant, Network Intelligence, OnNet Providers)
- HubSpot CRM API
- RapidFuzz (fuzzy string matching)
- Google Cloud Storage
- Google Cloud Logging

## File Structure

```
dqe-map-backend/
├── battlecard_generator.py   # Pipeline orchestrator (entry point)
├── pipeline_config.py        # All configuration, thresholds, field mappings
├── tenant_enrichment.py      # Step 1: Enrich EY CSV with ConnectBase data
├── hubspot_update.py         # Step 2: Refresh HubSpot company cache
├── battlecard_processor.py   # Step 3: geocode + LLM + matching per row
├── battlecard_llm.py         # Gemini client + prompt validator + fallbacks
├── battlecard_config.py      # LLM prompt templates (research + analysis)
├── hubspot_matcher.py        # Fuzzy + Gemini company name matching
├── netsuite_matcher.py       # Fuzzy + Gemini address matching
├── battlecard_storage.py     # Save shard + summary to GCS
├── merge_shards.py           # Step 4: Merge parallel task outputs
├── requirements.txt          # Python dependencies
├── dockerfile                # Container image definition
└── cloudbuild.yaml           # Cloud Build deployment config
```

## Pipeline Steps

```
Task 0 only (skipped via checkpoint if already complete):
  Step 1: Tenant Enrichment (ConnectBase APIs)
  Step 2: HubSpot Refresh

All 4 tasks in parallel (no polling — each task starts Step 3 immediately):
  Step 3: Battle Card Generation — per row in this task's shard:
          - Geocode via Google Maps
          - LLM analysis via Gemini 2.5 Flash + google_search grounding (2 calls: research + scoring)
          - HubSpot fuzzy match (+ Gemini confirm if ambiguous)
          - NetSuite fuzzy match (+ Gemini confirm if ambiguous)
          - Assemble battle card JSON
          - Write shard to gs://.../csv-battle-cards/dqe_prospects_shard_{N}.json

Last task only (after all shards written):
  Step 4: Merge all shards into dqe_prospects.json (final frontend input)
          The last task also writes gs://.../EY-file/last_run.txt — a plain-text
          run report summarizing scores, match counts, and any errors.
```

Cloud Run Job is configured for 4 tasks × `MAX_WORKERS=3` threads per task = **12 concurrent rows**. 429 rate limits from Gemini are handled by `_generate_with_retry` with exponential backoff.

> **Heads up on task startup:** the code does not poll for task 0's outputs before tasks 1-3 run Step 3. In a cold bucket (no prior `tenants_enriched.csv` / `hubspot_companies.json`), tasks 1-3 will fail fast. In practice those blobs exist from the previous run, so Step 3 reads them. If you've just wiped everything to force a clean run, execute task 0 alone first (e.g. temporarily set `--tasks=1`), then scale back to `--tasks=4`.

## File-by-File Breakdown

### battlecard_generator.py — Pipeline orchestrator

Key helpers:
- `_read_checkpoint()` / `_write_checkpoint()` — reads/writes `gs://.../checkpoints/pipeline_checkpoint.json`. Step 1 and Step 2 gate on `enrichment_complete` and `hubspot_complete` keys, so completed steps are skipped across runs.
- `_write_run_report()` — last task writes a human-readable summary of the run to `EY-file/last_run.txt` (score distribution, geocoding, match counts, token usage, per-row errors).
- `CSVBattleCardGenerator.process_csv()` / `.save_to_gcs()` — Step 3 worker class: processes this task's shard of rows and writes a shard JSON.

`main()` orchestrates all of the above.

> **Code-smell note:** the file currently defines `def main()` twice (an earlier stub and the active one). The second definition wins at runtime. Worth cleaning up separately.

### pipeline_config.py — Configuration

Central config. Key settings:

| Setting | Default | Purpose |
|---------|---------|---------|
| `PROJECT_ID` | `manifest-altar-490719-j7` | GCP project |
| `GCS_BUCKET` | `csv-battle-cards-dqe` | Storage bucket |
| `MAX_WORKERS` | 3 | Parallel threads per task (Step 3). Also used by tenant enrichment via `ENRICHMENT_MAX_WORKERS`, default 2 |
| `MAX_ROWS` | unlimited (0) | Set to smoke-test size like 500 |
| `HUBSPOT_FUZZY_CUTOFF` | 35 | Min fuzzy score for HubSpot candidates |
| `NETSUITE_FUZZY_CUTOFF` | 40 | Min fuzzy score for NetSuite candidates |
| `HUBSPOT_TOP_N` / `NETSUITE_TOP_N` | 8 / 6 | Candidate count passed to the Gemini confirmer |
| `LLM_MAX_RETRIES` / `LLM_BASE_DELAY` | 5 / 10s | Gemini retries for the Step 3 analysis call |
| `MATCHER_MAX_RETRIES` / `MATCHER_BASE_DELAY` | 6 / 15s | Gemini retries for matcher confirmation |
| `RATE_LIMIT_RETRIES` / `RATE_LIMIT_BACKOFF` | 8 / 5s | ConnectBase retry budget |
| `CONNECTBASE_COMPANY_ID` | 1646 | DQE's ConnectBase ID |

**Matcher short-circuit thresholds are 90** (raised from the default-on-fuzzy quality cutoff 35/40): if fuzzy is that confident, skip the Gemini confirmation call. Gemini still runs on ambiguous matches.

### tenant_enrichment.py — Step 1

For each row in the EY CSV, calls 3 APIs in parallel:
1. **Tenant API** (`api.connected2fiber.com/v2/tenants/`) — company info (employees, revenue, industry, spend)
2. **Network Intelligence API** — DQE network distance / connection status
3. **OnNet Providers API** — competitor data

Writes to `gs://.../enriched-data/tenants_enriched.csv`.

### hubspot_update.py — Step 2

Fetches all companies from HubSpot CRM via paginated API. Properties: name, owner, NetSuite status, last contacted, lead source. Saves to `gs://.../hubspot-data/hubspot_companies.json`. Has robust retry (5 attempts, 60s timeout, 10-30s waits, detailed error logging).

### battlecard_processor.py — Step 3

Per-row processing pipeline:

1. **Extract** EY + ConnectBase fields from the enriched CSV row
2. **LLM analysis** via `battlecard_llm.analyze_prospect()` — 2-pass Gemini:
   - Pass 1 (research): Google Search tool enabled, temperature 1.0
   - Pass 2 (scoring): JSON output, temperature 0.2
3. **Geocode** the address via Google Maps
4. **HubSpot + NetSuite matches in parallel** (independent, both may call Gemini for confirmation)
5. **Assemble battle card** JSON and append to the shard

### battlecard_llm.py — Gemini client + prompt validator

- `BattleCardLLM.analyze_prospect()` — 2-pass research + scoring using Gemini 2.5 Flash
- `_generate_with_retry()` — exponential backoff on 429 (5 retries, 10s base delay)
- `_validate_llm_response()` — schema validator, fills missing keys with defaults
- `_create_fallback_analysis()` — zero-score fallback when LLM fails

### hubspot_matcher.py / netsuite_matcher.py

Fuzzy + Gemini-based matchers.
- Fuzzy candidates via RapidFuzz with a cutoff (35 for HubSpot, 40 for NetSuite)
- Short-circuit at score >= 90 (avoids Gemini confirmation for obvious matches)
- Gemini confirmation (6 retries, 15s base delay) for the ambiguous cases

### merge_shards.py — Step 4

Last task waits up to 60 min for all shards to appear, then merges them into the single `dqe_prospects.json` the frontend consumes. Requires ≥75% of expected shards.

## Checkpointing

`gs://.../checkpoints/pipeline_checkpoint.json` tracks per-step completion and **is load-bearing across runs**. If `enrichment_complete` is set, Step 1 is skipped on the next Execute; same for `hubspot_complete` and Step 2. Step 3 always runs (there is no shard-level checkpoint).

To force a clean re-run (e.g. after uploading a new EY CSV, updating HubSpot, or rotating NetSuite data):

```bash
gsutil rm gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json
```

See [Running the Backend Pipeline](./05-running-backend.md) for the full re-run recipe.

## Error Handling

| Component | Failure Mode | Recovery |
|-----------|--------------|----------|
| ConnectBase API (429) | Rate limited | Exponential backoff, max 8 retries |
| HubSpot API (5xx / timeout) | Transient | 5 retries, 60s timeout, 10-30s waits |
| Google Geocoding | Address not found | Returns error status; card still generated |
| Gemini LLM (429) | Rate limited | Exponential backoff, max 5 retries (10s base) |
| Gemini matcher (429) | Rate limited | Exponential backoff, max 6 retries (15s base) |
| LLM response (bad JSON) | Invalid response | Falls back to zero-score card with `_error` field |
| Matcher (no candidates) | Below fuzzy cutoff | Returns `{"matched": false}` with reason |
| Shard merge | <75% shards | Merge aborts, whole run fails |
| Row processing | Any exception | Returns error card, continues to next row |

## Dependencies (requirements.txt)

```
google-cloud-storage==2.18.2
google-cloud-logging==3.11.3
google-genai==0.2.2
requests==2.31.0
google-maps-addressvalidation
rapidfuzz
```

Note: `google-genai` is pinned at **0.2.2**. Upgrading to the 1.x series will require reviewing the `generate_content` call sites in `battlecard_llm.py`, `hubspot_matcher.py`, and `netsuite_matcher.py` — the tools/config shape changed.

## IAM Notes

The Cloud Run Job's service account (`battle-card-sa@...`) needs:

- `storage.objectAdmin` on the GCS bucket
- `logging.logWriter`
- `secretmanager.secretAccessor` on the mounted secrets
- `aiplatform.user` (for Vertex AI Gemini calls) — **grant this if not already present**

## Future Optimization

If you need to process 13k+ rows repeatedly and the online per-row path gets too slow, consider migrating Step 3's LLM call to BigQuery-source batch predictions. Benefits: server-side parallelism with no RPM cap, ~30-60 min for 13k rows. Blockers worked around: BigQuery's typed schema accepts the empty `{"google_search": {}}` struct that Gemini 2.5+ requires (GCS-JSONL rejects it; inline batch is Developer-API-only). Est. 2-4 hours of work.
