# Troubleshooting

## Frontend

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Blank map, no markers | GCS fetch failed or empty JSON | Check browser console for errors; verify `dqe_prospects.json` exists in GCS |
| "Processing geocoded data..." stuck forever | JSON fetch failed or returned invalid data | Check network tab; try the JSON URL directly in browser: `https://storage.googleapis.com/csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json` |
| Map loads but markers are wrong colors | Score thresholds or data issue | Check `getScoreColor()` in DQEBattleCardMap.tsx |
| Search doesn't autocomplete | Google Maps API key issue | Verify Places API is enabled for the key in GCP console |
| Search returns nothing on Enter | Geocoding API not enabled | Verify Geocoding API is enabled for the key in GCP console |
| No session tracking in Firestore | User not authenticated | Check Firebase Auth setup; verify `currentUser` is not null in console |
| Events not logging to Firestore | Auth issue or Firestore rules | Check browser console for "Event log failed" errors; verify Firestore security rules allow writes |
| `firebase: command not found` | Firebase CLI not installed globally | Use `npx firebase` instead, or `npm install --save-dev firebase-tools` |
| `Authentication Error` on deploy | Firebase credentials expired | Run `npx firebase login --reauth` |
| `EACCES: permission denied` on npm install -g | No write access to global node_modules | Use `npm install --save-dev` instead and use `npx` prefix |
| Build fails with TypeScript errors | Type errors in code | Run `npx tsc --noEmit` to see specific errors |
| CORS error fetching from GCS | Bucket CORS not configured | Set CORS policy on GCS bucket to allow your domain |
| "Hide Existing Customers" doesn't filter anything | No HubSpot matches with "Customer" status | Check that `hubspot_match.netsuite_status` contains "Customer" for some cards |

## Backend

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Tasks 1-3 error with 404 on `tenants_enriched.csv` or `hubspot_companies.json` | Cold bucket: tasks 1-3 don't wait for task 0 | Execute with `--tasks=1` first so only task 0 runs and populates Step 1 + Step 2 outputs. Then scale back to `--tasks=4`. If the blobs *should* already exist, check task 0's logs for a Step 1/2 failure |
| Pipeline skips Step 1 or Step 2 when you want them to re-run (e.g., uploaded a new EY CSV) | Checkpoint marks them complete | Delete `gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json` before the next Execute |
| Step 3 fails with `PermissionDenied` from Vertex AI | SA missing Vertex AI User role | `gcloud projects add-iam-policy-binding manifest-altar-490719-j7 --member=serviceAccount:battle-card-sa@manifest-altar-490719-j7.iam.gserviceaccount.com --role=roles/aiplatform.user` |
| Many rows have `_error` in `llm_analysis` / score 0 | Individual LLM calls failed (JSON parse, timeout, 429 exhausted retries) | Check `last_run.txt` for failure breakdown. Reduce `MAX_WORKERS` if 429 rate is unmanageable |
| `429 Too Many Requests` from ConnectBase | Step 1 API rate limiting | Handled with backoff (8 retries, 5s base). If persistent, reduce `MAX_WORKERS` |
| `429 Too Many Requests` from Gemini (LLM analysis) | Too many concurrent Gemini calls | Handled with backoff (5 retries, 10s base). Consider reducing `MAX_WORKERS` from 3 to 2 or 1 |
| `429 Too Many Requests` from Gemini (matcher) | Matcher confirm call rate limited | Handled with backoff (6 retries, 15s base). Most matches short-circuit at fuzzy score ≥90 so this is rare |
| `429 Too Many Requests` from HubSpot | CRM rate limiting | Pipeline handles with `Retry-After` header (5 retries, 60s timeout, 10-30s waits) |
| Geocoding failures for many rows | Invalid addresses or API quota | Check Google Maps API quota in GCP console; verify addresses in EY CSV are valid |
| Missing shards after Step 3 | One or more tasks crashed/timed out | Check Cloud Run execution logs; re-execute the job |
| Merge fails with "<75% shards" | Multiple tasks failed | Fix underlying issue, re-run |
| ConnectBase returns empty/no data | Invalid company ID | Verify `CONNECTBASE_COMPANY_ID=1646` |
| HubSpot token expired | Private app token regenerate | HubSpot → Settings → Integrations → Private Apps; update Secret Manager |
| "No companies loaded" in HubSpot matcher | HubSpot JSON missing or empty | Re-run Step 2, or verify `hubspot_companies.json` in GCS |
| NetSuite matcher finds no matches | CSV file missing or wrong format | Verify `netsuite_data_mar3.csv` exists in GCS |
| Cloud Run Job stuck | Task timeout or deadlock | Check logs; 24-hour timeout; cancel and re-run if needed |
| `google.auth.exceptions.DefaultCredentialsError` locally | Missing GCP auth | `gcloud auth application-default login --project=manifest-altar-490719-j7` |
| Docker build fails | Missing a file from dockerfile COPY list | Verify all `.py` files referenced by `import` statements are listed in the dockerfile |

## Useful Debug Commands

```bash
# Check if final output exists
gsutil stat gs://csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json

# Read the latest run report (human-readable summary of the last job)
gsutil cat gs://csv-battle-cards-dqe/EY-file/last_run.txt

# Check shard files (should be cleaned up after merge)
gsutil ls gs://csv-battle-cards-dqe/csv-battle-cards/dqe_prospects_shard_*.json

# Check checkpoint state (flags for Step 1 + Step 2 skipping)
gsutil cat gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json

# View Cloud Run Job logs
gcloud logging read 'resource.type="cloud_run_job"' \
  --project=manifest-altar-490719-j7 \
  --limit=50 --format="table(timestamp,textPayload)"

# Check HubSpot company count
gsutil cat gs://csv-battle-cards-dqe/hubspot-data/hubspot_companies.json | python3 -c "import sys,json; print(len(json.load(sys.stdin)))"

# Validate final JSON structure
gsutil cat gs://csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Cards: {len(d.get(\"battle_cards\",[]))}')"

# Scale back workers if Gemini is throwing too many 429s (default is 3)
gcloud run jobs update dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1 \
  --update-env-vars=MAX_WORKERS=2

# Run a smoke test with just 500 rows
gcloud run jobs update dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1 \
  --update-env-vars=MAX_ROWS=500
gcloud run jobs execute dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1
# Reset to full run:
gcloud run jobs update dqe-battle-card-job \
  --project=manifest-altar-490719-j7 --region=us-central1 \
  --update-env-vars=MAX_ROWS=0
```
