# GCS Bucket Structure

**Bucket:** `gs://csv-battle-cards-dqe/`

```
csv-battle-cards-dqe/
│
├── EY-file/
│   ├── *.csv                          # INPUT: Raw EY prospect list
│   │                                   # Columns: Name, Address, City, State,
│   │                                   # Zipcode, No Of Employees, etc.
│   └── last_run.txt                   # Plain-text run report written by the
│                                       # last task at end of every pipeline run
│
├── enriched-data/
│   └── tenants_enriched.csv           # Step 1 output
│                                       # EY data + ConnectBase API responses
│                                       # Columns: original + API_EntityName,
│                                       # API_MonthlyNetworkSpend, DQE_Site_Distance, etc.
│
├── hubspot-data/
│   └── hubspot_companies.json         # Step 2 output
│                                       # All HubSpot CRM companies
│                                       # Fields: id, name, owner, netsuite_status, etc.
│
├── netsuite/
│   └── netsuite_data_mar3.csv         # INPUT: NetSuite structure database
│                                       # Columns: Address 1, Structure Zip Code,
│                                       # Structure Type, Structure Status, NS Status,
│                                       # Distance Band, Primary Cost Total
│
├── csv-battle-cards/
│   ├── dqe_prospects.json             # FINAL OUTPUT — frontend reads this
│   ├── dqe_prospects_shard_0.json     # Temporary shard (deleted after merge)
│   ├── dqe_prospects_shard_1.json     # Temporary shard
│   ├── dqe_prospects_shard_2.json     # Temporary shard
│   └── dqe_prospects_shard_3.json     # Temporary shard
│
├── checkpoints/
│   └── pipeline_checkpoint.json       # Pipeline state tracking. Load-bearing
│                                       # across runs: if `enrichment_complete`
│                                       # or `hubspot_complete` is set, the
│                                       # corresponding step is skipped on the
│                                       # next Execute. Delete this blob to
│                                       # force a full re-run from Step 1.
│
└── sales-map.geojson                  # Fiber route overlay — frontend reads this
                                        # GeoJSON with DQE fiber network paths
```

**Note:** There are legacy `batch-input/` and `batch-output/` folders from a previous experiment with Vertex AI Gemini Batch predictions. That path was abandoned (see `docs/02-backend-pipeline.md` for context). Safe to delete manually if present:

```bash
gsutil -m rm -r gs://csv-battle-cards-dqe/batch-input/
gsutil -m rm -r gs://csv-battle-cards-dqe/batch-output/
```

## Input Files

| File | Format | Source | Updated By |
|------|--------|--------|------------|
| `EY-file/*.csv` | CSV | EY consulting | Manual upload |
| `netsuite/netsuite_data_mar3.csv` | CSV | NetSuite export | Manual upload |

## Output Files

| File | Format | Produced By | Consumed By |
|------|--------|-------------|-------------|
| `enriched-data/tenants_enriched.csv` | CSV | Step 1 (`tenant_enrichment.py`) | Step 3 (`battlecard_processor.py`) |
| `hubspot-data/hubspot_companies.json` | JSON | Step 2 (`hubspot_update.py`) | Step 3 (`hubspot_matcher.py`) |
| `csv-battle-cards/dqe_prospects_shard_N.json` | JSON | Step 3 (per task) | Step 4 (`merge_shards.py`) |
| `csv-battle-cards/dqe_prospects.json` | JSON | Step 4 (`merge_shards.py`) | Frontend (`DQEBattleCardMap.tsx`) |
| `EY-file/last_run.txt` | Text | Last task after Step 4 | Humans (status report) |
| `sales-map.geojson` | GeoJSON | Manual upload | Frontend (fiber route overlay) |

## Checkpointing Across Runs

`checkpoints/pipeline_checkpoint.json` controls whether Step 1 (enrichment) and Step 2 (HubSpot refresh) run on the next Execute:

- If the checkpoint shows `enrichment_complete: true`, Step 1 is skipped and Step 3 reads the existing `enriched-data/tenants_enriched.csv`.
- If it shows `hubspot_complete: true`, Step 2 is skipped and Step 3 reads the existing `hubspot-data/hubspot_companies.json`.
- Step 3 always runs against whatever blobs currently exist.

Tasks 1-3 do **not** poll for task 0 — they start Step 3 immediately. On a cold bucket (no prior enriched CSV or HubSpot JSON), run the first Execute with `--tasks=1` so only task 0 runs and populates the inputs; subsequent Executes can scale back to `--tasks=4`.

To force a full re-run after uploading a new EY CSV or rotating HubSpot/NetSuite data:

```bash
gsutil rm gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json
```

## Access

The frontend reads from GCS via public HTTPS URLs:
- `https://storage.googleapis.com/csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json`
- `https://storage.googleapis.com/csv-battle-cards-dqe/sales-map.geojson`

The backend reads/writes via the GCS SDK using service account credentials.

## Useful Commands

```bash
# List bucket contents
gsutil ls gs://csv-battle-cards-dqe/

# Check if final output exists
gsutil stat gs://csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json

# Read the latest run report
gsutil cat gs://csv-battle-cards-dqe/EY-file/last_run.txt

# Download final output locally
gsutil cp gs://csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json .

# Delete checkpoint to force Steps 1 + 2 to re-run on the next Execute
gsutil rm gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json

# Upload a new EY CSV (remember to also delete the checkpoint so Steps 1+2 re-run)
gsutil cp my_prospects.csv gs://csv-battle-cards-dqe/EY-file/
gsutil rm gs://csv-battle-cards-dqe/checkpoints/pipeline_checkpoint.json
```
