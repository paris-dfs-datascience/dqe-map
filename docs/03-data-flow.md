# Data Flow

## End-to-End Pipeline

```
EY Prospect CSV (Name, Address, City, State, Zip, Employees, etc.)
        │
        ▼
   ┌───────────────────────┐
   │  Step 1: Enrich        │ ── ConnectBase Tenant API
   │  tenant_enrichment     │ ── ConnectBase Network API
   │  (task 0 only; skipped │ ── ConnectBase OnNet API
   │   if checkpoint set)   │
   └────────┬───────────────┘
            │ enriched CSV
            ▼
   ┌───────────────────────┐
   │  Step 2: HubSpot       │ ── HubSpot CRM API (paginated fetch)
   │  hubspot_update        │  (task 0 only; skipped if checkpoint set)
   └────────┬───────────────┘
            │ hubspot_companies.json
            ▼
   ┌──────────────────────────────────────────┐
   │  Step 3: Battle Card Build (all 4 tasks) │
   │  battlecard_processor                      │
   │                                            │
   │  Each task reads the enriched CSV +        │
   │  HubSpot JSON already in GCS and           │
   │  processes its shard of rows:              │
   │    → Google Geocoding API (lat/lng)        │
   │    → Gemini LLM (2 calls: research w/      │
   │      google_search + structured scoring)   │
   │    → HubSpot fuzzy + Gemini confirm  ║    │ parallel
   │    → NetSuite fuzzy + Gemini confirm ║    │ threads
   │    → assemble + append to shard JSON       │
   └────────┬──────────────────────────────────┘
            │ dqe_prospects_shard_{0..3}.json
            ▼
   ┌────────────────────────┐
   │  Step 4: Merge + report │  (last task only)
   │  merge_shards            │  writes dqe_prospects.json
   │                          │  + EY-file/last_run.txt
   └────────┬────────────────┘
            │
            ▼
   dqe_prospects.json  ──→  GCS bucket  ──→  Frontend fetches on load
```

## Why this shape?

- **Task 0 runs Steps 1-2 alone** because they're global (one enrichment call per row, one HubSpot CRM pull). Running them 4× would waste API calls and money.
- **Tasks 1-3 do not wait** — they immediately start Step 3 and read whatever `enriched-data/tenants_enriched.csv` and `hubspot-data/hubspot_companies.json` are in the bucket. Since those blobs normally exist from the previous run, this works. On a cold bucket (first run or after a manual wipe), tasks 1-3 will fail — run with `--tasks=1` once to populate, then scale back.
- **Step 3 is the parallelizable part** — each task processes an independent shard of rows with its own `MAX_WORKERS` threads. With `MAX_WORKERS=3` × 4 tasks, that's 12 concurrent rows.
- **Checkpoints carry state across runs**. `checkpoints/pipeline_checkpoint.json` gates Step 1 and Step 2; if it exists from a previous successful run, those steps are skipped. Delete the blob to force a full re-run.

## Battle Card JSON Structure

Each card in the final `dqe_prospects.json` contains:

```json
{
  "ey_file_data": {
    "Name": "Acme Corp",
    "Address": "123 Main St",
    "City": "Pittsburgh",
    "State": "PA",
    "Zipcode": "15213",
    "No Of Employees": "150"
  },
  "connectbase_data": {
    "API_EntityName": "Acme Corporation",
    "API_NoOfEmployees": 145,
    "API_MonthlyNetworkSpend": 2500,
    "API_Revenue": 15000000,
    "API_Industry": "Technology",
    "DQE_Site_Distance": "250",
    "DQE_Connection_Status": "Near Net",
    "DQE_Network_Status": "Available",
    "SITE_All_Competitors": "Comcast, Verizon"
  },
  "geocode_data": {
    "latitude": 40.4406,
    "longitude": -79.9959,
    "formatted_address": "123 Main St, Pittsburgh, PA 15213",
    "geocode_status": "success"
  },
  "llm_analysis": {
    "overall_score": 72,
    "data_confidence": {
      "confidence_score": 0.85,
      "business_status": "operating",
      "validated_employee_count": 145,
      "employee_count_confidence": "high",
      "business_status_points": 0.38,
      "employee_validation_points": 0.32,
      "source_quality_points": 0.15
    },
    "icp_fit": {
      "icp_fit_score": 85,
      "network_economics_points": 15,
      "business_scale_need_points": 70,
      "business_assessment": {
        "business_criticality": "high",
        "bandwidth_requirements": "high",
        "estimated_monthly_spend": 3500,
        "infrastructure_needs": ["DIA", "SD-WAN"]
      }
    },
    "sales_intelligence": {
      "priority_level": "high",
      "priority_reasoning": "...",
      "key_selling_points": ["...", "..."],
      "likely_pain_points": ["...", "..."],
      "competitive_angles": ["...", "..."],
      "recommended_services": ["DIA", "SD-WAN"],
      "next_best_actions": ["...", "..."],
      "recommended_approach": "..."
    }
  },
  "hubspot_match": {
    "matched": true,
    "hubspot_id": "123456",
    "hubspot_name": "Acme Corp",
    "hubspot_owner_id": "owner_123",
    "netsuite_status": "Prospect",
    "match_confidence": "high",
    "match_reason": "Exact name match"
  },
  "netsuite_match": {
    "matched": true,
    "netsuite_internal_id": "789",
    "netsuite_address": "123 Main St",
    "structure_type": "Office",
    "structure_status": "Active",
    "distance_band": "Near Net",
    "primary_cost_total": 12500,
    "match_confidence": "high",
    "match_reason": "Exact address and zip match"
  },
  "additional_tenants": ["Tenant A", "Tenant B"],
  "metadata": {
    "analysis_date": "2026-04-17 16:30:00",
    "csv_row_index": 5
  }
}
```


## Scoring Formula

```
overall_score = data_confidence.confidence_score (0.0 - 1.0)  x  icp_fit.icp_fit_score (0 - 100)

Data Confidence (max 1.0):
  Business Status:       0 - 0.40  (operating/closed/moved/uncertain)
  Employee Validation:   0 - 0.40  (confidence in employee count)
  Source Quality:         0 - 0.20  (recency and reliability)

ICP Fit (max 100):
  Network Economics:     0 - 20  (on-net / near-net / not-on-net)
  Business Scale & Need: 0 - 80  (employee count + business criticality)
```

**Score thresholds on the map:**
| Score | Color | Label |
|-------|-------|-------|
| 80-100 | Green (#00C853) | Excellent |
| 60-79 | Yellow (#FFD600) | Good |
| 40-59 | Orange (#FF6D00) | Fair |
| 0-39 | Red (#D32F2F) | Poor |

## How Frontend Consumes the Data

1. Fetches `dqe_prospects.json` from GCS on page load
2. Filters to cards with `overall_score > 0` and valid lat/lng
3. Groups cards at the same coordinates
4. Shows top 100 groups in current viewport sorted by score
5. On marker click, renders all card fields in an InfoWindow popup
