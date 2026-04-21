"""
pipeline_config.py
Single source of truth for all shared configuration: project IDs, bucket names,
GCS paths, column mappings, and tuning constants.
"""

import os

# ── Google Cloud ─────────────────────────────────────────────────────────────
PROJECT_ID = os.environ.get("PROJECT_ID")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
GCP_LOCATION = "us-central1"

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID env var is required")
if not GCS_BUCKET:
    raise RuntimeError("GCS_BUCKET env var is required")

# ── GCS paths ────────────────────────────────────────────────────────────────
EY_INPUT_FOLDER = "EY-file"
ENRICHED_CSV_BLOB = "enriched-data/tenants_enriched.csv"
HUBSPOT_BLOB = "hubspot-data/hubspot_companies.json"
NETSUITE_BLOB = "netsuite/netsuite_data_mar3.csv"
BATTLECARD_OUTPUT_PREFIX = "csv-battle-cards"
DEFAULT_OUTPUT_NAME = "dqe_prospects"

# ── Cloud Run ────────────────────────────────────────────────────────────────
CLOUD_RUN_TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
CLOUD_RUN_TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))

# ── Parallelism ──────────────────────────────────────────────────────────────
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "3"))
MAX_ROWS_ENV = os.environ.get("MAX_ROWS", "0")
MAX_ROWS = int(MAX_ROWS_ENV) if MAX_ROWS_ENV and MAX_ROWS_ENV != "0" else None

# ── Tenant enrichment tuning ─────────────────────────────────────────────────
ENRICHMENT_MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "2"))
API_TIMEOUT = 20
RETRY_ATTEMPTS = 3
RATE_LIMIT_RETRIES = 8
RATE_LIMIT_BACKOFF = 5
# ConnectBase per-endpoint budget is 180/min. Using 170 leaves headroom for
# clock drift and the occasional retry burst.
CONNECTBASE_CALLS_PER_MINUTE = int(os.environ.get("CONNECTBASE_CALLS_PER_MINUTE", "170"))

# ── LLM tuning ───────────────────────────────────────────────────────────────
LLM_MAX_RETRIES = 5
LLM_BASE_DELAY = 10
MATCHER_MAX_RETRIES = 6
MATCHER_BASE_DELAY = 15

# ── Matcher tuning ───────────────────────────────────────────────────────────
HUBSPOT_FUZZY_CUTOFF = 35
HUBSPOT_TOP_N = 8
NETSUITE_FUZZY_CUTOFF = 40
NETSUITE_TOP_N = 6

# ── API keys (from environment) ──────────────────────────────────────────────
CONNECTBASE_TENANT_API_KEY = os.environ.get("CONNECTBASE_TENANT_API_KEY")
CONNECTBASE_NETWORK_API_KEY = os.environ.get("CONNECTBASE_NETWORK_API_KEY")
CONNECTBASE_COMPANY_ID = os.environ.get("CONNECTBASE_COMPANY_ID", "1646")
HUBSPOT_ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")

# ── ConnectBase field mappings ───────────────────────────────────────────────
# Maps internal field names to the ConnectBase tenant API response keys
CB_TENANT_FIELD_MAP = {
    "API_EntityName": "entityName",
    "API_Website": "website",
    "API_Phone": "phone",
    "API_LinkedIn": "linkedIn",
    "API_NoOfEmployees": "noOfEmployees",
    "API_MonthlyNetworkSpend": "monthlyNetworkSpend",
    "API_Revenue": "revenue",
    "API_Industry": "industry",
    "API_FoundedYear": "foundedYear",
    "API_LocationType": "locationType",
    "API_LocationCount": "locationCount",
}

# All ConnectBase fields extracted per row (used by processor + enrichment)
CONNECTBASE_FIELDS = list(CB_TENANT_FIELD_MAP.keys()) + [
    "SITE_All_Competitors",
]

# ── Checkpoint paths ─────────────────────────────────────────────────────────
CHECKPOINT_BLOB = "checkpoints/pipeline_checkpoint.json"

# ── LLM response required keys (for validation) ─────────────────────────────
LLM_REQUIRED_TOP_KEYS = {"overall_score", "data_confidence", "icp_fit", "sales_intelligence"}
LLM_REQUIRED_CONFIDENCE_KEYS = {
    "confidence_score", "business_status_points", "employee_validation_points",
    "source_quality_points", "business_status", "validated_employee_count",
    "employee_count_confidence",
}
LLM_REQUIRED_ICP_KEYS = {
    "icp_fit_score", "network_economics_points", "business_scale_need_points",
    "network_analysis", "business_assessment", "competitive_context",
}
LLM_REQUIRED_SALES_KEYS = {
    "priority_level", "key_selling_points", "likely_pain_points",
    "recommended_services", "next_best_actions",
}


def gcs_shard_path(output_name: str, task_index: int) -> str:
    """Return the GCS blob path for a given shard."""
    return f"{BATTLECARD_OUTPUT_PREFIX}/{output_name}_shard_{task_index}.json"


def gcs_output_path(output_name: str) -> str:
    """Return the GCS blob path for the merged output."""
    return f"{BATTLECARD_OUTPUT_PREFIX}/{output_name}.json"
