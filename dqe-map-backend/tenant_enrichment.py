"""
tenant_enrichment.py
Reads EY source file from GCS, hits ConnectBase APIs for each row,
writes enriched CSV back to GCS. Step 1 of the pipeline.
"""

import csv
import io
import os
import requests
import json
import logging
from typing import List, Dict, Union, Tuple
import time
import concurrent.futures
import threading
from google.cloud import storage
import google.cloud.logging

from pipeline_config import (
    GCS_BUCKET, EY_INPUT_FOLDER, ENRICHED_CSV_BLOB,
    CONNECTBASE_TENANT_API_KEY, CONNECTBASE_NETWORK_API_KEY,
    CONNECTBASE_COMPANY_ID, ENRICHMENT_MAX_WORKERS,
    API_TIMEOUT, RETRY_ATTEMPTS, RATE_LIMIT_RETRIES, RATE_LIMIT_BACKOFF,
    CB_TENANT_FIELD_MAP, CB_NETWORK_FIELD_MAP,
)

# ── Cloud Logging ─────────────────────────────────────────────────────────────
log_client = google.cloud.logging.Client()
log_client.setup_logging()
logger = logging.getLogger("tenant-enrichment")

# ── Validate required keys at import ─────────────────────────────────────────
if not CONNECTBASE_TENANT_API_KEY:
    raise EnvironmentError("CONNECTBASE_TENANT_API_KEY not set")
if not CONNECTBASE_NETWORK_API_KEY:
    raise EnvironmentError("CONNECTBASE_NETWORK_API_KEY not set")

_print_lock = threading.Lock()

def tprint(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    with _print_lock:
        logger.info(msg)

def normalize_name(name: str) -> str:
    if not name:
        return ""
    return name.lower().strip().replace(",", "").replace(".", "").replace("-", " ")

# ── GCS helpers ───────────────────────────────────────────────────────────────

def _read_ey_csv_from_gcs() -> List[Dict]:
    """Auto-discover and read the first CSV in the EY-file folder."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blobs  = [b for b in bucket.list_blobs(prefix=EY_INPUT_FOLDER) if b.name.endswith(".csv")]
    if not blobs:
        raise FileNotFoundError(f"No .csv found in gs://{GCS_BUCKET}/{EY_INPUT_FOLDER}/")
    blob = blobs[0]
    logger.info(f"Reading EY file: gs://{GCS_BUCKET}/{blob.name}")
    content = blob.download_as_text(encoding="utf-8")
    return list(csv.DictReader(io.StringIO(content)))

def _write_enriched_csv_to_gcs(enriched: List[Dict]):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob   = bucket.blob(ENRICHED_CSV_BLOB)
    buf    = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=enriched[0].keys())
    writer.writeheader()
    writer.writerows(enriched)
    blob.upload_from_string(buf.getvalue(), content_type="text/csv")
    logger.info(f"Saved enriched CSV: gs://{GCS_BUCKET}/{ENRICHED_CSV_BLOB}")

# ── API helpers ───────────────────────────────────────────────────────────────

def _get_with_retry(url: str, params: dict, headers: dict, label: str):
    timeout_attempts    = 0
    rate_limit_attempts = 0
    while True:
        try:
            response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
            if response.status_code == 429:
                if rate_limit_attempts >= RATE_LIMIT_RETRIES:
                    logger.warning(f"{label}: rate-limited after {RATE_LIMIT_RETRIES} retries - giving up")
                    return None
                wait = int(response.headers.get("Retry-After", RATE_LIMIT_BACKOFF * (2 ** rate_limit_attempts)))
                logger.warning(f"{label}: 429 - waiting {wait}s (attempt {rate_limit_attempts + 1}/{RATE_LIMIT_RETRIES})")
                time.sleep(wait)
                rate_limit_attempts += 1
                continue
            return response
        except requests.exceptions.Timeout:
            timeout_attempts += 1
            if timeout_attempts >= RETRY_ATTEMPTS:
                logger.warning(f"{label}: timed out after {RETRY_ATTEMPTS} attempts")
                return None
            time.sleep(1)
        except Exception as e:
            logger.error(f"{label}: unexpected error - {e}")
            return None

def call_network_intelligence_api(address: str, city: str, state: str) -> Dict:
    url         = "https://api.connected2fiber.com/network-intelligence/v6/"
    address_str = f"{address} {city} {state}".strip()
    params      = {"companyid": CONNECTBASE_COMPANY_ID, "address": address_str, "validation": "true"}
    headers     = {"Cache-Control": "no-cache", "Ocp-Apim-Subscription-Key": CONNECTBASE_NETWORK_API_KEY}
    response    = _get_with_retry(url, params, headers, label=f"Network [{address_str}]")
    if response is None or response.status_code != 200:
        return {}
    data_list   = response.json().get("body", {}).get("data", [])
    if not data_list:
        return {}
    all_providers = sorted(set(item.get("provider", "Unknown") for item in data_list))
    dqe_options   = [item for item in data_list if "DQE" in item.get("provider", "").upper()]
    if not dqe_options:
        return {"all_site_providers": ", ".join(all_providers)}
    onnet_options = [x for x in dqe_options if x.get("connectionStatus", "").strip().lower() == "onnet"]
    if onnet_options:
        best = onnet_options[0]
    else:
        non_zero = [x for x in dqe_options if x.get("siteDistance", 0) > 0]
        best = min(non_zero, key=lambda x: x.get("siteDistance", 999999)) if non_zero else dqe_options[0]
    best["all_site_providers"] = ", ".join(all_providers)
    return best

def call_onnet_providers_api(address: str, city: str, state: str) -> str:
    url         = "https://api.connectbase.com/network-intelligence/v6/onnet"
    address_str = f"{address} {city} {state}".strip()
    params      = {"companyId": CONNECTBASE_COMPANY_ID, "address": address_str, "validation": "true"}
    headers     = {"Cache-Control": "no-cache", "Ocp-Apim-Subscription-Key": CONNECTBASE_NETWORK_API_KEY}
    response    = _get_with_retry(url, params, headers, label=f"Onnet [{address_str}]")
    if response is None or response.status_code != 200:
        return ""
    providers = response.json().get("body", {}).get("providers", [])
    return ", ".join(providers) if providers else ""

def call_tenant_api(company_id: str, address: str, city: str, state: str) -> List[Union[Dict, str]]:
    url      = "https://api.connected2fiber.com/v2/tenants/"
    params   = {"companyId": company_id, "streetName": address, "city": city, "state": state}
    headers  = {"Cache-Control": "no-cache", "Ocp-Apim-Subscription-Key": CONNECTBASE_TENANT_API_KEY}
    response = _get_with_retry(url, params, headers, label=f"Tenant [{address}]")
    if response is None or response.status_code != 200:
        return []
    return response.json()

# ── Row merge ─────────────────────────────────────────────────────────────────

def merge_data(ey_row: Dict, api_tenant: Dict, other_tenants: List[Dict],
               dqe_data: Dict, site_providers: str) -> Dict:
    merged = ey_row.copy()

    # Merge tenant fields using centralized mapping
    for internal_key, api_key in CB_TENANT_FIELD_MAP.items():
        if api_tenant:
            val = api_tenant.get(api_key)
            merged[internal_key] = val if val is not None else None
        else:
            merged[internal_key] = None

    # Merge DQE network fields
    if dqe_data.get("provider"):
        for internal_key, api_key in CB_NETWORK_FIELD_MAP.items():
            val = dqe_data.get(api_key)
            merged[internal_key] = val if val is not None else None
    else:
        merged["DQE_Site_Distance"] = "NOT_FOUND"
        for internal_key in list(CB_NETWORK_FIELD_MAP.keys()):
            if internal_key != "DQE_Site_Distance":
                merged[internal_key] = None

    merged["SITE_All_Competitors"] = site_providers if site_providers else None

    other_names = [t.get("entityName", "") for t in other_tenants
                   if isinstance(t, dict) and t.get("entityName")]
    merged["API_Additional_Tenants"] = ", ".join(other_names) if other_names else None
    return merged

# ── Row processing ────────────────────────────────────────────────────────────

def process_single_row(row_data: Tuple[int, int, Dict]) -> Tuple[int, Dict]:
    idx, total, row = row_data
    name, address, city, state = (
        row.get("Name"), row.get("Address"),
        row.get("City"), row.get("State")
    )
    logger.info(f"[{idx}/{total}] Starting: {name} at {address}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as inner:
        tenant_future  = inner.submit(call_tenant_api, CONNECTBASE_COMPANY_ID, address, city, state)
        network_future = inner.submit(call_network_intelligence_api, address, city, state)
        onnet_future   = inner.submit(call_onnet_providers_api, address, city, state)
        api_tenants      = tenant_future.result()
        dqe_network_info = network_future.result()
        site_providers   = onnet_future.result()

    ey_norm        = normalize_name(name)
    matched_tenant = next(
        (t for t in api_tenants
         if isinstance(t, dict) and ey_norm in normalize_name(t.get("entityName", ""))),
        None
    )
    other_tenants = [t for t in api_tenants if t != matched_tenant]
    merged        = merge_data(row, matched_tenant, other_tenants, dqe_network_info, site_providers)
    logger.info(f"[{idx}/{total}] Done: {name} - DQE: {merged.get('DQE_Connection_Status', 'N/A')}")
    return (idx, merged)

# ── Main entry point ──────────────────────────────────────────────────────────

def process_csv():
    rows  = _read_ey_csv_from_gcs()
    total = len(rows)
    logger.info(f"Processing {total} records with {ENRICHMENT_MAX_WORKERS} parallel workers")
    start        = time.time()
    indexed_rows = [(idx + 1, total, row) for idx, row in enumerate(rows)]
    results      = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=ENRICHMENT_MAX_WORKERS) as executor:
        future_to_idx = {executor.submit(process_single_row, r): r[0] for r in indexed_rows}
        for future in concurrent.futures.as_completed(future_to_idx):
            try:
                results.append(future.result())
            except Exception as e:
                idx = future_to_idx[future]
                logger.error(f"Row {idx} failed: {e}")

    results.sort(key=lambda x: x[0])
    enriched = [r[1] for r in results]
    if enriched:
        _write_enriched_csv_to_gcs(enriched)
        logger.info(f"Complete! {total} records processed in {time.time() - start:.1f}s")

if __name__ == "__main__":
    process_csv()
