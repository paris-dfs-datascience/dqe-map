"""
hubspot_update.py
Fetches all companies from HubSpot CRM and saves to GCS.
Step 2 of the pipeline -- refreshes the data that hubspot_matcher.py reads.
"""

import os
import json
import time
import requests
import logging
from google.cloud import storage
import google.cloud.logging

from pipeline_config import GCS_BUCKET, HUBSPOT_BLOB, HUBSPOT_ACCESS_TOKEN

# ── Cloud Logging ─────────────────────────────────────────────────────────────
log_client = google.cloud.logging.Client()
log_client.setup_logging()
logger = logging.getLogger("hubspot-update")

PROPERTIES = [
    "name",
    "owner_name_field",
    "netsuite_status",
    "notes_last_contacted",
    "lead_source__netsuite_",
    "lead_source_type",
    "hs_analytics_last_visit_timestamp",
    "zoominfo_company_id"
]

def refresh_hubspot_companies():
    access_token = HUBSPOT_ACCESS_TOKEN
    if not access_token:
        raise EnvironmentError("HUBSPOT_ACCESS_TOKEN environment variable not set")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json"
    }

    MAX_PAGES = 500  # Safety limit to prevent infinite loops
    MAX_RETRIES_PER_PAGE = 5

    companies = []
    url    = "https://api.hubapi.com/crm/v3/objects/companies"
    params = {"limit": 100, "properties": ",".join(PROPERTIES)}
    page   = 1

    while url:
        if page > MAX_PAGES:
            logger.error(f"Exceeded max page limit ({MAX_PAGES}). Stopping pagination.")
            break

        logger.info(f"Fetching HubSpot page {page}...")

        # Retry logic for transient failures
        response = None
        last_error = None
        for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=60)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 15))
                    wait_time = max(retry_after, 10 * attempt)
                    logger.warning(f"HubSpot rate limited (429). Waiting {wait_time}s (attempt {attempt}/{MAX_RETRIES_PER_PAGE})")
                    time.sleep(wait_time)
                    continue
                if response.status_code != 200:
                    last_error = f"HTTP {response.status_code}: {response.text[:500]}"
                    logger.warning(f"HubSpot returned {response.status_code} on page {page} (attempt {attempt}/{MAX_RETRIES_PER_PAGE}): {response.text[:500]}")
                    time.sleep(5 * attempt)
                    continue
                break
            except requests.exceptions.Timeout as e:
                last_error = f"Timeout after 60s: {e}"
                logger.warning(f"HubSpot request timed out on page {page} (attempt {attempt}/{MAX_RETRIES_PER_PAGE}): {e}")
                time.sleep(10 * attempt)
            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {e}"
                logger.warning(f"HubSpot connection error on page {page} (attempt {attempt}/{MAX_RETRIES_PER_PAGE}): {e}")
                time.sleep(10 * attempt)
            except requests.exceptions.RequestException as e:
                last_error = f"Request error: {type(e).__name__}: {e}"
                logger.warning(f"HubSpot request failed on page {page} (attempt {attempt}/{MAX_RETRIES_PER_PAGE}): {type(e).__name__}: {e}")
                time.sleep(10 * attempt)

        if response is None or response.status_code != 200:
            status = response.status_code if response else "no response"
            text = response.text[:500] if response and response.text else last_error or "unknown"
            logger.error(f"HubSpot FAILED on page {page} after {MAX_RETRIES_PER_PAGE} attempts. Status: {status}. Detail: {text}. Companies fetched so far: {len(companies)}")
            raise Exception(f"HubSpot API error {status} on page {page}: {text}")

        data = response.json()
        for company in data.get("results", []):
            props = company.get("properties", {})
            companies.append({
                "id":                     company.get("id"),
                "name":                   props.get("name"),
                "hubspot_owner_id":       props.get("owner_name_field"),
                "netsuite_status":        props.get("netsuite_status"),
                "notes_last_contacted":   props.get("notes_last_contacted"),
                "lead_source__netsuite_": props.get("lead_source__netsuite_"),
                "lead_source_type":       props.get("lead_source_type")
            })

        after = data.get("paging", {}).get("next", {}).get("after")
        if after:
            params["after"] = after
            page += 1
        else:
            break

    logger.info(f"Fetched {len(companies)} companies from HubSpot")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob   = bucket.blob(HUBSPOT_BLOB)
    blob.upload_from_string(
        json.dumps(companies, indent=2),
        content_type="application/json"
    )
    logger.info(f"Saved to gs://{GCS_BUCKET}/{HUBSPOT_BLOB}")

if __name__ == "__main__":
    refresh_hubspot_companies()
