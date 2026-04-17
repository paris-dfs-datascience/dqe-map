"""Data processing logic for battle card generation."""

import time
import os
import io
import logging
from typing import Dict, List, Tuple
import concurrent.futures
import requests
import urllib.parse
from google.cloud import storage

logger = logging.getLogger("battlecard-processor")

from pipeline_config import (
    PROJECT_ID, GCS_BUCKET, GOOGLE_MAPS_API_KEY,
    CB_TENANT_FIELD_MAP, CB_NETWORK_FIELD_MAP,
)
from battlecard_llm import BattleCardLLM
from hubspot_matcher import HubSpotMatcher
from netsuite_matcher import NetSuiteMatcher


class BattleCardProcessor:
    """Handles CSV processing and battle card generation."""

    def __init__(self, llm: BattleCardLLM, project_id: str = PROJECT_ID):
        self.llm        = llm
        self.project_id = project_id

        self.maps_api_key = GOOGLE_MAPS_API_KEY
        if not self.maps_api_key:
            print("WARNING: GOOGLE_MAPS_API_KEY not set in environment")

        gcs_bucket    = GCS_BUCKET
        self.hubspot  = HubSpotMatcher(gcs_bucket=gcs_bucket, project_id=project_id)
        self.netsuite = NetSuiteMatcher(gcs_bucket=gcs_bucket, project_id=project_id)

    def _geocode_address(self, address: str, city: str, state: str, zipcode: str) -> Dict:
        """Geocode an address using Google Geocoding API v1."""
        try:
            full_address = f"{address}, {city}, {state} {zipcode}".strip()
            url = "https://maps.googleapis.com/maps/api/geocode/json"

            response = requests.get(
                url,
                params={
                    "address": full_address,
                    "key": self.maps_api_key,
                    "region": "us",
                },
                timeout=10
            )

            if response.status_code != 200:
                print(f"    Geocoding API error: {response.status_code}")
                return {
                    "latitude": None, "longitude": None,
                    "geocode_quality": None,
                    "formatted_address": full_address,
                    "geocode_status": f"error_{response.status_code}"
                }

            data = response.json()
            status = data.get("status", "UNKNOWN")

            if status == "OK" and data.get("results"):
                result   = data["results"][0]
                location = result.get("geometry", {}).get("location", {})
                return {
                    "latitude":          location.get("lat"),
                    "longitude":         location.get("lng"),
                    "formatted_address": result.get("formatted_address", full_address),
                    "geocode_status":    "success",
                    "geocode_quality": {
                        "granularity":    result.get("geometry", {}).get("location_type", "UNKNOWN"),
                        "place_id":       result.get("place_id", "")
                    }
                }

            print(f"    Geocoding status: {status}")
            return {
                "latitude": None, "longitude": None,
                "geocode_quality": None,
                "formatted_address": full_address,
                "geocode_status": status.lower()
            }

        except Exception as e:
            print(f"    Geocoding error: {str(e)}")
            return {
                "latitude": None, "longitude": None,
                "geocode_quality": None,
                "formatted_address": f"{address}, {city}, {state} {zipcode}",
                "geocode_status": "error",
                "geocode_error": str(e)
            }

    def _extract_ey_data(self, row: Dict) -> Dict:
        """Pass through all EY file columns unchanged."""
        return dict(row)

    def _extract_connectbase_data(self, row: Dict) -> Dict:
        """Extract ConnectBase fields using the centralized field mapping."""
        data = {}
        for internal_key in CB_TENANT_FIELD_MAP:
            val = row.get(internal_key)
            data[internal_key] = val if val else None
        for internal_key in CB_NETWORK_FIELD_MAP:
            val = row.get(internal_key)
            data[internal_key] = val if val else None
        val = row.get("SITE_All_Competitors")
        data["SITE_All_Competitors"] = val if val else None
        return data

    def _extract_additional_tenants(self, row: Dict) -> List[str]:
        val = row.get("API_Additional_Tenants")
        if val:
            return [t.strip() for t in val.split(",")]
        return []

    def _process_single_row(self, row_data: Tuple[int, Dict]) -> Tuple[int, Dict]:
        """Process a single row -- designed for parallel execution."""
        idx, row = row_data
        print(f"[{idx}] Processing: {row.get('Name', 'Unknown')}")

        ey_data            = self._extract_ey_data(row)
        connectbase_data   = self._extract_connectbase_data(row)
        additional_tenants = self._extract_additional_tenants(row)

        # Geocode
        print(f"  Geocoding address...")
        geocode_data = self._geocode_address(
            row.get('Address', ''),
            row.get('City', ''),
            row.get('State', ''),
            row.get('Zipcode', '')
        )
        if geocode_data['geocode_status'] == 'success':
            print(f"    Geocoded: {geocode_data['latitude']}, {geocode_data['longitude']}")
        else:
            print(f"    Geocoding {geocode_data['geocode_status']}")

        # LLM analysis
        if connectbase_data.get("API_EntityName"):
            print(f"  Has ConnectBase data")
        else:
            print(f"  No ConnectBase data - analyzing with EY data only")

        llm_analysis = self.llm.analyze_prospect(ey_data, connectbase_data)

        # HubSpot match
        company_name  = row.get("Name", "")
        print(f"  Checking HubSpot for: {company_name}")
        hubspot_match = self.hubspot.match(company_name)
        if hubspot_match.get("matched"):
            print(f"  HubSpot match: {hubspot_match['hubspot_name']} "
                  f"(confidence: {hubspot_match['match_confidence']})")
        else:
            print(f"  HubSpot: no match ({hubspot_match.get('match_reason', '')})")

        # NetSuite match
        street  = row.get("Address", "")
        zipcode = row.get("Zipcode", "")
        print(f"  Checking NetSuite for: {street}, {zipcode}")
        netsuite_match = self.netsuite.match(street, zipcode)
        if netsuite_match.get("matched"):
            print(f"  NetSuite match: {netsuite_match.get('netsuite_name')} "
                  f"(confidence: {netsuite_match.get('match_confidence')})")
        else:
            print(f"  NetSuite: no match ({netsuite_match.get('match_reason', '')})")

        battle_card = {
            "ey_file_data":       ey_data,
            "connectbase_data":   connectbase_data,
            "geocode_data":       geocode_data,
            "llm_analysis":       llm_analysis,
            "hubspot_match":      hubspot_match,
            "netsuite_match":     netsuite_match,
            "additional_tenants": additional_tenants,
            "metadata": {
                "analysis_date": time.strftime('%Y-%m-%d %H:%M:%S'),
                "csv_row_index": idx
            }
        }

        return (idx, battle_card)

    # Timeout per row: geocode + LLM + HubSpot + NetSuite can each take time
    ROW_TIMEOUT_SECONDS = 300  # 5 minutes per row

    def process_rows_parallel(self, rows: List[Dict], max_workers: int = 10, row_offset: int = 0) -> List[Dict]:
        """Process CSV rows in parallel and return battle cards.
        row_offset: global starting index so row IDs are unique across shards."""
        indexed_rows = [(row_offset + idx + 1, row) for idx, row in enumerate(rows)]

        results = []
        failed_rows = []
        failure_reasons = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(self._process_single_row, row_data): row_data
                for row_data in indexed_rows
            }

            completed = 0
            for future in concurrent.futures.as_completed(future_to_idx):
                idx, row = future_to_idx[future]
                company_name = row.get("Name", "Unknown")
                try:
                    result = future.result(timeout=self.ROW_TIMEOUT_SECONDS)
                    results.append(result)
                    completed += 1
                    score = result[1].get("llm_analysis", {}).get("overall_score", 0)
                    geocode_ok = result[1].get("geocode_data", {}).get("geocode_status") == "success"
                    if score == 0:
                        reason = "LLM returned score 0"
                        if not geocode_ok:
                            reason = f"Geocoding failed ({result[1].get('geocode_data', {}).get('geocode_status', 'unknown')})"
                        elif result[1].get("llm_analysis", {}).get("_error"):
                            reason = f"LLM error: {result[1]['llm_analysis']['_error']}"
                        logger.warning(f"Row {idx} ({company_name}) produced score 0 — will not appear on map. Reason: {reason}")
                        failure_reasons[idx] = reason
                    print(f"\nProgress: {completed}/{len(rows)} complete (score: {score})\n")
                except concurrent.futures.TimeoutError:
                    error_msg = f"Timed out after {self.ROW_TIMEOUT_SECONDS}s"
                    logger.error(f"Row {idx} ({company_name}) TIMEOUT: {error_msg}")
                    failed_rows.append(idx)
                    failure_reasons[idx] = error_msg
                    results.append((idx, self._create_error_card(idx, row, error_msg)))
                except Exception as e:
                    error_msg = f"{type(e).__name__}: {e}"
                    logger.error(f"Row {idx} ({company_name}) FAILED: {error_msg}")
                    failed_rows.append(idx)
                    failure_reasons[idx] = error_msg
                    results.append((idx, self._create_error_card(idx, row, error_msg)))

        # Summary
        total = len(rows)
        errored = len(failed_rows)
        zero_score = len([r for r in results if r[1].get("llm_analysis", {}).get("overall_score", 0) == 0])
        map_visible = total - zero_score
        logger.info(f"Processing complete: {total} rows total, {errored} errored, {zero_score} scored 0, {map_visible} will appear on map")
        if failure_reasons:
            # Group by reason type
            reason_counts = {}
            for reason in failure_reasons.values():
                key = reason.split(":")[0] if ":" in reason else reason
                reason_counts[key] = reason_counts.get(key, 0) + 1
            logger.warning(f"Failure breakdown: {reason_counts}")
            for idx, reason in list(failure_reasons.items())[:10]:
                logger.warning(f"  Row {idx}: {reason}")
            if len(failure_reasons) > 10:
                logger.warning(f"  ... and {len(failure_reasons) - 10} more")

        results.sort(key=lambda x: x[0])
        return [r[1] for r in results]

    def _create_error_card(self, idx: int, row: Dict, error_msg: str) -> Dict:
        """Create a fallback battle card for a failed row, preserving available data."""
        return {
            "ey_file_data":       dict(row) if row else {},
            "connectbase_data":   {},
            "geocode_data": {
                "latitude": None, "longitude": None,
                "geocode_quality": None,
                "formatted_address": f"{row.get('Address', '')}, {row.get('City', '')}, {row.get('State', '')} {row.get('Zipcode', '')}".strip(', ') if row else "",
                "geocode_status": "error"
            },
            "llm_analysis":       self.llm._create_fallback_analysis(error_msg),
            "hubspot_match":      {"matched": False, "match_reason": f"processing error: {error_msg}"},
            "netsuite_match":     {"matched": False, "match_reason": f"processing error: {error_msg}"},
            "additional_tenants": [],
            "metadata": {
                "analysis_date": time.strftime('%Y-%m-%d %H:%M:%S'),
                "csv_row_index": idx,
                "error": error_msg
            }
        }
