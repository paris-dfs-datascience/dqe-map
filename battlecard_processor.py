"""Data processing logic for battle card generation."""

import time
import os
import io
from typing import Dict, List, Tuple
import concurrent.futures
import requests
import urllib.parse
from google.cloud import storage

from battlecard_llm import BattleCardLLM
from hubspot_matcher import HubSpotMatcher
from netsuite_matcher import NetSuiteMatcher


class BattleCardProcessor:
    """Handles CSV processing and battle card generation."""

    def __init__(self, llm: BattleCardLLM, project_id: str = "lma-website-461920"):
        self.llm = llm
        self.project_id = project_id

        self.maps_api_key = os.environ.get('GOOGLE_MAPS_API_KEY', '')
        if not self.maps_api_key:
            print("⚠️  WARNING: GOOGLE_MAPS_API_KEY not set in environment")

        gcs_bucket = os.environ.get('GCS_BUCKET', 'dqe-fiber-data')
        self.hubspot = HubSpotMatcher(gcs_bucket=gcs_bucket, project_id=project_id)
        self.netsuite = NetSuiteMatcher(gcs_bucket=gcs_bucket, project_id=project_id)

    def _geocode_address(self, address: str, city: str, state: str, zipcode: str) -> Dict:
        """Geocode an address using Google Geocoding API v4."""
        try:
            full_address    = f"{address}, {city}, {state} {zipcode}".strip()
            encoded_address = urllib.parse.quote(full_address)
            url             = f"https://geocode.googleapis.com/v4beta/geocode/address/{encoded_address}"

            response = requests.get(
                url,
                headers={"X-Goog-Api-Key": self.maps_api_key},
                params={"regionCode": "US"},
                timeout=10
            )

            if response.status_code != 200:
                print(f"    ⚠ Geocoding API error: {response.status_code}")
                return {
                    "latitude": None, "longitude": None,
                    "geocode_quality": None,
                    "formatted_address": full_address,
                    "geocode_status": f"error_{response.status_code}"
                }

            data = response.json()
            if data.get("results"):
                result   = data["results"][0]
                location = result.get("location", {})
                return {
                    "latitude":          location.get("latitude"),
                    "longitude":         location.get("longitude"),
                    "formatted_address": result.get("formattedAddress", full_address),
                    "geocode_status":    "success",
                    "geocode_quality": {
                        "granularity": result.get("granularity", "UNKNOWN"),
                        "place_id":    result.get("placeId", "")
                    }
                }

            return {
                "latitude": None, "longitude": None,
                "geocode_quality": None,
                "formatted_address": full_address,
                "geocode_status": "no_results"
            }

        except Exception as e:
            print(f"    ⚠ Geocoding error: {str(e)}")
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
        return {
            "API_EntityName":          row.get("API_EntityName", "N/A"),
            "API_Website":             row.get("API_Website", "N/A"),
            "API_Phone":               row.get("API_Phone", "N/A"),
            "API_LinkedIn":            row.get("API_LinkedIn", "N/A"),
            "API_NoOfEmployees":       row.get("API_NoOfEmployees", "N/A"),
            "API_MonthlyNetworkSpend": row.get("API_MonthlyNetworkSpend", "N/A"),
            "API_Revenue":             row.get("API_Revenue", "N/A"),
            "API_Industry":            row.get("API_Industry", "N/A"),
            "API_FoundedYear":         row.get("API_FoundedYear", "N/A"),
            "API_LocationType":        row.get("API_LocationType", "N/A"),
            "API_LocationCount":       row.get("API_LocationCount", "N/A"),
            "DQE_Site_Distance":       row.get("DQE_Site_Distance", "N/A"),
            "DQE_Connection_Status":   row.get("DQE_Connection_Status", "N/A"),
            "DQE_Access_Medium":       row.get("DQE_Access_Medium", "N/A"),
            "DQE_Network_Status":      row.get("DQE_Network_Status", "N/A"),
            "SITE_All_Competitors":    row.get("SITE_All_Competitors", "N/A"),
        }

    def _extract_additional_tenants(self, row: Dict) -> List[str]:
        val = row.get("API_Additional_Tenants", "N/A")
        if val and val != "N/A":
            return [t.strip() for t in val.split(",")]
        return []

    def _process_single_row(self, row_data: Tuple[int, Dict]) -> Tuple[int, Dict]:
        """Process a single row — designed for parallel execution."""
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
            print(f"    ✓ Geocoded: {geocode_data['latitude']}, {geocode_data['longitude']}")
        else:
            print(f"    ⚠ Geocoding {geocode_data['geocode_status']}")

        # LLM analysis
        if connectbase_data.get("API_EntityName") != "N/A":
            print(f"  ✓ Has ConnectBase data")
        else:
            print(f"  ⚠ No ConnectBase data — analyzing with EY data only")

        llm_analysis = self.llm.analyze_prospect(ey_data, connectbase_data)

        # HubSpot match
        company_name  = row.get("Name", "")
        print(f"  Checking HubSpot for: {company_name}")
        hubspot_match = self.hubspot.match(company_name)
        if hubspot_match.get("matched"):
            print(f"  ✓ HubSpot match: {hubspot_match['hubspot_name']} "
                  f"(confidence: {hubspot_match['match_confidence']})")
        else:
            print(f"  — HubSpot: no match ({hubspot_match.get('match_reason', '')})")

        # NetSuite match
        street  = row.get("Address", "")
        zipcode = row.get("Zipcode", "")
        print(f"  Checking NetSuite for: {street}, {zipcode}")
        netsuite_match = self.netsuite.match(street, zipcode)
        if netsuite_match.get("matched"):
            print(f"  ✓ NetSuite match: {netsuite_match.get('netsuite_name')} "
                  f"(confidence: {netsuite_match.get('match_confidence')})")
        else:
            print(f"  — NetSuite: no match ({netsuite_match.get('match_reason', '')})")

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

    def process_rows_parallel(self, rows: List[Dict], max_workers: int = 10) -> List[Dict]:
        """Process CSV rows in parallel and return battle cards."""
        indexed_rows = [(idx + 1, row) for idx, row in enumerate(rows)]

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(self._process_single_row, row_data): row_data[0]
                for row_data in indexed_rows
            }

            completed = 0
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    result = future.result()
                    results.append(result)
                    completed += 1
                    print(f"\n✓ Progress: {completed}/{len(rows)} complete\n")
                except Exception as e:
                    print(f"\n✗ Error processing row {idx}: {str(e)}\n")
                    results.append((idx, {
                        "ey_file_data":       {},
                        "connectbase_data":   {},
                        "geocode_data": {
                            "latitude": None, "longitude": None,
                            "geocode_quality": None,
                            "formatted_address": "",
                            "geocode_status": "error"
                        },
                        "llm_analysis":       self.llm._create_fallback_analysis(str(e)),
                        "hubspot_match":      {"matched": False, "match_reason": f"processing error: {str(e)}"},
                        "netsuite_match":     {"matched": False, "match_reason": f"processing error: {str(e)}"},
                        "additional_tenants": [],
                        "metadata": {
                            "analysis_date": time.strftime('%Y-%m-%d %H:%M:%S'),
                            "csv_row_index": idx,
                            "error": str(e)
                        }
                    }))

        results.sort(key=lambda x: x[0])
        return [r[1] for r in results]