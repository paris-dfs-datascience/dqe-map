"""Data processing logic for battle card generation."""

import time
from typing import Dict, List, Tuple, Optional
import concurrent.futures
import requests
import urllib.parse
import os

from battlecard_llm import BattleCardLLM


class BattleCardProcessor:
    """Handles CSV processing and battle card generation."""
    
    def __init__(self, llm: BattleCardLLM, project_id: str = "lma-website-461920"):
        """Initialize with LLM handler."""
        self.llm = llm
        self.project_id = project_id
        
        # Get API key from environment (should be set in Cloud Run)
        self.maps_api_key = os.environ.get('GOOGLE_MAPS_API_KEY', '')
        if not self.maps_api_key:
            print("⚠️  WARNING: GOOGLE_MAPS_API_KEY not set in environment")
    
    def _geocode_address(self, address: str, city: str, state: str, zipcode: str) -> Dict:
        """
        Geocode an address using Google Geocoding API v4.
        
        Returns dict with lat, lng, and validation info.
        """
        try:
            # Build full address string
            full_address = f"{address}, {city}, {state} {zipcode}".strip()
            
            # URL encode the address
            encoded_address = urllib.parse.quote(full_address)
            
            # Use Google Geocoding API v4 beta
            url = f"https://geocode.googleapis.com/v4beta/geocode/address/{encoded_address}"
            
            headers = {
                "X-Goog-Api-Key": self.maps_api_key
            }
            
            params = {
                "regionCode": "US"  # Bias results to US
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code != 200:
                print(f"    ⚠ Geocoding API error: {response.status_code}")
                return {
                    "latitude": None,
                    "longitude": None,
                    "geocode_quality": None,
                    "formatted_address": full_address,
                    "geocode_status": f"error_{response.status_code}"
                }
            
            data = response.json()
            
            # Check if we have results
            if data.get("results") and len(data["results"]) > 0:
                result = data["results"][0]
                location = result.get("location", {})
                
                return {
                    "latitude": location.get("latitude"),
                    "longitude": location.get("longitude"),
                    "formatted_address": result.get("formattedAddress", full_address),
                    "geocode_status": "success",
                    "geocode_quality": {
                        "granularity": result.get("granularity", "UNKNOWN"),
                        "place_id": result.get("placeId", "")
                    }
                }
            else:
                return {
                    "latitude": None,
                    "longitude": None,
                    "geocode_quality": None,
                    "formatted_address": full_address,
                    "geocode_status": "no_results"
                }
                
        except Exception as e:
            print(f"    ⚠ Geocoding error: {str(e)}")
            return {
                "latitude": None,
                "longitude": None,
                "geocode_quality": None,
                "formatted_address": f"{address}, {city}, {state} {zipcode}",
                "geocode_status": "error",
                "geocode_error": str(e)
            }
    
    def _extract_ey_data(self, row: Dict) -> Dict:
        """Extract EY file data from CSV row."""
        return {
            "Name": row.get("Name", ""),
            "Address": row.get("Address", ""),
            "City": row.get("City", ""),
            "State": row.get("State", ""),
            "Zipcode": row.get("Zipcode", ""),
            "Bandwidth": row.get("Bandwitdh ( DIA or Broadband)", ""),
            "Prediction": row.get("Prediction", ""),
            "Direct or Wholesale Prediction": row.get("Direct or Wholesale Prediction", ""),
            "DQE ICP Focus": row.get("DQE ICP Focus", ""),
            "Provider And Connectivity": row.get("Provider And Connectivity", ""),
            "Website": row.get("Website", ""),
            "Phone": row.get("Phone", ""),
            "LinkedIn Url": row.get("LinkedIn Url", ""),
            "No Of Employees": row.get("No Of Employees", ""),
            "Est Telco Spend": row.get("Est Telco Spend", ""),
            "Network Build Status": row.get("Network Build Status", ""),
            "Building Connection Status": row.get("Building Connection Status", ""),
            "Building Category": row.get("Building Category", ""),
            "Access Medium": row.get("Access Medium", ""),
            "Supplier Name": row.get("Supplier Name", ""),
            "Global Location ID": row.get("Global Location ID", ""),
        }
    
    def _extract_connectbase_data(self, row: Dict) -> Dict:
        """Extract ConnectBase data from CSV row."""
        return {
            "API_EntityName": row.get("API_EntityName", "N/A"),
            "API_Website": row.get("API_Website", "N/A"),
            "API_Phone": row.get("API_Phone", "N/A"),
            "API_LinkedIn": row.get("API_LinkedIn", "N/A"),
            "API_NoOfEmployees": row.get("API_NoOfEmployees", "N/A"),
            "API_MonthlyNetworkSpend": row.get("API_MonthlyNetworkSpend", "N/A"),
            "API_Revenue": row.get("API_Revenue", "N/A"),
            "API_Industry": row.get("API_Industry", "N/A"),
            "API_FoundedYear": row.get("API_FoundedYear", "N/A"),
            "API_LocationType": row.get("API_LocationType", "N/A"),
            "API_LocationCount": row.get("API_LocationCount", "N/A"),
            "DQE_Site_Distance": row.get("DQE_Site_Distance", "N/A"),
            "DQE_Connection_Status": row.get("DQE_Connection_Status", "N/A"),
            "DQE_Access_Medium": row.get("DQE_Access_Medium", "N/A"),
            "DQE_Network_Status": row.get("DQE_Network_Status", "N/A"),
            "SITE_All_Competitors": row.get("SITE_All_Competitors", "N/A"),
        }
    
    def _extract_additional_tenants(self, row: Dict) -> List[str]:
        """Extract additional tenants from CSV row."""
        additional_tenants_str = row.get("API_Additional_Tenants", "N/A")
        if additional_tenants_str != "N/A" and additional_tenants_str:
            return [t.strip() for t in additional_tenants_str.split(",")]
        return []
    
    def _process_single_row(self, row_data: Tuple[int, Dict]) -> Tuple[int, Dict]:
        """Process a single row - designed for parallel execution."""
        idx, row = row_data
        
        print(f"[{idx}] Processing: {row.get('Name', 'Unknown')}")
        
        # Extract data sections
        ey_data = self._extract_ey_data(row)
        connectbase_data = self._extract_connectbase_data(row)
        additional_tenants = self._extract_additional_tenants(row)
        
        # Geocode the address
        print(f"  Geocoding address...")
        geocode_data = self._geocode_address(
            ey_data['Address'],
            ey_data['City'],
            ey_data['State'],
            ey_data['Zipcode']
        )
        
        if geocode_data['geocode_status'] == 'success':
            print(f"    ✓ Geocoded: {geocode_data['latitude']}, {geocode_data['longitude']}")
        else:
            print(f"    ⚠ Geocoding {geocode_data['geocode_status']}")
        
        # ALWAYS run LLM Analysis - even without ConnectBase data
        # The LLM will work with whatever data is available
        if connectbase_data.get("API_EntityName") != "N/A":
            print(f"  ✓ Has ConnectBase data")
        else:
            print(f"  ⚠ No ConnectBase data - analyzing with EY data only")
        
        llm_analysis = self.llm.analyze_prospect(ey_data, connectbase_data)
        
        # Create battle card with 5 sections (added geocode section)
        battle_card = {
            "ey_file_data": ey_data,
            "connectbase_data": connectbase_data,
            "geocode_data": geocode_data,
            "llm_analysis": llm_analysis,
            "additional_tenants": additional_tenants,
            "metadata": {
                "analysis_date": time.strftime('%Y-%m-%d %H:%M:%S'),
                "csv_row_index": idx
            }
        }
        
        return (idx, battle_card)
    
    def process_rows_parallel(self, rows: List[Dict], max_workers: int = 10) -> List[Dict]:
        """Process CSV rows in parallel and return battle cards."""
        # Prepare indexed rows
        indexed_rows = [(idx + 1, row) for idx, row in enumerate(rows)]
        
        # Process in parallel
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_idx = {
                executor.submit(self._process_single_row, row_data): row_data[0] 
                for row_data in indexed_rows
            }
            
            # Collect results as they complete
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
                    # Create a fallback result
                    results.append((idx, {
                        "ey_file_data": {},
                        "connectbase_data": {},
                        "geocode_data": {
                            "latitude": None,
                            "longitude": None,
                            "geocode_quality": None,
                            "formatted_address": "",
                            "geocode_status": "error"
                        },
                        "llm_analysis": self.llm._create_fallback_analysis(str(e)),
                        "additional_tenants": [],
                        "metadata": {
                            "analysis_date": time.strftime('%Y-%m-%d %H:%M:%S'),
                            "csv_row_index": idx,
                            "error": str(e)
                        }
                    }))
        
        # Sort results by original index
        results.sort(key=lambda x: x[0])
        battle_cards = [r[1] for r in results]
        
        return battle_cards