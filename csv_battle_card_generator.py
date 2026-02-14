import csv
import json
import time
from typing import Dict, List, Optional
from google.cloud import storage
from google import genai
from google.genai import types
import concurrent.futures
from functools import partial

class CSVBattleCardGenerator:
    """Generates battle cards from enriched CSV data for map visualization."""
    
    def __init__(self, gcs_bucket: str, project_id: str = "lma-website-461920"):
        """
        Initialize the CSV battle card generator.
        
        Args:
            gcs_bucket: GCS bucket name for storing results
            project_id: GCP project ID
        """
        self.gcs_bucket = gcs_bucket
        self.storage_client = storage.Client()
        
        # Initialize Gemini client
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location="us-central1"
        )

        # Tool for research
        self.google_search_tool = types.Tool(
            google_search=types.GoogleSearch()
        )
    
        # Research config with search
        self.research_config = types.GenerateContentConfig(
            temperature=1.0,
            tools=[self.google_search_tool]
        )

        # Formatting config with JSON output
        self.formatting_config = types.GenerateContentConfig(
            temperature=0.2,
            top_p=0.8,
            top_k=40,
            max_output_tokens=8192,
            response_mime_type="application/json"
        )
        
        # Token tracking (thread-safe)
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self._token_lock = None  # Will be initialized when needed
        
        print(f"Initialized CSVBattleCardGenerator")
    
    def _track_tokens(self, response):
        """Helper to track tokens in a thread-safe way."""
        if hasattr(response, 'usage_metadata'):
            usage = response.usage_metadata
            input_tokens = getattr(usage, 'prompt_token_count', 0)
            output_tokens = getattr(usage, 'candidates_token_count', 0)
            
            # Thread-safe token tracking
            if self._token_lock:
                with self._token_lock:
                    self.total_input_tokens += input_tokens
                    self.total_output_tokens += output_tokens
            else:
                self.total_input_tokens += input_tokens
                self.total_output_tokens += output_tokens
    
    def analyze_with_llm(self, ey_data: Dict, connectbase_data: Dict) -> Dict:
        """
        Use LLM to analyze and score the prospect.
        Two-pass approach: research then format.
        """
        business_name = ey_data.get('Name', 'Unknown')
        address = ey_data.get('Address', '')
        city = ey_data.get('City', '')
        state = ey_data.get('State', '')
        
        # Get employee count from best source
        ey_employees = ey_data.get('No Of Employees', 'N/A')
        cb_employees = connectbase_data.get('API_NoOfEmployees', 'N/A')
        
        try:
            print(f"  Researching: {business_name}...")
            
            # --- PASS 1: RESEARCH ---
            research_prompt = f"""Search for and research the following business:
            Name: {business_name}
            Address: {address}, {city}, {state}
            EY Employee Count: {ey_employees}
            Connectbase Employee Count: {cb_employees}
            Connectbase LinkedIn: {connectbase_data.get('API_LinkedIn', 'N/A')}
            
            Provide a detailed report on:
            1. Current operating status at this location
            2. Best guess at employee count for the location at {address}, {city}, {state} (look for LinkedIn data, company releases, ZoomInfo, official site, if not available make an assumption based on size of company and other locations)
            3. Business type, vertical, and infrastructure needs
            4. Revenue potential indicators
            
            Focus on validating the employee count and business status.
            """
            
            research_resp = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=research_prompt,
                config=self.research_config
            )
            research_text = research_resp.text
            self._track_tokens(research_resp)

            # --- PASS 2: FORMATTING ---
            print(f"    Creating battle card...")

            prompt = f"""
RESEARCH DATA FOUND:
{research_text}

---
You are a B2B sales intelligence analyst for DQE Communications, a fiber-optic telecommunications provider.

TASK: Validate data accuracy and score this business as a potential customer on a 0-100 scale.

BUSINESS DETAILS FROM EY:
- Business Name: {business_name}
- Address: {address}, {city}, {state}
- EY Employee Count: {ey_employees}
- Network Build Status: {ey_data.get('Network Build Status', 'N/A')}
- Building Connection: {ey_data.get('Building Connection Status', 'N/A')}

CONNECTBASE API DATA:
- CB Entity Name: {connectbase_data.get('API_EntityName', 'N/A')}
- CB Employee Count: {cb_employees}
- CB Industry: {connectbase_data.get('API_Industry', 'N/A')}
- CB Monthly Network Spend: {connectbase_data.get('API_MonthlyNetworkSpend', 'N/A')}
- CB Revenue: {connectbase_data.get('API_Revenue', 'N/A')}
- CB Location Type: {connectbase_data.get('API_LocationType', 'N/A')}
- CB LinkedIn: {connectbase_data.get('API_LinkedIn', 'N/A')}

SCORING METHODOLOGY (0-100 points total):

1. DATA VALIDITY (0-40 points):
   - Employee Count Accuracy (0-20 points):
     * Compare EY vs Connectbase vs your research
     * 20 pts: Validated exact match within ±2 employees
     * 15 pts: Validated within ±5 employees
     * 10 pts: Validated within ±10 employees
     * 5 pts: Company exists but headcount unclear
     * 0 pts: Cannot validate or business appears closed
   
   - Business Information Accuracy (0-20 points):
     * Operating Status (0-10 pts): Currently active at this location
     * Contact Info Validity (0-10 pts): Data sources align
   
2. ICP FIT (0-60 points):
   - Infrastructure Criticality (0-30 points):
     * 20 pts: Mission-critical (financial services HQs, healthcare, data centers, tech)
     * 15 pts: High criticality (legal, accounting, engineering, large corporate)
     * 10 pts: Moderate criticality (medium professional services, regional offices)
     * 5 pts: Standard needs (small offices, retail, general services)
     * 0 pts: Low criticality (individual practitioners, very small offices)
     * 0 pts: Minimal needs (single consultant)
   
   - Revenue Potential (0-30 points):
     Based on validated employee count and industry:
     * 20 pts: 100+ employees or high-value vertical
     * 10 pts: 50-99 employees or medium-value vertical
     * 5 pts: 25-49 employees
     * 3 pts: 10-24 employees
     * 1 pts: 1-9 employees
     * 0 pts: Unable to determine or inactive

DQE CONTEXT:
- Building is {ey_data.get('Network Build Status', 'unknown')}
- Connection Status: {ey_data.get('Building Connection Status', 'unknown')}
- If Near Net or On Net, this is HIGH priority (lower build costs)

OUTPUT FORMAT (strict JSON):
{{
  "overall_score": <0-100>,
  "score_breakdown": {{
    "data_validity_score": <0-40>,
    "icp_fit_score": <0-60>
  }},
  "validation": {{
    "validated_employee_count": <number or null>,
    "employee_validation_points": <0-20>,
    "employee_validation_method": "<how you validated>",
    "employee_count_comparison": "<EY: X, CB: Y, Validated: Z>",
    "business_info_points": <0-20>,
    "business_status": "<operating|closed|moved|uncertain>",
    "location_type": "<headquarters|branch_office|individual_office|unknown>",
    "validation_sources": ["source1", "source2"],
    "validation_confidence": "<high|medium|low>",
    "data_quality_notes": "<concerns or clarifications>"
  }},
  "icp_assessment": {{
    "infrastructure_criticality_points": <0-30>,
    "infrastructure_criticality_level": "<mission_critical|high|moderate|standard|low|minimal>",
    "revenue_potential_points": <0-50>,
    "infrastructure_needs": ["need1", "need2"],
    "estimated_monthly_spend": <number>,
    "reasoning": "<2-3 sentences explaining the score>"
  }},
  "sales_intelligence": {{
    "priority_level": "<immediate|high|medium|low|disqualify>",
    "key_talking_points": ["point1", "point2", "point3"],
    "likely_pain_points": ["pain1", "pain2"],
    "recommended_approach": "<how sales should approach>",
    "recommended_services": ["DIA", "SD-WAN", "Managed Security", "etc"],
    "dqe_advantage": "<why DQE is positioned well for this prospect>"
  }}
}}

IMPORTANT: Return ONLY valid JSON, no additional text
"""
            
            format_resp = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=self.formatting_config
            )
            self._track_tokens(format_resp)
            
            # Extract JSON
            response_text = format_resp.text.strip()
            if response_text.startswith('```json'):
                response_text = response_text.replace('```json', '').replace('```', '').strip()
            elif response_text.startswith('```'):
                response_text = response_text.replace('```', '').strip()
            
            llm_analysis = json.loads(response_text)
            
            score = llm_analysis['overall_score']
            validated_emp = llm_analysis['validation']['validated_employee_count']
            confidence = llm_analysis['validation']['validation_confidence']
            
            print(f"    ✓ Score: {score}/100")
            print(f"    ✓ Validated Employees: {validated_emp} ({confidence} confidence)")
            print(f"    ✓ Priority: {llm_analysis['sales_intelligence']['priority_level']}")
            
            return llm_analysis
            
        except json.JSONDecodeError as e:
            print(f"    ✗ JSON parsing error: {str(e)}")
            return self._create_fallback_analysis(str(e))
        except Exception as e:
            print(f"    ✗ Error: {str(e)}")
            return self._create_fallback_analysis(str(e))
    
    def _create_fallback_analysis(self, error: str) -> Dict:
        """Create minimal analysis when LLM fails."""
        return {
            "overall_score": 0,
            "score_breakdown": {
                "data_validity_score": 0,
                "icp_fit_score": 0
            },
            "validation": {
                "validated_employee_count": None,
                "employee_validation_points": 0,
                "employee_validation_method": "Validation failed",
                "employee_count_comparison": "N/A",
                "business_info_points": 0,
                "business_status": "unknown",
                "location_type": "unknown",
                "validation_sources": [],
                "validation_confidence": "none",
                "data_quality_notes": f"Error: {error}"
            },
            "icp_assessment": {
                "infrastructure_criticality_points": 0,
                "infrastructure_criticality_level": "unknown",
                "revenue_potential_points": 0,
                "infrastructure_needs": [],
                "estimated_monthly_spend": 0,
                "reasoning": "Unable to assess due to validation failure"
            },
            "sales_intelligence": {
                "priority_level": "disqualify",
                "key_talking_points": [],
                "likely_pain_points": [],
                "recommended_approach": "Unable to provide recommendation",
                "recommended_services": [],
                "dqe_advantage": "N/A"
            }
        }
    
    def _process_single_row(self, row_data: tuple) -> Dict:
        """Process a single row - designed for parallel execution."""
        idx, row = row_data
        
        print(f"[{idx}] Processing: {row.get('Name', 'Unknown')}")
        
        # Section 1: EY File Data
        ey_data = {
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
        
        # Section 2: Connectbase Data
        connectbase_data = {
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
        }
        
        # Section 4: Additional Tenants
        additional_tenants_str = row.get("API_Additional_Tenants", "N/A")
        additional_tenants = []
        if additional_tenants_str != "N/A" and additional_tenants_str:
            additional_tenants = [t.strip() for t in additional_tenants_str.split(",")]
        
        # Section 3: LLM Analysis (only if we have data)
        llm_analysis = None
        if connectbase_data.get("API_EntityName") != "N/A":
            llm_analysis = self.analyze_with_llm(ey_data, connectbase_data)
        else:
            print(f"  ⚠ No Connectbase data - skipping LLM analysis")
            llm_analysis = self._create_fallback_analysis("No Connectbase data available")
        
        # Create battle card with 4 sections
        battle_card = {
            "ey_file_data": ey_data,
            "connectbase_data": connectbase_data,
            "llm_analysis": llm_analysis,
            "additional_tenants": additional_tenants,
            "metadata": {
                "analysis_date": time.strftime('%Y-%m-%d %H:%M:%S'),
                "csv_row_index": idx
            }
        }
        
        return (idx, battle_card)
    
    def process_csv(self, csv_file: str, max_workers: int = 10) -> List[Dict]:
        """
        Process enriched CSV and generate battle cards with parallel execution.
        
        Args:
            csv_file: Path to input CSV
            max_workers: Number of parallel workers (default 10)
        
        Returns list of battle cards with 4 sections:
        1. EY File Data
        2. Connectbase Data
        3. LLM Analysis
        4. Additional Tenants
        """
        print(f"\n=== Processing CSV: {csv_file} ===")
        print(f"Using {max_workers} parallel workers\n")
        
        # Reset counters
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        
        # Initialize thread lock for token tracking
        import threading
        self._token_lock = threading.Lock()
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        print(f"Found {len(rows)} records to process\n")
        
        # Prepare indexed rows for parallel processing
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
                        "llm_analysis": self._create_fallback_analysis(str(e)),
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
        
        # Print summary
        print(f"\n=== Token Usage ===")
        print(f"Total Input Tokens: {self.total_input_tokens:,}")
        print(f"Total Output Tokens: {self.total_output_tokens:,}")
        print(f"Total Tokens: {self.total_input_tokens + self.total_output_tokens:,}")
        
        return battle_cards
    
    def save_to_gcs(self, battle_cards: List[Dict], output_name: str = "battle_cards") -> bool:
        """Save battle cards to GCS for map visualization."""
        try:
            blob_path = f"csv-battle-cards/{output_name}.json"
            bucket = self.storage_client.bucket(self.gcs_bucket)
            blob = bucket.blob(blob_path)
            
            # Calculate statistics
            total = len(battle_cards)
            with_analysis = sum(1 for bc in battle_cards if bc['llm_analysis']['overall_score'] > 0)
            
            scores = [bc['llm_analysis']['overall_score'] for bc in battle_cards if bc['llm_analysis']['overall_score'] > 0]
            avg_score = round(sum(scores) / len(scores), 1) if scores else 0
            
            # Score distribution
            score_ranges = {
                "80-100 (Excellent)": sum(1 for s in scores if s >= 80),
                "60-79 (Good)": sum(1 for s in scores if 60 <= s < 80),
                "40-59 (Fair)": sum(1 for s in scores if 40 <= s < 60),
                "20-39 (Poor)": sum(1 for s in scores if 20 <= s < 40),
                "0-19 (Disqualified)": sum(1 for s in scores if s < 20)
            }
            
            # Top prospects
            top_prospects = sorted(
                [
                    {
                        "business_name": bc['ey_file_data']['Name'],
                        "address": bc['ey_file_data']['Address'],
                        "city": bc['ey_file_data']['City'],
                        "state": bc['ey_file_data']['State'],
                        "score": bc['llm_analysis']['overall_score'],
                        "validated_employees": bc['llm_analysis']['validation']['validated_employee_count'],
                        "priority": bc['llm_analysis']['sales_intelligence']['priority_level'],
                        "network_status": bc['ey_file_data']['Network Build Status']
                    }
                    for bc in battle_cards if bc['llm_analysis']['overall_score'] > 0
                ],
                key=lambda x: x['score'],
                reverse=True
            )[:20]
            
            output = {
                "summary": {
                    "total_records": total,
                    "analyzed_records": with_analysis,
                    "skipped_records": total - with_analysis,
                    "generation_date": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "avg_score": avg_score,
                    "score_distribution": score_ranges,
                    "token_usage": {
                        "input_tokens": self.total_input_tokens,
                        "output_tokens": self.total_output_tokens,
                        "total_tokens": self.total_input_tokens + self.total_output_tokens
                    },
                    "top_prospects": top_prospects
                },
                "battle_cards": battle_cards
            }
            
            blob.upload_from_string(
                json.dumps(output, indent=2),
                content_type='application/json'
            )
            
            print(f"\n✓ Battle cards saved: gs://{self.gcs_bucket}/{blob_path}")
            print(f"\n=== SUMMARY ===")
            print(f"Total Records: {total}")
            print(f"Analyzed: {with_analysis}")
            print(f"Average Score: {avg_score}/100")
            print(f"\nTop 5 Prospects:")
            for i, p in enumerate(top_prospects[:5], 1):
                print(f"  {i}. {p['business_name']} - Score: {p['score']}, Priority: {p['priority']}")
            
            return True
            
        except Exception as e:
            print(f"Error saving battle cards: {str(e)}")
            return False
    
    def save_to_local(self, battle_cards: List[Dict], output_file: str = "battle_cards_output.json") -> bool:
        """Save battle cards locally for testing."""
        try:
            # Same summary as GCS version
            total = len(battle_cards)
            with_analysis = sum(1 for bc in battle_cards if bc['llm_analysis']['overall_score'] > 0)
            scores = [bc['llm_analysis']['overall_score'] for bc in battle_cards if bc['llm_analysis']['overall_score'] > 0]
            avg_score = round(sum(scores) / len(scores), 1) if scores else 0
            
            output = {
                "summary": {
                    "total_records": total,
                    "analyzed_records": with_analysis,
                    "avg_score": avg_score,
                    "generation_date": time.strftime('%Y-%m-%d %H:%M:%S')
                },
                "battle_cards": battle_cards
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            print(f"\n✓ Battle cards saved locally: {output_file}")
            return True
            
        except Exception as e:
            print(f"Error saving locally: {str(e)}")
            return False


def main():
    """Main execution."""
    # Configuration
    GCS_BUCKET = "dqe-fiber-data"
    INPUT_CSV = "tenants_enriched.csv"
    OUTPUT_NAME = "dqe_prospects"
    MAX_WORKERS = 10  # Adjust based on your needs
    
    # Initialize generator
    generator = CSVBattleCardGenerator(gcs_bucket=GCS_BUCKET)
    
    # Process CSV with parallel execution
    battle_cards = generator.process_csv(INPUT_CSV, max_workers=MAX_WORKERS)
    
    # Save to GCS
    generator.save_to_gcs(battle_cards, output_name=OUTPUT_NAME)
    
    # Also save locally for debugging
    generator.save_to_local(battle_cards, "battle_cards_local.json")


if __name__ == "__main__":
    main()