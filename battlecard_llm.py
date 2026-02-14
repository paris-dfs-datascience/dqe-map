"""LLM interaction logic for battle card generation."""

import json
import threading
from typing import Dict
from google import genai
from google.genai import types

from battlecard_config import get_research_prompt, get_analysis_prompt


class BattleCardLLM:
    """Handles all LLM interactions for battle card generation."""
    
    def __init__(self, project_id: str = "lma-website-461920"):
        """Initialize Gemini client and configs."""
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
        self._token_lock = threading.Lock()
    
    def _track_tokens(self, response):
        """Helper to track tokens in a thread-safe way."""
        if hasattr(response, 'usage_metadata'):
            usage = response.usage_metadata
            input_tokens = getattr(usage, 'prompt_token_count', 0)
            output_tokens = getattr(usage, 'candidates_token_count', 0)
            
            with self._token_lock:
                self.total_input_tokens += input_tokens
                self.total_output_tokens += output_tokens
    
    def analyze_prospect(self, ey_data: Dict, connectbase_data: Dict) -> Dict:
        """
        Use LLM to analyze and score the prospect.
        Two-pass approach: research then format.
        """
        business_name = ey_data.get('Name', 'Unknown')
        address = ey_data.get('Address', '')
        city = ey_data.get('City', '')
        state = ey_data.get('State', '')
        
        ey_employees = ey_data.get('No Of Employees', 'N/A')
        cb_employees = connectbase_data.get('API_NoOfEmployees', 'N/A')
        cb_linkedin = connectbase_data.get('API_LinkedIn', 'N/A')
        
        try:
            print(f"  Researching: {business_name}...")
            
            # --- PASS 1: RESEARCH ---
            research_prompt = get_research_prompt(
                business_name, address, city, state,
                ey_employees, cb_employees, cb_linkedin
            )
            
            research_resp = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=research_prompt,
                config=self.research_config
            )
            research_text = research_resp.text
            self._track_tokens(research_resp)

            # --- PASS 2: ANALYSIS & SCORING ---
            print(f"    Creating battle card...")

            analysis_prompt = get_analysis_prompt(
                research_text, business_name, address, city, state,
                ey_data, connectbase_data
            )
            
            format_resp = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=analysis_prompt,
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
            confidence = llm_analysis['data_confidence']['confidence_score']
            icp_score = llm_analysis['icp_fit']['icp_fit_score']
            validated_emp = llm_analysis['data_confidence']['validated_employee_count']
            
            print(f"    ✓ Confidence: {confidence:.2f} × ICP: {icp_score} = Final: {score}")
            print(f"    ✓ Validated Employees: {validated_emp}")
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
            
            "data_confidence": {
                "confidence_score": 0.0,
                "business_status_points": 0.0,
                "employee_validation_points": 0.0,
                "source_quality_points": 0.0,
                "business_status": "unknown",
                "business_status_evidence": "Error during validation",
                "validated_employee_count": None,
                "employee_count_confidence": "none",
                "employee_count_basis": "validation failed",
                "employee_count_sources": [],
                "employee_comparison": "N/A",
                "location_type": "unknown",
                "data_quality_notes": f"Error: {error}"
            },
            
            "icp_fit": {
                "icp_fit_score": 0,
                "network_economics_points": 0,
                "business_scale_need_points": 0,
                "network_analysis": {
                    "dqe_distance_feet": "ERROR",
                    "network_category": "unknown",
                    "build_cost_assessment": "unknown",
                    "network_advantage": "Unable to assess"
                },
                "business_assessment": {
                    "business_criticality": "unknown",
                    "criticality_reasoning": "Unable to assess",
                    "infrastructure_needs": [],
                    "bandwidth_requirements": "unknown",
                    "estimated_monthly_spend": None
                },
                "competitive_context": {
                    "competitors_at_site": "N/A",
                    "competitive_position": "Unable to assess"
                },
                "icp_fit_summary": "Unable to assess due to validation failure"
            },
            
            "sales_intelligence": {
                "priority_level": "disqualify",
                "priority_reasoning": "Data validation failed",
                "key_selling_points": [],
                "likely_pain_points": [],
                "competitive_angles": [],
                "data_gaps_to_resolve": ["Complete data validation required"],
                "recommended_approach": "Unable to provide recommendation",
                "recommended_services": [],
                "next_best_actions": ["Retry data enrichment"]
            }
        }