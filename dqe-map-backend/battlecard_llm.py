"""LLM interaction logic for battle card generation."""

import json
import time
import random
import threading
from typing import Dict
from google import genai
from google.genai import types

from pipeline_config import (
    PROJECT_ID, GCP_LOCATION, LLM_MAX_RETRIES, LLM_BASE_DELAY,
    LLM_REQUIRED_TOP_KEYS, LLM_REQUIRED_CONFIDENCE_KEYS,
    LLM_REQUIRED_ICP_KEYS, LLM_REQUIRED_SALES_KEYS,
)
from battlecard_config import get_research_prompt, get_analysis_prompt


class BattleCardLLM:
    """Handles all LLM interactions for battle card generation."""

    def __init__(self, project_id: str = PROJECT_ID):
        """Initialize Gemini client and configs."""
        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=GCP_LOCATION
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
            max_output_tokens=9128,
            response_mime_type="application/json"
        )

        # Token tracking (thread-safe)
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self._token_lock = threading.Lock()

    def _generate_with_retry(self, model: str, contents, config) -> object:
        """Call generate_content with exponential backoff on 429 errors."""
        for attempt in range(LLM_MAX_RETRIES):
            try:
                return self.client.models.generate_content(
                    model=model,
                    contents=contents,
                    config=config
                )
            except Exception as e:
                if '429' in str(e) and attempt < LLM_MAX_RETRIES - 1:
                    delay = LLM_BASE_DELAY * (2 ** attempt) + random.uniform(0, 2)
                    print(f"    Rate limited, retrying in {delay:.1f}s "
                          f"(attempt {attempt + 1}/{LLM_MAX_RETRIES})")
                    time.sleep(delay)
                else:
                    raise

    def _track_tokens(self, response):
        """Helper to track tokens in a thread-safe way."""
        if hasattr(response, 'usage_metadata'):
            usage = response.usage_metadata
            input_tokens  = getattr(usage, 'prompt_token_count', 0) or 0
            output_tokens = getattr(usage, 'candidates_token_count', 0) or 0

            with self._token_lock:
                self.total_input_tokens  += input_tokens
                self.total_output_tokens += output_tokens

    def _validate_llm_response(self, analysis: Dict) -> Dict:
        """Validate LLM JSON has all required keys, fill missing with defaults."""
        # Top-level keys
        missing_top = LLM_REQUIRED_TOP_KEYS - set(analysis.keys())
        if missing_top:
            print(f"    Warning: LLM response missing top-level keys: {missing_top}")

        # Ensure nested dicts exist
        if "data_confidence" not in analysis:
            analysis["data_confidence"] = {}
        if "icp_fit" not in analysis:
            analysis["icp_fit"] = {}
        if "sales_intelligence" not in analysis:
            analysis["sales_intelligence"] = {}

        # Validate data_confidence
        dc = analysis["data_confidence"]
        missing_dc = LLM_REQUIRED_CONFIDENCE_KEYS - set(dc.keys())
        if missing_dc:
            print(f"    Warning: LLM response missing confidence keys: {missing_dc}")
        dc.setdefault("confidence_score", 0.0)
        dc.setdefault("business_status_points", 0.0)
        dc.setdefault("employee_validation_points", 0.0)
        dc.setdefault("source_quality_points", 0.0)
        dc.setdefault("business_status", "unknown")
        dc.setdefault("business_status_evidence", "")
        dc.setdefault("validated_employee_count", None)
        dc.setdefault("employee_count_confidence", "none")
        dc.setdefault("employee_count_basis", "")
        dc.setdefault("employee_count_sources", [])
        dc.setdefault("employee_comparison", "")
        dc.setdefault("location_type", "unknown")
        dc.setdefault("data_quality_notes", "")

        # Validate icp_fit
        icp = analysis["icp_fit"]
        missing_icp = LLM_REQUIRED_ICP_KEYS - set(icp.keys())
        if missing_icp:
            print(f"    Warning: LLM response missing ICP keys: {missing_icp}")
        icp.setdefault("icp_fit_score", 0)
        icp.setdefault("network_economics_points", 0)
        icp.setdefault("business_scale_need_points", 0)
        icp.setdefault("network_analysis", {
            "network_category": "unknown",
            "build_cost_assessment": "unknown",
            "network_advantage": ""
        })
        icp.setdefault("business_assessment", {
            "business_criticality": "unknown",
            "criticality_reasoning": "",
            "infrastructure_needs": [],
            "bandwidth_requirements": "unknown",
            "estimated_monthly_spend": None
        })
        icp.setdefault("competitive_context", {
            "competitors_at_site": "Unknown",
            "competitive_position": "Unable to assess"
        })
        icp.setdefault("icp_fit_summary", "")

        # Validate sales_intelligence
        si = analysis["sales_intelligence"]
        missing_si = LLM_REQUIRED_SALES_KEYS - set(si.keys())
        if missing_si:
            print(f"    Warning: LLM response missing sales keys: {missing_si}")
        si.setdefault("priority_level", "disqualify")
        si.setdefault("priority_reasoning", "")
        si.setdefault("key_selling_points", [])
        si.setdefault("likely_pain_points", [])
        si.setdefault("competitive_angles", [])
        si.setdefault("data_gaps_to_resolve", [])
        si.setdefault("recommended_approach", "")
        si.setdefault("recommended_services", [])
        si.setdefault("next_best_actions", [])

        # Ensure overall_score is present and numeric
        analysis.setdefault("overall_score", 0)
        if not isinstance(analysis["overall_score"], (int, float)):
            analysis["overall_score"] = 0

        return analysis

    def analyze_prospect(self, ey_data: Dict, connectbase_data: Dict) -> Dict:
        """
        Use LLM to analyze and score the prospect.
        Two-pass approach: research then format.
        """
        business_name = ey_data.get('Name', 'Unknown')
        address  = ey_data.get('Address', '')
        city     = ey_data.get('City', '')
        state    = ey_data.get('State', '')

        ey_employees = ey_data.get('No Of Employees', '')
        cb_employees = connectbase_data.get('API_NoOfEmployees', '')
        cb_linkedin  = connectbase_data.get('API_LinkedIn', '')

        try:
            print(f"  Researching: {business_name}...")

            # --- PASS 1: RESEARCH ---
            research_prompt = get_research_prompt(
                business_name, address, city, state,
                ey_employees, cb_employees, cb_linkedin
            )

            research_resp = self._generate_with_retry(
                "gemini-2.5-flash", research_prompt, self.research_config
            )
            research_text = research_resp.text
            self._track_tokens(research_resp)

            # --- PASS 2: ANALYSIS & SCORING ---
            print(f"    Creating battle card...")

            analysis_prompt = get_analysis_prompt(
                research_text, business_name, address, city, state,
                ey_data, connectbase_data
            )

            format_resp = self._generate_with_retry(
                "gemini-2.5-flash", analysis_prompt, self.formatting_config
            )
            self._track_tokens(format_resp)

            # Extract JSON
            response_text = format_resp.text.strip()
            if response_text.startswith('```json'):
                response_text = response_text.replace('```json', '').replace('```', '').strip()
            elif response_text.startswith('```'):
                response_text = response_text.replace('```', '').strip()

            llm_analysis = json.loads(response_text)

            # Validate response structure
            llm_analysis = self._validate_llm_response(llm_analysis)

            score         = llm_analysis['overall_score']
            confidence    = llm_analysis['data_confidence']['confidence_score']
            icp_score     = llm_analysis['icp_fit']['icp_fit_score']
            validated_emp = llm_analysis['data_confidence']['validated_employee_count']

            print(f"    Confidence: {confidence:.2f} x ICP: {icp_score} = Final: {score}")
            print(f"    Validated Employees: {validated_emp}")
            print(f"    Priority: {llm_analysis['sales_intelligence']['priority_level']}")

            return llm_analysis

        except json.JSONDecodeError as e:
            print(f"    JSON parsing error: {str(e)}")
            return self._create_fallback_analysis(str(e))
        except Exception as e:
            print(f"    Error: {str(e)}")
            return self._create_fallback_analysis(str(e))

    def _create_fallback_analysis(self, error: str) -> Dict:
        """Create minimal analysis when LLM fails."""
        return {
            "overall_score": 0,
            "_error": error,

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
                "employee_comparison": "",
                "location_type": "unknown",
                "data_quality_notes": f"Error: {error}"
            },

            "icp_fit": {
                "icp_fit_score": 0,
                "network_economics_points": 0,
                "business_scale_need_points": 0,
                "network_analysis": {
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
                    "competitors_at_site": None,
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
