"""Configuration and prompt templates for battle card generation."""

def get_research_prompt(business_name: str, address: str, city: str, state: str, 
                       ey_employees: str, cb_employees: str, cb_linkedin: str) -> str:
    """Generate the research prompt for initial data gathering."""
    return f"""Search for and research the following business:
Name: {business_name}
Address: {address}, {city}, {state}
EY Employee Count: {ey_employees}
ConnectBase Employee Count: {cb_employees}
ConnectBase LinkedIn: {cb_linkedin}

CRITICAL: Focus on RECENT data from 2024-2026 only. Prioritize:
1. LinkedIn company page and employee profiles (check for recent activity in 2024-2026)
2. Company website (verify this location is still listed)
3. Recent news articles or press releases (2024-2026)
4. Google Maps/Business listings with recent reviews (2024-2026)
5. Recent social media activity

IGNORE data older than 2024 unless no recent data exists. Data from 2022-2023 should be considered potentially outdated.

Provide a detailed report on:
1. Current operating status at this location (is business still active HERE in 2025-2026?)
   - Check LinkedIn for recent employee posts from this location
   - Check Google Maps for recent reviews/photos
   - Verify company website lists this address currently
   
2. Best estimate of employee count for THIS SPECIFIC LOCATION at {address}, {city}, {state}
   - Look for LinkedIn employee counts at THIS specific office (recent data)
   - Check recent company announcements or news
   - If only company-wide data available, estimate this location's share
   
3. Business type, vertical, and infrastructure/connectivity needs

4. This location's potential company footprint (e.g. headquarters, regional office, branch, etc.)

Focus on validating the employee count for THIS SPECIFIC OFFICE and confirming business is CURRENTLY ACTIVE (2025-2026) at this address.

BE SKEPTICAL of data from 2022-2023. Businesses move, close, or restructure frequently.
"""


def get_analysis_prompt(research_text: str, business_name: str, address: str, city: str, 
                       state: str, ey_data: dict, connectbase_data: dict) -> str:
    """Generate the analysis and scoring prompt."""
    
    ey_employees = ey_data.get('No Of Employees', 'N/A')
    dqe_distance = connectbase_data.get('DQE_Site_Distance', 'N/A')
    dqe_connection = connectbase_data.get('DQE_Connection_Status', 'N/A')
    dqe_network_status = connectbase_data.get('DQE_Network_Status', 'N/A')
    
    # Check if we have ConnectBase data
    has_connectbase = connectbase_data.get('API_EntityName', 'N/A') != 'N/A'
    
    connectbase_section = ""
    if has_connectbase:
        connectbase_section = f"""
CONNECTBASE DATA:
- CB Entity Name: {connectbase_data.get('API_EntityName', 'N/A')}
- CB Employee Count: {connectbase_data.get('API_NoOfEmployees', 'N/A')}
- CB Industry: {connectbase_data.get('API_Industry', 'N/A')}
- CB Location Type: {connectbase_data.get('API_LocationType', 'N/A')}
- CB LinkedIn: {connectbase_data.get('API_LinkedIn', 'N/A')}
- CB Revenue: {connectbase_data.get('API_Revenue', 'N/A')}
- CB Monthly Network Spend: {connectbase_data.get('API_MonthlyNetworkSpend', 'N/A')}

DQE NETWORK INTELLIGENCE:
- DQE Site Distance: {dqe_distance} feet (distance to nearest DQE fiber)
- DQE Connection Status: {dqe_connection}
- DQE Network Status: {dqe_network_status}
- Competitors at Site: {connectbase_data.get('SITE_All_Competitors', 'N/A')}
"""
    else:
        connectbase_section = """
CONNECTBASE DATA:
⚠️  No ConnectBase data available for this location.

DQE NETWORK INTELLIGENCE:
- Not available without ConnectBase data

NOTE: You must rely entirely on EY data and your web research for this analysis.
Focus extra effort on validating the business and finding employee count data.
"""
    
    return f"""
RESEARCH DATA FOUND:
{research_text}

---
You are a sales intelligence analyst for DQE Communications, a fiber-optic telecommunications provider.

IMPORTANT: Today is February 2026. Only trust data from 2024-2026 as "recent" or "current".
Data from 2022-2023 should be considered potentially outdated for business operating status.

The best companies for you are companies that would have a mission critical need for fast, reliable internet services.

TASK: Score this business on two dimensions: data confidence and ICP fit.

BUSINESS DETAILS FROM EY:
- Business Name: {business_name}
- Address: {address}, {city}, {state}
- EY Employee Count: {ey_employees}

{connectbase_section}

SCORING METHODOLOGY:

COMPONENT 1: DATA CONFIDENCE SCORE (0.0 - 1.0 multiplier)
How confident are we that our data is accurate for THIS specific location?

A. Business Operating Status (0.0 - 0.40):
   - 0.40: Confirmed active at this address with RECENT evidence (2024-2026: LinkedIn activity, website, news, Google reviews)
   - 0.30: Strong indicators of recent activity (LinkedIn employees at location, recent reviews/posts from 2024-2026)
   - 0.20: Appears active but only older evidence (2022-2023 data, unclear if still current)
   - 0.10: Uncertain - only very old data (pre-2022) or conflicting signals
   - 0.00: Clear evidence of closure, move, or address mismatch

B. Employee Count Validation for THIS Location (0.0 - 0.40):
   {"Compare EY vs ConnectBase vs your research for THIS SPECIFIC OFFICE:" if has_connectbase else "Use EY data and your research to validate employee count for THIS SPECIFIC OFFICE:"}
   - 0.40: Multiple sources align within ±10 employees, confident this is location-specific
   - 0.30: Sources generally agree (±25 employees), likely accurate for this location
   - 0.20: Moderate agreement OR only company-wide data (must estimate location split)
   - 0.10: Significant discrepancies OR company-wide only with unclear allocation
   - 0.00: Cannot validate employee presence or major data conflicts

C. Source Quality & Data Recency (0.0 - 0.20):
   - 0.20: Multiple authoritative sources from 2024-2026 (LinkedIn, filings, recent announcements, company website)
   - 0.15: Reliable sources but single-source dependent or mix of recent/older data
   - 0.10: Limited sources or most data from 2022-2023
   - 0.05: Data appears stale (pre-2022) or contradictory
   - 0.00: No reliable data sources

CONFIDENCE SCORE = A + B + C (max 1.0)

COMPONENT 2: ICP FIT SCORE (0-100 points)
If the data IS accurate, how valuable is this customer?

A. Network Economics (0-20 points):
   {"Based on DQE Site Distance and connection status:" if has_connectbase else "Without network data, use conservative estimates:"}
   
   {"- 20 pts: On-net (distance = 0 or Connection Status indicates 'on-net' or 'connected')" if has_connectbase else "- 10 pts: Network proximity unknown - assume moderate build cost"}
   {"- 10 pts: Near-net (distance > 0, any distance showing near-net status)" if has_connectbase else ""}
   {"- 0 pts: Not near DQE network or NOT_FOUND" if has_connectbase else ""}
   
   {f"NOTE: On-net prospects have zero build cost advantage, but business characteristics drive overall fit." if has_connectbase else "NOTE: Without network data, focus scoring on business characteristics."}

B. Business Scale & Infrastructure Need (0-80 points):
   Combine validated employee count with business criticality:
   
   HIGH-CRITICALITY businesses (need dedicated fiber/DIA):
   - Technology, data centers, financial services, healthcare facilities
   - Legal/accounting firms, engineering, media/production companies
   - Businesses with real-time data needs, cloud infrastructure, distributed teams
   - Government, research facilities, higher education
   
   MODERATE-CRITICALITY businesses:
   - General corporate offices, professional services
   - Standard business operations needing reliable connectivity
   
   LOW-CRITICALITY businesses (unlikely to need DIA):
   - Retail, food service, personal services, residential
   - Small consumer-facing businesses with minimal data needs
   
   SCORING:
   - 80 pts: 100+ employees AND high-criticality business
   - 60 pts: 50-99 employees AND high-criticality OR 100+ moderate-criticality
   - 40 pts: 25-49 high-criticality OR 50-99 moderate-criticality
   - 25 pts: 25-49 moderate-criticality OR 10-24 high-criticality
   - 15 pts: 10-24 moderate-criticality OR small high-criticality
   - 5 pts: Small office (1-9 employees) but high-criticality
   - 0 pts: Low-criticality business type unlikely to need dedicated fiber

ICP FIT SCORE = Network Economics + Business Scale & Need (max 100)

FINAL SCORE = Data Confidence × ICP Fit Score

Example Calculations:
- Confidence: 0.85, ICP: 80 → Final Score: 68
- Confidence: 0.50, ICP: 90 → Final Score: 45 (good opportunity but uncertain data)
- Confidence: 0.95, ICP: 25 → Final Score: 24 (confident it's not a good fit)
- Confidence: 0.30, ICP: 60 → Final Score: 18 (too uncertain to pursue)

OUTPUT FORMAT (strict JSON):
{{
  "overall_score": <0-100, calculated as confidence_score × icp_fit_score>,
  
  "data_confidence": {{
    "confidence_score": <0.0-1.0>,
    "business_status_points": <0.0-0.40>,
    "employee_validation_points": <0.0-0.40>,
    "source_quality_points": <0.0-0.20>,
    
    "business_status": "<operating|closed|moved|uncertain>",
    "business_status_evidence": "<key evidence for status determination>",
    
    "validated_employee_count": <number or null>,
    "employee_count_confidence": "<high|medium|low>",
    "employee_count_basis": "<location-specific or company-wide estimate>",
    "employee_count_sources": ["source1", "source2"],
    "employee_comparison": "<EY: X, CB: Y, Validated: Z, differences explained>",
    
    "location_type": "<headquarters|regional_office|branch_office|unclear>",
    "data_quality_notes": "<key concerns or validation details>"
  }},
  
  "icp_fit": {{
    "icp_fit_score": <0-100>,
    "network_economics_points": <0-20>,
    "business_scale_need_points": <0-80>,
    
    "network_analysis": {{
      "dqe_distance_feet": <number or "NOT_FOUND" or "NO_DATA">,
      "network_category": "<on_net|near_net|not_near_net|not_found|no_data>",
      "build_cost_assessment": "<zero|low|moderate|high|not_viable|unknown>",
      "network_advantage": "<why DQE is well-positioned or challenges>"
    }},
    
    "business_assessment": {{
      "business_criticality": "<high|moderate|low>",
      "criticality_reasoning": "<why this business type needs/doesn't need dedicated fiber>",
      "infrastructure_needs": ["need1", "need2", "need3"],
      "bandwidth_requirements": "<high|moderate|low>",
      "estimated_monthly_spend": <number or null>
    }},
    
    "competitive_context": {{
      "competitors_at_site": "<list from SITE_All_Competitors or 'Unknown - no network data'>",
      "competitive_position": "<DQE advantage or disadvantages or 'Unknown without network data'>"
    }},
    
    "icp_fit_summary": "<2-3 sentences on overall fit>"
  }},
  
  "sales_intelligence": {{
    "priority_level": "<immediate|high|medium|low|disqualify>",
    "priority_reasoning": "<explain priority based on final score: confidence × ICP{' and note lack of network data' if not has_connectbase else ''}>",
    
    "key_selling_points": ["point1", "point2", "point3"],
    "likely_pain_points": ["pain1", "pain2"],
    "competitive_angles": ["angle1", "angle2"],
    
    "data_gaps_to_resolve": ["what sales should validate before outreach"],
    "recommended_approach": "<specific approach based on confidence and opportunity>",
    "recommended_services": ["DIA", "SD-WAN", "Managed Security", "etc"],
    "next_best_actions": ["action1", "action2", "action3"]
  }}
}}

CRITICAL PRINCIPLES:
- Final score naturally reflects reality: high confidence + high ICP = high score
- Low confidence suppresses scores even for great opportunities (need validation first)
- High confidence about non-ICP businesses = low scores (confident they're not a fit)
- {"On-net with validated data and strong ICP fit should score 70-95 range" if has_connectbase else "Without network data, scores will be lower (max ~60-70) due to unknown build costs"}
- {"Use DQE Site Distance to determine on-net vs near-net status" if has_connectbase else "Without DQE distance data, default to 'NO_DATA' and 'unknown' for network fields"}
- Be realistic about employee counts - many won't have location-specific data
- Focus on business types and scale that need dedicated fiber connectivity
- Prioritize recent data (2024-2026) when assessing business status and confidence
- {"NOT_FOUND or not near DQE network should score low on network economics" if has_connectbase else "Without network data, adjust expectations - even good prospects will have moderate scores"}

Return ONLY valid JSON, no additional text.
"""