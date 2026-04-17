"""
HubSpot company matcher -- loads companies from GCS and matches
a prospect name using rapidfuzz + Gemini. Designed to be called
once per battle card during battlecard_processor.py processing.
"""

import json
import time
import random
from typing import Optional
from google.cloud import storage
from google import genai
from google.genai import types
import re
from rapidfuzz import process, fuzz, utils

from pipeline_config import (
    PROJECT_ID, GCP_LOCATION, HUBSPOT_BLOB,
    HUBSPOT_FUZZY_CUTOFF, HUBSPOT_TOP_N,
    MATCHER_MAX_RETRIES, MATCHER_BASE_DELAY,
)

# Legal suffixes to strip for normalized comparison
_LEGAL_SUFFIXES = re.compile(
    r'\b(inc|incorporated|llc|llp|ltd|limited|corp|corporation|co|company|'
    r'lp|plc|pllc|group|holdings|enterprises|services|solutions)\b\.?',
    re.IGNORECASE
)

def _normalize_company(name: str) -> str:
    """Normalize company name: lowercase, strip legal suffixes and punctuation."""
    if not name:
        return ""
    s = name.lower().strip()
    s = _LEGAL_SUFFIXES.sub('', s)
    s = re.sub(r'[^\w\s]', '', s)
    return re.sub(r'\s+', ' ', s).strip()


class HubSpotMatcher:

    def __init__(self, gcs_bucket: str, project_id: str = PROJECT_ID):
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self._companies: list[dict] = []
        self._names: list[str] = []
        self._load_companies()

        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=GCP_LOCATION
        )

    def _load_companies(self):
        try:
            sc     = storage.Client()
            bucket = sc.bucket(self.gcs_bucket)
            blob   = bucket.blob(HUBSPOT_BLOB)
            self._companies = json.loads(blob.download_as_text())
            self._names     = [c.get("name") or "" for c in self._companies]
            # Pre-compute normalized names for better matching
            self._normalized_names = [_normalize_company(n) for n in self._names]
            print(f"HubSpot: loaded {len(self._companies)} companies")
        except Exception as e:
            print(f"HubSpot: could not load companies - {e}")
            self._companies = []
            self._names     = []
            self._normalized_names = []

    def _fuzzy_candidates(self, query: str) -> list[dict]:
        """Multi-scorer fuzzy matching: runs WRatio on both raw and normalized names,
        plus token_sort_ratio for reordered words. Takes the best score per candidate."""
        if not self._companies:
            return []

        norm_query = _normalize_company(query)
        candidate_scores: dict[int, float] = {}

        # Score against raw names with WRatio
        for _name, score, idx in process.extract(
            query, self._names, scorer=fuzz.WRatio,
            limit=HUBSPOT_TOP_N * 2, score_cutoff=HUBSPOT_FUZZY_CUTOFF
        ):
            candidate_scores[idx] = max(candidate_scores.get(idx, 0), score)

        # Score against normalized names with WRatio
        for _name, score, idx in process.extract(
            norm_query, self._normalized_names, scorer=fuzz.WRatio,
            limit=HUBSPOT_TOP_N * 2, score_cutoff=HUBSPOT_FUZZY_CUTOFF
        ):
            candidate_scores[idx] = max(candidate_scores.get(idx, 0), score)

        # Score with token_sort_ratio (handles word reordering)
        for _name, score, idx in process.extract(
            norm_query, self._normalized_names, scorer=fuzz.token_sort_ratio,
            limit=HUBSPOT_TOP_N * 2, score_cutoff=HUBSPOT_FUZZY_CUTOFF
        ):
            candidate_scores[idx] = max(candidate_scores.get(idx, 0), score)

        # Score with token_set_ratio (handles subsets — e.g. "AHN" in "Allegheny Health Network")
        for _name, score, idx in process.extract(
            norm_query, self._normalized_names, scorer=fuzz.token_set_ratio,
            limit=HUBSPOT_TOP_N * 2, score_cutoff=HUBSPOT_FUZZY_CUTOFF
        ):
            candidate_scores[idx] = max(candidate_scores.get(idx, 0), score)

        # Build candidate list, sorted by best score
        candidates = []
        for idx, score in sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)[:HUBSPOT_TOP_N]:
            c = dict(self._companies[idx])
            c["_fuzzy_score"] = score
            candidates.append(c)

        return candidates

    def _generate_with_retry(self, prompt: str) -> object:
        """Call Gemini with exponential backoff on 429 errors."""
        for attempt in range(MATCHER_MAX_RETRIES):
            try:
                return self.client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        response_mime_type="application/json"
                    )
                )
            except Exception as e:
                if '429' in str(e) and attempt < MATCHER_MAX_RETRIES - 1:
                    delay = MATCHER_BASE_DELAY * (2 ** attempt) + random.uniform(0, 2)
                    print(f"    HubSpot rate limited, retrying in {delay:.1f}s "
                          f"(attempt {attempt + 1}/{MATCHER_MAX_RETRIES})")
                    time.sleep(delay)
                else:
                    raise

    def _gemini_confirm(self, query: str, candidates: list[dict]) -> Optional[dict]:
        candidate_list = "\n".join(
            f"{i+1}. ID={c['id']} | Name={c.get('name')} | "
            f"Owner={c.get('hubspot_owner_id')} | "
            f"NetSuite Status={c.get('netsuite_status')} | "
            f"Last Contacted={c.get('notes_last_contacted')} | "
            f"Lead Source={c.get('lead_source__netsuite_')} | "
            f"Lead Source Type={c.get('lead_source_type')}"
            for i, c in enumerate(candidates)
        )

        prompt = f"""You are a data matching assistant for DQE Communications, a telecom company.

A prospect from the sales pipeline is named: "{query}"

Candidates from HubSpot CRM:
{candidate_list}

Determine if any candidate is the same company as "{query}". Account for:
- Abbreviations (e.g. "AHN" = "Allegheny Health Network")
- Legal suffixes (LLC, Inc, Corp, Ltd, Co)
- Common name vs. formal name
- DBA / subsidiary relationships
- Partial matches where one name contains the other

If confident in a match: {{"match": true, "id": "<hubspot_id>", "name": "<matched_name>", "confidence": "<high|medium>", "reason": "<brief>"}}
If no clear match: {{"match": false, "id": null, "name": null, "confidence": null, "reason": "<brief>"}}

Respond ONLY with valid JSON."""

        try:
            resp = self._generate_with_retry(prompt)

            # Robust JSON parsing: handle markdown code blocks, lists, etc.
            text = resp.text.strip()
            # Strip markdown code fences if present
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\s*', '', text)
                text = re.sub(r'\s*```$', '', text)

            raw = json.loads(text)
            if isinstance(raw, list):
                result = raw[0] if raw else {}
            elif isinstance(raw, dict):
                result = raw
            else:
                print(f"    HubSpot Gemini returned unexpected type: {type(raw)}")
                result = {}

            if result.get("match"):
                # Match by ID, handling string/int type mismatches
                matched = next(
                    (c for c in candidates if str(c["id"]) == str(result.get("id", ""))),
                    None
                )
                if matched:
                    return {
                        "hubspot_id":           matched["id"],
                        "hubspot_name":         matched.get("name"),
                        "hubspot_owner_id":     matched.get("hubspot_owner_id"),
                        "netsuite_status":      matched.get("netsuite_status"),
                        "notes_last_contacted": matched.get("notes_last_contacted"),
                        "lead_source":          matched.get("lead_source__netsuite_"),
                        "lead_source_type":     matched.get("lead_source_type"),
                        "match_confidence":     result.get("confidence", "medium"),
                        "match_reason":         result.get("reason", ""),
                        "fuzzy_score":          matched.get("_fuzzy_score")
                    }
        except Exception as e:
            print(f"    HubSpot Gemini match error: {e}")

        return None

    def match(self, company_name: str) -> dict:
        """
        Match a company name against HubSpot CRM companies.
        Always returns a dict -- check 'matched' key.
        """
        if not company_name or not self._companies:
            return {"matched": False, "match_reason": "no data available"}

        candidates = self._fuzzy_candidates(company_name)

        if not candidates:
            return {"matched": False, "match_reason": "no fuzzy candidates above threshold"}

        # Short-circuit: very high fuzzy score AND normalized names match closely
        top = candidates[0]
        norm_query = _normalize_company(company_name)
        norm_top   = _normalize_company(top.get("name", ""))
        # Require both high fuzzy score AND normalized names to be very similar
        norm_ratio = fuzz.ratio(norm_query, norm_top)
        if top["_fuzzy_score"] >= 95 and norm_ratio >= 90:
            return {
                "matched":              True,
                "hubspot_id":           top["id"],
                "hubspot_name":         top.get("name"),
                "hubspot_owner_id":     top.get("hubspot_owner_id"),
                "netsuite_status":      top.get("netsuite_status"),
                "notes_last_contacted": top.get("notes_last_contacted"),
                "lead_source":          top.get("lead_source__netsuite_"),
                "lead_source_type":     top.get("lead_source_type"),
                "match_confidence":     "high",
                "match_reason":         f"exact/near-exact fuzzy match (score={top['_fuzzy_score']:.0f}, normalized={norm_ratio:.0f})",
                "fuzzy_score":          top["_fuzzy_score"]
            }

        # Gemini for ambiguous cases
        result = self._gemini_confirm(company_name, candidates)
        if result:
            return {"matched": True, **result}

        return {"matched": False, "match_reason": "no confident match found"}
