"""
HubSpot company matcher — loads companies from GCS and matches
a prospect name using rapidfuzz + Gemini. Designed to be called
once per battle card during battlecard_processor.py processing.
"""

import json
from typing import Optional
from google.cloud import storage
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz


HUBSPOT_GCS_PATH = "hubspot-data/hubspot_companies.json"
FUZZY_CUTOFF     = 35
TOP_N            = 8


class HubSpotMatcher:

    def __init__(self, gcs_bucket: str, project_id: str = "lma-website-461920"):
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self._companies: list[dict] = []
        self._names: list[str] = []
        self._load_companies()

        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location="us-central1"
        )

    def _load_companies(self):
        try:
            sc     = storage.Client()
            bucket = sc.bucket(self.gcs_bucket)
            blob   = bucket.blob(HUBSPOT_GCS_PATH)
            self._companies = json.loads(blob.download_as_text())
            self._names     = [c.get("name") or "" for c in self._companies]
            print(f"✓ HubSpot: loaded {len(self._companies)} companies")
        except Exception as e:
            print(f"⚠ HubSpot: could not load companies — {e}")
            self._companies = []
            self._names     = []

    def _fuzzy_candidates(self, query: str) -> list[dict]:
        if not self._companies:
            return []
        results = process.extract(
            query,
            self._names,
            scorer=fuzz.WRatio,
            limit=TOP_N,
            score_cutoff=FUZZY_CUTOFF
        )
        candidates = []
        for _name, score, idx in results:
            c = dict(self._companies[idx])
            c["_fuzzy_score"] = score
            candidates.append(c)
        candidates.sort(key=lambda x: x["_fuzzy_score"], reverse=True)
        return candidates

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
            resp = self.client.models.generate_content(
                model="gemini-2.0-flash-001",
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    response_mime_type="application/json"
                )
            )

            # Gemini sometimes returns a list instead of a dict — normalize it
            raw = json.loads(resp.text)
            result = raw[0] if isinstance(raw, list) else raw

            if result.get("match"):
                matched = next((c for c in candidates if c["id"] == result["id"]), None)
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
            print(f"    ⚠ HubSpot Gemini match error: {e}")

        return None

    def match(self, company_name: str) -> dict:
        """
        Match a company name against HubSpot CRM companies.
        Always returns a dict — check 'matched' key.
        """
        if not company_name or not self._companies:
            return {"matched": False, "match_reason": "no data available"}

        candidates = self._fuzzy_candidates(company_name)

        if not candidates:
            return {"matched": False, "match_reason": "no fuzzy candidates above threshold"}

        # Short-circuit: very high fuzzy score, skip Gemini
        top = candidates[0]
        if top["_fuzzy_score"] >= 95:
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
                "match_reason":         "exact/near-exact fuzzy match",
                "fuzzy_score":          top["_fuzzy_score"]
            }

        # Gemini for ambiguous cases
        result = self._gemini_confirm(company_name, candidates)
        if result:
            return {"matched": True, **result}

        return {"matched": False, "match_reason": "no confident match found"}