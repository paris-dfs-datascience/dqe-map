"""
NetSuite structure matcher — loads structures from GCS and matches
a prospect address (street + zip) using rapidfuzz + Gemini fallback.
Returns structure_type, structure_status, and distance_band.
Designed to be called once per battle card during battlecard_processor.py processing.
"""

import csv
import io
import json
import re
import time
import random
from typing import Optional
from google.cloud import storage
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz


from pipeline_config import (
    PROJECT_ID, GCP_LOCATION, NETSUITE_BLOB,
    NETSUITE_FUZZY_CUTOFF, NETSUITE_TOP_N,
    MATCHER_MAX_RETRIES, MATCHER_BASE_DELAY,
)

NETSUITE_GCS_PATH = NETSUITE_BLOB
FUZZY_CUTOFF      = NETSUITE_FUZZY_CUTOFF
TOP_N             = NETSUITE_TOP_N
MAX_RETRIES       = MATCHER_MAX_RETRIES
BASE_DELAY        = MATCHER_BASE_DELAY


def _normalize_street(street: str) -> str:
    """Lowercase, strip punctuation, normalize common abbreviations."""
    if not street:
        return ""
    s = street.lower().strip()
    s = re.sub(r'[^\w\s]', '', s)
    abbrevs = {
        r'\bstreet\b': 'st', r'\bavenue\b': 'ave', r'\bboulevard\b': 'blvd',
        r'\bdrive\b': 'dr', r'\broad\b': 'rd', r'\bcourt\b': 'ct',
        r'\blane\b': 'ln', r'\bplace\b': 'pl', r'\bcircle\b': 'cir',
        r'\bsuite\b': 'ste', r'\bnorth\b': 'n', r'\bsouth\b': 's',
        r'\beast\b': 'e', r'\bwest\b': 'w',
        # Additional common abbreviations
        r'\bterrace\b': 'ter', r'\bhighway\b': 'hwy', r'\bparkway\b': 'pkwy',
        r'\bexpressway\b': 'expy', r'\bfreeway\b': 'fwy',
        r'\bsquare\b': 'sq', r'\btrail\b': 'trl', r'\bway\b': 'way',
        r'\bapartment\b': 'apt', r'\bbuilding\b': 'bldg', r'\bfloor\b': 'fl',
        r'\broom\b': 'rm', r'\bdepartment\b': 'dept',
        r'\bnortheast\b': 'ne', r'\bnorthwest\b': 'nw',
        r'\bsoutheast\b': 'se', r'\bsouthwest\b': 'sw',
        r'\bmount\b': 'mt', r'\bmountain\b': 'mtn', r'\bfort\b': 'ft',
        r'\bsaint\b': 'st',
    }
    for pattern, replacement in abbrevs.items():
        s = re.sub(pattern, replacement, s)
    # Strip unit/suite/apt numbers for comparison (e.g. "ste 200" -> "")
    # Keep the street number but remove secondary unit identifiers
    s = re.sub(r'\b(ste|apt|unit|rm|fl|bldg)\s*\w*', '', s)
    return re.sub(r'\s+', ' ', s).strip()


def _make_addr_key(street: str, zipcode: str) -> str:
    """Normalized composite key: '<street>|<zip5>'"""
    zip5 = str(zipcode).strip()[:5] if zipcode else ""
    return f"{_normalize_street(street)}|{zip5}"


class NetSuiteMatcher:

    def __init__(self, gcs_bucket: str, project_id: str = PROJECT_ID):
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self._structures: list[dict] = []
        self._addr_keys: list[str] = []
        self._load_structures()

        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location=GCP_LOCATION
        )

    def _load_structures(self):
        try:
            sc      = storage.Client()
            bucket  = sc.bucket(self.gcs_bucket)
            blob    = bucket.blob(NETSUITE_GCS_PATH)
            content = blob.download_as_text(encoding="utf-8-sig")  # utf-8-sig strips BOM if present
            reader  = csv.DictReader(io.StringIO(content))
            self._structures = [row for row in reader]
            # Pre-build normalized address keys for fuzzy matching
            self._addr_keys = [
                _make_addr_key(
                    s.get("Address 1", ""),
                    s.get("Structure Zip Code", "")
                )
                for s in self._structures
            ]
            print(f"✓ NetSuite: loaded {len(self._structures)} structures")
        except Exception as e:
            print(f"⚠ NetSuite: could not load structures — {e}")
            self._structures = []
            self._addr_keys  = []

    def _extract_fields(self, s: dict) -> dict:
        """Pull only the three target fields (plus identifiers) from a structure record."""
        return {
            "netsuite_internal_id":   s.get("Internal ID"),
            "netsuite_name":          s.get("Structure Name (Address 1)"),
            "netsuite_address":       s.get("Address 1"),
            "netsuite_zip":           s.get("Structure Zip Code"),
            "structure_type":         s.get("Structure Type"),
            "structure_status":       s.get("Structure Status"),
            "ns_status":              s.get("NS Status"),
            "distance_band":          s.get("Distance Band"),
            "primary_cost_total":     s.get("Primary Cost Total"),
        }

    def _fuzzy_candidates(self, addr_key: str, zipcode: str) -> list[dict]:
        """Multi-scorer fuzzy matching on address keys, with zip-code boosting."""
        if not self._structures:
            return []

        candidate_scores: dict[int, float] = {}
        query_zip = str(zipcode).strip()[:5] if zipcode else ""

        # WRatio on full composite key (street|zip)
        for _key, score, idx in process.extract(
            addr_key, self._addr_keys, scorer=fuzz.WRatio,
            limit=TOP_N * 2, score_cutoff=FUZZY_CUTOFF
        ):
            candidate_scores[idx] = max(candidate_scores.get(idx, 0), score)

        # token_sort_ratio handles word reordering (e.g. "123 Main St" vs "Main St 123")
        for _key, score, idx in process.extract(
            addr_key, self._addr_keys, scorer=fuzz.token_sort_ratio,
            limit=TOP_N * 2, score_cutoff=FUZZY_CUTOFF
        ):
            candidate_scores[idx] = max(candidate_scores.get(idx, 0), score)

        # Also match on just the street portion (before the |) for partial matches
        query_street = addr_key.split("|")[0] if "|" in addr_key else addr_key
        street_keys = [k.split("|")[0] if "|" in k else k for k in self._addr_keys]
        for _key, score, idx in process.extract(
            query_street, street_keys, scorer=fuzz.WRatio,
            limit=TOP_N * 2, score_cutoff=FUZZY_CUTOFF
        ):
            # Boost street-only matches that also have matching zip
            cand_zip = str(self._structures[idx].get("Structure Zip Code", "")).strip()[:5]
            if query_zip and cand_zip == query_zip:
                score = min(score + 10, 100)  # Zip match bonus
            candidate_scores[idx] = max(candidate_scores.get(idx, 0), score)

        # Build candidate list sorted by best score
        candidates = []
        for idx, score in sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)[:TOP_N]:
            c = dict(self._structures[idx])
            c["_fuzzy_score"] = score
            c["_addr_key"]    = self._addr_keys[idx]
            candidates.append(c)

        return candidates

    def _generate_with_retry(self, prompt: str) -> object:
        """Call Gemini with exponential backoff on 429 errors."""
        for attempt in range(MAX_RETRIES):
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
                if '429' in str(e) and attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt) + random.uniform(0, 2)
                    print(f"    ⏳ NetSuite rate limited, retrying in {delay:.1f}s "
                          f"(attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(delay)
                else:
                    raise

    def _gemini_confirm(self, street: str, zipcode: str, candidates: list[dict]) -> Optional[dict]:
        candidate_list = "\n".join(
            f"{i+1}. ID={c.get('Internal ID')} | "
            f"Name={c.get('Structure Name (Address 1)')} | "
            f"Address={c.get('Address 1')} | "
            f"Zip={c.get('Structure Zip Code')} | "
            f"Structure Type={c.get('Structure Type')} | "
            f"Structure Status={c.get('Structure Status')} | "
            f"Distance Band={c.get('Distance Band')}"
            for i, c in enumerate(candidates)
        )

        prompt = f"""You are an address matching assistant for DQE Communications, a telecom company.

A prospect building has this address:
  Street: "{street}"
  Zip:    "{zipcode}"

Candidates from the NetSuite structure database:
{candidate_list}

Determine if any candidate refers to the same physical building/address. Account for:
- Street number and name variations (abbreviations, spelling differences)
- Suite/floor numbers that may or may not be present
- Minor formatting differences (Ave vs Avenue, St vs Street)
- Zip code must match exactly

If confident in a match: {{"match": true, "id": "<internal_id>", "confidence": "<high|medium>", "reason": "<brief>"}}
If no clear match: {{"match": false, "id": null, "confidence": null, "reason": "<brief>"}}

Respond ONLY with valid JSON."""

        try:
            resp = self._generate_with_retry(prompt)

            # Robust JSON parsing: handle markdown code blocks, lists, etc.
            text = resp.text.strip()
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\s*', '', text)
                text = re.sub(r'\s*```$', '', text)

            raw = json.loads(text)
            if isinstance(raw, list):
                result = raw[0] if raw else {}
            elif isinstance(raw, dict):
                result = raw
            else:
                print(f"    NetSuite Gemini returned unexpected type: {type(raw)}")
                result = {}

            if result.get("match"):
                matched = next(
                    (c for c in candidates if str(c.get("Internal ID")) == str(result["id"])),
                    None
                )
                if matched:
                    fields = self._extract_fields(matched)
                    return {
                        **fields,
                        "match_confidence": result.get("confidence", "medium"),
                        "match_reason":     result.get("reason", ""),
                        "fuzzy_score":      matched.get("_fuzzy_score"),
                    }
        except Exception as e:
            print(f"    ⚠ NetSuite Gemini match error: {e}")

        return None

    def match(self, street: str, zipcode: str) -> dict:
        """
        Match a prospect address against NetSuite structures.
        Always returns a dict — check 'matched' key.
        Returns structure_type, structure_status, distance_band on success.
        """
        if not street or not zipcode or not self._structures:
            return {"matched": False, "match_reason": "no data available"}

        addr_key   = _make_addr_key(street, zipcode)
        candidates = self._fuzzy_candidates(addr_key, zipcode)

        if not candidates:
            return {"matched": False, "match_reason": "no fuzzy candidates above threshold"}

        # Short-circuit: very high fuzzy score — but ONLY if zip codes match exactly
        top = candidates[0]
        query_zip = str(zipcode).strip()[:5] if zipcode else ""
        cand_zip  = str(top.get("Structure Zip Code", "")).strip()[:5]
        zip_match = query_zip and cand_zip and query_zip == cand_zip

        if top["_fuzzy_score"] >= 95 and zip_match:
            fields = self._extract_fields(top)
            return {
                "matched":          True,
                **fields,
                "match_confidence": "high",
                "match_reason":     f"exact/near-exact address match (score={top['_fuzzy_score']:.0f}, zip verified)",
                "fuzzy_score":      top["_fuzzy_score"],
            }

        # Gemini for ambiguous cases
        result = self._gemini_confirm(street, zipcode, candidates)
        if result:
            return {"matched": True, **result}

        return {"matched": False, "match_reason": "no confident match found"}