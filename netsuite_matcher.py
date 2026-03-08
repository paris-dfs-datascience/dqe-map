"""
NetSuite structure matcher — loads structures from GCS and matches
a prospect address (street + zip) using rapidfuzz + Gemini fallback.
Returns structure_type, structure_status, and distance_band.
Designed to be called once per battle card during battlecard_processor.py processing.
"""

import csv
import io
import re
from typing import Optional
from google.cloud import storage
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz


NETSUITE_GCS_PATH = "netsuite/netsuite_data_mar3.csv"
FUZZY_CUTOFF      = 40
TOP_N             = 6


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
    }
    for pattern, replacement in abbrevs.items():
        s = re.sub(pattern, replacement, s)
    return re.sub(r'\s+', ' ', s).strip()


def _make_addr_key(street: str, zipcode: str) -> str:
    """Normalized composite key: '<street>|<zip5>'"""
    zip5 = str(zipcode).strip()[:5] if zipcode else ""
    return f"{_normalize_street(street)}|{zip5}"


class NetSuiteMatcher:

    def __init__(self, gcs_bucket: str, project_id: str = "lma-website-461920"):
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self._structures: list[dict] = []
        self._addr_keys: list[str] = []
        self._load_structures()

        self.client = genai.Client(
            vertexai=True,
            project=project_id,
            location="us-central1"
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

    def _fuzzy_candidates(self, addr_key: str) -> list[dict]:
        if not self._structures:
            return []
        results = process.extract(
            addr_key,
            self._addr_keys,
            scorer=fuzz.WRatio,
            limit=TOP_N,
            score_cutoff=FUZZY_CUTOFF
        )
        candidates = []
        for _key, score, idx in results:
            c = dict(self._structures[idx])
            c["_fuzzy_score"] = score
            c["_addr_key"]    = self._addr_keys[idx]
            candidates.append(c)
        candidates.sort(key=lambda x: x["_fuzzy_score"], reverse=True)
        return candidates

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
            resp = self.client.models.generate_content(
                model="gemini-2.0-flash-001",
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    response_mime_type="application/json"
                )
            )
            result = json.loads(resp.text)

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
        candidates = self._fuzzy_candidates(addr_key)

        if not candidates:
            return {"matched": False, "match_reason": "no fuzzy candidates above threshold"}

        # Short-circuit: very high fuzzy score — skip Gemini
        top = candidates[0]
        if top["_fuzzy_score"] >= 95:
            fields = self._extract_fields(top)
            return {
                "matched":          True,
                **fields,
                "match_confidence": "high",
                "match_reason":     "exact/near-exact address match",
                "fuzzy_score":      top["_fuzzy_score"],
            }

        # Gemini for ambiguous cases
        result = self._gemini_confirm(street, zipcode, candidates)
        if result:
            return {"matched": True, **result}

        return {"matched": False, "match_reason": "no confident match found"}