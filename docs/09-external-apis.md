# External APIs

## Frontend APIs

| Service | Used For | Auth Method |
|---------|----------|-------------|
| **Google Maps JavaScript API** | Map rendering, markers, InfoWindows | API key via script tag (browser-restricted) |
| **Google Places Autocomplete** | Address search autocomplete | Included with Maps JS API |
| **Google Maps Geocoding API** | Manual address search (Enter key fallback) | API key in query param |
| **Firebase Auth** | User authentication | Firebase SDK config |
| **Firestore** | Session tracking + event logging | Firebase SDK config |
| **Google Cloud Storage** | Fetch battle card JSON + fiber GeoJSON | Public HTTPS URLs (no auth) |

### Google Maps API Key

- Key: set in `.env` as `REACT_APP_GOOGLE_MAPS_API_KEY`
- Must have these APIs enabled in GCP console:
  - Maps JavaScript API
  - Places API
  - Geocoding API
- Should have HTTP referrer restrictions set to your domain(s)

### GCS URLs (public, no auth)

```
https://storage.googleapis.com/csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json
https://storage.googleapis.com/csv-battle-cards-dqe/sales-map.geojson
```

---

## Backend APIs

### Google Maps Geocoding API

| | |
|---|---|
| **Endpoint** | `https://maps.googleapis.com/maps/api/geocode/json` |
| **Auth** | API key in `key` query parameter |
| **Used by** | `battlecard_processor.py` |
| **Purpose** | Convert addresses to lat/lng coordinates |
| **Rate handling** | Basic retry on failure |

### ConnectBase Tenant API

| | |
|---|---|
| **Endpoint** | `https://api.connected2fiber.com/v2/tenants/` |
| **Auth** | `Ocp-Apim-Subscription-Key` header |
| **Secret** | `CONNECTBASE_TENANT_API_KEY` |
| **Used by** | `tenant_enrichment.py` |
| **Purpose** | Company info: employees, revenue, industry, spend, location count |
| **Rate handling** | Exponential backoff, max 8 retries, 5s base delay |
| **Key param** | `companyId=1646` (DQE's ConnectBase ID) |

### ConnectBase Network Intelligence API

| | |
|---|---|
| **Endpoint** | `https://api.connected2fiber.com/network-intelligence/v6/` |
| **Auth** | `Ocp-Apim-Subscription-Key` header |
| **Secret** | `CONNECTBASE_NETWORK_API_KEY` |
| **Used by** | `tenant_enrichment.py` |
| **Purpose** | DQE network distance, connection status, access medium |
| **Rate handling** | Exponential backoff, max 8 retries, 5s base delay |

### ConnectBase OnNet Providers API

| | |
|---|---|
| **Endpoint** | `https://api.connectbase.com/network-intelligence/v6/onnet` |
| **Auth** | `Ocp-Apim-Subscription-Key` header |
| **Secret** | `CONNECTBASE_TENANT_API_KEY` (same key as Tenant API) |
| **Used by** | `tenant_enrichment.py` |
| **Purpose** | List competitor providers at an address |
| **Rate handling** | Exponential backoff, max 8 retries, 5s base delay |

### HubSpot CRM API

| | |
|---|---|
| **Endpoint** | `https://api.hubapi.com/crm/v3/objects/companies` |
| **Auth** | `Authorization: Bearer {token}` header |
| **Secret** | `HUBSPOT_ACCESS_TOKEN` |
| **Used by** | `hubspot_update.py` |
| **Purpose** | Fetch all CRM companies for matching |
| **Properties fetched** | name, owner_name_field, netsuite_status, notes_last_contacted, lead_source__netsuite_, lead_source_type, hs_analytics_last_visit_timestamp, zoominfo_company_id |
| **Rate handling** | Retry on 429 with `Retry-After` header, max 5 retries per page, 60s request timeout, 10-30s wait between attempts. Detailed error logging includes HTTP status and companies fetched-so-far. |
| **Note** | Token is a HubSpot private app token — must be regenerated if it expires |

### Gemini 2.5 Flash (Vertex AI) — Online API

| | |
|---|---|
| **SDK** | `google-genai` |
| **Auth** | GCP service account with `roles/aiplatform.user` |
| **Model** | `gemini-2.5-flash` |
| **Used by** | `battlecard_llm.py` (Step 3 per-row), `hubspot_matcher.py`, `netsuite_matcher.py` |
| **Purpose** | ICP analysis + match confirmation |
| **Rate handling** | Exponential backoff on 429. LLM: 5 retries, 10s base. Matchers: 6 retries, 15s base. |

**LLM usage in the pipeline:**

| Call Site | Model | Temperature | Tools | Purpose |
|-----------|-------|------------|-------|---------|
| Step 3 research pass | gemini-2.5-flash | 1.0 | Google Search | Research company via web grounding |
| Step 3 scoring pass | gemini-2.5-flash | 0.2 | None | Structured JSON scoring based on research |
| HubSpot matcher | gemini-2.5-flash | 0.1 | None | Confirm company name match (only for ambiguous fuzzy results) |
| NetSuite matcher | gemini-2.5-flash | 0.1 | None | Confirm address match (only for ambiguous fuzzy results) |

### Google Cloud Storage

| | |
|---|---|
| **SDK** | `google-cloud-storage` Python package |
| **Auth** | GCP service account (automatic in Cloud Run) |
| **Used by** | All backend scripts |
| **Bucket** | `csv-battle-cards-dqe` |
| **Purpose** | Read inputs, write outputs, checkpointing |

### Google Cloud Logging

| | |
|---|---|
| **SDK** | `google-cloud-logging` Python package |
| **Auth** | GCP service account (automatic in Cloud Run) |
| **Used by** | All backend scripts |
| **Logger names** | `tenant-enrichment`, `hubspot-update`, `battlecard-generator`, `merge-shards` |
