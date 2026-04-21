# Frontend

## Tech Stack

- React 19 + TypeScript
- Google Maps JavaScript API
- Firebase (Auth, Firestore, Hosting)
- Tailwind CSS
- Create React App (build tooling)

## File Structure

```
dqe-map/
├── public/
│   └── index.html              # HTML shell
├── src/
│   ├── index.tsx                # React entry point
│   ├── App.tsx                  # Root component (renders DQEBattleCardMap)
│   ├── firebase.ts              # Firebase config (auth + db exports)
│   ├── declarations.d.ts        # Google Maps TypeScript declarations
│   ├── components/
│   │   └── DQEBattleCardMap.tsx # THE main component (~564 lines)
│   ├── index.css                # Tailwind directives
│   ├── reportWebVitals.ts       # Performance monitoring
│   └── setupTests.ts            # Jest config
├── .env                         # Environment variables (not committed)
├── firebase.json                # Firebase Hosting config
├── .firebaserc                  # Firebase project binding
├── package.json                 # Dependencies and scripts
├── tsconfig.json                # TypeScript config
└── postcss.config.js            # Tailwind/PostCSS config
```

## How It Works

1. App loads, authenticates user via Firebase Auth
2. Google Maps JavaScript API is lazy-loaded via script tag
3. Battle card JSON is fetched from GCS: `https://storage.googleapis.com/csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json`
4. Fiber route GeoJSON is fetched from GCS: `https://storage.googleapis.com/csv-battle-cards-dqe/sales-map.geojson`
5. Map renders centered on Pittsburgh (40.4406, -79.9959) at zoom 10
6. As the user pans/zooms, the top 100 prospects in the viewport are shown as color-coded markers
7. Clicking a marker opens a battle card InfoWindow with all scored data
8. User interactions (card views, searches, filter toggles) are logged to Firestore `events` collection
9. Session duration is tracked in Firestore `sessions` collection

## Key Features

- **Score-coded markers:** Green (80+), Yellow (60-79), Orange (40-59), Red (<40)
- **Battle card sections (in order):** HubSpot CRM, Sales Intelligence (pain points, selling points), Business Assessment, DQE Network Data, Data Confidence, EY File, ConnectBase Data, Additional Tenants, NetSuite Structure
- **Address search:** Google Places Autocomplete + manual geocoding fallback
- **Recently Viewed:** Click history that re-opens battle cards
- **Hide Existing Customers:** Filters out prospects with HubSpot NetSuite status containing "Customer"
- **Fiber Routes:** Toggle GeoJSON overlay of DQE fiber network
- **JSON loader:** Manual paste option for development/testing

## NPM Scripts

| Command | Purpose |
|---------|---------|
| `npm start` | Dev server on http://localhost:3000 |
| `npm run build` | Production build to `/build` directory |
| `npm test` | Run test suite |

## Key Component: DQEBattleCardMap.tsx

This is the entire application in one component (~564 lines). It contains:

### State

| State Variable | Purpose |
|---------------|---------|
| `allBattleCards` | All loaded battle card data |
| `showFiberRoutes` | Fiber route GeoJSON toggle |
| `hideCustomers` | Filter to hide existing customers |
| `currentUser` | Firebase Auth user |
| `searchHistory` | Recent address searches (localStorage, max 15) |
| `clickHistory` | Recently viewed cards (localStorage, max 10) |
| `loading` / `error` | UI state |

### Key Functions

| Function | Purpose |
|----------|---------|
| `useSessionTracking(user)` | Tracks session duration in Firestore (idle timeout: 2 min, save interval: 30s) |
| `useGoogleMaps(apiKey)` | Lazy-loads Google Maps JavaScript API |
| `showBattleCardInfo(marker, card, map)` | Renders the full battle card InfoWindow |
| `showTenantPicker(marker, cards, map)` | Shows picker when multiple prospects share an address |
| `updateMarkersForViewport(map, cards)` | Renders top 100 markers visible in current map bounds |
| `panToAndOpenCard(lat, lng, name)` | Pans to location and opens matching battle card |
| `logUsageEvent(user, type, metadata)` | Logs interaction events to Firestore |

### Marker Rendering

- Only renders markers within the current map viewport
- Limits to top 100 locations by score
- Groups prospects at the same lat/lng into multi-dot markers with count badges
- Marker size scales with score: 10px (80+), 8px (60-79), 6px (<60)
- Z-index prioritizes higher scores

### Data Sources (fetched on load)

| URL | Purpose |
|-----|---------|
| `https://storage.googleapis.com/csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json` | Battle card data |
| `https://storage.googleapis.com/csv-battle-cards-dqe/sales-map.geojson` | Fiber route overlay |
