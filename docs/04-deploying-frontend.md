# Deploying the Frontend

## Prerequisites

- Node.js installed
- Firebase CLI available (`npm install --save-dev firebase-tools` if not global)
- Firebase login active (`npx firebase login`)
- `.env` file with all required variables (see [Environment Variables](./06-environment-variables.md))

## Steps

```bash
# 1. Navigate to project root
cd dqe-map

# 2. Install dependencies (first time or after package.json changes)
npm install

# 3. Build production bundle
npm run build

# 4. Deploy to Firebase Hosting
npx firebase deploy --only hosting
```

## What Each Step Does

### `npm run build`

- Compiles TypeScript via `react-scripts build`
- Bundles with Webpack, minifies JS and CSS
- Outputs to `/build` directory
- Hashes filenames for cache-busting (e.g., `main.0c975649.js`)
- Injects all `REACT_APP_*` environment variables from `.env` into the bundle

### `npx firebase deploy --only hosting`

- Uploads everything in `/build` to Firebase Hosting
- Configured in `firebase.json`:
  - **public:** `build`
  - **rewrites:** All routes → `index.html` (SPA)
  - **ignore:** `firebase.json`, dotfiles, `node_modules`
- Firebase serves with HTTPS automatically
- Index.html: no-cache (revalidates on every load)
- Static assets: cached forever (hashed filenames)

## Live URL

`https://manifest-altar-490719-j7.firebaseapp.com`

## Firebase Project Config

**firebase.json:**
```json
{
  "hosting": {
    "public": "build",
    "ignore": ["firebase.json", "**/.*", "**/node_modules/**"],
    "rewrites": [{ "source": "**", "destination": "/index.html" }]
  }
}
```

**.firebaserc:**
```json
{
  "projects": {
    "default": "manifest-altar-490719-j7"
  }
}
```

## Common Issues

| Issue | Fix |
|-------|-----|
| `firebase: command not found` | Use `npx firebase` instead, or run `npm install --save-dev firebase-tools` |
| `Authentication Error: Your credentials are no longer valid` | Run `npx firebase login --reauth` |
| Build fails with TypeScript errors | Run `npx tsc --noEmit` to see specific errors |
| Map doesn't load after deploy | Check `REACT_APP_GOOGLE_MAPS_API_KEY` in `.env` is valid and has Maps JS API + Places API + Geocoding API enabled |
| No markers appear on map | Check browser console for CORS or fetch errors from GCS; verify `dqe_prospects.json` exists in the bucket |
| `EACCES: permission denied` on global install | Use `npm install --save-dev` instead of `-g`, then use `npx` prefix |
