import { useEffect, useRef, useState, useCallback } from 'react';
import { onAuthStateChanged, User } from 'firebase/auth';
import { auth, db } from '../firebase';
import { doc, setDoc, updateDoc, addDoc, collection, serverTimestamp } from 'firebase/firestore';

const GEOJSON_URL = 'https://storage.googleapis.com/csv-battle-cards-dqe/sales-map.geojson';
const BATTLE_CARDS_URL = 'https://storage.googleapis.com/csv-battle-cards-dqe/csv-battle-cards/dqe_prospects.json';

// ── LocalStorage keys ─────────────────────────────────────────────────────────
const LS_SEARCH_HISTORY = 'dqe_search_history';
const LS_CLICK_HISTORY  = 'dqe_click_history';
const MAX_SEARCH_HISTORY = 15;
const MAX_CLICK_HISTORY  = 10;

interface SearchEntry { address: string; lat: number; lng: number; timestamp: number; }
interface ClickEntry  { name: string; score: number; address: string; lat: number; lng: number; timestamp: number; }

function loadSearchHistory(): SearchEntry[] {
  try { return JSON.parse(localStorage.getItem(LS_SEARCH_HISTORY) || '[]'); } catch { return []; }
}
function saveSearchHistory(h: SearchEntry[]) { localStorage.setItem(LS_SEARCH_HISTORY, JSON.stringify(h)); }

function loadClickHistory(): ClickEntry[] {
  try { return JSON.parse(localStorage.getItem(LS_CLICK_HISTORY) || '[]'); } catch { return []; }
}
function saveClickHistory(h: ClickEntry[]) { localStorage.setItem(LS_CLICK_HISTORY, JSON.stringify(h)); }

// ── Session Tracking (Firestore) ───────────────────────────────────────────
const IDLE_TIMEOUT_MS  = 120_000;
const SAVE_INTERVAL_MS = 30_000;

function uuidv4(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function useSessionTracking(user: User | null) {
  const sessionIdRef     = useRef<string>(uuidv4());
  const activeStartRef   = useRef<number>(Date.now());
  const accActiveSecsRef = useRef<number>(0);
  const isIdleRef        = useRef<boolean>(false);
  const idleTimerRef     = useRef<ReturnType<typeof setTimeout>  | null>(null);
  const saveTimerRef     = useRef<ReturnType<typeof setInterval> | null>(null);
  const docCreatedRef    = useRef<boolean>(false);

  const activeSecs = () =>
    isIdleRef.current
      ? accActiveSecsRef.current
      : accActiveSecsRef.current + Math.round((Date.now() - activeStartRef.current) / 1000);

  const activeMinutes = () => parseFloat((activeSecs() / 60).toFixed(2));

  const sessionDocId = (uid: string) => `${uid}_${sessionIdRef.current}`;

  const createSessionDoc = async (user: User) => {
    const now   = new Date();
    const month = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    try {
      await setDoc(doc(db, 'sessions', sessionDocId(user.uid)), {
        uid:            user.uid,
        email:          user.email ?? '',
        display_name:   user.displayName ?? '',
        started_at:     serverTimestamp(),
        ended_at:       null,
        active_minutes: 0,
        month,
      });
      docCreatedRef.current = true;
    } catch (e) {
      console.error('Session create failed:', e);
    }
  };

  const saveProgress = async (uid: string) => {
    if (!docCreatedRef.current) return;
    try {
      await updateDoc(doc(db, 'sessions', sessionDocId(uid)), {
        active_minutes: activeMinutes(),
      });
    } catch (e) {
      console.error('Session update failed:', e);
    }
  };

  const closeSession = async (uid: string) => {
    if (!docCreatedRef.current) return;
    try {
      await updateDoc(doc(db, 'sessions', sessionDocId(uid)), {
        active_minutes: activeMinutes(),
        ended_at:       serverTimestamp(),
      });
    } catch (e) {
      console.error('Session close failed:', e);
    }
  };

  const resetIdleTimer = () => {
    if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
    if (isIdleRef.current) {
      isIdleRef.current      = false;
      activeStartRef.current = Date.now();
    }
    idleTimerRef.current = setTimeout(() => {
      accActiveSecsRef.current += Math.round((Date.now() - activeStartRef.current) / 1000);
      isIdleRef.current = true;
    }, IDLE_TIMEOUT_MS);
  };

  useEffect(() => {
    if (!user) return;
    sessionIdRef.current     = uuidv4();
    activeStartRef.current   = Date.now();
    accActiveSecsRef.current = 0;
    isIdleRef.current        = false;
    docCreatedRef.current    = false;
    createSessionDoc(user);
    saveTimerRef.current = setInterval(() => saveProgress(user.uid), SAVE_INTERVAL_MS);
    const activityEvents = ['mousemove', 'keydown', 'scroll', 'click', 'touchstart'];
    activityEvents.forEach(e => window.addEventListener(e, resetIdleTimer, { passive: true }));
    resetIdleTimer();
    const handleVisibility = () => {
      if (document.visibilityState === 'hidden') {
        accActiveSecsRef.current += Math.round((Date.now() - activeStartRef.current) / 1000);
        isIdleRef.current = true;
        closeSession(user.uid);
      } else {
        sessionIdRef.current     = uuidv4();
        activeStartRef.current   = Date.now();
        accActiveSecsRef.current = 0;
        isIdleRef.current        = false;
        docCreatedRef.current    = false;
        createSessionDoc(user);
        resetIdleTimer();
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);
    const handleUnload = () => {
      accActiveSecsRef.current += Math.round((Date.now() - activeStartRef.current) / 1000);
      closeSession(user.uid);
    };
    window.addEventListener('beforeunload', handleUnload);
    return () => {
      closeSession(user.uid);
      if (saveTimerRef.current) clearInterval(saveTimerRef.current);
      if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
      activityEvents.forEach(e => window.removeEventListener(e, resetIdleTimer));
      document.removeEventListener('visibilitychange', handleVisibility);
      window.removeEventListener('beforeunload', handleUnload);
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user?.uid]);
}

// ── Event Logging (Firestore) ─────────────────────────────────────────────
async function logUsageEvent(user: User | null, eventType: string, metadata: Record<string, any> = {}) {
  if (!user) return;
  const now = new Date();
  const month = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  try {
    await addDoc(collection(db, 'events'), {
      uid: user.uid,
      email: user.email ?? '',
      event_type: eventType,
      metadata,
      timestamp: serverTimestamp(),
      month,
    });
  } catch (e) {
    console.error('Event log failed:', e);
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────
const roundCoord = (n: number) => Math.round(n * 10000) / 10000;
const locationKey = (lat: number, lng: number) => `${roundCoord(lat)},${roundCoord(lng)}`;
const groupScore = (cards: any[]) =>
  Math.max(...cards.map(c => c.llm_analysis?.overall_score || 0));

const getScoreColor = (score: number) => {
  if (score >= 80) return '#00C853';
  if (score >= 60) return '#FFD600';
  if (score >= 40) return '#FF6D00';
  return '#D32F2F';
};
const getScoreLabel = (score: number) => {
  if (score >= 80) return 'Excellent';
  if (score >= 60) return 'Good';
  if (score >= 40) return 'Fair';
  return 'Poor';
};
const formatCost = (val: any): string => {
  if (!val || val === 'N/A') return 'N/A';
  const num = Number(String(val).replace(/[$,]/g, ''));
  if (isNaN(num)) return 'N/A';
  return '$' + num.toLocaleString();
};

function useGoogleMaps(apiKey: string) {
  const [loaded, setLoaded] = useState(false);
  useEffect(() => {
    if ((window as any).google?.maps?.Map) { setLoaded(true); return; }
    if (document.querySelector('script[src*="maps.googleapis.com"]')) {
      const check = setInterval(() => {
        if ((window as any).google?.maps?.Map) { setLoaded(true); clearInterval(check); }
      }, 100);
      return () => clearInterval(check);
    }
    const script = document.createElement('script');
    script.src = `https://maps.googleapis.com/maps/api/js?key=${apiKey}&libraries=places&loading=async`;
    script.async = true; script.defer = true;
    script.onload = () => {
      const check = setInterval(() => {
        if ((window as any).google?.maps?.Map) { setLoaded(true); clearInterval(check); }
      }, 50);
    };
    script.onerror = () => console.error('Failed to load Google Maps');
    document.head.appendChild(script);
    return () => { if (document.head.contains(script)) document.head.removeChild(script); };
  }, [apiKey]);
  return loaded;
}

const multiDotSvg = (color: string, count: number) => {
  const r = 12;
  const badge = count > 9 ? 14 : 12;
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${r * 2 + badge + 4}" height="${r * 2 + 4}" viewBox="0 0 ${r * 2 + badge + 4} ${r * 2 + 4}">
    <circle cx="${r + 2}" cy="${r + 2}" r="${r}" fill="${color}" fill-opacity="0.25" stroke="${color}" stroke-width="1.5"/>
    <circle cx="${r + 2}" cy="${r + 2}" r="${r - 4}" fill="${color}" fill-opacity="0.55" stroke="white" stroke-width="1.5"/>
    <circle cx="${r + 2}" cy="${r + 2}" r="${r - 8}" fill="${color}" stroke="white" stroke-width="1.5"/>
    <circle cx="${r * 2 + badge - 2}" cy="4" r="${badge / 2 + 1}" fill="#1a1a2e" stroke="white" stroke-width="1.2"/>
    <text x="${r * 2 + badge - 2}" y="8" text-anchor="middle" font-size="8" font-weight="bold" fill="white" font-family="Arial">${count > 99 ? '99+' : count}</text>
  </svg>`;
  return 'data:image/svg+xml;charset=UTF-8,' + encodeURIComponent(svg);
};

// ── Component ──────────────────────────────────────────────────────────────
export default function DQEBattleCardMap() {
  const mapRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const autocompleteRef = useRef<google.maps.places.Autocomplete | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [allBattleCards, setAllBattleCards] = useState<any[]>([]);
  const [showFiberRoutes, setShowFiberRoutes] = useState(true);
  const [hideCustomers, setHideCustomers] = useState(false);
  const hideCustomersRef = useRef(false);
  const [showJsonInput, setShowJsonInput] = useState(false);
  const [jsonInput, setJsonInput] = useState('');
  const [currentUser, setCurrentUser] = useState<User | null>(null);
  const [searchHistory, setSearchHistory] = useState<SearchEntry[]>(loadSearchHistory);
  const [clickHistory, setClickHistory] = useState<ClickEntry[]>(loadClickHistory);

  useEffect(() => { const u = onAuthStateChanged(auth, user => setCurrentUser(user)); return u; }, []);
  useSessionTracking(currentUser);

  const battleCardMarkersRef = useRef<google.maps.Marker[]>([]);
  const mapInstanceRef = useRef<google.maps.Map | null>(null);
  const isUpdatingMarkersRef = useRef(false);
  const activeInfoWindowRef = useRef<google.maps.InfoWindow | null>(null);
  const allBattleCardsRef = useRef<any[]>([]);
  const mapsLoaded = useGoogleMaps(process.env.REACT_APP_GOOGLE_MAPS_API_KEY || '');

  useEffect(() => { allBattleCardsRef.current = allBattleCards; }, [allBattleCards]);

  const addToSearchHistory = useCallback((entry: SearchEntry) => {
    setSearchHistory(prev => {
      const filtered = prev.filter(e => !(Math.abs(e.lat - entry.lat) < 0.0001 && Math.abs(e.lng - entry.lng) < 0.0001));
      const next = [entry, ...filtered].slice(0, MAX_SEARCH_HISTORY);
      saveSearchHistory(next);
      return next;
    });
  }, []);

  const addToClickHistory = useCallback((entry: ClickEntry) => {
    setClickHistory(prev => {
      const filtered = prev.filter(e => e.name !== entry.name || e.address !== entry.address);
      const next = [entry, ...filtered].slice(0, MAX_CLICK_HISTORY);
      saveClickHistory(next);
      return next;
    });
  }, []);

  const panTo = useCallback((lat: number, lng: number, zoom: number = 15) => {
    const map = mapInstanceRef.current;
    if (!map) return;
    map.panTo({ lat, lng });
    (map as any).setZoom(zoom);
  }, []);

  const panToAndOpenCard = useCallback((lat: number, lng: number, name: string) => {
    const map = mapInstanceRef.current;
    if (!map) return;
    map.panTo({ lat, lng });
    (map as any).setZoom(16);
    const cards = allBattleCardsRef.current;
    const match = cards.find(c => {
      const gc = c.geocode_data;
      return gc && Math.abs(gc.latitude - lat) < 0.0001 && Math.abs(gc.longitude - lng) < 0.0001;
    }) || cards.find(c => c.ey_file_data?.Name === name);
    if (match) {
      const gc = match.geocode_data || {};
      const marker = new google.maps.Marker({ position: { lat: gc.latitude || lat, lng: gc.longitude || lng }, map, visible: false });
      setTimeout(() => showBattleCardInfo(marker, match, map), 300);
    }
  }, []);

  // ── InfoWindow builders (unchanged logic) ─────────────────────────────────

  const showTenantPicker = (marker: google.maps.Marker, cards: any[], map: google.maps.Map) => {
    if (activeInfoWindowRef.current) activeInfoWindowRef.current.close();
    const sorted = [...cards].sort((a, b) => (b.llm_analysis?.overall_score || 0) - (a.llm_analysis?.overall_score || 0));
    const rows = sorted.map((card, i) => {
      const name = card.ey_file_data?.Name || 'Unknown';
      const score = card.llm_analysis?.overall_score || 0;
      const color = getScoreColor(score);
      return `<div data-idx="${i}" style="display:flex;align-items:center;gap:10px;padding:9px 10px;cursor:pointer;border-bottom:1px solid #f0f0f0;border-radius:4px;transition:background 0.12s;" onmouseover="this.style.background='#f0f4ff'" onmouseout="this.style.background='transparent'"><div style="width:11px;height:11px;border-radius:50%;flex-shrink:0;background:${color};border:2px solid white;box-shadow:0 0 0 1px rgba(0,0,0,0.18);"></div><span style="flex:1;font-size:13px;font-weight:500;color:#222;">${name}</span><span style="font-size:11px;font-weight:700;color:${color};white-space:nowrap;">${score}/100</span></div>`;
    }).join('');
    const addr = sorted[0]?.geocode_data?.formatted_address || '';
    const content = `<div style="min-width:320px;max-width:400px;max-height:380px;overflow-y:auto;font-family:Arial,sans-serif;"><div style="background:#1a1a2e;color:white;padding:12px 14px;margin:-12px -12px 10px -12px;border-radius:4px 4px 0 0;"><div style="font-size:14px;font-weight:bold;">${cards.length} prospects at this address</div>${addr ? `<div style="font-size:11px;opacity:0.7;margin-top:3px;">${addr}</div>` : ''}</div><div style="font-size:11px;color:#888;padding:0 2px 6px;">Select a prospect to view its battle card</div><div id="tenant-rows">${rows}</div></div>`;
    const iw = new google.maps.InfoWindow({ content, maxWidth: 430 });
    activeInfoWindowRef.current = iw;
    iw.addListener('closeclick', () => { activeInfoWindowRef.current = null; });
    iw.open(map, marker);
    google.maps.event.addListenerOnce(iw, 'domready', () => {
      document.querySelectorAll('#tenant-rows [data-idx]').forEach(el => {
        el.addEventListener('click', () => { const idx = Number((el as HTMLElement).dataset.idx); iw.close(); activeInfoWindowRef.current = null; showBattleCardInfo(marker, sorted[idx], map); });
      });
    });
  };

  const showBattleCardInfo = (marker: google.maps.Marker, card: any, map: google.maps.Map) => {
    if (activeInfoWindowRef.current) activeInfoWindowRef.current.close();
    const gc = card.geocode_data || {};
    if (gc.latitude && gc.longitude) {
      addToClickHistory({ name: card.ey_file_data?.Name || 'Unknown', score: card.llm_analysis?.overall_score || 0, address: gc.formatted_address || `${card.ey_file_data?.Address || ''}, ${card.ey_file_data?.City || ''}`, lat: gc.latitude, lng: gc.longitude, timestamp: Date.now() });
    }
    logUsageEvent(currentUser, 'card_view', { name: card.ey_file_data?.Name, score: card.llm_analysis?.overall_score, lat: gc.latitude, lng: gc.longitude });
    const eyData = card.ey_file_data || {}; const analysis = card.llm_analysis || {}; const score = analysis.overall_score || 0; const cbData = card.connectbase_data || {}; const dataConfidence = analysis.data_confidence || {}; const icpFit = analysis.icp_fit || {}; const businessAssessment = icpFit.business_assessment || {}; const sales = analysis.sales_intelligence || {}; const hubspot = card.hubspot_match || {}; const netsuite = card.netsuite_match || {};
    const eyRows = Object.entries(eyData).filter(([, val]) => val !== undefined && val !== null && val !== '').map(([key, val]) => `<tr style="border-bottom:1px solid #f0f0f0;"><td style="padding:4px 8px 4px 4px;font-weight:bold;width:45%;color:#555;vertical-align:top;">${key}:</td><td style="padding:4px;">${String(val) !== 'N/A' ? String(val) : '<span style="color:#bbb;">N/A</span>'}</td></tr>`).join('');

    const infoContent = `<div style="max-width:500px;max-height:600px;overflow-y:auto;font-family:Arial,sans-serif;">
      <div style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:white;padding:16px;margin:-12px -12px 12px -12px;border-radius:4px 4px 0 0;">
        <h2 style="margin:0 0 8px 0;font-size:18px;">${eyData.Name || 'Unknown'}</h2>
        <div style="font-size:32px;font-weight:bold;margin:8px 0;">${score}/100</div>
        <div style="font-size:13px;opacity:0.9;">${getScoreLabel(score)} - Priority: ${sales.priority_level?.toUpperCase() || 'N/A'}</div>
        <div style="font-size:11px;opacity:0.8;margin-top:4px;">Confidence: ${(dataConfidence.confidence_score * 100).toFixed(0)}% x ICP: ${icpFit.icp_fit_score || 0}</div>
      </div>
      <details open style="margin-bottom:12px;border:1px solid #ddd;border-radius:4px;"><summary style="background:${hubspot.matched?'#E8F5E9':'#f5f5f5'};padding:10px;cursor:pointer;font-weight:bold;font-size:14px;">HubSpot CRM ${hubspot.matched?`<span style="margin-left:8px;background:#4CAF50;color:white;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:bold;">MATCHED</span>`:`<span style="margin-left:8px;background:#9E9E9E;color:white;padding:2px 8px;border-radius:10px;font-size:10px;">NOT IN CRM</span>`}</summary><div style="padding:12px;font-size:12px;">${hubspot.matched?`<table style="width:100%;border-collapse:collapse;"><tr><td style="padding:4px;font-weight:bold;width:45%;">CRM Name:</td><td style="padding:4px;">${hubspot.hubspot_name||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">HubSpot ID:</td><td style="padding:4px;font-family:monospace;font-size:11px;color:#666;">${hubspot.hubspot_id||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Owner ID:</td><td style="padding:4px;">${hubspot.hubspot_owner_id||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">NetSuite Status:</td><td style="padding:4px;">${hubspot.netsuite_status?`<span style="background:#E3F2FD;color:#1565C0;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:bold;">${hubspot.netsuite_status}</span>`:'<span style="color:#999;">N/A</span>'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Last Contacted:</td><td style="padding:4px;">${hubspot.notes_last_contacted?new Date(hubspot.notes_last_contacted).toLocaleDateString():'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Lead Source:</td><td style="padding:4px;">${hubspot.lead_source||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Lead Source Type:</td><td style="padding:4px;">${hubspot.lead_source_type||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Match Confidence:</td><td style="padding:4px;"><span style="background:${hubspot.match_confidence==='high'?'#4CAF50':'#FF9800'};color:white;padding:2px 8px;border-radius:3px;font-size:10px;text-transform:uppercase;">${hubspot.match_confidence||'N/A'}</span></td></tr></table>${hubspot.match_reason?`<div style="margin-top:8px;padding:8px;background:#F1F8E9;border-left:3px solid #8BC34A;font-size:11px;color:#555;"><strong>Match Reason:</strong> ${hubspot.match_reason}</div>`:''}`:`<p style="color:#999;font-style:italic;margin:0;">This company was not found in HubSpot CRM.</p>${hubspot.match_reason?`<p style="color:#bbb;font-size:11px;margin:6px 0 0 0;">${hubspot.match_reason}</p>`:''}`}</div></details>
      <details open style="margin-bottom:12px;border:1px solid #ddd;border-radius:4px;"><summary style="background:#f5f5f5;padding:10px;cursor:pointer;font-weight:bold;font-size:14px;">Sales Intelligence</summary><div style="padding:12px;font-size:12px;">${sales.priority_reasoning?`<div style="margin-bottom:12px;padding:10px;background:#E3F2FD;border-left:3px solid #2196F3;font-size:11px;line-height:1.5;"><strong>Priority Reasoning:</strong><br/>${sales.priority_reasoning}</div>`:''}${sales.key_selling_points?.length>0?`<div style="margin-bottom:12px;"><strong style="font-size:11px;">Key Selling Points:</strong><ul style="margin:4px 0;padding-left:20px;font-size:11px;line-height:1.5;">${sales.key_selling_points.map((p:string)=>`<li>${p}</li>`).join('')}</ul></div>`:''}${sales.likely_pain_points?.length>0?`<div style="margin-bottom:12px;"><strong style="font-size:11px;">Likely Pain Points:</strong><ul style="margin:4px 0;padding-left:20px;font-size:11px;line-height:1.5;color:#d32f2f;">${sales.likely_pain_points.map((p:string)=>`<li>${p}</li>`).join('')}</ul></div>`:''}${sales.competitive_angles?.length>0?`<div style="margin-bottom:12px;"><strong style="font-size:11px;">Competitive Angles:</strong><ul style="margin:4px 0;padding-left:20px;font-size:11px;line-height:1.5;">${sales.competitive_angles.map((a:string)=>`<li>${a}</li>`).join('')}</ul></div>`:''}${sales.data_gaps_to_resolve?.length>0?`<div style="margin-bottom:12px;padding:8px;background:#FFF3E0;border-left:3px solid #FF9800;"><strong style="font-size:11px;">Data Gaps to Resolve:</strong><ul style="margin:4px 0;padding-left:20px;font-size:11px;">${sales.data_gaps_to_resolve.map((g:string)=>`<li>${g}</li>`).join('')}</ul></div>`:''}${sales.recommended_approach?`<div style="margin-bottom:12px;"><strong style="font-size:11px;">Recommended Approach:</strong><p style="margin:4px 0;font-size:11px;line-height:1.5;color:#555;">${sales.recommended_approach}</p></div>`:''}${sales.recommended_services?.length>0?`<div style="margin-bottom:12px;"><strong style="font-size:11px;">Recommended Services:</strong><br/><div style="margin-top:4px;">${sales.recommended_services.map((s:string)=>`<span style="display:inline-block;background:#E3F2FD;padding:4px 8px;margin:2px;border-radius:3px;font-size:10px;font-weight:500;">${s}</span>`).join('')}</div></div>`:''}${sales.next_best_actions?.length>0?`<div style="margin-top:12px;padding:10px;background:#E8F5E9;border-left:3px solid #4CAF50;"><strong style="font-size:11px;color:#2E7D32;">Next Best Actions:</strong><ul style="margin:4px 0;padding-left:20px;font-size:11px;">${sales.next_best_actions.map((a:string)=>`<li>${a}</li>`).join('')}</ul></div>`:''}</div></details>
      <details open style="margin-bottom:12px;border:1px solid #ddd;border-radius:4px;"><summary style="background:#f5f5f5;padding:10px;cursor:pointer;font-weight:bold;font-size:14px;">Business Assessment: ${icpFit.business_scale_need_points||0}/80 pts</summary><div style="padding:12px;font-size:12px;"><table style="width:100%;margin-bottom:8px;"><tr><td style="padding:4px;font-weight:bold;width:50%;">Criticality:</td><td style="padding:4px;"><span style="background:${businessAssessment.business_criticality==='high'?'#D32F2F':businessAssessment.business_criticality==='moderate'?'#FF9800':'#666'};color:white;padding:2px 8px;border-radius:3px;font-size:11px;text-transform:uppercase;font-weight:bold;">${businessAssessment.business_criticality||'N/A'}</span></td></tr><tr><td style="padding:4px;font-weight:bold;">Bandwidth Need:</td><td style="padding:4px;text-transform:uppercase;">${businessAssessment.bandwidth_requirements||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Est. Monthly Spend:</td><td style="padding:4px;font-weight:bold;color:green;">${formatCost(businessAssessment.estimated_monthly_spend)}</td></tr></table>${businessAssessment.criticality_reasoning?`<div style="margin-bottom:12px;padding:10px;background:#F3E5F5;border-left:3px solid #9C27B0;font-size:11px;line-height:1.5;"><strong>Why This Criticality:</strong><br/>${businessAssessment.criticality_reasoning}</div>`:''}${businessAssessment.infrastructure_needs?.length>0?`<div style="margin-top:8px;"><strong style="font-size:11px;">Infrastructure Needs:</strong><ul style="margin:4px 0;padding-left:20px;font-size:11px;line-height:1.5;">${businessAssessment.infrastructure_needs.map((n:string)=>`<li>${n}</li>`).join('')}</ul></div>`:''}${icpFit.icp_fit_summary?`<div style="margin-top:12px;padding:10px;background:#E8EAF6;border-left:3px solid #3F51B5;font-size:11px;line-height:1.5;"><strong>ICP Fit Summary:</strong><br/>${icpFit.icp_fit_summary}</div>`:''}</div></details>
      <details style="margin-bottom:12px;border:1px solid #ddd;border-radius:4px;"><summary style="background:#f5f5f5;padding:10px;cursor:pointer;font-weight:bold;font-size:14px;">Data Confidence: ${(dataConfidence.confidence_score * 100).toFixed(0)}%</summary><div style="padding:12px;font-size:12px;"><div style="background:#f9f9f9;padding:8px;border-radius:4px;margin-bottom:8px;"><strong>Score Breakdown:</strong><div style="margin-top:4px;"><div style="display:flex;justify-content:space-between;margin:4px 0;"><span>Business Status:</span><span style="font-weight:bold;">${(dataConfidence.business_status_points||0).toFixed(2)}/0.40</span></div><div style="display:flex;justify-content:space-between;margin:4px 0;"><span>Employee Validation:</span><span style="font-weight:bold;">${(dataConfidence.employee_validation_points||0).toFixed(2)}/0.40</span></div><div style="display:flex;justify-content:space-between;margin:4px 0;"><span>Source Quality:</span><span style="font-weight:bold;">${(dataConfidence.source_quality_points||0).toFixed(2)}/0.20</span></div></div></div><table style="width:100%;margin-bottom:8px;"><tr><td style="padding:4px;font-weight:bold;width:50%;">Business Status:</td><td style="padding:4px;"><span style="background:${dataConfidence.business_status==='operating'?'#4CAF50':'#FF9800'};color:white;padding:2px 8px;border-radius:3px;font-size:11px;text-transform:uppercase;">${dataConfidence.business_status||'N/A'}</span></td></tr><tr><td style="padding:4px;font-weight:bold;">Validated Employees:</td><td style="padding:4px;font-weight:bold;color:#667eea;">${dataConfidence.validated_employee_count?.toLocaleString()||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Employee Confidence:</td><td style="padding:4px;text-transform:uppercase;font-size:11px;">${dataConfidence.employee_count_confidence||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Data Basis:</td><td style="padding:4px;font-size:11px;">${dataConfidence.employee_count_basis||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Location Type:</td><td style="padding:4px;text-transform:capitalize;">${dataConfidence.location_type||'N/A'}</td></tr></table>${dataConfidence.business_status_evidence?`<div style="margin-bottom:8px;padding:8px;background:#E3F2FD;border-left:3px solid #2196F3;"><strong style="font-size:11px;">Evidence:</strong><p style="margin:4px 0 0 0;font-size:11px;color:#555;">${dataConfidence.business_status_evidence}</p></div>`:''}${dataConfidence.employee_comparison?`<div style="margin-bottom:8px;padding:8px;background:#F3E5F5;border-left:3px solid #9C27B0;"><strong style="font-size:11px;">Employee Count Comparison:</strong><p style="margin:4px 0 0 0;font-size:11px;color:#555;">${dataConfidence.employee_comparison}</p></div>`:''}${dataConfidence.employee_count_sources?.length>0?`<div style="margin-top:8px;"><strong style="font-size:11px;">Sources:</strong><div style="margin-top:4px;">${dataConfidence.employee_count_sources.map((s:string)=>`<span style="display:inline-block;background:#E8F5E9;padding:2px 6px;margin:2px;border-radius:3px;font-size:10px;">${s}</span>`).join('')}</div></div>`:''}${dataConfidence.data_quality_notes?`<div style="margin-top:8px;padding:8px;background:#FFF3E0;border-left:3px solid #FF9800;font-size:11px;"><strong>Data Quality Notes:</strong><br/>${dataConfidence.data_quality_notes}</div>`:''}</div></details>
      <details style="margin-bottom:12px;border:1px solid #ddd;border-radius:4px;"><summary style="background:#f5f5f5;padding:10px;cursor:pointer;font-weight:bold;font-size:14px;">EY File Information</summary><div style="padding:12px;font-size:12px;">${eyRows ? `<table style="width:100%;border-collapse:collapse;">${eyRows}</table>` : '<p style="color:#999;font-style:italic;">No EY file data</p>'}</div></details>
      <details style="margin-bottom:12px;border:1px solid #ddd;border-radius:4px;"><summary style="background:#f5f5f5;padding:10px;cursor:pointer;font-weight:bold;font-size:14px;">ConnectBase Data</summary><div style="padding:12px;font-size:12px;">${!cbData.API_EntityName||cbData.API_EntityName==='N/A'?'<p style="color:#999;font-style:italic;">No ConnectBase data available</p>':`<table style="width:100%;border-collapse:collapse;"><tr><td style="padding:4px;font-weight:bold;width:45%;">Entity:</td><td style="padding:4px;">${cbData.API_EntityName||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">CB Employees:</td><td style="padding:4px;">${Number(cbData.API_NoOfEmployees||0).toLocaleString()}</td></tr><tr><td style="padding:4px;font-weight:bold;">Monthly Network Spend:</td><td style="padding:4px;color:green;font-weight:bold;">${formatCost(cbData.API_MonthlyNetworkSpend)}</td></tr><tr><td style="padding:4px;font-weight:bold;">Revenue:</td><td style="padding:4px;">${formatCost(cbData.API_Revenue)}</td></tr><tr><td style="padding:4px;font-weight:bold;">Industry:</td><td style="padding:4px;">${cbData.API_Industry||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Location Type:</td><td style="padding:4px;">${cbData.API_LocationType||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Total Locations:</td><td style="padding:4px;">${cbData.API_LocationCount||'N/A'}</td></tr></table>`}</div></details>
      ${card.additional_tenants?.length>0?`<details style="margin-bottom:12px;border:1px solid #ddd;border-radius:4px;"><summary style="background:#f5f5f5;padding:10px;cursor:pointer;font-weight:bold;font-size:14px;">Additional Tenants (${card.additional_tenants.length})</summary><div style="padding:12px;font-size:11px;"><ul style="margin:0;padding-left:20px;">${card.additional_tenants.slice(0,10).map((t:string)=>`<li>${t}</li>`).join('')}${card.additional_tenants.length>10?`<li style="color:#999;">... and ${card.additional_tenants.length-10} more</li>`:''}</ul></div></details>`:''}
      <details style="margin-bottom:12px;border:1px solid #ddd;border-radius:4px;"><summary style="background:${netsuite.matched?'#E8F0FE':'#f5f5f5'};padding:10px;cursor:pointer;font-weight:bold;font-size:14px;">NetSuite Structure ${netsuite.matched?`<span style="margin-left:8px;background:#1A73E8;color:white;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:bold;">MATCHED</span>`:`<span style="margin-left:8px;background:#9E9E9E;color:white;padding:2px 8px;border-radius:10px;font-size:10px;">NOT FOUND</span>`}</summary><div style="padding:12px;font-size:12px;">${netsuite.matched?`<table style="width:100%;border-collapse:collapse;"><tr><td style="padding:4px;font-weight:bold;width:45%;">Structure Name:</td><td style="padding:4px;">${netsuite.netsuite_name||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Internal ID:</td><td style="padding:4px;font-family:monospace;font-size:11px;color:#666;">${netsuite.netsuite_internal_id||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Address:</td><td style="padding:4px;">${netsuite.netsuite_address||'N/A'}, ${netsuite.netsuite_zip||''}</td></tr><tr><td style="padding:4px;font-weight:bold;">Structure Type:</td><td style="padding:4px;">${netsuite.structure_type?`<span style="background:#E8EAF6;color:#283593;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:bold;">${netsuite.structure_type}</span>`:'<span style="color:#999;">N/A</span>'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Structure Status:</td><td style="padding:4px;">${netsuite.structure_status?`<span style="background:#E8F5E9;color:#1B5E20;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:bold;">${netsuite.structure_status}</span>`:'<span style="color:#999;">N/A</span>'}</td></tr><tr><td style="padding:4px;font-weight:bold;">NS Status:</td><td style="padding:4px;">${netsuite.ns_status?`<span style="background:#E3F2FD;color:#1565C0;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:bold;">${netsuite.ns_status}</span>`:'<span style="color:#999;">N/A</span>'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Distance Band:</td><td style="padding:4px;">${netsuite.distance_band||'N/A'}</td></tr><tr><td style="padding:4px;font-weight:bold;">Primary Cost Total:</td><td style="padding:4px;font-weight:bold;color:#c62828;">${formatCost(netsuite.primary_cost_total)}</td></tr></table><div style="margin-top:8px;padding:8px;background:#E8F0FE;border-left:3px solid #1A73E8;font-size:11px;color:#555;"><strong>Match Confidence:</strong> <span style="margin-left:6px;background:${netsuite.match_confidence==='high'?'#4CAF50':'#FF9800'};color:white;padding:2px 8px;border-radius:3px;font-size:10px;text-transform:uppercase;">${netsuite.match_confidence||'N/A'}</span>${netsuite.match_reason?`<br/><strong>Reason:</strong> ${netsuite.match_reason}`:''}</div>`:`<p style="color:#999;font-style:italic;margin:0;">This address was not found in the NetSuite structure database.</p>${netsuite.match_reason?`<p style="color:#bbb;font-size:11px;margin:6px 0 0 0;">${netsuite.match_reason}</p>`:''}`}</div></details>
    </div>`;

    const iw = new google.maps.InfoWindow({ content: infoContent, maxWidth: 550 });
    activeInfoWindowRef.current = iw;
    iw.addListener('closeclick', () => { activeInfoWindowRef.current = null; });
    iw.open(map, marker);
  };

  // ── Marker rendering ─────────────────────────────────────────────────────

  const updateMarkersForViewport = (map: google.maps.Map, cards: any[]) => {
    if (isUpdatingMarkersRef.current || activeInfoWindowRef.current) return;
    isUpdatingMarkersRef.current = true;
    const bounds = map.getBounds();
    if (!bounds) { isUpdatingMarkersRef.current = false; return; }
    const visible = cards.filter(card => {
      const score = card.llm_analysis?.overall_score || 0;
      const gc = card.geocode_data || {};
      if (score === 0 || !gc.latitude || !gc.longitude) return false;
      if (hideCustomersRef.current) {
        const nsStatus = (card.hubspot_match?.netsuite_status || '').toLowerCase();
        if (nsStatus.includes('customer')) return false;
      }
      return bounds.contains({ lat: gc.latitude, lng: gc.longitude });
    });
    const groups = new Map<string, any[]>();
    visible.forEach(card => { const gc = card.geocode_data; const key = locationKey(gc.latitude, gc.longitude); if (!groups.has(key)) groups.set(key, []); groups.get(key)!.push(card); });
    const sorted = Array.from(groups.entries()).sort((a, b) => groupScore(b[1]) - groupScore(a[1])).slice(0, 100);
    battleCardMarkersRef.current.forEach(m => m.setMap(null));
    battleCardMarkersRef.current = [];
    const markers: google.maps.Marker[] = [];
    for (const [, groupCards] of sorted) {
      const best = groupScore(groupCards); const color = getScoreColor(best); const gc = groupCards[0].geocode_data; const isMulti = groupCards.length > 1;
      const marker = new google.maps.Marker({ position: { lat: gc.latitude, lng: gc.longitude }, map, icon: isMulti ? { url: multiDotSvg(color, groupCards.length), anchor: new google.maps.Point(14, 14) } : { path: google.maps.SymbolPath.CIRCLE, scale: best >= 80 ? 10 : best >= 60 ? 8 : 6, fillColor: color, fillOpacity: 0.9, strokeColor: '#ffffff', strokeWeight: 2 }, title: isMulti ? `${groupCards.length} prospects - best score: ${best}` : `${groupCards[0].ey_file_data?.Name || ''} - Score: ${best}`, zIndex: best >= 80 ? 1000 : best >= 60 ? 900 : 800 });
      marker.addListener('click', () => isMulti ? showTenantPicker(marker, groupCards, map) : showBattleCardInfo(marker, groupCards[0], map));
      markers.push(marker);
    }
    battleCardMarkersRef.current = markers;
    isUpdatingMarkersRef.current = false;
  };

  const loadBattleCards = async (map: google.maps.Map, data: any) => {
    const cards = data.battle_cards || [];
    setAllBattleCards(cards);
    updateMarkersForViewport(map, cards);
    map.addListener('idle', () => updateMarkersForViewport(map, allBattleCardsRef.current.length > 0 ? allBattleCardsRef.current : cards));
    setLoading(false);
  };

  const initMap = () => {
    if (!mapRef.current || !(window as any).google?.maps?.Map) return;
    const map = new google.maps.Map(mapRef.current, { center: { lat: 40.4406, lng: -79.9959 }, zoom: 10, mapTypeId: 'roadmap', mapTypeControl: true, streetViewControl: false, fullscreenControl: true });
    mapInstanceRef.current = map;
    if (showFiberRoutes) {
      fetch(GEOJSON_URL).then(r => r.json()).then(gj => { map.data.addGeoJson(gj); map.data.setStyle({ strokeColor: '#1967D2', strokeOpacity: 0.6, strokeWeight: 2, clickable: false }); }).catch(err => console.error('Fiber routes:', err));
    }
    return map;
  };

  useEffect(() => {
    if (!mapsLoaded || !searchInputRef.current || autocompleteRef.current) return;
    const ac = new google.maps.places.Autocomplete(searchInputRef.current, { types: ['address'], componentRestrictions: { country: 'us' } });
    ac.setBounds(new google.maps.LatLngBounds({ lat: 40.2, lng: -80.2 }, { lat: 40.7, lng: -79.7 }));
    ac.addListener('place_changed', () => {
      const place = ac.getPlace();
      if (place.geometry?.location) {
        const lat = place.geometry.location.lat(); const lng = place.geometry.location.lng();
        const address = place.formatted_address || place.name || '';
        panTo(lat, lng, 16);
        addToSearchHistory({ address, lat, lng, timestamp: Date.now() });
        logUsageEvent(currentUser, 'search', { query: address, lat, lng });
      }
    });
    autocompleteRef.current = ac;
  }, [mapsLoaded, addToSearchHistory, panTo, currentUser]);

  const handleJsonSubmit = () => {
    try { const data = JSON.parse(jsonInput); setShowJsonInput(false); setError(null); const map = initMap(); if (map) loadBattleCards(map, data); } catch (err: any) { setError(`Invalid JSON: ${err.message}`); }
  };

  useEffect(() => {
    if (!mapsLoaded || !mapRef.current) return;
    const map = initMap(); if (!map) return;
    fetch(BATTLE_CARDS_URL).then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }).then(data => loadBattleCards(map, data)).catch(err => { console.error(err); setError(`CORS Error: Cannot load from GCS bucket. Click "Load JSON Manually" to paste data.`); setLoading(false); setShowJsonInput(true); });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mapsLoaded, showFiberRoutes]);

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div style={{ width: '100%', height: '100vh', position: 'relative' }}>
      {loading && !showJsonInput && (
        <div style={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%,-50%)', zIndex: 1000, background: 'white', padding: '24px', borderRadius: '8px', boxShadow: '0 4px 12px rgba(0,0,0,0.15)', textAlign: 'center' }}>
          <div style={{ fontSize: '18px', fontWeight: 'bold', marginBottom: '8px' }}>Loading Battle Cards...</div>
          <div style={{ fontSize: '13px', color: '#666' }}>Processing geocoded data and scoring...</div>
        </div>
      )}
      {showJsonInput && (
        <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, background: 'rgba(0,0,0,0.5)', zIndex: 2000, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
          <div style={{ background: 'white', borderRadius: '8px', padding: '24px', maxWidth: '800px', width: '100%', maxHeight: '90vh', overflow: 'auto' }}>
            <h2 style={{ margin: '0 0 16px 0' }}>Load Battle Cards JSON</h2>
            <p style={{ marginBottom: '16px', color: '#666', fontSize: '14px' }}>Paste your dqe_prospects.json data below:</p>
            <textarea value={jsonInput} onChange={e => setJsonInput(e.target.value)} placeholder='{"summary": {...}, "battle_cards": [...]}' style={{ width: '100%', height: '400px', padding: '12px', fontFamily: 'monospace', fontSize: '12px', border: '2px solid #ddd', borderRadius: '4px', resize: 'vertical', marginBottom: '16px' }} />
            <div style={{ display: 'flex', gap: '12px' }}>
              <button onClick={handleJsonSubmit} disabled={!jsonInput} style={{ flex: 1, padding: '12px', background: jsonInput ? '#4CAF50' : '#ccc', color: 'white', border: 'none', borderRadius: '4px', fontSize: '14px', fontWeight: 'bold', cursor: jsonInput ? 'pointer' : 'not-allowed' }}>Load Data</button>
              <button onClick={() => { setShowJsonInput(false); setError(null); }} style={{ padding: '12px 24px', background: '#666', color: 'white', border: 'none', borderRadius: '4px', fontSize: '14px', cursor: 'pointer' }}>Cancel</button>
            </div>
          </div>
        </div>
      )}
      {error && !showJsonInput && (
        <div style={{ position: 'absolute', top: 20, left: '50%', transform: 'translateX(-50%)', background: '#f44336', color: 'white', padding: '12px 24px', borderRadius: '4px', zIndex: 1000, boxShadow: '0 2px 10px rgba(0,0,0,0.2)', maxWidth: '80%', textAlign: 'center' }}>
          <div>{error}</div>
          <button onClick={() => setShowJsonInput(true)} style={{ marginTop: '8px', padding: '6px 12px', background: 'white', color: '#f44336', border: 'none', borderRadius: '4px', cursor: 'pointer', fontSize: '12px', fontWeight: 'bold' }}>Load JSON Manually</button>
        </div>
      )}

      {/* ── Sidebar ───────────────────────────────────────────────────────── */}
      {!loading && !error && (
        <div style={{ position: 'absolute', top: 20, right: 20, background: 'white', padding: '16px', borderRadius: '8px', boxShadow: '0 2px 8px rgba(0,0,0,0.2)', fontSize: '13px', zIndex: 1000, minWidth: '280px', maxWidth: '320px', maxHeight: 'calc(100vh - 40px)', overflowY: 'auto' }}>
          <div style={{ fontWeight: 'bold', marginBottom: '12px', fontSize: '16px', borderBottom: '2px solid #667eea', paddingBottom: '8px' }}>DQE Battle Cards</div>
          {currentUser && (<div style={{ marginBottom: '10px', padding: '6px 8px', background: '#E8F5E9', borderRadius: '4px', fontSize: '11px', color: '#2E7D32' }}>Logged in as <strong>{currentUser.email}</strong></div>)}
          <div style={{ marginBottom: '12px', fontSize: '12px', color: '#666' }}>Total Prospects: <strong>{allBattleCards.length}</strong></div>

          {/* Address Search */}
          <div style={{ marginBottom: '16px' }}>
            <div style={{ fontSize: '12px', fontWeight: 'bold', marginBottom: '6px' }}>Search Address</div>
            <input ref={searchInputRef} type="text" placeholder="Type an address..." style={{ width: '100%', padding: '8px 10px', fontSize: '13px', border: '2px solid #ddd', borderRadius: '4px', outline: 'none', boxSizing: 'border-box' }} onFocus={e => { e.target.style.borderColor = '#667eea'; }} onBlur={e => { e.target.style.borderColor = '#ddd'; }} onKeyDown={e => {
              if (e.key === 'Enter') {
                e.preventDefault();
                const query = (e.target as HTMLInputElement).value.trim();
                if (!query) return;
                const apiKey = process.env.REACT_APP_GOOGLE_MAPS_API_KEY || '';
                fetch(`https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(query)}&region=us&key=${apiKey}`)
                  .then(r => r.json())
                  .then(data => {
                    if (data.status === 'OK' && data.results?.length) {
                      const { lat, lng } = data.results[0].geometry.location;
                      const address = data.results[0].formatted_address || query;
                      panTo(lat, lng, 16);
                      addToSearchHistory({ address, lat, lng, timestamp: Date.now() });
                      logUsageEvent(currentUser, 'search', { query: address, lat, lng });
                    }
                  })
                  .catch(err => console.error('Geocode search failed:', err));
              }
            }} />
          </div>

          {/* Recent Searches */}
          {searchHistory.length > 0 && (
            <div style={{ marginBottom: '16px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                <span style={{ fontSize: '11px', fontWeight: 'bold', color: '#888', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Recent Searches</span>
                <span onClick={() => { setSearchHistory([]); saveSearchHistory([]); }} style={{ fontSize: '10px', color: '#999', cursor: 'pointer' }}>Clear</span>
              </div>
              {searchHistory.map((entry, i) => (
                <div key={i} onClick={() => panTo(entry.lat, entry.lng, 16)} style={{ padding: '6px 8px', marginBottom: '3px', cursor: 'pointer', borderRadius: '4px', fontSize: '11px', color: '#444', background: '#f8f9fa', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} onMouseOver={e => (e.currentTarget.style.background = '#e8eaf6')} onMouseOut={e => (e.currentTarget.style.background = '#f8f9fa')} title={entry.address}>{entry.address}</div>
              ))}
            </div>
          )}

          {/* Recently Viewed */}
          {clickHistory.length > 0 && (
            <div style={{ marginBottom: '16px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                <span style={{ fontSize: '11px', fontWeight: 'bold', color: '#888', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Recently Viewed</span>
                <span onClick={() => { setClickHistory([]); saveClickHistory([]); }} style={{ fontSize: '10px', color: '#999', cursor: 'pointer' }}>Clear</span>
              </div>
              {clickHistory.map((entry, i) => (
                <div key={i} onClick={() => panToAndOpenCard(entry.lat, entry.lng, entry.name)} style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '6px 8px', marginBottom: '3px', cursor: 'pointer', borderRadius: '4px', fontSize: '11px', color: '#444', background: '#f8f9fa' }} onMouseOver={e => (e.currentTarget.style.background = '#e8eaf6')} onMouseOut={e => (e.currentTarget.style.background = '#f8f9fa')} title={entry.address}>
                  <div style={{ width: '8px', height: '8px', borderRadius: '50%', flexShrink: 0, background: getScoreColor(entry.score), border: '1.5px solid white', boxShadow: '0 0 0 1px rgba(0,0,0,0.15)' }} />
                  <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{entry.name}</span>
                  <span style={{ fontWeight: 'bold', color: getScoreColor(entry.score), flexShrink: 0 }}>{entry.score}</span>
                </div>
              ))}
            </div>
          )}

          {/* Score Legend */}
          <div style={{ marginBottom: '12px', padding: '8px', background: '#f8f9fa', borderRadius: '4px', fontSize: '11px' }}>
            <div style={{ fontWeight: 'bold', marginBottom: '4px', fontSize: '10px', color: '#888', textTransform: 'uppercase' }}>Score Legend</div>
            <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
              {[{ label: '80+', color: '#00C853' }, { label: '60-79', color: '#FFD600' }, { label: '40-59', color: '#FF6D00' }, { label: '<40', color: '#D32F2F' }].map(({ label, color }) => (
                <span key={label} style={{ display: 'flex', alignItems: 'center', gap: '3px' }}><span style={{ width: '8px', height: '8px', borderRadius: '50%', background: color, display: 'inline-block' }} />{label}</span>
              ))}
            </div>
          </div>

          {/* Filters */}
          <div onClick={() => { const next = !hideCustomers; setHideCustomers(next); hideCustomersRef.current = next; logUsageEvent(currentUser, 'filter_toggle', { filter: 'hide_customers', enabled: next }); if (activeInfoWindowRef.current) { activeInfoWindowRef.current.close(); activeInfoWindowRef.current = null; } if (mapInstanceRef.current) updateMarkersForViewport(mapInstanceRef.current, allBattleCardsRef.current); }} style={{ display: 'flex', alignItems: 'center', padding: '8px', marginBottom: '6px', cursor: 'pointer', borderRadius: '4px', background: hideCustomers ? '#FFF3E0' : '#f5f5f5', border: `2px solid ${hideCustomers ? '#FF6D00' : '#ddd'}` }}>
            <input type="checkbox" checked={hideCustomers} readOnly style={{ marginRight: '8px' }} />
            <span style={{ fontSize: '12px' }}>Hide Existing Customers</span>
          </div>

          {/* Fiber Routes */}
          <div onClick={() => { setShowFiberRoutes(!showFiberRoutes); logUsageEvent(currentUser, 'fiber_toggle', { enabled: !showFiberRoutes }); }} style={{ display: 'flex', alignItems: 'center', padding: '8px', cursor: 'pointer', borderRadius: '4px', background: showFiberRoutes ? '#E3F2FD' : '#f5f5f5', border: `2px solid ${showFiberRoutes ? '#1967D2' : '#ddd'}` }}>
            <input type="checkbox" checked={showFiberRoutes} readOnly style={{ marginRight: '8px' }} />
            <span style={{ fontSize: '12px' }}>Show Fiber Routes</span>
          </div>
        </div>
      )}

      <div ref={mapRef} style={{ width: '100%', height: '100%' }} />
    </div>
  );
}
