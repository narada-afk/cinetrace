/**
 * South Cinema Insights — Content Script
 * Detects text selection → queries actor API → renders floating popup.
 */

const API_BASE = SCA_CONFIG.API_BASE;
const CURRENT_YEAR = new Date().getFullYear();

// ─── State ─────────────────────────────────────────────────────────────────
let popup       = null;
let debounceTimer = null;
let lastQuery   = '';
let inflightCtrl = null; // AbortController for in-flight requests

// ─── Entry point ───────────────────────────────────────────────────────────
document.addEventListener('mouseup',   onMouseUp);
document.addEventListener('mousedown', onMouseDown);

function onMouseUp(e) {
  if (popup && popup.contains(e.target)) return;
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => handleSelection(), 200);
}

function onMouseDown(e) {
  if (popup && !popup.contains(e.target)) dismissPopup();
}

// ─── Selection handler ─────────────────────────────────────────────────────
async function handleSelection() {
  const selection = window.getSelection();
  const text = selection ? selection.toString().trim() : '';

  if (!text || text.length < 3 || text.length > 40) return;
  if (/^\d+$/.test(text)) return;
  if (text === lastQuery && popup) return;

  // Capture rect before async gap (selection may change)
  const rect = getSelectionRect(selection);
  if (!rect) return;

  lastQuery = text;

  // Cancel any previous in-flight request
  if (inflightCtrl) inflightCtrl.abort();
  inflightCtrl = new AbortController();
  const signal = inflightCtrl.signal;

  const actor = await fetchActorFull(text, signal);
  if (!actor || signal.aborted) return;

  showPopup(actor, rect);
}

// ─── API ───────────────────────────────────────────────────────────────────

/** Step 1: search by name; Step 2: parallel-fetch profile + collaborators + movies */
async function fetchActorFull(query, signal) {
  try {
    // Step 1 — search
    const searchRes = await fetch(
      `${API_BASE}/actors/search?q=${encodeURIComponent(query)}`,
      { signal }
    );
    if (!searchRes.ok) return null;
    const results = await searchRes.json();
    const match = Array.isArray(results) ? results[0] : results;
    if (!match?.id) return null;

    const id = match.id;

    // Step 2 — parallel: profile + top blockbuster + latest movie
    const [profile, blockbusters, movies] = await Promise.all([
      apiFetch(`/actors/${id}`, signal),
      apiFetch(`/actors/${id}/blockbusters`, signal),
      apiFetch(`/actors/${id}/movies`, signal),
    ]);

    if (!profile) return null;

    // Top box office: highest grossing film (blockbusters sorted desc already)
    const topHit = Array.isArray(blockbusters) && blockbusters.length > 0
      ? blockbusters[0]
      : null;
    const topBoxOffice = topHit
      ? `${topHit.title} · ₹${Math.round(topHit.box_office_crore)} Cr`
      : null;

    const latestMovie = Array.isArray(movies)
      ? movies
          .filter(m => m.release_year && m.release_year <= CURRENT_YEAR)
          .sort((a, b) => b.release_year - a.release_year)[0]
      : null;

    return {
      id:            profile.id,
      name:          profile.name,
      industry:      profile.industry   || '',
      film_count:    profile.film_count || 0,
      first_year:    profile.first_film_year || null,
      last_year:     profile.last_film_year
                       ? Math.min(profile.last_film_year, CURRENT_YEAR)
                       : null,
      top_box_office: topBoxOffice,
      latest_movie:  latestMovie ? `${latestMovie.title} (${latestMovie.release_year})` : null,
    };

  } catch (err) {
    if (err.name === 'AbortError') return null;
    return null; // Silently handle all other errors
  }
}

async function apiFetch(path, signal) {
  try {
    const res = await fetch(`${API_BASE}${path}`, { signal });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

// ─── Popup ─────────────────────────────────────────────────────────────────
function showPopup(actor, rect) {
  dismissPopup();

  popup = document.createElement('div');
  popup.id = 'sca-popup';
  popup.innerHTML = buildHTML(actor);
  document.body.appendChild(popup);

  // Position after insertion so offsetWidth/Height are available
  positionPopup(rect);

  // Animate in on next frame
  requestAnimationFrame(() => {
    requestAnimationFrame(() => popup && popup.classList.add('sca-visible'));
  });
}

function buildHTML(actor) {
  const span = actor.first_year && actor.last_year
    ? `${actor.first_year} – ${actor.last_year}`
    : actor.first_year || '—';

  return `
    <div class="sca-header">
      <span class="sca-name">${esc(actor.name)}</span>
      ${actor.industry ? `<span class="sca-industry">${esc(actor.industry)}</span>` : ''}
    </div>
    <div class="sca-divider"></div>
    <div class="sca-stats">
      <div class="sca-row">
        <span class="sca-icon">🎬</span>
        <span class="sca-label">Films</span>
        <span class="sca-value">${actor.film_count || '—'}</span>
      </div>
      <div class="sca-row">
        <span class="sca-icon">📅</span>
        <span class="sca-label">Active</span>
        <span class="sca-value">${span}</span>
      </div>
      <div class="sca-row">
        <span class="sca-icon">🤝</span>
        <span class="sca-label">Top Hit</span>
        <span class="sca-value">${esc(actor.top_box_office || '—')}</span>
      </div>
      <div class="sca-row">
        <span class="sca-icon">🔥</span>
        <span class="sca-label">Latest</span>
        <span class="sca-value sca-latest">${esc(actor.latest_movie || '—')}</span>
      </div>
    </div>
    <div class="sca-footer">South Cinema Analytics</div>
  `;
}

function positionPopup(rect) {
  const MARGIN  = 10;
  const pW = popup.offsetWidth  || 240;
  const pH = popup.offsetHeight || 170;
  const vW = window.innerWidth;
  const vH = window.innerHeight;
  const sX = window.scrollX;
  const sY = window.scrollY;

  // Default: below selection, horizontally centred on it
  let left = sX + rect.left + rect.width / 2 - pW / 2;
  let top  = sY + rect.bottom + MARGIN;

  // Flip above if would go off bottom of viewport
  if (rect.bottom + pH + MARGIN > vH) {
    top = sY + rect.top - pH - MARGIN;
  }

  // Clamp horizontally within viewport
  left = Math.max(sX + MARGIN, Math.min(left, sX + vW - pW - MARGIN));

  popup.style.left = `${left}px`;
  popup.style.top  = `${top}px`;
}

function dismissPopup() {
  if (popup) {
    popup.remove();
    popup    = null;
    lastQuery = '';
  }
}

// ─── Helpers ───────────────────────────────────────────────────────────────
function getSelectionRect(selection) {
  if (!selection || selection.rangeCount === 0) return null;
  const r = selection.getRangeAt(0).getBoundingClientRect();
  // getBoundingClientRect returns all-zeros for collapsed selections
  if (r.width === 0 && r.height === 0) return null;
  return r;
}

function esc(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
