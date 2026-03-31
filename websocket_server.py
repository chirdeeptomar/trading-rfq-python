"""
FastAPI REST Server

Responsibilities:
  - On startup: load all 100 instruments from Soul REST API into a static cache
  - REST endpoint GET /isins  — returns the full ISIN/name cache
  - REST endpoint GET /health — liveness check
  - GET /              — browser test client that connects to the pipeline WebSocket
                         (ws://localhost:9001) served by the Bytewax WebSocketSink

The live enriched-RFQ stream is served directly by the Bytewax pipeline on
ws://localhost:9001 — no NATS round-trip here.

Run with:
    uvicorn websocket_server:app --host 0.0.0.0 --port 9000
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

SOUL_URL = "http://localhost:8000"

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

isin_cache: list[dict[str, str]] = []   # [{"isin": "...", "name": "..."}]


# ---------------------------------------------------------------------------
# Startup lifecycle
# ---------------------------------------------------------------------------

async def _load_isin_cache() -> None:
    global isin_cache
    print("Loading ISIN cache from Soul ...")
    async with httpx.AsyncClient(base_url=SOUL_URL, timeout=10.0) as client:
        try:
            resp = await client.get(
                "/api/tables/instruments/rows",
                params={"_limit": "200"},
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            isin_cache = [{"isin": row["isin"], "name": row["name"]} for row in data]
            print(f"Loaded {len(isin_cache)} instruments into ISIN cache")
        except Exception as e:
            print(f"WARNING: Could not load ISIN cache from Soul: {e}")
            isin_cache = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _load_isin_cache()
    yield


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="RFQ REST Server", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/isins")
async def get_isins() -> dict[str, Any]:
    return {"count": len(isin_cache), "instruments": isin_cache}


# ---------------------------------------------------------------------------
# Browser test client — connects to the Bytewax WebSocketSink on port 9001
# ---------------------------------------------------------------------------

_TEST_HTML = """
<!DOCTYPE html>
<html>
<head><title>RFQ Stream</title>
<style>
  body { background: #1a1a2e; color: #e0e0e0; font-family: monospace; padding: 20px; }
  h1 { color: #00d4ff; }
  #status { color: #00ff88; margin-bottom: 10px; }
  #log { height: 80vh; overflow-y: scroll; border: 1px solid #333; padding: 10px; }
  .rfq { border-bottom: 1px solid #333; padding: 4px 0; }
  .QUOTED { color: #00d4ff; }
  .PRICED { color: #ffcc00; }
  .EXPIRED { color: #ff4444; }
  .CONCLUDED { color: #00ff88; }
  .REJECTED { color: #ff8800; }
  .ACKNOWLEDGED { color: #aaffaa; }
</style>
</head>
<body>
<h1>Live RFQ Stream</h1>
<div id="status">Connecting...</div>
<div id="log"></div>
<script>
  const ws = new WebSocket(`ws://${location.hostname}:9001`);
  const log = document.getElementById('log');
  const status = document.getElementById('status');

  ws.onopen = () => { status.textContent = 'Connected to pipeline (port 9001)'; };
  ws.onclose = () => { status.textContent = 'Disconnected'; status.style.color = '#ff4444'; };
  ws.onerror = () => { status.textContent = 'Connection error — is the Bytewax pipeline running?'; status.style.color = '#ff4444'; };

  ws.onmessage = (e) => {
    const data = JSON.parse(e.data);

    const div = document.createElement('div');
    div.className = `rfq ${data.state}`;
    div.textContent = `[${data.state}] ${data.rfq_id?.slice(0,8)}... | ${data.isin} | ${data.instrument_name} | ${data.direction} ${data.quantity?.toLocaleString()} | bid=${data.bid?.toFixed(4)} ask=${data.ask?.toFixed(4)}`;
    log.prepend(div);

    // Keep log trimmed
    while (log.children.length > 200) log.removeChild(log.lastChild);
  };
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def test_client() -> str:
    return _TEST_HTML


# ---------------------------------------------------------------------------
# AG Grid negotiation blotter — one row per rfq_id, updated in-place
# ---------------------------------------------------------------------------

_TICKETS_HTML = """
<!DOCTYPE html>
<html>
<head>
<title>RFQ Blotter</title>
<script src="https://cdn.jsdelivr.net/npm/ag-grid-community@33/dist/ag-grid-community.min.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; background: #13131f; color: #e0e0e0; font-family: 'Segoe UI', monospace; }
  header {
    display: flex; align-items: center; gap: 16px;
    padding: 10px 20px; background: #1a1a2e; border-bottom: 1px solid #2a2a4a;
  }
  header h1 { font-size: 1.1rem; color: #00d4ff; letter-spacing: 0.05em; }
  #ws-status {
    font-size: 0.75rem; padding: 3px 10px; border-radius: 12px;
    background: #1e3a2e; color: #00ff88; border: 1px solid #00ff8840;
  }
  #ws-status.disconnected { background: #3a1e1e; color: #ff4444; border-color: #ff444440; }
  #queue-count { font-size: 0.75rem; color: #aaa; }
  #row-count   { font-size: 0.75rem; color: #888; margin-left: auto; }
  #grid { height: calc(100vh - 50px); width: 100%; }

  /* Row highlight colours injected via rowStyle — no CSS class needed */
</style>
</head>
<body>
<header>
  <h1>RFQ Blotter</h1>
  <span id="ws-status">Connecting…</span>
  <span id="queue-count"></span>
  <span id="row-count">0 / 10 rows</span>
</header>
<div id="grid" class="ag-theme-alpine-dark"></div>

<script>
// ─── Constants ────────────────────────────────────────────────────────────────
const MAX_ROWS    = 10;
const ADMIT_MS    = 400;         // stagger between queue admissions
const FLASH_MS    = 1200;        // whole-row flash duration after terminal
const FADE_MS     = 800;         // fade-out duration after flash
const UI_TTL_MS   = 60 * 1000;  // 1 min client-side hard expiry

const STATE_ORDER = {
  RECEIVED: 0, ENRICHED: 1, PRICED: 2, QUOTED: 3,
  ACKNOWLEDGED: 4, CONCLUDED: 5, REJECTED: 5, EXPIRED: 5,
};
const TERMINAL = new Set(['CONCLUDED', 'REJECTED', 'EXPIRED']);

const STATE_COLOR = {
  RECEIVED:     '#888888',
  ENRICHED:     '#ffcc00',
  PRICED:       '#ff9900',
  QUOTED:       '#00d4ff',
  ACKNOWLEDGED: '#aaffaa',
  CONCLUDED:    '#00ff88',
  REJECTED:     '#ff6644',
  EXPIRED:      '#ff4444',
};

// ─── Helpers ──────────────────────────────────────────────────────────────────
function fmtPrice(p)  { return p.value != null ? p.value.toFixed(4) : '—'; }
function fmtQty(p)    { return p.value != null ? Number(p.value).toLocaleString() : '—'; }
function fmtTime(p)   { return p.value ? new Date(p.value).toISOString().slice(11,23) : '—'; }
function stateCell(p) {
  const c = STATE_COLOR[p.value] || '#e0e0e0';
  return `<span style="color:${c};font-weight:700">${p.value ?? ''}</span>`;
}
function elapsedGetter(p) {
  if (!p.data.created_at) return '—';
  const end = p.data.concluded_at || p.data.rejected_at || p.data.expired_at;
  if (!end) return '—';
  return ((new Date(end) - new Date(p.data.created_at)) / 1000).toFixed(2) + 's';
}

// ─── Grid setup ───────────────────────────────────────────────────────────────
// rowFlashMap: rfq_id → 'flashing' | 'fading' — drives rowStyle
const rowFlashMap = new Map();

const columnDefs = [
  { field: 'rfq_id',          headerName: 'RFQ ID',    width: 110,
    valueFormatter: p => p.value ? p.value.slice(0,8)+'…' : '', tooltipField: 'rfq_id' },
  { field: 'state',           headerName: 'State',      width: 140, cellRenderer: stateCell },
  { field: 'client_id',       headerName: 'Client',     width: 105 },
  { field: 'direction',       headerName: 'Side',       width: 68,
    cellStyle: p => ({ color: p.value === 'BUY' ? '#00ff88' : '#ff6644', fontWeight: 700 }) },
  { field: 'isin',            headerName: 'ISIN',       width: 130 },
  { field: 'instrument_name', headerName: 'Instrument', flex: 1, minWidth: 160 },
  { field: 'quantity',        headerName: 'Qty',        width: 108, valueFormatter: fmtQty },
  { field: 'bid',             headerName: 'Bid',        width: 90,  valueFormatter: fmtPrice },
  { field: 'ask',             headerName: 'Ask',        width: 90,  valueFormatter: fmtPrice },
  { field: 'spread_bps',      headerName: 'Sprd bps',   width: 88,
    valueFormatter: p => p.value != null ? p.value.toFixed(2) : '—' },
  { field: 'created_at',      headerName: 'Created',    width: 112, valueFormatter: fmtTime },
  { field: 'quoted_at',       headerName: 'Quoted',     width: 112, valueFormatter: fmtTime },
  { field: 'concluded_at',    headerName: 'Concluded',  width: 112, valueFormatter: fmtTime },
  { field: 'rejected_at',     headerName: 'Rejected',   width: 112, valueFormatter: fmtTime },
  { field: 'expired_at',      headerName: 'Expired',    width: 112, valueFormatter: fmtTime },
  { headerName: 'Elapsed',    width: 88, valueGetter: elapsedGetter },
];

const grid = agGrid.createGrid(document.getElementById('grid'), {
  columnDefs,
  rowData: [],
  getRowId:  p => p.data.rfq_id,
  defaultColDef: { sortable: true, resizable: true },
  animateRows: false,                // keep off — row animation moves DOM nodes, breaking style lookups
  enableCellChangeFlash: true,
  suppressScrollOnNewData: true,
  // Drive whole-row background via rowStyle so it survives virtual rendering
  getRowStyle: p => {
    const phase = rowFlashMap.get(p.data?.rfq_id);
    if (phase === 'flashing') return { background: 'rgba(255,220,60,0.45)', transition: 'background 0.3s' };
    if (phase === 'fading')   return { opacity: '0',                        transition: 'opacity 0.8s'    };
    return undefined;
  },
});

// ─── Client bookkeeping ───────────────────────────────────────────────────────
const statusEl = document.getElementById('ws-status');
const countEl  = document.getElementById('row-count');
const queueEl  = document.getElementById('queue-count');

const pendingUpdates = new Map();   // rfq_id → latest data buffered before admission
const admitQueue     = [];          // rfq_ids awaiting a free slot (FIFO)
const activeRows     = new Set();   // rfq_ids currently in the grid
const removingRows   = new Set();   // rfq_ids in flash/fade removal phase
const ttlTimers      = new Map();   // rfq_id → setTimeout handle

function updateCounts() {
  countEl.textContent = `${activeRows.size} / ${MAX_ROWS} rows`;
  queueEl.textContent = admitQueue.length ? `${admitQueue.length} queued` : '';
}

function redrawRow(rfq_id) {
  const node = grid.getRowNode(rfq_id);
  if (node) grid.redrawRows({ rowNodes: [node] });
}

// ─── Removal: flash → fade → delete ─────────────────────────────────────────
function flashAndRemove(rfq_id) {
  if (removingRows.has(rfq_id)) return;
  removingRows.add(rfq_id);
  clearTtl(rfq_id);

  // Phase 1: flash (yellow highlight)
  rowFlashMap.set(rfq_id, 'flashing');
  redrawRow(rfq_id);

  setTimeout(() => {
    // Phase 2: fade out
    rowFlashMap.set(rfq_id, 'fading');
    redrawRow(rfq_id);

    setTimeout(() => {
      // Phase 3: remove from grid
      rowFlashMap.delete(rfq_id);
      const node = grid.getRowNode(rfq_id);
      if (node) grid.applyTransaction({ remove: [node.data] });
      activeRows.delete(rfq_id);
      removingRows.delete(rfq_id);
      pendingUpdates.delete(rfq_id);
      updateCounts();
      setTimeout(tryAdmitNext, ADMIT_MS);
    }, FADE_MS);

  }, FLASH_MS);
}

// ─── Client-side 1-min TTL ───────────────────────────────────────────────────
function setTtl(rfq_id) {
  clearTtl(rfq_id);
  ttlTimers.set(rfq_id, setTimeout(() => {
    if (removingRows.has(rfq_id)) return;
    const node = grid.getRowNode(rfq_id);
    if (!node || TERMINAL.has(node.data.state)) return;
    const expired = { ...node.data, state: 'EXPIRED', expired_at: new Date().toISOString() };
    grid.applyTransaction({ update: [expired] });
    flashAndRemove(rfq_id);
  }, UI_TTL_MS));
}

function clearTtl(rfq_id) {
  const h = ttlTimers.get(rfq_id);
  if (h != null) { clearTimeout(h); ttlTimers.delete(rfq_id); }
}

// ─── Admission queue ──────────────────────────────────────────────────────────
function tryAdmitNext() {
  while (admitQueue.length > 0 && activeRows.size < MAX_ROWS) {
    const rfq_id = admitQueue.shift();
    if (removingRows.has(rfq_id) || activeRows.has(rfq_id)) continue;
    const data = pendingUpdates.get(rfq_id);
    if (!data) continue;
    pendingUpdates.delete(rfq_id);

    // If the row is already terminal when admitted, add it briefly then remove it
    if (TERMINAL.has(data.state)) {
      activeRows.add(rfq_id);
      grid.applyTransaction({ add: [data], addIndex: 0 });
      updateCounts();
      flashAndRemove(rfq_id);
      if (admitQueue.length > 0) setTimeout(tryAdmitNext, ADMIT_MS);
      return;
    }

    activeRows.add(rfq_id);
    grid.applyTransaction({ add: [data], addIndex: 0 });
    setTtl(rfq_id);
    updateCounts();
    if (admitQueue.length > 0) setTimeout(tryAdmitNext, ADMIT_MS);
    return;
  }
  updateCounts();
}

// ─── Incoming message handler ─────────────────────────────────────────────────
function handleRfq(rfq) {
  const id         = rfq.rfq_id;
  const isActive   = activeRows.has(id);
  const isRemoving = removingRows.has(id);

  if (isActive && !isRemoving) {
    const node = grid.getRowNode(id);
    if (!node) return;
    if ((STATE_ORDER[rfq.state] ?? -1) >= (STATE_ORDER[node.data.state] ?? -1)) {
      grid.applyTransaction({ update: [rfq] });
      if (TERMINAL.has(rfq.state)) flashAndRemove(id);
    }
    return;
  }

  if (!isRemoving) {
    // Buffer latest state; enqueue once
    const alreadyQueued = pendingUpdates.has(id) || admitQueue.includes(id);
    pendingUpdates.set(id, rfq);
    if (!alreadyQueued) {
      admitQueue.push(id);
    }
    // Always try to admit — a terminal queued row takes a slot briefly then flashes out
    tryAdmitNext();
  }
}

// ─── WebSocket ────────────────────────────────────────────────────────────────
function connect() {
  const ws = new WebSocket(`ws://${location.hostname}:9001`);
  ws.onopen    = () => { statusEl.textContent = 'Live'; statusEl.className = ''; };
  ws.onclose   = () => { statusEl.textContent = 'Disconnected — retrying…'; statusEl.className = 'disconnected'; setTimeout(connect, 2000); };
  ws.onerror   = () => { statusEl.textContent = 'Connection error';          statusEl.className = 'disconnected'; };
  ws.onmessage = e  => handleRfq(JSON.parse(e.data));
}
connect();
</script>
</body>
</html>
"""


@app.get("/tickets", response_class=HTMLResponse)
async def tickets() -> str:
    return _TICKETS_HTML
