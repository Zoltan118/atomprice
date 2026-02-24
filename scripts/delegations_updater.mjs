// scripts/delegations_updater.mjs
// Atomprice delegation snapshot + persistent Top lists
// - Fetches recent MsgDelegate txs via Tendermint RPC /tx_search
// - Maintains:
//    1) whales_top: Top 20 delegations >= 25,000 ATOM (persistent, keeps biggest ever seen)
//    2) mids_top:   Top 100 delegations 1,000–24,999 ATOM (persistent, keeps biggest ever seen)
//    3) ticker:     Fresh mixed feed (recent fetched + some whales/mids), always allowed to change
//
// Output files:
//   - data/delegations_24h.json (main file, keeps name for compatibility)
//   - data/recent-delegations.json
//   - data/top-delegations.json
//
// Env (optional):
//   RPC_BASES       comma-separated RPCs (preferred)
//   RPC_BASE        single RPC fallback if RPC_BASES not set
//   REST_BASE       default: https://rest.cosmos.directory/cosmoshub
//   WINDOW_HOURS    default: 48 (changed from 24)
//   MIN_ATOM        default: 1
//   LIMIT_PAGES     default: 5 (increased for 48h window)
//   PER_PAGE        default: 100 (increased for more coverage)
//   WHALE_MIN       default: 25000
//   WHALES_KEEP     default: 20
//   MIDS_MIN        default: 1000
//   MIDS_MAX        default: 24999.999999
//   MIDS_KEEP       default: 100
//   TICKER_KEEP     default: 200 (increased for more ticker items)

import fs from "node:fs/promises";

const OUT_FILE = "data/delegations_24h.json";  // Keep filename for compatibility

const RPC_BASES = (process.env.RPC_BASES
  ? process.env.RPC_BASES.split(",")
  : [process.env.RPC_BASE || "https://rpc.silknodes.io/cosmos"])
  .map((s) => s.trim().replace(/\/+$/, ""))
  .filter(Boolean);
const REST_BASE = (process.env.REST_BASE || "https://rest.cosmos.directory/cosmoshub").replace(/\/+$/, "");
const WINDOW_HOURS = Number(process.env.WINDOW_HOURS ?? "48");  // Changed to 48 hours
const MIN_ATOM = Number(process.env.MIN_ATOM ?? "1");

const LIMIT_PAGES = Number(process.env.LIMIT_PAGES ?? "5");    // Increased for 48h
const PER_PAGE = Number(process.env.PER_PAGE ?? "100");        // Increased for coverage

const WHALE_MIN = Number(process.env.WHALE_MIN ?? "50000");
const WHALES_KEEP = Number(process.env.WHALES_KEEP ?? "20");

const MIDS_MIN = Number(process.env.MIDS_MIN ?? "1000");
const MIDS_MAX = Number(process.env.MIDS_MAX ?? "49999.999999");
const MIDS_KEEP = Number(process.env.MIDS_KEEP ?? "100");

const TICKER_KEEP = Number(process.env.TICKER_KEEP ?? "200");  // Increased

const MSG_DELEGATE = "/cosmos.staking.v1beta1.MsgDelegate";
const EVENT_QUERY = `message.action='${MSG_DELEGATE}'`;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

const rpcStats = {};
for (const rpc of RPC_BASES) {
  rpcStats[rpc] = { ok: 0, fail: 0, empty: 0, last_error: null };
}

async function fetchJson(url, timeoutMs = 15000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ac.signal, headers: { accept: "application/json" } });
    const text = await res.text();
    let json = null;
    try { json = JSON.parse(text); } catch {}
    if (!res.ok) {
      const msg = json?.error?.data || json?.error?.message || json?.message || text?.slice(0, 200);
      throw new Error(`HTTP ${res.status} for ${url}${msg ? ` :: ${msg}` : ""}`);
    }
    return json;
  } finally {
    clearTimeout(t);
  }
}

async function fetchJsonFromRpcPath(path, timeoutMs = 15000, requireNonEmptyTxs = false) {
  let lastErr = null;
  let firstEmpty = null;

  for (const rpc of RPC_BASES) {
    const url = `${rpc}${path}`;
    try {
      const data = await fetchJson(url, timeoutMs);
      if (data?.error) {
        throw new Error(data?.error?.message || JSON.stringify(data.error));
      }
      const txs = data?.result?.txs;
      if (requireNonEmptyTxs && Array.isArray(txs) && txs.length === 0) {
        rpcStats[rpc].empty++;
        if (!firstEmpty) firstEmpty = { data, rpc };
        continue;
      }
      rpcStats[rpc].ok++;
      return { data, rpc };
    } catch (e) {
      rpcStats[rpc].fail++;
      rpcStats[rpc].last_error = String(e?.message || e);
      lastErr = e;
    }
  }

  if (firstEmpty) return firstEmpty;
  throw lastErr || new Error(`All RPCs failed for path: ${path}`);
}

async function ensureOutDir() {
  await fs.mkdir("data", { recursive: true });
}

async function readPrev() {
  try {
    const txt = await fs.readFile(OUT_FILE, "utf8");
    return JSON.parse(txt);
  } catch {
    return null;
  }
}

function getAttr(event, key) {
  const attrs = event?.attributes ?? [];
  for (const a of attrs) if (a?.key === key) return a?.value;
  return null;
}

function parseAmountAtom(amountStr) {
  if (!amountStr || typeof amountStr !== "string") return null;
  const m = amountStr.match(/^(\d+)\s*uatom$/i);
  if (!m) return null;
  return Number(m[1]) / 1_000_000;
}

function toTsMs(iso) {
  if (!iso) return null;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

function normalizeItem(it) {
  return {
    amount_atom: Number(it.amount_atom),
    delegator: it.delegator || null,
    validator: it.validator || null,
    height: Number(it.height || 0),
    txhash: it.txhash,
    timestamp: it.timestamp || null,
  };
}

function uniqueByTxhash(items) {
  const seen = new Set();
  const out = [];
  for (const it of items) {
    const h = it?.txhash;
    if (!h || seen.has(h)) continue;
    seen.add(h);
    out.push(it);
  }
  return out;
}

function sortByBiggest(items) {
  return items.slice().sort((a, b) => {
    const da = Number(a.amount_atom || 0);
    const db = Number(b.amount_atom || 0);
    if (db !== da) return db - da;

    const ta = toTsMs(a.timestamp) ?? 0;
    const tb = toTsMs(b.timestamp) ?? 0;
    if (tb !== ta) return tb - ta;

    return (Number(b.height || 0) - Number(a.height || 0));
  });
}

function sortByNewest(items) {
  return items.slice().sort((a, b) => {
    const ta = toTsMs(a.timestamp);
    const tb = toTsMs(b.timestamp);

    if (ta != null && tb != null) return tb - ta;
    if (ta != null && tb == null) return -1;
    if (ta == null && tb != null) return 1;

    return (Number(b.height || 0) - Number(a.height || 0));
  });
}

async function fetchTimestampByTxhash(txhash) {
  if (!txhash) return null;
  try {
    const j = await fetchJson(`${REST_BASE}/cosmos/tx/v1beta1/txs/${txhash}`);
    const ts = j?.tx_response?.timestamp || j?.timestamp || null;
    if (!ts) return null;
    const ms = Date.parse(ts);
    return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
  } catch {
    return null;
  }
}

async function backfillMissingTimestamps(items) {
  const out = items.map(normalizeItem);
  const missing = out.filter((it) => !it.timestamp && it.txhash);
  if (!missing.length) return out;

  console.log(`⏱️  Backfilling ${missing.length} missing timestamps from tx hashes...`);
  let filled = 0;
  for (const it of missing) {
    const ts = await fetchTimestampByTxhash(it.txhash);
    if (ts) {
      it.timestamp = ts;
      filled++;
    }
    await sleep(120);
  }
  console.log(`  ✅ Backfilled ${filled}/${missing.length} timestamps`);
  return out;
}

async function fetchRecentDelegations(opts = {}) {
  const cutoffHeight = Number(opts.cutoffHeight || 0);
  const knownTxHashes = opts.knownTxHashes instanceof Set ? opts.knownTxHashes : new Set();
  const statusResults = await Promise.allSettled(
    RPC_BASES.map((rpc) => fetchJson(`${rpc}/status`, 10000).then((data) => ({ rpc, data })))
  );
  const healthyStatus = statusResults
    .filter((r) => r.status === "fulfilled" && !r.value?.data?.error)
    .map((r) => r.value);
  if (!healthyStatus.length) {
    throw new Error(`No healthy /status response from RPC_BASES`);
  }
  const bestStatus = healthyStatus
    .map((s) => ({
      rpc: s.rpc,
      latestTime: s.data?.result?.sync_info?.latest_block_time,
      latestHeight: Number(s.data?.result?.sync_info?.latest_block_height || 0),
    }))
    .filter((s) => s.latestTime)
    .sort((a, b) => b.latestHeight - a.latestHeight)[0];

  const nowMs = Date.parse(bestStatus.latestTime);
  const cutoffMs = nowMs - WINDOW_HOURS * 60 * 60 * 1000;

  const items = [];
  const seen = new Set();
  let pagesScanned = 0;
  let stoppedEarly = false;

  for (let page = 1; page <= LIMIT_PAGES; page++) {
    if (page > 1) await sleep(350);

    const params = new URLSearchParams();
    params.set("query", JSON.stringify(EVENT_QUERY));
    params.set("prove", "false");
    params.set("page", String(page));
    params.set("per_page", String(PER_PAGE));
    params.set("order_by", JSON.stringify("desc"));

    const path = `/tx_search?${params.toString()}`;

    let data;
    let usedRpc = null;
    try {
      const res = await fetchJsonFromRpcPath(path, 15000, true);
      data = res.data;
      usedRpc = res.rpc;
    } catch (e) {
      const msg = String(e?.message || e);
      if (msg.includes("page should be within")) break;
      throw e;
    }

    const txs = data?.result?.txs ?? [];
    if (!txs.length) break;
    pagesScanned++;

    const txsForParse = cutoffHeight > 0
      ? txs.filter((tx) => {
          const h = Number(tx?.height || 0);
          const hash = tx?.hash || "";
          return h > cutoffHeight || (h === cutoffHeight && hash && !knownTxHashes.has(hash));
        })
      : txs;

    if (cutoffHeight > 0 && txsForParse.length === 0) {
      console.log(`  Page ${page} via ${usedRpc}: all txs at/under cutoff (h=${cutoffHeight}), stopping early`);
      stoppedEarly = true;
      break;
    }

    for (const tx of txsForParse) {
      const txhash = tx?.hash;
      const height = Number(tx?.height ?? 0);
      if (!txhash || !height) continue;
      if (seen.has(txhash)) continue;
      seen.add(txhash);

      const tsStr = tx?.timestamp || tx?.tx_result?.timestamp || null;
      const tsMs = tsStr ? Date.parse(tsStr) : null;
      if (tsMs && tsMs < cutoffMs) {
        page = LIMIT_PAGES + 1;
        break;
      }

      const events = tx?.tx_result?.events ?? [];
      let delegator = null;
      let validator = null;
      let amount_atom = null;

      for (const ev of events) {
        if (ev?.type === "delegate") {
          delegator = delegator || getAttr(ev, "delegator") || getAttr(ev, "delegator_address");
          validator = validator || getAttr(ev, "validator") || getAttr(ev, "validator_address");
          if (!amount_atom) amount_atom = parseAmountAtom(getAttr(ev, "amount"));
        }
      }

      if (!amount_atom || amount_atom < MIN_ATOM) continue;

      items.push(
        normalizeItem({
          amount_atom,
          delegator,
          validator,
          height,
          txhash,
          timestamp: tsMs ? new Date(tsMs).toISOString() : null,
        })
      );
    }
    console.log(`  Page ${page} via ${usedRpc}: ${txs.length} txs (${txsForParse.length} new) scanned`);
  }

  return {
    now_iso: new Date().toISOString(),
    status_rpc: bestStatus.rpc,
    items: uniqueByTxhash(items),
    pages_scanned: pagesScanned,
    incremental: {
      enabled: cutoffHeight > 0,
      cutoff_height: cutoffHeight || null,
      stopped_early: stoppedEarly,
    },
  };
}

function mergeTopByAmount(prevList, newItems, filterFn, keepN) {
  const prev = Array.isArray(prevList) ? prevList.map(normalizeItem) : [];
  const incoming = newItems.filter(filterFn).map(normalizeItem);

  const merged = uniqueByTxhash([...incoming, ...prev]);

  return sortByBiggest(merged).slice(0, keepN);
}

function buildTicker(freshItems, whalesTop, midsTop) {
  const combined = uniqueByTxhash([
    ...(freshItems || []),
    ...(whalesTop || []),
    ...(midsTop || []),
  ]);

  return sortByNewest(combined).slice(0, TICKER_KEEP);
}

async function main() {
  await ensureOutDir();

  const prev = await readPrev();
  const prevWhales = prev?.whales_top || prev?.featured?.whales_top || [];
  const prevMids = prev?.mids_top || prev?.featured?.mids_top || [];
  const prevFresh = Array.isArray(prev?.fresh) ? prev.fresh : [];
  const prevMaxHeight = prevFresh.reduce((m, i) => Math.max(m, Number(i?.height || 0)), 0);
  const prevTxHashes = new Set(prevFresh.map((i) => i?.txhash).filter(Boolean));

  try {
    const {
      items: freshItems,
      status_rpc,
      pages_scanned,
      incremental,
    } = await fetchRecentDelegations({
      cutoffHeight: prevMaxHeight,
      knownTxHashes: prevTxHashes,
    });

    const whales_top = mergeTopByAmount(
      prevWhales,
      freshItems,
      (it) => it.amount_atom >= WHALE_MIN,
      WHALES_KEEP
    );
    const whales_top_with_timestamps = await backfillMissingTimestamps(whales_top);

    const mids_top = mergeTopByAmount(
      prevMids,
      freshItems,
      (it) => it.amount_atom >= MIDS_MIN && it.amount_atom <= MIDS_MAX,
      MIDS_KEEP
    );

    const ticker = buildTicker(freshItems, whales_top_with_timestamps, mids_top);

    const out = {
      generated_at: new Date().toISOString(),
      window_hours: WINDOW_HOURS,
      min_atom: MIN_ATOM,
      source: {
        rpc_bases: RPC_BASES,
        status_rpc,
        query: EVENT_QUERY,
        note: "fresh items are recent; whales_top/mids_top are persistent top-by-amount sets",
      },
      ingestion_health: {
        pages: LIMIT_PAGES,
        pages_scanned,
        per_page: PER_PAGE,
        window_hours: WINDOW_HOURS,
        incremental,
        rpc_stats: rpcStats,
      },

      // Persistent leaderboards
      whales_top: whales_top_with_timestamps,
      mids_top,

      // Fresh feeds for UI
      fresh: sortByNewest(freshItems).slice(0, 200),
      ticker,
    };

    // Write main file
    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));

    // Write recent-delegations.json
    await fs.writeFile(
      "data/recent-delegations.json",
      JSON.stringify(
        {
          updated_at: new Date().toISOString(),
          window_hours: WINDOW_HOURS,
          delegations: out.fresh || []
        },
        null,
        2
      )
    );

    // Write top-delegations.json
    await fs.writeFile(
      "data/top-delegations.json",
      JSON.stringify(
        {
          updated_at: new Date().toISOString(),
          threshold_atom: WHALE_MIN,
          delegations: whales_top_with_timestamps || []
        },
        null,
        2
      )
    );

    console.log(`✅ Wrote snapshot: whales=${whales_top_with_timestamps.length}, mids=${mids_top.length}, ticker=${ticker.length}, fresh=${freshItems.length}`);
  } catch (e) {
    // Preserve previous top lists even on failure
    const out = {
      generated_at: new Date().toISOString(),
      window_hours: WINDOW_HOURS,
      min_atom: MIN_ATOM,
      source: { rpc_bases: RPC_BASES, query: EVENT_QUERY },
      whales_top: Array.isArray(prevWhales) ? prevWhales : [],
      mids_top: Array.isArray(prevMids) ? prevMids : [],
      fresh: [],
      ticker: [],
      error: String(e?.message || e),
    };

    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
    console.error("❌ Snapshot failed:", out.error);
    process.exit(1);
  }
}

main();
