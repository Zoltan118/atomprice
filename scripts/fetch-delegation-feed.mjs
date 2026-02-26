// scripts/fetch-delegation-feed.mjs
// Fetches recent MsgDelegate + MsgUndelegate txs from Cosmos Hub
// Resolves validator monikers, includes timestamps
// Outputs: data/delegation_feed.json
// Also outputs:
//   - data/delegation-events-raw.json (event-level raw ledger; append-only by default)
//   - data/delegation-flow-hourly.json (hour buckets)
//   - data/delegation-flow-daily.json (day buckets)
//
// Env (optional):
//   RPC_BASES     comma-separated RPCs (preferred)
//   RPC_BASE      single RPC fallback if RPC_BASES not set
//   REST_BASE     default: https://rest.cosmos.directory/cosmoshub
//   FEED_MIN      default: 1000 (minimum ATOM to include)
//   FEED_KEEP     default: 1000 (max items to keep in feed)
//   WHALE_FEED_MIN default: 50000 (always keep whale rows in feed)
//   WHALE_FEED_DAYS default: 30 (rolling whale retention window)
//   PER_PAGE      default: 100
//   LIMIT_PAGES   default: 5
//   EXEC_LIMIT_PAGES default: 2 (MsgExec scans)
//   RAW_KEEP_DAYS default: 90 (used only when RAW_IMMUTABLE=false)
//   RAW_IMMUTABLE default: true (append-only; no trimming)
//   HOURLY_KEEP_DAYS default: 370
//   INCREMENTAL  default: true (set false for deep historical backfill)

import fs from "node:fs/promises";

const OUT_FILE = "data/delegation_feed.json";
const RAW_FILE = "data/delegation-events-raw.json";
const HOURLY_FILE = "data/delegation-flow-hourly.json";
const DAILY_FILE = "data/delegation-flow-daily.json";

const RPC_BASES = (process.env.RPC_BASES
  ? process.env.RPC_BASES.split(",")
  : [process.env.RPC_BASE || "https://rpc.silknodes.io/cosmos"])
  .map((s) => s.trim().replace(/\/+$/, ""))
  .filter(Boolean);
const REST_BASE = (process.env.REST_BASE || "https://rest.cosmos.directory/cosmoshub").replace(/\/+$/, "");
const FEED_MIN = Number(process.env.FEED_MIN ?? "1000");
const FEED_KEEP = Number(process.env.FEED_KEEP ?? "1000");
const WHALE_FEED_MIN = Number(process.env.WHALE_FEED_MIN ?? "50000");
const WHALE_FEED_DAYS = Number(process.env.WHALE_FEED_DAYS ?? "30");
const PER_PAGE = Number(process.env.PER_PAGE ?? "100");
const LIMIT_PAGES = Number(process.env.LIMIT_PAGES ?? "5");
const EXEC_LIMIT_PAGES = Number(process.env.EXEC_LIMIT_PAGES ?? "2");
const RAW_KEEP_DAYS = Number(process.env.RAW_KEEP_DAYS ?? "90");
const HOURLY_KEEP_DAYS = Number(process.env.HOURLY_KEEP_DAYS ?? "370");
const INCREMENTAL = String(process.env.INCREMENTAL ?? "true").toLowerCase() !== "false";
const RAW_IMMUTABLE = String(process.env.RAW_IMMUTABLE ?? "true").toLowerCase() !== "false";

const MSG_DELEGATE = "/cosmos.staking.v1beta1.MsgDelegate";
const MSG_UNDELEGATE = "/cosmos.staking.v1beta1.MsgUndelegate";
const MSG_EXEC = "/cosmos.authz.v1beta1.MsgExec";

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

// ── Validator moniker cache ──
const validatorCache = {};

async function loadValidatorCache() {
  try {
    const txt = await fs.readFile("data/validator_cache.json", "utf8");
    const d = JSON.parse(txt);
    Object.assign(validatorCache, d);
    console.log(`📦 Loaded ${Object.keys(d).length} cached validators`);
  } catch { /* no cache yet */ }
}

async function saveValidatorCache() {
  await fs.writeFile("data/validator_cache.json", JSON.stringify(validatorCache, null, 2));
}

async function resolveMoniker(valoperAddr) {
  if (!valoperAddr) return "";
  if (validatorCache[valoperAddr]) return validatorCache[valoperAddr];
  try {
    const d = await fetchJson(`${REST_BASE}/cosmos/staking/v1beta1/validators/${valoperAddr}`);
    const moniker = d?.validator?.description?.moniker || "";
    if (moniker) {
      validatorCache[valoperAddr] = moniker;
      return moniker;
    }
  } catch (e) {
    console.log(`  ⚠️ Could not resolve ${valoperAddr.slice(0, 24)}: ${e.message}`);
  }
  return "";
}

function toIsoHour(iso) {
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return null;
  d.setUTCMinutes(0, 0, 0);
  return d.toISOString();
}

function toIsoDay(iso) {
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

// ── Parse events ──
function getAttr(event, key) {
  for (const a of event?.attributes ?? []) {
    if (a?.key === key) return a?.value;
  }
  return null;
}

function parseUatom(str) {
  if (!str || typeof str !== "string") return 0;
  const m = str.match(/^(\d+)\s*uatom$/i);
  return m ? Number(m[1]) / 1_000_000 : 0;
}

function makeEventId(item) {
  const atom = Number(item?.amount_atom || 0);
  const atomKey = Number.isFinite(atom) ? atom.toFixed(6) : "0";
  return [
    item?.txhash || "",
    item?.type || "",
    item?.delegator || "",
    item?.validator_addr || "",
    atomKey,
    String(item?.height || "")
  ].join(":");
}

function parseTxEvents(txs, actionType) {
  const eventType = actionType === "delegate" ? "delegate" : "unbond";
  const items = [];

  for (const tx of txs) {
    const txhash = tx?.hash;
    const height = Number(tx?.height ?? 0);
    if (!txhash || !height) continue;

    for (const ev of tx?.tx_result?.events ?? []) {
      if (ev?.type !== eventType) continue;

      const amount = parseUatom(getAttr(ev, "amount"));
      const validator = getAttr(ev, "validator") || "";
      const delegator = getAttr(ev, "delegator") || "";

      if (amount < FEED_MIN) continue;

      items.push({
        type: actionType,
        amount_atom: amount,
        delegator,
        validator_addr: validator,
        validator_name: "",
        height,
        txhash,
        timestamp: null,
      });
    }
  }

  return items;
}

// ── Fetch txs from RPC ──
async function fetchTxsByAction(msgAction, actionType, opts = {}) {
  const cutoffHeight = Number(opts.cutoffHeight || 0);
  const limitPages = Number(opts.limitPages || LIMIT_PAGES);
  const knownTxHashes = opts.knownTxHashes instanceof Set ? opts.knownTxHashes : new Set();
  const query = `message.action='${msgAction}'`;
  const allItems = [];
  const seen = new Set();
  let pagesScanned = 0;
  let stoppedEarly = false;

  for (let page = 1; page <= limitPages; page++) {
    if (page > 1) await sleep(400);

    const params = new URLSearchParams();
    params.set("query", JSON.stringify(query));
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
      if (String(e?.message || "").includes("page should be within")) break;
      throw e;
    }

    const txs = data?.result?.txs ?? [];
    if (!txs.length) break;
    pagesScanned++;

    // Incremental mode: only keep txs above cutoff or unknown at cutoff height.
    // When a page is fully known/old, stop scanning further pages.
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

    for (const tx of txs) {
      if (!tx?.hash || seen.has(tx.hash)) continue;
      seen.add(tx.hash);
    }

    // Parse events from this page
    const parsed = parseTxEvents(txsForParse, actionType);
    allItems.push(...parsed);

    console.log(`  Page ${page} via ${usedRpc}: ${txs.length} txs (${txsForParse.length} new), ${parsed.length} qualifying ${actionType}s`);
  }

  return { items: allItems, pagesScanned, stoppedEarly };
}

// ── Resolve block timestamps ──
async function resolveTimestamps(items) {
  const heightsToResolve = [...new Set(items.filter(i => !i.timestamp).map(i => i.height))];
  console.log(`⏱️  Resolving ${heightsToResolve.length} block timestamps...`);

  const heightToTime = {};

  // Batch resolve — do 5 at a time
  for (let i = 0; i < heightsToResolve.length; i += 5) {
    const batch = heightsToResolve.slice(i, i + 5);
    const results = await Promise.allSettled(
      batch.map(async (h) => {
        const { data: d } = await fetchJsonFromRpcPath(`/block?height=${h}`, 10000, false);
        const time = d?.result?.block?.header?.time;
        if (time) heightToTime[h] = new Date(time).toISOString();
      })
    );
    if (i + 5 < heightsToResolve.length) await sleep(300);
  }

  // Apply timestamps
  for (const item of items) {
    if (!item.timestamp && heightToTime[item.height]) {
      item.timestamp = heightToTime[item.height];
    }
  }

  console.log(`  ✅ Resolved ${Object.keys(heightToTime).length} timestamps`);
}

function aggregateByTime(items, keyFn, includeWindowDays = null) {
  const nowMs = Date.now();
  const cutoffMs = includeWindowDays ? nowMs - includeWindowDays * 86400000 : null;
  const buckets = new Map();

  for (const item of items) {
    if (!item?.timestamp) continue;
    const tsMs = Date.parse(item.timestamp);
    if (!Number.isFinite(tsMs)) continue;
    if (cutoffMs && tsMs < cutoffMs) continue;
    const key = keyFn(item.timestamp);
    if (!key) continue;
    if (!buckets.has(key)) {
      buckets.set(key, {
        key,
        delegate_atom: 0,
        undelegate_atom: 0,
        net_atom: 0,
        delegates_count: 0,
        undelegates_count: 0,
        total_count: 0,
      });
    }
    const b = buckets.get(key);
    const amt = Number(item.amount_atom || 0);
    if (!Number.isFinite(amt) || amt <= 0) continue;
    if (item.type === "delegate") {
      b.delegate_atom += amt;
      b.delegates_count += 1;
      b.net_atom += amt;
    } else if (item.type === "undelegate") {
      b.undelegate_atom += amt;
      b.undelegates_count += 1;
      b.net_atom -= amt;
    }
    b.total_count += 1;
  }

  return Array.from(buckets.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));
}

// ── Main ──
async function main() {
  await fs.mkdir("data", { recursive: true });
  await loadValidatorCache();

  // Load previous feed for merging
  let prevItems = [];
  try {
    const txt = await fs.readFile(OUT_FILE, "utf8");
    const prev = JSON.parse(txt);
    prevItems = prev?.items || [];
    console.log(`📦 Previous feed: ${prevItems.length} items`);
  } catch { /* fresh start */ }
  const prevMaxHeight = prevItems.reduce((m, i) => Math.max(m, Number(i?.height || 0)), 0);
  const prevTxHashes = new Set(prevItems.map((i) => i?.txhash).filter(Boolean));
  const cutoffHeight = INCREMENTAL ? Math.max(0, prevMaxHeight - 100) : 0;
  const knownTxHashes = INCREMENTAL ? prevTxHashes : new Set();

  let prevRawItems = [];
  try {
    const txt = await fs.readFile(RAW_FILE, "utf8");
    prevRawItems = JSON.parse(txt)?.items || [];
    console.log(`📦 Previous raw archive: ${prevRawItems.length} items`);
  } catch { /* fresh start */ }

  let prevDaily = [];
  try {
    const txt = await fs.readFile(DAILY_FILE, "utf8");
    prevDaily = JSON.parse(txt)?.items || [];
  } catch { /* fresh start */ }

  // Fetch delegates and undelegates
  console.log(`\n📥 Fetching MsgDelegate (min ${FEED_MIN} ATOM)...`);
  const delegateRes = await fetchTxsByAction(MSG_DELEGATE, "delegate", { cutoffHeight, knownTxHashes });
  console.log(`  → ${delegateRes.items.length} delegates (${delegateRes.pagesScanned} pages${delegateRes.stoppedEarly ? ", stopped early" : ""})`);

  console.log(`📥 Fetching MsgExec for delegated events...`);
  const execDelegateRes = await fetchTxsByAction(MSG_EXEC, "delegate", { cutoffHeight, knownTxHashes, limitPages: EXEC_LIMIT_PAGES });
  const delegates = [...delegateRes.items, ...execDelegateRes.items];
  console.log(`  → +${execDelegateRes.items.length} delegates via MsgExec (${execDelegateRes.pagesScanned} pages${execDelegateRes.stoppedEarly ? ", stopped early" : ""})`);
  console.log(`  → ${delegates.length} total delegates\n`);

  console.log(`📥 Fetching MsgUndelegate (min ${FEED_MIN} ATOM)...`);
  const undelegateRes = await fetchTxsByAction(MSG_UNDELEGATE, "undelegate", { cutoffHeight, knownTxHashes });
  console.log(`  → ${undelegateRes.items.length} undelegates (${undelegateRes.pagesScanned} pages${undelegateRes.stoppedEarly ? ", stopped early" : ""})`);

  console.log(`📥 Fetching MsgExec for undelegated events...`);
  const execUndelegateRes = await fetchTxsByAction(MSG_EXEC, "undelegate", { cutoffHeight, knownTxHashes, limitPages: EXEC_LIMIT_PAGES });
  const undelegates = [...undelegateRes.items, ...execUndelegateRes.items];
  console.log(`  → +${execUndelegateRes.items.length} undelegates via MsgExec (${execUndelegateRes.pagesScanned} pages${execUndelegateRes.stoppedEarly ? ", stopped early" : ""})`);
  console.log(`  → ${undelegates.length} total undelegates\n`);

  const freshItems = [...delegates, ...undelegates];
  await resolveTimestamps(freshItems);

  // Merge with previous feed, dedupe by stable event identity
  const seenKeys = new Set();
  const merged = [];
  for (const item of [...freshItems, ...prevItems]) {
    const key = makeEventId(item);
    if (seenKeys.has(key)) continue;
    seenKeys.add(key);
    merged.push(item);
  }

  // Sort by height descending (newest first)
  merged.sort((a, b) => Number(b.height) - Number(a.height));

  // Resolve timestamps for any merged rows still missing them
  const mergedMissingTs = merged.filter((i) => !i.timestamp);
  if (mergedMissingTs.length) {
    await resolveTimestamps(mergedMissingTs);
  }

  // Build raw ledger
  // - Immutable mode: append-only, never trims previous rows
  // - Legacy mode: rolling retention by RAW_KEEP_DAYS
  const prevRawIds = new Set(prevRawItems.map(makeEventId));
  const newRawItems = freshItems.filter((i) => !prevRawIds.has(makeEventId(i)));
  await resolveTimestamps(newRawItems);
  let rawArchive = [...prevRawItems, ...newRawItems];

  if (!RAW_IMMUTABLE) {
    const rawCutoffIso = new Date(Date.now() - RAW_KEEP_DAYS * 86400000).toISOString();
    rawArchive = rawArchive.filter((i) => !i.timestamp || i.timestamp >= rawCutoffIso);
  }

  rawArchive.sort((a, b) => {
    const ta = Date.parse(a.timestamp || 0) || 0;
    const tb = Date.parse(b.timestamp || 0) || 0;
    if (tb !== ta) return tb - ta;
    return Number(b.height || 0) - Number(a.height || 0);
  });

  if (RAW_IMMUTABLE && prevRawItems.length && rawArchive.length < prevRawItems.length) {
    throw new Error(`Raw ledger shrink blocked in immutable mode (${prevRawItems.length} -> ${rawArchive.length})`);
  }

  // Build feed set:
  // 1) recent live window capped by FEED_KEEP
  // 2) always include whale events (>= WHALE_FEED_MIN) from last WHALE_FEED_DAYS
  const recentFeed = merged.slice(0, FEED_KEEP);
  const whaleCutoffIso = new Date(Date.now() - WHALE_FEED_DAYS * 86400000).toISOString();
  const whaleFeed = rawArchive.filter((i) =>
    Number(i.amount_atom || 0) >= WHALE_FEED_MIN &&
    (!i.timestamp || i.timestamp >= whaleCutoffIso)
  );

  const feedById = new Map();
  for (const item of [...recentFeed, ...whaleFeed]) {
    feedById.set(makeEventId(item), item);
  }
  const feed = Array.from(feedById.values()).sort((a, b) => {
    const ta = Date.parse(a.timestamp || 0) || 0;
    const tb = Date.parse(b.timestamp || 0) || 0;
    if (tb !== ta) return tb - ta;
    return Number(b.height || 0) - Number(a.height || 0);
  });

  // Resolve validator monikers
  const unknownAddrs = [...new Set(feed.filter(i => i.validator_addr && !validatorCache[i.validator_addr]).map(i => i.validator_addr))];
  console.log(`\n🏷️  Resolving ${unknownAddrs.length} validator monikers...`);
  for (let i = 0; i < unknownAddrs.length; i += 5) {
    const batch = unknownAddrs.slice(i, i + 5);
    await Promise.allSettled(batch.map(addr => resolveMoniker(addr)));
    if (i + 5 < unknownAddrs.length) await sleep(300);
  }

  // Apply resolved names
  for (const item of feed) {
    if (item.validator_addr && validatorCache[item.validator_addr]) {
      item.validator_name = validatorCache[item.validator_addr];
    }
  }

  // Count stats
  const dCount = feed.filter(i => i.type === "delegate").length;
  const uCount = feed.filter(i => i.type === "undelegate").length;

  const output = {
    generated_at: new Date().toISOString(),
    min_atom: FEED_MIN,
    total: feed.length,
    delegates: dCount,
    undelegates: uCount,
    ingestion_health: {
      rpc_bases: RPC_BASES,
      pages: LIMIT_PAGES,
      exec_pages: EXEC_LIMIT_PAGES,
      pages_scanned: {
        delegate: delegateRes.pagesScanned,
        undelegate: undelegateRes.pagesScanned,
        exec_delegate: execDelegateRes.pagesScanned,
        exec_undelegate: execUndelegateRes.pagesScanned,
      },
      incremental: {
        enabled: INCREMENTAL && cutoffHeight > 0,
        cutoff_height: cutoffHeight || null,
        stopped_early: {
          delegate: delegateRes.stoppedEarly,
          undelegate: undelegateRes.stoppedEarly,
          exec_delegate: execDelegateRes.stoppedEarly,
          exec_undelegate: execUndelegateRes.stoppedEarly,
        }
      },
      per_page: PER_PAGE,
      feed_keep: FEED_KEEP,
      whale_feed_min: WHALE_FEED_MIN,
      whale_feed_days: WHALE_FEED_DAYS,
      raw_keep_days: RAW_IMMUTABLE ? null : RAW_KEEP_DAYS,
      raw_immutable: RAW_IMMUTABLE,
      raw_appended_in_run: newRawItems.length,
      rpc_stats: rpcStats,
    },
    items: feed,
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(output, null, 2));

  await fs.writeFile(
    RAW_FILE,
    JSON.stringify({
      generated_at: new Date().toISOString(),
      timezone: "UTC",
      min_atom: FEED_MIN,
      retention_days: RAW_IMMUTABLE ? null : RAW_KEEP_DAYS,
      immutable: RAW_IMMUTABLE,
      appended_in_run: newRawItems.length,
      total: rawArchive.length,
      items: rawArchive,
    }, null, 2)
  );

  const hourly = aggregateByTime(rawArchive, toIsoHour, HOURLY_KEEP_DAYS);
  await fs.writeFile(
    HOURLY_FILE,
    JSON.stringify({
      generated_at: new Date().toISOString(),
      timezone: "UTC",
      source: "delegation-events-raw",
      retention_days: HOURLY_KEEP_DAYS,
      total: hourly.length,
      items: hourly,
    }, null, 2)
  );

  const dailyFresh = aggregateByTime(rawArchive, toIsoDay, null);
  const dailyMap = new Map();
  for (const d of prevDaily) {
    if (d?.key) dailyMap.set(d.key, d);
  }
  for (const d of dailyFresh) {
    dailyMap.set(d.key, d);
  }
  const daily = Array.from(dailyMap.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));
  await fs.writeFile(
    DAILY_FILE,
    JSON.stringify({
      generated_at: new Date().toISOString(),
      timezone: "UTC",
      source: "delegation-events-raw",
      total: daily.length,
      items: daily,
    }, null, 2)
  );

  await saveValidatorCache();

  console.log(`\n✅ Feed saved: ${dCount} delegates + ${uCount} undelegates = ${feed.length} total`);
  console.log(`✅ Raw archive: ${rawArchive.length} events${RAW_IMMUTABLE ? " (immutable)" : ` (${RAW_KEEP_DAYS}d)`}`);
  console.log(`✅ Hourly buckets: ${hourly.length}`);
  console.log(`✅ Daily buckets: ${daily.length}`);
  console.log(`   Output: ${OUT_FILE}`);

  // ── Whale events accumulation (≥50K ATOM individual transactions) ──
  const WHALE_MIN = Number(process.env.WHALE_EVENT_MIN ?? "50000");
  const WHALE_FILE = "data/whale-events.json";

  // Load previous whale events for merging
  let prevWhaleEvents = [];
  try {
    const txt = await fs.readFile(WHALE_FILE, "utf8");
    prevWhaleEvents = JSON.parse(txt)?.events || [];
  } catch { /* fresh start */ }

  // Filter from raw archive so multi-event txs are preserved for markers.
  // Keep height for timestamp resolution, then strip it
  const freshWhales = rawArchive
    .filter(i => i.amount_atom >= WHALE_MIN)
    .map(i => ({
      type: i.type,
      atom: Math.round(i.amount_atom),
      timestamp: i.timestamp,
      height: i.height,
      txhash: i.txhash,
      validator_name: i.validator_name || (i.validator_addr ? (validatorCache[i.validator_addr] || "") : ""),
      validator_addr: i.validator_addr || "",
      delegator: i.delegator || ""
    }));

  // Resolve timestamps for whale events missing them
  const whalesNeedTime = freshWhales.filter(w => !w.timestamp && w.height);
  if (whalesNeedTime.length) {
    console.log(`\n🐋 Resolving ${whalesNeedTime.length} whale event timestamps...`);
    await resolveTimestamps(whalesNeedTime);
    // Copy resolved timestamps back
    for (const w of whalesNeedTime) {
      const match = freshWhales.find(f => f.txhash === w.txhash);
      if (match && w.timestamp) match.timestamp = w.timestamp;
    }
  }

  // Merge with previous, dedupe by event identity (not just txhash)
  const seenWhale = new Set();
  const allWhales = [];
  for (const w of [...freshWhales, ...prevWhaleEvents]) {
    const key = [
      w.txhash || '',
      w.type || '',
      String(Math.round(Number(w.atom || 0))),
      String(w.delegator || ''),
      String(w.validator_addr || w.delegator || '')
    ].join(':');
    if (!key || seenWhale.has(key)) continue;
    seenWhale.add(key);
    allWhales.push(w);
  }

  // Prune events older than 365 days
  const yearAgo = new Date(Date.now() - 365 * 86400000).toISOString();
  const prunedWhales = allWhales
    .filter(w => !w.timestamp || w.timestamp >= yearAgo)
    .map(({ height, ...rest }) => rest); // strip height from output

  // Sort by timestamp descending
  prunedWhales.sort((a, b) => (b.timestamp || "").localeCompare(a.timestamp || ""));

  await fs.writeFile(WHALE_FILE, JSON.stringify({
    generated_at: new Date().toISOString(),
    whale_min_atom: WHALE_MIN,
    total: prunedWhales.length,
    events: prunedWhales,
  }, null, 2));

  console.log(`🐋 Whale events: ${prunedWhales.length} events ≥${WHALE_MIN.toLocaleString()} ATOM → ${WHALE_FILE}`);
}

main();
