/**
 * Pull recent MsgDelegate txs via Tendermint RPC `tx_search`,
 * filter to the last WINDOW_HOURS, store > MIN_ATOM to data/delegations_24h.json
 *
 * Output format:
 * {
 *   generated_at, window_hours, min_atom,
 *   source: { rpc, query },
 *   items: [{ amount_atom, delegator, validator, height, txhash, timestamp }]
 * }
 */

import fs from "node:fs/promises";

const OUT_FILE = "data/delegations_24h.json";

const MIN_ATOM = Number(process.env.MIN_ATOM ?? "1");
const WINDOW_HOURS = Number(process.env.WINDOW_HOURS ?? "24");
const LIMIT_PAGES = Number(process.env.LIMIT_PAGES ?? "6");
const PER_PAGE = Number(process.env.PER_PAGE ?? "100");

const PRIMARY_RPC = (process.env.RPC_BASE ?? "").trim();

// Fallback public RPCs (no keys). If one is down/rate-limited, next one is tried.
// These are examples of public Cosmos Hub RPC endpoints. :contentReference[oaicite:0]{index=0}
const FALLBACK_RPCS = [
  PRIMARY_RPC,
  "https://cosmos-rpc.publicnode.com",
  "https://cosmos-rpc.polkachu.com",
  "https://cosmos.blockpi.network/rpc/v1/public",
  "https://cosmoshub-mainnet-rpc.itrocket.net",
  "https://rpc.cosmos.nodestake.org",
  "https://cosmoshub-rpc.stakely.io",
  "https://cosmoshub.rpc.stakin-nodes.com",
].filter(Boolean);

// Cosmos SDK MsgDelegate type URL used in events.
const MSG_DELEGATE = "/cosmos.staking.v1beta1.MsgDelegate";

// Tendermint event query (NO surrounding quotes in the query param)
const EVENT_QUERY = `message.action='${MSG_DELEGATE}'`;

function withTimeout(ms) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  return { ac, cancel: () => clearTimeout(t) };
}

async function fetchJson(url, timeoutMs = 15000) {
  const { ac, cancel } = withTimeout(timeoutMs);
  try {
    const res = await fetch(url, { signal: ac.signal, headers: { "accept": "application/json" } });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = null; }

    if (!res.ok) {
      const msg = json?.error?.data || json?.error?.message || text?.slice(0, 200);
      throw new Error(`HTTP ${res.status} for ${url}${msg ? ` :: ${msg}` : ""}`);
    }
    return json;
  } finally {
    cancel();
  }
}

function normalizeRpcBase(rpcBase) {
  // Some providers end with '/', some not
  return rpcBase.replace(/\/+$/, "");
}

function getAttr(event, key) {
  // Tendermint returns either {attributes:[{key,value}]} or legacy {attributes:[{key,value}]} with base64 sometimes.
  const attrs = event?.attributes ?? [];
  for (const a of attrs) {
    if (a?.key === key) return a?.value;
  }
  return null;
}

function parseAmountAtom(amountStr) {
  // Most chains emit amount in uatom form: "12345uatom"
  // Sometimes it can be "12345" + denom elsewhere. We only accept uatom.
  if (!amountStr || typeof amountStr !== "string") return null;

  const m = amountStr.match(/^(\d+)\s*uatom$/i);
  if (!m) return null;

  const uatom = BigInt(m[1]);
  // 1 ATOM = 1_000_000 uatom
  const atom = Number(uatom) / 1_000_000;
  return atom;
}

async function ensureOutDir() {
  await fs.mkdir("data", { recursive: true });
}

async function writeSnapshot(payload) {
  await ensureOutDir();
  await fs.writeFile(OUT_FILE, JSON.stringify(payload, null, 2));
}

async function tryRpc(rpcBase) {
  const base = normalizeRpcBase(rpcBase);

  // 1) get latest block time (for window cut)
  const status = await fetchJson(`${base}/status`);
  const latestHeight = Number(status?.result?.sync_info?.latest_block_height ?? 0);
  const latestTime = status?.result?.sync_info?.latest_block_time;
  if (!latestHeight || !latestTime) throw new Error(`Bad /status response from ${base}`);

  const nowMs = Date.parse(latestTime);
  const cutoffMs = nowMs - WINDOW_HOURS * 60 * 60 * 1000;

  // Cache block time per height to avoid refetching
  const blockTimeCache = new Map();

  async function getBlockTimeMs(height) {
    if (blockTimeCache.has(height)) return blockTimeCache.get(height);
    const b = await fetchJson(`${base}/block?height=${height}`);
    const t = b?.result?.block?.header?.time;
    const ms = t ? Date.parse(t) : null;
    blockTimeCache.set(height, ms);
    return ms;
  }

  const items = [];
  const seen = new Set();

  // 2) walk newest->older delegate txs
  for (let page = 1; page <= LIMIT_PAGES; page++) {
    const url =
      `${base}/tx_search` +
      `?query=${encodeURIComponent(EVENT_QUERY)}` +
      `&prove=false&page=${page}&per_page=${PER_PAGE}&order_by=desc`;

    const data = await fetchJson(url);

    const txs = data?.result?.txs ?? [];
    if (!txs.length) break;

    for (const tx of txs) {
      const height = Number(tx?.height ?? tx?.tx_result?.height ?? 0);
      const txhash = tx?.hash;
      if (!height || !txhash) continue;
      if (seen.has(txhash)) continue;
      seen.add(txhash);

      const tsMs = await getBlockTimeMs(height);
      if (!tsMs) continue;

      // stop condition: we are beyond the window and pages are ordered desc (newest first)
      if (tsMs < cutoffMs) {
        // We can bail hard because all remaining pages are older
        page = LIMIT_PAGES + 1;
        break;
      }

      // Extract delegate info from events
      const events = tx?.tx_result?.events ?? [];
      let delegator = null;
      let validator = null;
      let amount_atom = null;

      for (const ev of events) {
        // Some chains emit a "delegate" event with amount/validator/delegator
        if (ev?.type === "delegate") {
          const d = getAttr(ev, "delegator") || getAttr(ev, "delegator_address");
          const v = getAttr(ev, "validator") || getAttr(ev, "validator_address");
          const amt = getAttr(ev, "amount");
          delegator = delegator || d;
          validator = validator || v;
          if (!amount_atom) amount_atom = parseAmountAtom(amt);
        }
      }

      // fallback: some nodes only emit amount in "message" event as "amount"
      if (!amount_atom) {
        for (const ev of events) {
          if (ev?.type === "message") {
            const amt = getAttr(ev, "amount");
            const parsed = parseAmountAtom(amt);
            if (parsed) {
              amount_atom = parsed;
              break;
            }
          }
        }
      }

      if (!amount_atom || amount_atom < MIN_ATOM) continue;

      items.push({
        amount_atom,
        delegator: delegator || null,
        validator: validator || null,
        height,
        txhash,
        timestamp: new Date(tsMs).toISOString(),
      });
    }
  }

  // sort biggest first (useful for hero)
  items.sort((a, b) => b.amount_atom - a.amount_atom);

  return {
    generated_at: new Date().toISOString(),
    window_hours: WINDOW_HOURS,
    min_atom: MIN_ATOM,
    source: { rpc: base, query: EVENT_QUERY },
    items,
  };
}

async function main() {
  let lastErr = null;

  for (const rpc of FALLBACK_RPCS) {
    try {
      const snap = await tryRpc(rpc);
      await writeSnapshot(snap);
      console.log(`✅ Wrote ${snap.items.length} items using ${snap.source.rpc}`);
      return;
    } catch (e) {
      lastErr = e;
      console.warn(`⚠️ RPC failed (${rpc}): ${e?.message || e}`);
    }
  }

  const payload = {
    generated_at: new Date().toISOString(),
    window_hours: WINDOW_HOURS,
    min_atom: MIN_ATOM,
    source: { rpc: null, query: EVENT_QUERY },
    items: [],
    error: lastErr?.message || String(lastErr || "Unknown error"),
  };
  await writeSnapshot(payload);
  console.error("❌ Wrote empty snapshot due to error:", payload.error);
  process.exit(1);
}

main().catch(async (e) => {
  const payload = {
    generated_at: new Date().toISOString(),
    window_hours: WINDOW_HOURS,
    min_atom: MIN_ATOM,
    source: { rpc: null, query: EVENT_QUERY },
    items: [],
    error: e?.message || String(e),
  };
  await writeSnapshot(payload);
  console.error("❌ Fatal:", payload.error);
  process.exit(1);
});
