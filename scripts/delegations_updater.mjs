import fs from "node:fs/promises";

const OUT_FILE = "data/delegations_24h.json";

const MIN_ATOM = Number(process.env.MIN_ATOM ?? "1");
const WINDOW_HOURS = Number(process.env.WINDOW_HOURS ?? "24");
const LIMIT_PAGES = Number(process.env.LIMIT_PAGES ?? "3");   // keep small to avoid rate-limits
const PER_PAGE = Number(process.env.PER_PAGE ?? "50");        // keep smaller than 100

const PRIMARY_RPC = (process.env.RPC_BASE ?? "").trim();

const FALLBACK_RPCS = [
  PRIMARY_RPC,
  "https://cosmos-rpc.publicnode.com",
  "https://cosmos-rpc.polkachu.com",
  "https://cosmoshub-mainnet-rpc.itrocket.net",
  "https://rpc.cosmos.nodestake.org",
  "https://cosmoshub-rpc.stakely.io",
].filter(Boolean);

const MSG_DELEGATE = "/cosmos.staking.v1beta1.MsgDelegate";
const EVENT_QUERY = `message.action='${MSG_DELEGATE}'`;

function normalizeRpcBase(rpcBase) {
  return rpcBase.replace(/\/+$/, "");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
      const msg = json?.error?.data || json?.error?.message || text?.slice(0, 160);
      throw new Error(`HTTP ${res.status} for ${url}${msg ? ` :: ${msg}` : ""}`);
    }
    return json;
  } finally {
    clearTimeout(t);
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

async function ensureOutDir() {
  await fs.mkdir("data", { recursive: true });
}

async function writeSnapshot(payload) {
  await ensureOutDir();
  await fs.writeFile(OUT_FILE, JSON.stringify(payload, null, 2));
}

async function tryRpc(rpcBase) {
  const base = normalizeRpcBase(rpcBase);

  // Use /status only once (light)
  const status = await fetchJson(`${base}/status`);
  const latestTime = status?.result?.sync_info?.latest_block_time;
  if (!latestTime) throw new Error(`Bad /status from ${base}`);

  const nowMs = Date.parse(latestTime);
  const cutoffMs = nowMs - WINDOW_HOURS * 60 * 60 * 1000;

  const items = [];
  const seen = new Set();

  for (let page = 1; page <= LIMIT_PAGES; page++) {
    // Gentle pacing to avoid 429
    if (page > 1) await sleep(350);

    const params = new URLSearchParams();
    params.set("query", JSON.stringify(EVENT_QUERY));
    params.set("prove", "false");
    params.set("page", String(page));
    params.set("per_page", String(PER_PAGE));
    params.set("order_by", JSON.stringify("desc"));
    const url = `${base}/tx_search?${params.toString()}`;

    let data;
    try {
      data = await fetchJson(url);
    } catch (e) {
      // If RPC says page out of range, just stop paging (not fatal)
      const msg = String(e?.message || e);
      if (msg.includes("page should be within")) break;
      throw e;
    }

    const txs = data?.result?.txs ?? [];
    if (!txs.length) break;

    for (const tx of txs) {
      const txhash = tx?.hash;
      const height = Number(tx?.height ?? 0);
      if (!txhash || !height) continue;
      if (seen.has(txhash)) continue;
      seen.add(txhash);

      // BEST CASE: timestamp is provided in tx_search response (many RPCs do)
      const tsStr = tx?.timestamp || tx?.tx_result?.timestamp || null;
      const tsMs = tsStr ? Date.parse(tsStr) : null;

      // If we have a timestamp and it's older than window -> we can stop hard
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

      items.push({
        amount_atom,
        delegator,
        validator,
        height,
        txhash,
        timestamp: tsMs ? new Date(tsMs).toISOString() : null,
      });
    }
  }

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
