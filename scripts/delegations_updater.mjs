import fs from "node:fs";

const RPC_BASE = process.env.RPC_BASE || "https://rpc.silknodes.io/cosmos";
const WINDOW_HOURS = Number(process.env.WINDOW_HOURS || "24");
const MIN_ATOM = Number(process.env.MIN_ATOM || "1");
const LIMIT_PAGES = Number(process.env.LIMIT_PAGES || "2");
const PAGE_LIMIT = Number(process.env.PAGE_LIMIT || "50");

const OUT_FILE = "data/delegations_24h.json";

function isoNow() {
  return new Date().toISOString();
}
function cutoffMs(hours) {
  return Date.now() - hours * 60 * 60 * 1000;
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { accept: "application/json" } });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

function extractEventAttrs(events, type) {
  const ev = (events || []).find(e => e.type === type);
  if (!ev?.attributes) return {};
  const out = {};
  for (const a of ev.attributes) {
    // tendermint may return base64 keys/values on some nodes; yours looks plain JSON for other endpoints
    // so we assume plain text here
    out[a.key] = a.value;
  }
  return out;
}

function uatomToAtomFromString(s) {
  // expects something like "1234567uatom"
  if (!s || typeof s !== "string") return null;
  if (!s.endsWith("uatom")) return null;
  const n = Number(s.replace("uatom", ""));
  if (!Number.isFinite(n)) return null;
  return n / 1_000_000;
}

async function getBlockTimeISO(height) {
  const b = await fetchJson(`${RPC_BASE}/block?height=${height}`);
  const t = b?.result?.block?.header?.time;
  return t ? new Date(t).toISOString() : null;
}

async function txSearch(query, page, per_page) {
  const params = new URLSearchParams();
  params.set("query", query);
  params.set("prove", "false");
  params.set("page", String(page));
  params.set("per_page", String(per_page));
  params.set("order_by", "desc");

  const url = `${RPC_BASE}/tx_search?${params.toString()}`;
  return fetchJson(url);
}

function parseDelegationsFromTx(tx) {
  // tx_result.events usually contains:
  // - message.action
  // - delegate.amount / delegate.delegator / delegate.validator (often)
  const events = tx?.tx_result?.events || [];

  const msg = extractEventAttrs(events, "message");
  const action = msg["action"];

  // If action isn’t present, skip; we want real delegates only
  if (!action || !String(action).includes("MsgDelegate")) return [];

  const del = extractEventAttrs(events, "delegate");
  const amountAtom = uatomToAtomFromString(del["amount"]);
  if (!amountAtom || amountAtom < MIN_ATOM) return [];

  return [{
    height: Number(tx?.height || 0) || null,
    time: null, // filled later from block header
    txhash: tx?.hash || null,
    delegator: del["delegator"] || null,
    validator: del["validator"] || null,
    amount_atom: amountAtom
  }];
}

async function main() {
  fs.mkdirSync("data", { recursive: true });

  const cutoff = cutoffMs(WINDOW_HOURS);

  // Try a couple query variants because chains can differ in indexed keys
 const queries = [
  "message.action='/cosmos.staking.v1beta1.MsgDelegate'",
  "message.module='staking'"
 ];
  
  let usedQuery = null;
  let items = [];

  try {
    for (const q of queries) {
      items = [];
      usedQuery = q;

      for (let page = 1; page <= LIMIT_PAGES; page++) {
        const data = await txSearch(q, page, PAGE_LIMIT);
        const txs = data?.result?.txs || [];
        if (!txs.length) break;

        for (const tx of txs) {
          const parsed = parseDelegationsFromTx(tx);
          for (const d of parsed) items.push(d);
        }

        // If we didn’t even get any message.action matches, don’t early stop yet; keep paging a bit.
        // Early stopping by time needs block times, handled after dedupe.
      }

      if (items.length) break; // found something with this query
    }

    // Deduplicate
    const seen = new Set();
    const deduped = [];
    for (const it of items) {
      const k = `${it.txhash}|${it.amount_atom}|${it.delegator}|${it.validator}`;
      if (seen.has(k)) continue;
      seen.add(k);
      deduped.push(it);
    }

    // Fill timestamps (cache by height)
    const heightToTime = new Map();
    for (const it of deduped) {
      if (!it.height) continue;
      if (!heightToTime.has(it.height)) {
        heightToTime.set(it.height, await getBlockTimeISO(it.height));
      }
      it.time = heightToTime.get(it.height);
    }

    // Filter to last WINDOW_HOURS using block time
    const filtered = deduped.filter(it => {
      if (!it.time) return true; // keep if unknown time
      return new Date(it.time).getTime() >= cutoff;
    });

    // Sort biggest first
    filtered.sort((a, b) => (b.amount_atom || 0) - (a.amount_atom || 0));

    const out = {
      generated_at: isoNow(),
      window_hours: WINDOW_HOURS,
      min_atom: MIN_ATOM,
      source: { rpc: RPC_BASE, query: usedQuery },
      items: filtered
    };

    fs.writeFileSync(OUT_FILE, JSON.stringify(out, null, 2));
    console.log(`✅ Wrote ${filtered.length} delegations to ${OUT_FILE}`);
  } catch (e) {
    const out = {
      generated_at: isoNow(),
      window_hours: WINDOW_HOURS,
      min_atom: MIN_ATOM,
      source: { rpc: RPC_BASE, query: usedQuery },
      items: [],
      error: String(e?.message || e)
    };
    fs.writeFileSync(OUT_FILE, JSON.stringify(out, null, 2));
    console.log(`⚠️ Wrote empty snapshot due to error: ${out.error}`);
    process.exitCode = 1;
  }
}

main();
