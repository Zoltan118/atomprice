import fs from "node:fs";

const LCD_BASE = process.env.LCD_BASE || "https://api.silknodes.io/cosmos";
const WINDOW_HOURS = Number(process.env.WINDOW_HOURS || "24");
const MIN_ATOM = Number(process.env.MIN_ATOM || "1");
const LIMIT_PAGES = Number(process.env.LIMIT_PAGES || "10"); // safety
const PAGE_LIMIT = Number(process.env.PAGE_LIMIT || "100");  // per request

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

function uatomToAtom(amountStr) {
  // amountStr may look like "1234567uatom"
  if (!amountStr || typeof amountStr !== "string") return null;
  if (!amountStr.endsWith("uatom")) return null;
  const n = Number(amountStr.replace("uatom", ""));
  if (!Number.isFinite(n)) return null;
  return n / 1_000_000;
}

function getEventAttr(events, type, key) {
  const ev = events?.find(e => e.type === type);
  if (!ev) return null;
  const attr = ev.attributes?.find(a => a.key === key);
  return attr?.value ?? null;
}

function parseDelegateFromTx(txResp) {
  // Prefer events (most reliable for amount + addresses)
  const events = txResp?.logs?.flatMap(l => l?.events || []) || txResp?.events || [];

  const action =
    getEventAttr(events, "message", "action") ||
    getEventAttr(events, "message", "action");

  if (!action || !String(action).includes("MsgDelegate")) return [];

  const amountStr = getEventAttr(events, "delegate", "amount");
  const delegator = getEventAttr(events, "delegate", "delegator");
  const validator = getEventAttr(events, "delegate", "validator");

  const amountAtom = uatomToAtom(amountStr);
  if (!amountAtom || amountAtom < MIN_ATOM) return [];

  return [{
    height: txResp?.height ? Number(txResp.height) : null,
    time: txResp?.timestamp ? new Date(txResp.timestamp).toISOString() : null,
    txhash: txResp?.txhash || null,
    delegator: delegator || null,
    validator: validator || null,
    amount_atom: amountAtom
  }];
}

async function fetchDelegations24h() {
  const cutoff = cutoffMs(WINDOW_HOURS);

  // Two query variants; use the first that yields results.
  const eventQueries = [
    "message.action='/cosmos.staking.v1beta1.MsgDelegate'",
    "message.module='staking'"
  ];

  let usedEvents = null;
  let items = [];

  for (const evQ of eventQueries) {
    items = [];
    usedEvents = evQ;

    let nextKey = null;
    for (let page = 0; page < LIMIT_PAGES; page++) {
      const params = new URLSearchParams();
      params.set("events", evQ);
      params.set("order_by", "ORDER_BY_DESC");
      params.set("limit", String(PAGE_LIMIT));
      if (nextKey) params.set("pagination.key", nextKey);

      const url = `${LCD_BASE}/cosmos/tx/v1beta1/txs?${params.toString()}`;
      const data = await fetchJson(url);

      const txs = data?.tx_responses || [];
      if (!txs.length) break;

      for (const txResp of txs) {
        // stop once older than cutoff (DESC order)
        const ts = txResp?.timestamp ? new Date(txResp.timestamp).getTime() : null;
        if (ts && Number.isFinite(ts) && ts < cutoff) {
          return { usedEvents, items }; // early exit
        }

        const parsed = parseDelegateFromTx(txResp);
        for (const d of parsed) items.push(d);
      }

      nextKey = data?.pagination?.next_key || null;
      if (!nextKey) break;
    }

    if (items.length) break; // this query worked
  }

  return { usedEvents, items };
}

async function main() {
  fs.mkdirSync("data", { recursive: true })
