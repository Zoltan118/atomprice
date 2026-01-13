import fs from "node:fs";
import path from "node:path";

const RPC_HTTP = process.env.RPC_HTTP || "https://rpc.silknodes.io/cosmos";
const MIN_ATOM = Number(process.env.MIN_ATOM || "1");
const WINDOW_HOURS = Number(process.env.WINDOW_HOURS || "24");
const LIMIT_TXS = Number(process.env.LIMIT_TXS || "500");

const OUTFILE = path.join("data", "delegations_24h.json");

function isoNow() {
  return new Date().toISOString();
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { "accept": "application/json" }});
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

/**
 * Tendermint tx_search - searches for transactions
 */
async function txSearch(query, page = 1, perPage = 100) {
  const url =
    `${RPC_HTTP}/tx_search?query=${encodeURIComponent(query)}` +
    `&prove=false&page=${page}&per_page=${perPage}&order_by="desc"`;

  return fetchJson(url);
}

/**
 * Parse events from a transaction to extract delegation info
 */
function parseEventsToDelegation(tx) {
  const events = tx?.tx_result?.events || [];
  const txhash = tx?.hash || null;
  const height = tx?.height || null;
  
  // Find delegate event
  const delegateEvent = events.find(e => e.type === 'delegate');
  if (!delegateEvent) return null;
  
  // Helper to get attribute value (handles base64)
  const getAttr = (attrs, key) => {
    const attr = attrs?.find(a => {
      let k = a.key;
      try { if (k && k.length > 10) k = atob(k); } catch(e) {}
      return k === key;
    });
    if (!attr) return null;
    let v = attr.value;
    try { if (v && v.length > 10) v = atob(v); } catch(e) {}
    return v;
  };
  
  const amountStr = getAttr(delegateEvent.attributes, 'amount');
  if (!amountStr || !amountStr.includes('uatom')) return null;
  
  const match = amountStr.match(/(\d+)uatom/i);
  if (!match) return null;
  
  const amountAtom = Number(match[1]) / 1_000_000;
  if (amountAtom < MIN_ATOM) return null;
  
  const validator = getAttr(delegateEvent.attributes, 'validator');
  
  // Get delegator from message event
  const messageEvent = events.find(e => e.type === 'message');
  const delegator = messageEvent ? getAttr(messageEvent.attributes, 'sender') : null;
  
  return {
    height: height ? Number(height) : null,
    txhash,
    delegator,
    validator,
    amount_atom: amountAtom
  };
}

async function main() {
  console.log(`Starting delegation fetch from ${RPC_HTTP}`);
  console.log(`Looking for delegations >= ${MIN_ATOM} ATOM in last ${WINDOW_HOURS} hours`);
  
  fs.mkdirSync(path.dirname(OUTFILE), { recursive: true });

  // Different query formats that different RPC nodes might support
  const queries = [
    `message.action='/cosmos.staking.v1beta1.MsgDelegate'`,
    `message.action='delegate'`,
    `delegate.validator EXISTS`
  ];

  let allTxs = [];
  let usedQuery = null;

  for (const q of queries) {
    try {
      console.log(`Trying query: ${q}`);
      const first = await txSearch(q, 1, 100);
      const total = Number(first?.result?.total_count || 0);
      const firstTxs = first?.result?.txs || [];
      
      console.log(`  Found ${total} total, got ${firstTxs.length} in first page`);
      
      if (total > 0 && firstTxs.length > 0) {
        usedQuery = q;
        allTxs = [...firstTxs];
        
        // Paginate to get more
        let page = 2;
        while (allTxs.length < Math.min(total, LIMIT_TXS)) {
          console.log(`  Fetching page ${page}...`);
          const nxt = await txSearch(q, page, 100);
          const chunk = nxt?.result?.txs || [];
          if (!chunk.length) break;
          allTxs.push(...chunk);
          page += 1;
        }
        console.log(`  Total fetched: ${allTxs.length} transactions`);
        break;
      }
    } catch (e) {
      console.log(`  Query failed: ${e.message}`);
    }
  }

  if (!usedQuery) {
    console.log("No query worked. Writing empty snapshot.");
    const empty = {
      generated_at: isoNow(),
      window_hours: WINDOW_HOURS,
      source: { rpc: RPC_HTTP, query: null },
      items: []
    };
    fs.writeFileSync(OUTFILE, JSON.stringify(empty, null, 2));
    return;
  }

  // Parse all transactions
  const items = [];
  for (const tx of allTxs) {
    const delegation = parseEventsToDelegation(tx);
    if (delegation) {
      items.push(delegation);
    }
  }

  // Deduplicate by txhash
  const seen = new Set();
  const deduped = [];
  for (const it of items) {
    if (!it.txhash) continue;
    if (seen.has(it.txhash)) continue;
    seen.add(it.txhash);
    deduped.push(it);
  }

  // Sort by amount (biggest first)
  deduped.sort((a, b) => (b.amount_atom || 0) - (a.amount_atom || 0));

  const out = {
    generated_at: isoNow(),
    window_hours: WINDOW_HOURS,
    min_atom: MIN_ATOM,
    source: { rpc: RPC_HTTP, query: usedQuery },
    total_fetched: allTxs.length,
    items: deduped
  };

  fs.writeFileSync(OUTFILE, JSON.stringify(out, null, 2));
  console.log(`\n✅ Success! Wrote ${deduped.length} delegations to ${OUTFILE}`);
  
  // Show top 5
  console.log("\nTop 5 delegations:");
  deduped.slice(0, 5).forEach((d, i) => {
    console.log(`  ${i+1}. ${d.amount_atom.toFixed(2)} ATOM (${d.txhash?.slice(0,8)}...)`);
  });
}

main().catch(err => {
  console.error("Error:", err);
  process.exit(1);
});
