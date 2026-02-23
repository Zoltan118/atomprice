// scripts/fetch-unbonding-flows.mjs
// Tracks where recently-matured unbonded ATOM goes:
//   - MsgDelegate       → re-staked (bullish)
//   - MsgTransfer (IBC) → sent to DEX / other chain
//   - MsgSend + memo    → exchange deposit (sell pressure!)
//   - No qualifying tx  → held in wallet
//
// Key insight: Exchanges (Binance, Coinbase, Kraken) require memos for ATOM deposits.
// MsgSend with non-empty memo + large amount = highly likely exchange deposit.
//
// Two-step API approach:
//   1. RPC tx_search (message.sender) → find recent tx hashes
//   2. REST /cosmos/tx/v1beta1/txs/{hash} → get decoded tx with memo field
//
// Reads: data/pending-undelegations.json (delegators_by_date)
// Outputs: data/unbonding-flows.json
//
// Env (optional):
//   RPC_BASE        default: https://rpc.silknodes.io/cosmos
//   REST_BASE       default: https://rest.cosmos.directory/cosmoshub
//   FLOW_MIN_ATOM   default: 5000 (min ATOM per delegator to track)
//   MEMO_MIN_ATOM   default: 5000 (min ATOM in MsgSend+memo to count as exchange)
//   MAX_DELEGATORS  default: 50 (max delegators to track per date)
//   LOOKBACK_DAYS   default: 3 (how many past matured dates to check)

import fs from "node:fs/promises";

const UNDELEGATIONS_FILE = "data/pending-undelegations.json";
const OUT_FILE = "data/unbonding-flows.json";

const RPC_BASE = (process.env.RPC_BASE || "https://rpc.silknodes.io/cosmos").replace(/\/+$/, "");
const REST_BASE = (process.env.REST_BASE || "https://rest.cosmos.directory/cosmoshub").replace(/\/+$/, "");
const FLOW_MIN_ATOM = Number(process.env.FLOW_MIN_ATOM ?? "1000");
const MEMO_MIN_ATOM = Number(process.env.MEMO_MIN_ATOM ?? "1000");
const MAX_DELEGATORS = Number(process.env.MAX_DELEGATORS ?? "50");
const LOOKBACK_DAYS = Number(process.env.LOOKBACK_DAYS ?? "3");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchJson(url, timeoutMs = 20000) {
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

// ── IBC channel → chain mapping (common Cosmos Hub channels) ──
const IBC_CHANNELS = {
  "channel-0": "osmosis",
  "channel-141": "osmosis",
  "channel-1": "crypto-org",
  "channel-4": "iris",
  "channel-207": "stride",
  "channel-391": "stride",
  "channel-405": "dydx",
  "channel-569": "celestia",
  "channel-570": "neutron",
  "channel-585": "noble",
};

function resolveIbcChain(sourceChannel) {
  return IBC_CHANNELS[sourceChannel] || `ibc-${sourceChannel}`;
}

// ── Step 1: Find recent tx hashes for a sender via RPC ──
async function findRecentTxHashes(address, limit = 10) {
  const query = `message.sender='${address}'`;
  const params = new URLSearchParams();
  params.set("query", JSON.stringify(query));
  params.set("per_page", String(limit));
  params.set("order_by", JSON.stringify("desc"));
  params.set("prove", "false");

  const url = `${RPC_BASE}/tx_search?${params}`;
  const data = await fetchJson(url);
  const txs = data?.result?.txs ?? [];

  // Extract hashes and action types from events
  return txs.map(tx => {
    const hash = tx.hash;
    const events = tx.tx_result?.events ?? [];
    let actions = [];
    for (const ev of events) {
      if (ev.type === "message") {
        for (const attr of ev.attributes ?? []) {
          if (attr.key === "action") actions.push(attr.value);
        }
      }
    }
    return { hash, actions };
  });
}

// ── Step 2: Fetch decoded tx (with memo) via REST ──
async function fetchTxDetails(txhash) {
  const url = `${REST_BASE}/cosmos/tx/v1beta1/txs/${txhash}`;
  return await fetchJson(url, 15000);
}

// ── Classify a delegator's recent txs ──
async function classifyDelegator(address, maturedAtom) {
  const result = {
    address,
    matured_atom: maturedAtom,
    classification: "held", // default
    amount: 0,
    details: null,
  };

  try {
    // Step 1: Find recent tx hashes via RPC
    const txInfos = await findRecentTxHashes(address, 10);

    if (!txInfos.length) return result;

    // Quick classification from RPC events (no memo needed for delegate/IBC)
    for (const info of txInfos) {
      const hasDelegate = info.actions.some(a => a.includes("MsgDelegate") && !a.includes("MsgUndelegate"));
      const hasIbc = info.actions.some(a => a.includes("MsgTransfer"));
      const hasSend = info.actions.some(a => a.includes("MsgSend"));

      // For MsgDelegate — classify immediately (no memo needed)
      if (hasDelegate) {
        result.classification = "restaked";
        result.amount = maturedAtom;
        return result;
      }

      // For MsgTransfer — classify immediately, get chain details
      if (hasIbc) {
        try {
          const txData = await fetchTxDetails(info.hash);
          const msgs = txData?.tx?.body?.messages || [];
          for (const msg of msgs) {
            if ((msg["@type"] || "").includes("MsgTransfer")) {
              const amount = Number(msg.token?.amount || "0") / 1_000_000;
              const channel = msg.source_channel || "";
              result.classification = "ibc_transfer";
              result.amount = amount || maturedAtom;
              result.details = {
                chain: resolveIbcChain(channel),
                channel,
              };
              return result;
            }
          }
        } catch (e) {
          console.log(`    ⚠️ Failed to fetch IBC tx details: ${e.message}`);
        }
        // Still mark as IBC even if detail fetch fails
        result.classification = "ibc_transfer";
        result.amount = maturedAtom;
        return result;
      }

      // For MsgSend — need to check memo (Step 2)
      if (hasSend) {
        try {
          await sleep(200); // Rate limit before REST call
          const txData = await fetchTxDetails(info.hash);
          const memo = (txData?.tx?.body?.memo || "").trim();
          const msgs = txData?.tx?.body?.messages || [];

          for (const msg of msgs) {
            if (!(msg["@type"] || "").includes("MsgSend")) continue;

            const amounts = msg.amount || [];
            const atomAmount = amounts.reduce((sum, a) => {
              if ((a.denom || "").toLowerCase() === "uatom") {
                return sum + Number(a.amount || "0") / 1_000_000;
              }
              return sum;
            }, 0);

            // MsgSend with memo + large amount = exchange deposit!
            if (atomAmount >= MEMO_MIN_ATOM && memo.length > 0) {
              result.classification = "exchange";
              result.amount = atomAmount;
              result.details = {
                to_address: msg.to_address || "",
                memo_hint: memo.slice(0, 20) + (memo.length > 20 ? "…" : ""),
              };
              return result;
            }
          }
        } catch (e) {
          console.log(`    ⚠️ Failed to fetch Send tx details: ${e.message}`);
        }
      }
    }
  } catch (e) {
    console.log(`  ⚠️ Error classifying ${address.slice(0, 16)}: ${e.message}`);
  }

  return result;
}

// ── Main ──
async function main() {
  await fs.mkdir("data", { recursive: true });

  // 1. Load pending undelegations
  let undelegations;
  try {
    const txt = await fs.readFile(UNDELEGATIONS_FILE, "utf8");
    undelegations = JSON.parse(txt);
  } catch (e) {
    console.error(`❌ Cannot read ${UNDELEGATIONS_FILE}: ${e.message}`);
    console.log("   Run fetch-pending-undelegations.mjs first!");
    process.exit(1);
  }

  const delegatorsByDate = undelegations.delegators_by_date || {};
  const today = new Date().toISOString().slice(0, 10);

  // 2. Archive delegators so they persist after on-chain maturation
  //    Once unbonding completes, validators drop the entry → it disappears
  //    from pending-undelegations.json. The archive preserves them for flow analysis.
  const ARCHIVE_FILE = "data/undelegation-archive.json";
  let archive = {};
  try {
    archive = JSON.parse(await fs.readFile(ARCHIVE_FILE, "utf8"));
  } catch { /* fresh start */ }

  // Merge current delegators into archive (keep whichever has more entries)
  for (const [date, delegators] of Object.entries(delegatorsByDate)) {
    if (!archive[date] || delegators.length > (archive[date] || []).length) {
      archive[date] = delegators;
    }
  }

  console.log(`📦 Archive: ${Object.keys(archive).length} dates preserved`);

  // Use archive for matured date lookup (includes dates no longer on-chain)
  const allDelegatorsByDate = archive;

  // 3. Find matured dates (dates that have already passed = atoms now liquid)
  const maturedDates = Object.keys(allDelegatorsByDate)
    .filter(d => d <= today)
    .sort()
    .reverse()
    .slice(0, LOOKBACK_DAYS);

  if (!maturedDates.length) {
    console.log("ℹ️ No matured undelegation dates found (all dates are in the future).");
    // Still save the archive so current dates are preserved for next run
    await fs.writeFile(ARCHIVE_FILE, JSON.stringify(archive, null, 2));
    await fs.writeFile(OUT_FILE, JSON.stringify({ generated_at: new Date().toISOString(), daily_flows: [] }, null, 2));
    return;
  }

  console.log(`📅 Analyzing ${maturedDates.length} matured dates: ${maturedDates.join(", ")}`);

  // 3. Load previous flows for merging
  let prevFlows = [];
  try {
    const txt = await fs.readFile(OUT_FILE, "utf8");
    const prev = JSON.parse(txt);
    prevFlows = prev?.daily_flows || [];
  } catch { /* fresh start */ }

  const dailyFlows = [];

  for (const date of maturedDates) {
    // Check if we already have this date analyzed with decent coverage
    const existing = prevFlows.find(f => f.date === date);
    if (existing && existing.tracked_pct > 50) {
      console.log(`  ♻️ ${date}: reusing previous analysis (${existing.tracked_pct.toFixed(0)}% tracked)`);
      dailyFlows.push(existing);
      continue;
    }

    const delegators = allDelegatorsByDate[date] || [];
    const totalMatured = delegators.reduce((s, d) => s + d.atom, 0);

    // Filter to top delegators by amount
    const tracked = delegators
      .filter(d => d.atom >= FLOW_MIN_ATOM)
      .sort((a, b) => b.atom - a.atom)
      .slice(0, MAX_DELEGATORS);

    const trackedAtom = tracked.reduce((s, d) => s + d.atom, 0);

    console.log(`\n📊 ${date}: ${Math.round(totalMatured).toLocaleString()} ATOM matured, tracking ${tracked.length} delegators (${Math.round(trackedAtom).toLocaleString()} ATOM)`);

    // Classify each delegator (3 concurrent)
    const classifications = { restaked: 0, exchange: 0, ibc_transfer: 0, held: 0 };
    const ibcDestinations = {};
    const delegatorResults = [];

    for (let i = 0; i < tracked.length; i += 3) {
      const batch = tracked.slice(i, i + 3);
      const results = await Promise.allSettled(
        batch.map(d => classifyDelegator(d.address, d.atom))
      );

      for (const r of results) {
        if (r.status !== "fulfilled") continue;
        const res = r.value;
        const cls = res.classification;
        classifications[cls] = (classifications[cls] || 0) + res.matured_atom;

        if (cls === "ibc_transfer" && res.details?.chain) {
          ibcDestinations[res.details.chain] = (ibcDestinations[res.details.chain] || 0) + (res.amount || res.matured_atom);
        }

        // Save per-delegator record for proof layer
        delegatorResults.push({
          a: res.address,
          atom: Math.round(res.matured_atom),
          cls: res.classification,
          d: res.details || null,
        });

        const icon = { restaked: "🟢", exchange: "🔴", ibc_transfer: "🟡", held: "⚪" }[cls] || "❓";
        console.log(`  ${icon} ${res.address.slice(0, 16)}… → ${cls} (${Math.round(res.matured_atom).toLocaleString()} ATOM)`);
      }

      if (i + 3 < tracked.length) await sleep(600);
    }

    // Build flow entry
    const flow = {
      date,
      total_matured_atom: Math.round(totalMatured),
      tracked_atom: Math.round(trackedAtom),
      untracked_atom: Math.round(totalMatured - trackedAtom),
      tracked_pct: totalMatured > 0 ? Math.round((trackedAtom / totalMatured) * 1000) / 10 : 0,
      flows: {},
      top_ibc_destinations: [],
      delegators: delegatorResults,
    };

    for (const [key, atom] of Object.entries(classifications)) {
      flow.flows[key] = {
        atom: Math.round(atom),
        pct: trackedAtom > 0 ? Math.round((atom / trackedAtom) * 1000) / 10 : 0,
      };
    }

    // Top IBC destinations
    flow.top_ibc_destinations = Object.entries(ibcDestinations)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([chain, atom]) => ({ chain, atom: Math.round(atom) }));

    dailyFlows.push(flow);

    console.log(`\n  📈 ${date} summary:`);
    for (const [k, v] of Object.entries(flow.flows)) {
      if (v.atom > 0) console.log(`     ${k}: ${v.atom.toLocaleString()} ATOM (${v.pct}%)`);
    }
  }

  // Sort by date descending
  dailyFlows.sort((a, b) => (a.date > b.date ? -1 : 1));

  const output = {
    generated_at: new Date().toISOString(),
    daily_flows: dailyFlows,
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(output, null, 2));

  // Prune archive entries older than 365 days (keep 1 year for historical chart markers)
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 365);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  for (const date of Object.keys(archive)) {
    if (date < cutoffStr) delete archive[date];
  }
  await fs.writeFile(ARCHIVE_FILE, JSON.stringify(archive, null, 2));

  // Generate lightweight daily totals for front-end chart markers
  const HISTORY_FILE = "data/undelegation-history.json";
  const dailyTotals = Object.entries(archive)
    .map(([date, delegators]) => ({
      date,
      atom: Math.round(delegators.reduce((s, d) => s + d.atom, 0)),
      delegator_count: new Set(delegators.map(d => d.address)).size,
    }))
    .sort((a, b) => (a.date > b.date ? 1 : -1));

  await fs.writeFile(HISTORY_FILE, JSON.stringify({
    generated_at: new Date().toISOString(),
    daily_totals: dailyTotals,
  }, null, 2));

  console.log(`\n✅ Unbonding flows saved: ${dailyFlows.length} dates`);
  console.log(`   Archive: ${Object.keys(archive).length} dates kept (pruned < ${cutoffStr})`);
  console.log(`   History: ${dailyTotals.length} daily totals → ${HISTORY_FILE}`);
  console.log(`   Output: ${OUT_FILE}`);
}

main();
