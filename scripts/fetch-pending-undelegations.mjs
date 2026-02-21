// scripts/fetch-pending-undelegations.mjs
// Fetches pending (in-progress) unbonding delegations from Cosmos Hub validators
// Groups by completion_time date → daily schedule
// Stores delegator addresses per date for flow tracking (Phase 2)
// Outputs: data/pending-undelegations.json
//
// Env (optional):
//   REST_BASE     default: https://rest.cosmos.directory/cosmoshub
//   MIN_ATOM      default: 100 (minimum ATOM per entry to include)
//   BATCH_SIZE    default: 5 (concurrent validator requests)

import fs from "node:fs/promises";

const OUT_FILE = "data/pending-undelegations.json";

const REST_BASE = (process.env.REST_BASE || "https://rest.cosmos.directory/cosmoshub").replace(/\/+$/, "");
const MIN_ATOM = Number(process.env.MIN_ATOM ?? "100");
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? "5");

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

// ── Fetch all bonded validators ──
async function fetchBondedValidators() {
  const validators = [];
  let nextKey = null;

  for (let page = 0; page < 10; page++) {
    const params = new URLSearchParams();
    params.set("status", "BOND_STATUS_BONDED");
    params.set("pagination.limit", "100");
    if (nextKey) params.set("pagination.key", nextKey);

    const url = `${REST_BASE}/cosmos/staking/v1beta1/validators?${params}`;
    console.log(`📡 Fetching validators page ${page + 1}...`);

    const data = await fetchJson(url);
    const vals = data?.validators ?? [];
    validators.push(...vals);

    nextKey = data?.pagination?.next_key;
    if (!nextKey || !vals.length) break;

    await sleep(300);
  }

  console.log(`  ✅ ${validators.length} bonded validators found`);
  return validators;
}

// ── Fetch unbonding delegations for a single validator ──
async function fetchUnbondingForValidator(valoperAddr) {
  const entries = [];
  let nextKey = null;

  for (let page = 0; page < 20; page++) {
    const params = new URLSearchParams();
    params.set("pagination.limit", "100");
    if (nextKey) params.set("pagination.key", nextKey);

    const url = `${REST_BASE}/cosmos/staking/v1beta1/validators/${valoperAddr}/unbonding_delegations?${params}`;

    let data;
    try {
      data = await fetchJson(url);
    } catch (e) {
      console.log(`  ⚠️ Error for ${valoperAddr.slice(0, 24)}: ${e.message}`);
      return entries;
    }

    const responses = data?.unbonding_responses ?? [];
    for (const resp of responses) {
      const delegator = resp?.delegator_address || "";
      for (const entry of resp?.entries ?? []) {
        const balance = Number(entry?.balance || "0") / 1_000_000;
        const completionTime = entry?.completion_time || "";
        if (balance >= MIN_ATOM && completionTime) {
          entries.push({
            delegator,
            validator: valoperAddr,
            atom: balance,
            completion_time: completionTime,
          });
        }
      }
    }

    nextKey = data?.pagination?.next_key;
    if (!nextKey || !responses.length) break;

    await sleep(200);
  }

  return entries;
}

// ── Main ──
async function main() {
  await fs.mkdir("data", { recursive: true });

  // 1. Fetch all bonded validators
  const validators = await fetchBondedValidators();

  // 2. Fetch unbonding delegations for each validator (batched)
  console.log(`\n📥 Fetching unbonding delegations (min ${MIN_ATOM} ATOM)...`);
  const allEntries = [];

  for (let i = 0; i < validators.length; i += BATCH_SIZE) {
    const batch = validators.slice(i, i + BATCH_SIZE);
    const results = await Promise.allSettled(
      batch.map(v => fetchUnbondingForValidator(v.operator_address))
    );

    for (const r of results) {
      if (r.status === "fulfilled") allEntries.push(...r.value);
    }

    const progress = Math.min(i + BATCH_SIZE, validators.length);
    console.log(`  Progress: ${progress}/${validators.length} validators, ${allEntries.length} unbonding entries`);

    if (i + BATCH_SIZE < validators.length) await sleep(400);
  }

  console.log(`\n📊 Total unbonding entries (≥${MIN_ATOM} ATOM): ${allEntries.length}`);

  // 3. Group by completion_time date
  const byDate = {};
  for (const entry of allEntries) {
    const date = entry.completion_time.slice(0, 10); // YYYY-MM-DD
    if (!byDate[date]) byDate[date] = [];
    byDate[date].push({
      address: entry.delegator,
      atom: entry.atom,
      validator: entry.validator,
    });
  }

  // 4. Build schedule (sorted by date ascending)
  const dates = Object.keys(byDate).sort();
  const schedule = dates.map(date => {
    const entries = byDate[date];
    const totalAtom = entries.reduce((s, e) => s + e.atom, 0);
    return {
      date,
      atom: Math.round(totalAtom),
      delegator_count: new Set(entries.map(e => e.address)).size,
    };
  });

  // 5. Build delegators_by_date (sorted by atom descending within each date)
  const delegatorsByDate = {};
  for (const date of dates) {
    delegatorsByDate[date] = byDate[date]
      .sort((a, b) => b.atom - a.atom)
      .map(e => ({
        address: e.address,
        atom: Math.round(e.atom * 1000) / 1000,
        validator: e.validator,
      }));
  }

  const totalUnbonding = schedule.reduce((s, d) => s + d.atom, 0);

  const output = {
    generated_at: new Date().toISOString(),
    total_unbonding_atom: totalUnbonding,
    schedule,
    delegators_by_date: delegatorsByDate,
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(output, null, 2));

  console.log(`\n✅ Pending undelegations saved:`);
  console.log(`   ${schedule.length} dates, ${totalUnbonding.toLocaleString()} total ATOM`);
  console.log(`   Output: ${OUT_FILE}`);

  // Show next 7 days preview
  const today = new Date().toISOString().slice(0, 10);
  const next7 = schedule.filter(d => d.date >= today).slice(0, 7);
  if (next7.length) {
    console.log(`\n📅 Next 7 days:`);
    for (const d of next7) {
      console.log(`   ${d.date}: ${d.atom.toLocaleString()} ATOM (${d.delegator_count} delegators)`);
    }
  }
}

main();
