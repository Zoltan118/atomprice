import fs from "node:fs/promises";

const OUT_FILE = "data/staking_ratio.json";

// Use your Silk Nodes LCD (server-to-server, no CORS issues)
const LCD_BASE = process.env.LCD_BASE || "https://api.silknodes.io/cosmos";

async function fetchJson(url, timeoutMs = 15000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ac.signal, headers: { accept: "application/json" } });
    const text = await res.text();
    let json = null;
    try { json = JSON.parse(text); } catch {}
    if (!res.ok) {
      const msg = json?.message || json?.error?.message || text?.slice(0, 200);
      throw new Error(`HTTP ${res.status} for ${url}${msg ? ` :: ${msg}` : ""}`);
    }
    return json;
  } finally {
    clearTimeout(t);
  }
}

async function ensureOutDir() {
  await fs.mkdir("data", { recursive: true });
}

async function main() {
  await ensureOutDir();

  try {
    // 1) bonded / not bonded from staking pool
    const pool = await fetchJson(`${LCD_BASE}/cosmos/staking/v1beta1/pool`);
    const bondedUatom = BigInt(pool.pool.bonded_tokens);
    const notBondedUatom = BigInt(pool.pool.not_bonded_tokens);

    // 2) total supply (uatom)
    // Prefer by_denom endpoint
    let supply;
    try {
      supply = await fetchJson(`${LCD_BASE}/cosmos/bank/v1beta1/supply/by_denom?denom=uatom`);
    } catch {
      // Fallback: some nodes expose supply?by_denom
      supply = await fetchJson(`${LCD_BASE}/cosmos/bank/v1beta1/supply?by_denom=uatom`);
    }

    const supplyUatom =
      supply?.amount?.amount ? BigInt(supply.amount.amount)
      : supply?.supply?.[0]?.amount ? BigInt(supply.supply[0].amount)
      : null;

    if (!supplyUatom) throw new Error("Could not parse total supply response");

    const ratioVsSupply = Number(bondedUatom) / Number(supplyUatom);
    const ratioVsPool = Number(bondedUatom) / Number(bondedUatom + notBondedUatom);

    const out = {
      generated_at: new Date().toISOString(),
      source: { lcd: LCD_BASE },
      denom: "uatom",
      bonded_uatom: bondedUatom.toString(),
      not_bonded_uatom: notBondedUatom.toString(),
      total_supply_uatom: supplyUatom.toString(),
      staking_ratio_vs_supply: ratioVsSupply,   // e.g. 0.62
      bonded_share_of_pool: ratioVsPool         // e.g. 0.80
    };

    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
    console.log(`✅ Wrote staking ratio snapshot to ${OUT_FILE}`);
  } catch (e) {
    const out = {
      generated_at: new Date().toISOString(),
      source: { lcd: LCD_BASE },
      denom: "uatom",
      error: String(e?.message || e)
    };
    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
    console.log(`⚠️ Wrote error snapshot to ${OUT_FILE}: ${out.error}`);
    process.exit(1);
  }
}

main();
