#!/usr/bin/env node
/**
 * build-whale-watchlist.mjs
 *
 * Reads the Cosmos ATOM Addresses CSV (760K addresses) and the
 * Whale Balances Tracker CSV (99 curated whales), then produces
 * a classified watchlist:
 *
 *   - STAKER whales  (totalStake >= 500K ATOM)
 *   - EXCHANGE addrs  (notStaked >= 1M, totalStake == 0)
 *   - MIXED           (significant in both)
 *
 * Output: data/whale-watchlist.json
 */

import { readFileSync, writeFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = resolve(__dirname, '..', 'data');

// ── CONFIG ──
const STAKER_MIN   = 500_000;   // ATOM staked to qualify as "whale staker"
const EXCHANGE_MIN = 1_000_000; // liquid ATOM, 0 staked → likely exchange
const MIXED_MIN    = 100_000;   // both staked + liquid significant

// Known labels (manually curated from on-chain data + mintscan)
const KNOWN_LABELS = {
  'cosmos1fl48vsnmsdzcv85q5d2q4z5ajdha8yu34mf0eh': { label: 'Binance',    type: 'exchange' },
  'cosmos1jv65s3grqf6v6jl3dp4t6c9t9rk99cd88lyufl': { label: 'Binance 2',  type: 'exchange' },
  'cosmos1p3ucd3ptpw902fluyjzhq3ffgq4ntddac9sa3s': { label: 'Coinbase',   type: 'exchange' },
  'cosmos1tygms3xhhs3yv487phx3dw4a95jn7t7lpm470r': { label: 'Kraken',     type: 'exchange' },
  'cosmos1hs6sl3u2lmrwr58khexm4v9yjk333c7ts0rcuz': { label: 'OKX',        type: 'exchange' },
  'cosmos18ejqp3d6yejcq3rxj4z6fsne63uj23cykw92pp': { label: 'Crypto.com', type: 'exchange' },
  'cosmos16desyyz0u8j8ujzpqtt3uqlny468yh8y5hgnr7': { label: 'Bybit',      type: 'exchange' },
  'cosmos1pyarvcy2ehrw86rcvfun34gyu2dlunnthvkc83':  { label: 'Gate.io',    type: 'exchange' },
  'cosmos1vrf87mrnep5zh3lm9wfk388r7xxqhz2jwauk4h': { label: 'KuCoin',     type: 'exchange' },
  'cosmos1ctpu5ssl0hys60ukglv9pwzmqtys3x9gqf3vny': { label: 'HTX',        type: 'exchange' },
  'cosmos1a9a3qkmllnqgn9z2kht0de0gsqxeq8tjat4ntq': { label: 'MEXC',       type: 'exchange' },
  'cosmos15ghk4pcyx7377m0s874nlg6yy2j4l2dc4zkfmw': { label: 'Upbit',      type: 'exchange' },
  'cosmos1vucy2a6znemuc4ke90kd0fpcuc7zs26kv6w7kj': { label: 'Bitget',     type: 'exchange' },
  'cosmos1sufkm72dw7ua9crpfhhp0dqpyuggtlhdse98e7': { label: 'ICF 1',      type: 'foundation' },
  'cosmos1z6czaavlk6kjd48rpf58kqqw9ssad2uaxnazgl': { label: 'ICF 2',      type: 'foundation' },
};

// ── PARSE CSV ──
function parseCSV(filepath) {
  const raw = readFileSync(filepath, 'utf8');
  const lines = raw.trim().split('\n');
  const header = lines[0].split(',');
  return lines.slice(1).map(line => {
    const vals = line.split(',');
    const obj = {};
    header.forEach((h, i) => obj[h.trim().replace(/"/g, '')] = vals[i]?.trim().replace(/"/g, '') || '');
    return obj;
  });
}

// ── MAIN ──
function main() {
  // 1. Read the big address file
  const addrFile = resolve(__dirname, '..', '..', 'Cosmos ATOM Addresses.csv');
  console.log(`📖 Reading ${addrFile}...`);
  const allAddrs = parseCSV(addrFile);
  console.log(`   ${allAddrs.length.toLocaleString()} addresses loaded`);

  // 2. Read the curated whale tracker
  const whaleFile = resolve(__dirname, '..', '..', 'ATOM Whale Balances Tracker Mar 2025 to Mar 2026.csv');
  let curatedWhales = new Set();
  try {
    const wRows = parseCSV(whaleFile);
    wRows.forEach(r => { if (r.address) curatedWhales.add(r.address); });
    console.log(`   ${curatedWhales.size} curated whale addresses loaded`);
  } catch (e) {
    console.warn(`   ⚠️ Whale tracker file not found, continuing without it`);
  }

  // 3. Classify addresses
  const stakers = [];
  const exchanges = [];
  const mixed = [];
  const exchangeAddrs = new Set();

  for (const row of allAddrs) {
    const addr = row.address;
    if (!addr || !addr.startsWith('cosmos1')) continue;

    const staked = parseFloat(row.totalStake) || 0;
    const liquid = parseFloat(row.notStaked) || 0;
    const total  = parseFloat(row.totalBalance) || 0;
    const known  = KNOWN_LABELS[addr];

    // Exchange detection: known label OR (high liquid, zero staked)
    if (known?.type === 'exchange' || (staked === 0 && liquid >= EXCHANGE_MIN)) {
      exchangeAddrs.add(addr);
      exchanges.push({
        address: addr,
        label: known?.label || null,
        type: 'exchange',
        liquid: Math.round(liquid),
        total: Math.round(total),
      });
      continue;
    }

    // Foundation
    if (known?.type === 'foundation') {
      stakers.push({
        address: addr,
        label: known.label,
        type: 'foundation',
        staked: Math.round(staked),
        liquid: Math.round(liquid),
        total: Math.round(total),
        curated: curatedWhales.has(addr),
      });
      continue;
    }

    // Whale staker
    if (staked >= STAKER_MIN) {
      stakers.push({
        address: addr,
        label: known?.label || null,
        type: 'staker',
        staked: Math.round(staked),
        liquid: Math.round(liquid),
        total: Math.round(total),
        curated: curatedWhales.has(addr),
        liquidRatio: total > 0 ? parseFloat((liquid / total).toFixed(4)) : 0,
      });
      continue;
    }

    // Mixed (some staked, some liquid, both significant)
    if (staked >= MIXED_MIN && liquid >= MIXED_MIN) {
      mixed.push({
        address: addr,
        label: known?.label || null,
        type: 'mixed',
        staked: Math.round(staked),
        liquid: Math.round(liquid),
        total: Math.round(total),
        curated: curatedWhales.has(addr),
      });
    }
  }

  // Sort by staked amount descending
  stakers.sort((a, b) => b.staked - a.staked);
  exchanges.sort((a, b) => b.liquid - a.liquid);
  mixed.sort((a, b) => b.total - a.total);

  // 4. Build output
  const output = {
    generated_at: new Date().toISOString(),
    stats: {
      total_addresses_scanned: allAddrs.length,
      whale_stakers: stakers.length,
      exchange_addresses: exchanges.length,
      mixed_addresses: mixed.length,
    },
    // Top 50 stakers for reward analysis
    stakers: stakers.slice(0, 50),
    // All detected exchanges (for send-to-exchange detection)
    exchanges: exchanges.slice(0, 30),
    // Exchange address set (flat list for quick lookup)
    exchange_addresses: [...exchangeAddrs],
    // Mixed
    mixed: mixed.slice(0, 20),
  };

  const outPath = resolve(DATA_DIR, 'whale-watchlist.json');
  writeFileSync(outPath, JSON.stringify(output, null, 2));

  console.log(`\n✅ Whale watchlist saved to ${outPath}`);
  console.log(`   🐋 ${stakers.length} whale stakers (top 50 saved)`);
  console.log(`   🏦 ${exchanges.length} exchange addresses (top 30 saved)`);
  console.log(`   🔀 ${mixed.length} mixed addresses (top 20 saved)`);
  console.log(`\n   Top 5 stakers:`);
  stakers.slice(0, 5).forEach((s, i) =>
    console.log(`     ${i+1}. ${s.label || s.address.slice(0, 20) + '...'} — ${(s.staked / 1e6).toFixed(2)}M staked`)
  );
  console.log(`\n   Top 5 exchanges:`);
  exchanges.slice(0, 5).forEach((e, i) =>
    console.log(`     ${i+1}. ${e.label || e.address.slice(0, 20) + '...'} — ${(e.liquid / 1e6).toFixed(1)}M liquid`)
  );
}

main();
