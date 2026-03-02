#!/usr/bin/env node
/**
 * monitor-whale-transfers.mjs
 *
 * Polls the latest Cosmos Hub blocks and detects whale activity:
 *   - MsgSend (transfers)
 *   - MsgDelegate / MsgUndelegate / MsgBeginRedelegate
 *   - MsgWithdrawDelegatorReward (reward claims)
 *
 * Writes detected events to data/whale-transfers.json
 *
 * Usage:
 *   node scripts/monitor-whale-transfers.mjs           # one-shot scan (last 20 blocks)
 *   node scripts/monitor-whale-transfers.mjs --watch   # continuous polling every 10s
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = resolve(__dirname, '..', 'data');

// ── CONFIG ──
const REST_ENDPOINTS = [
  'https://cosmos-rest.publicnode.com',
  'https://api.silknodes.io/cosmos',
];
const MIN_ATOM = 10_000;          // minimum ATOM for a transfer to count
const MAX_TRANSFERS = 200;        // max events to keep in file
const POLL_INTERVAL_MS = 10_000;  // poll every 10s in watch mode
const BLOCKS_PER_SCAN = 20;       // blocks to scan per cycle
const WATCH_MODE = process.argv.includes('--watch');

// ── LOAD WATCHLIST ──
const watchlistPath = resolve(DATA_DIR, 'whale-watchlist.json');
let WHALE_ADDRS = new Set();
let EXCHANGE_ADDRS = new Set();
let LABEL_MAP = {};

function loadWatchlist() {
  if (!existsSync(watchlistPath)) {
    console.error('❌ whale-watchlist.json not found. Run build-whale-watchlist.mjs first.');
    process.exit(1);
  }
  const wl = JSON.parse(readFileSync(watchlistPath, 'utf8'));

  // All whale stakers
  for (const s of wl.stakers || []) {
    WHALE_ADDRS.add(s.address);
    if (s.label) LABEL_MAP[s.address] = s.label;
  }
  // Exchanges
  for (const e of wl.exchanges || []) {
    EXCHANGE_ADDRS.add(e.address);
    WHALE_ADDRS.add(e.address);  // also track exchanges
    if (e.label) LABEL_MAP[e.address] = e.label;
  }
  // Mixed
  for (const m of wl.mixed || []) {
    WHALE_ADDRS.add(m.address);
    if (m.label) LABEL_MAP[m.address] = m.label;
  }

  console.log(`📋 Watchlist: ${WHALE_ADDRS.size} addresses (${EXCHANGE_ADDRS.size} exchanges)`);
}

// ── LOAD VALIDATOR CACHE ──
let VALIDATOR_NAMES = {};
function loadValidatorCache() {
  const vPath = resolve(DATA_DIR, 'validator_cache.json');
  if (existsSync(vPath)) {
    VALIDATOR_NAMES = JSON.parse(readFileSync(vPath, 'utf8'));
  }
}

// ── REST API ──
async function fetchRest(path) {
  for (const base of REST_ENDPOINTS) {
    try {
      const res = await fetch(base + path, {
        headers: { 'Accept': 'application/json' },
        signal: AbortSignal.timeout(10000),
      });
      if (!res.ok) continue;
      return await res.json();
    } catch { /* try next */ }
  }
  return null;
}

async function getLatestHeight() {
  const data = await fetchRest('/cosmos/base/tendermint/v1beta1/blocks/latest');
  return parseInt(data?.block?.header?.height) || 0;
}

async function getBlockTxs(height) {
  // Use the query= format that works
  const q = encodeURIComponent(`tx.height=${height}`);
  const data = await fetchRest(`/cosmos/tx/v1beta1/txs?query=${q}&pagination.limit=100`);
  return data?.tx_responses || [];
}

// ── TX PARSING ──
function parseTx(txResp) {
  const events = [];
  const msgs = txResp.tx?.body?.messages || [];
  const height = parseInt(txResp.height) || 0;
  const timestamp = txResp.timestamp || new Date().toISOString();
  const txhash = txResp.txhash || '';

  for (const msg of msgs) {
    const type = msg['@type'];

    // MsgSend
    if (type === '/cosmos.bank.v1beta1.MsgSend') {
      const from = msg.from_address;
      const to = msg.to_address;
      if (!WHALE_ADDRS.has(from) && !WHALE_ADDRS.has(to)) continue;

      const atomAmount = msg.amount?.find(a => a.denom === 'uatom');
      if (!atomAmount) continue;
      const atom = parseInt(atomAmount.amount) / 1e6;
      if (atom < MIN_ATOM) continue;

      events.push({
        type: 'send',
        atom,
        from,
        to,
        from_label: LABEL_MAP[from] || null,
        to_label: LABEL_MAP[to] || null,
        is_exchange_send: EXCHANGE_ADDRS.has(to),
        height,
        timestamp,
        txhash,
      });
    }

    // MsgDelegate
    if (type === '/cosmos.staking.v1beta1.MsgDelegate') {
      const delegator = msg.delegator_address;
      if (!WHALE_ADDRS.has(delegator)) continue;

      const atomAmount = msg.amount;
      if (atomAmount?.denom !== 'uatom') continue;
      const atom = parseInt(atomAmount.amount) / 1e6;
      if (atom < MIN_ATOM) continue;

      events.push({
        type: 'delegate',
        atom,
        from: delegator,
        from_label: LABEL_MAP[delegator] || null,
        validator: msg.validator_address,
        validator_name: VALIDATOR_NAMES[msg.validator_address] || null,
        height,
        timestamp,
        txhash,
      });
    }

    // MsgUndelegate
    if (type === '/cosmos.staking.v1beta1.MsgUndelegate') {
      const delegator = msg.delegator_address;
      if (!WHALE_ADDRS.has(delegator)) continue;

      const atomAmount = msg.amount;
      if (atomAmount?.denom !== 'uatom') continue;
      const atom = parseInt(atomAmount.amount) / 1e6;
      if (atom < MIN_ATOM) continue;

      events.push({
        type: 'undelegate',
        atom,
        from: delegator,
        from_label: LABEL_MAP[delegator] || null,
        validator: msg.validator_address,
        validator_name: VALIDATOR_NAMES[msg.validator_address] || null,
        height,
        timestamp,
        txhash,
      });
    }

    // MsgBeginRedelegate
    if (type === '/cosmos.staking.v1beta1.MsgBeginRedelegate') {
      const delegator = msg.delegator_address;
      if (!WHALE_ADDRS.has(delegator)) continue;

      const atomAmount = msg.amount;
      if (atomAmount?.denom !== 'uatom') continue;
      const atom = parseInt(atomAmount.amount) / 1e6;
      if (atom < MIN_ATOM) continue;

      events.push({
        type: 'redelegate',
        atom,
        from: delegator,
        from_label: LABEL_MAP[delegator] || null,
        validator_from: msg.validator_src_address,
        validator_to: msg.validator_dst_address,
        validator_name: VALIDATOR_NAMES[msg.validator_dst_address] || null,
        height,
        timestamp,
        txhash,
      });
    }

    // MsgWithdrawDelegatorReward — only if reward is large enough
    if (type === '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward') {
      const delegator = msg.delegator_address;
      if (!WHALE_ADDRS.has(delegator)) continue;

      // Extract reward amount from tx events
      let rewardAtom = 0;
      for (const ev of txResp.events || []) {
        if (ev.type === 'withdraw_rewards' || ev.type === 'coin_received') {
          for (const attr of ev.attributes || []) {
            if (attr.key === 'amount' && attr.value?.includes('uatom')) {
              const match = attr.value.match(/(\d+)uatom/);
              if (match) rewardAtom += parseInt(match[1]) / 1e6;
            }
          }
        }
      }
      if (rewardAtom < MIN_ATOM) continue;

      events.push({
        type: 'claim',
        atom: rewardAtom,
        from: delegator,
        from_label: LABEL_MAP[delegator] || null,
        height,
        timestamp,
        txhash,
      });
    }
  }

  return events;
}

// ── STATE ──
const outputPath = resolve(DATA_DIR, 'whale-transfers.json');

function loadExisting() {
  if (existsSync(outputPath)) {
    try {
      return JSON.parse(readFileSync(outputPath, 'utf8'));
    } catch { /* corrupt file */ }
  }
  return { transfers: [], last_height: 0, last_scan: null };
}

function save(state) {
  // Dedupe by txhash+type, keep latest MAX_TRANSFERS
  const seen = new Set();
  state.transfers = state.transfers
    .filter(t => {
      const key = `${t.txhash}-${t.type}-${t.from}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
    .slice(0, MAX_TRANSFERS);

  writeFileSync(outputPath, JSON.stringify(state, null, 2));
}

// ── SCAN CYCLE ──
async function scan() {
  const state = loadExisting();
  const latestHeight = await getLatestHeight();

  if (!latestHeight) {
    console.warn('⚠️ Could not get latest block height');
    return state;
  }

  // Determine scan range
  const startHeight = state.last_height > 0
    ? state.last_height + 1
    : latestHeight - BLOCKS_PER_SCAN;
  const endHeight = Math.min(startHeight + BLOCKS_PER_SCAN - 1, latestHeight);

  if (startHeight > latestHeight) {
    return state; // nothing new
  }

  const blocksToScan = endHeight - startHeight + 1;
  console.log(`🔍 Scanning blocks ${startHeight} → ${endHeight} (${blocksToScan} blocks)`);

  let newEvents = 0;
  for (let h = startHeight; h <= endHeight; h++) {
    const txs = await getBlockTxs(h);
    for (const tx of txs) {
      if (tx.code && tx.code !== 0) continue; // skip failed txs
      const events = parseTx(tx);
      if (events.length > 0) {
        state.transfers.push(...events);
        newEvents += events.length;
        for (const ev of events) {
          const label = ev.from_label || ev.from?.slice(0, 14) + '...';
          console.log(`   💸 ${ev.type} · ${fmtAtomLog(ev.atom)} ATOM · ${label} · block ${h}`);
        }
      }
    }
  }

  state.last_height = endHeight;
  state.last_scan = new Date().toISOString();
  save(state);

  if (newEvents > 0) {
    console.log(`✅ ${newEvents} whale events detected`);
  } else {
    console.log(`   No whale activity in this range`);
  }

  return state;
}

function fmtAtomLog(v) {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return `${Math.round(v)}`;
}

// ── MAIN ──
async function main() {
  loadWatchlist();
  loadValidatorCache();

  if (WATCH_MODE) {
    console.log(`\n🔄 Watch mode — polling every ${POLL_INTERVAL_MS / 1000}s (Ctrl+C to stop)\n`);
    while (true) {
      try {
        await scan();
      } catch (e) {
        console.warn(`⚠️ Scan error: ${e.message}`);
      }
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
    }
  } else {
    // One-shot: scan last 20 blocks
    await scan();
    console.log(`\n📁 Output: ${outputPath}`);
    console.log(`💡 Run with --watch for continuous monitoring`);
  }
}

main().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
