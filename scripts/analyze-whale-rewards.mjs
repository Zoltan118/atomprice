#!/usr/bin/env node
/**
 * analyze-whale-rewards.mjs
 *
 * Queries Cosmos REST API for each whale staker:
 *   1. MsgWithdrawDelegatorReward txs (reward claims)
 *   2. MsgSend txs (transfers out)
 *
 * Then correlates: after claiming rewards, did the whale
 * send ATOM to an exchange within 24 hours?
 *
 * Output: data/whale-reward-patterns.json
 */

import { readFileSync, writeFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = resolve(__dirname, '..', 'data');

// ── CONFIG ──
const REST_ENDPOINTS = [
  'https://cosmos-rest.publicnode.com',
  'https://api.silknodes.io/cosmos',
];
const SELL_WINDOW_MS = 24 * 60 * 60 * 1000;  // 24 hours
const TX_LIMIT = 100;                         // txs per query (publicnode returns up to 100)
const DELAY_MS = 800;                         // delay between API calls (rate limit)
const MAX_WHALES = 30;                        // top N stakers to analyze

const WITHDRAW_ACTION = '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward';
const SEND_ACTION = '/cosmos.bank.v1beta1.MsgSend';

// ── HELPERS ──
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchFromEndpoints(path) {
  for (const base of REST_ENDPOINTS) {
    try {
      const res = await fetch(base + path, {
        headers: { 'Accept': 'application/json' },
        signal: AbortSignal.timeout(20000),
      });
      if (!res.ok) continue;
      return await res.json();
    } catch (e) {
      // try next
    }
  }
  return null;
}

async function queryTxs(address, action, limit = TX_LIMIT) {
  // Use query= param format (works on publicnode, events= returns 500)
  const q = encodeURIComponent(`message.sender='${address}' AND message.action='${action}'`);
  const path = `/cosmos/tx/v1beta1/txs?query=${q}&pagination.limit=${limit}&order_by=ORDER_BY_DESC`;

  const data = await fetchFromEndpoints(path);
  if (!data || !data.tx_responses) return [];
  return data.tx_responses;
}

function extractTimestamp(txResp) {
  return txResp.timestamp ? new Date(txResp.timestamp).getTime() : null;
}

function extractRewardAmount(txResp) {
  // Look through events for "withdraw_rewards" with amount
  let total = 0;
  for (const log of txResp.logs || []) {
    for (const ev of log.events || []) {
      if (ev.type === 'withdraw_rewards' || ev.type === 'coin_received') {
        for (const attr of ev.attributes || []) {
          if (attr.key === 'amount' && attr.value?.includes('uatom')) {
            const match = attr.value.match(/(\d+)uatom/);
            if (match) total += parseInt(match[1]) / 1e6;
          }
        }
      }
    }
  }
  // Fallback: check raw events
  if (total === 0 && txResp.events) {
    for (const ev of txResp.events) {
      if (ev.type === 'withdraw_rewards' || ev.type === 'coin_received') {
        for (const attr of ev.attributes || []) {
          if (attr.key === 'amount' && attr.value?.includes('uatom')) {
            const match = attr.value.match(/(\d+)uatom/);
            if (match) total += parseInt(match[1]) / 1e6;
          }
        }
      }
    }
  }
  return total;
}

function extractSendDetails(txResp) {
  const sends = [];
  for (const tx of txResp.tx?.body?.messages || []) {
    if (tx['@type'] === SEND_ACTION) {
      const amountObj = tx.amount?.[0];
      if (amountObj?.denom === 'uatom') {
        sends.push({
          to: tx.to_address,
          atom: parseInt(amountObj.amount) / 1e6,
        });
      }
    }
  }
  return sends;
}

// ── MAIN ──
async function main() {
  // Load watchlist
  const watchlistPath = resolve(DATA_DIR, 'whale-watchlist.json');
  const watchlist = JSON.parse(readFileSync(watchlistPath, 'utf8'));

  const exchangeSet = new Set(watchlist.exchange_addresses || []);
  const exchangeMap = {};
  for (const ex of watchlist.exchanges || []) {
    exchangeMap[ex.address] = ex.label || 'Unknown Exchange';
  }

  const whales = watchlist.stakers.slice(0, MAX_WHALES);
  console.log(`\n🐋 Analyzing reward patterns for ${whales.length} whale stakers`);
  console.log(`🏦 ${exchangeSet.size} exchange addresses in detection set\n`);

  const results = [];
  const dayOfWeekClaims = Array(7).fill(0);  // Sun=0 .. Sat=6
  const dayOfWeekSells  = Array(7).fill(0);
  const hourOfDayClaims = Array(24).fill(0);
  const hourOfDaySells  = Array(24).fill(0);

  let totalClaims = 0;
  let totalSells = 0;
  let totalClaimAtom = 0;
  let totalSellAtom = 0;
  let apiErrors = 0;

  for (let i = 0; i < whales.length; i++) {
    const whale = whales[i];
    const addr = whale.address;
    const label = whale.label || addr.slice(0, 14) + '...';
    console.log(`[${i + 1}/${whales.length}] ${label} (${(whale.staked / 1e6).toFixed(2)}M staked)`);

    // 1. Fetch reward claims
    await sleep(DELAY_MS);
    const claimTxs = await queryTxs(addr, WITHDRAW_ACTION);
    if (!claimTxs.length) {
      console.log(`   No reward claims found`);
      continue;
    }
    console.log(`   📥 ${claimTxs.length} reward claims`);

    // 2. Fetch send transactions
    await sleep(DELAY_MS);
    const sendTxs = await queryTxs(addr, SEND_ACTION);
    console.log(`   📤 ${sendTxs.length} send transactions`);

    // Build send timeline: [{timestamp, to, atom}]
    const sendTimeline = [];
    for (const stx of sendTxs) {
      const ts = extractTimestamp(stx);
      if (!ts) continue;
      const sends = extractSendDetails(stx);
      for (const s of sends) {
        sendTimeline.push({ timestamp: ts, ...s });
      }
    }
    sendTimeline.sort((a, b) => a.timestamp - b.timestamp);

    // 3. For each claim, check if there's a send-to-exchange within 24h
    let whaleClaims = 0;
    let whaleSells = 0;
    let whaleClaimAtom = 0;
    let whaleSellAtom = 0;
    const sellEvents = [];

    for (const ctxResp of claimTxs) {
      const claimTs = extractTimestamp(ctxResp);
      if (!claimTs) continue;

      const claimAtom = extractRewardAmount(ctxResp);
      whaleClaims++;
      whaleClaimAtom += claimAtom;

      const claimDate = new Date(claimTs);
      const dow = claimDate.getUTCDay();
      const hour = claimDate.getUTCHours();
      dayOfWeekClaims[dow]++;
      hourOfDayClaims[hour]++;

      // Look for exchange sends within the window AFTER this claim
      for (const send of sendTimeline) {
        const diff = send.timestamp - claimTs;
        if (diff < 0) continue;            // send was before claim
        if (diff > SELL_WINDOW_MS) break;   // beyond 24h window

        if (exchangeSet.has(send.to)) {
          whaleSells++;
          whaleSellAtom += send.atom;
          dayOfWeekSells[dow]++;
          hourOfDaySells[hour]++;
          sellEvents.push({
            claim_time: claimDate.toISOString(),
            sell_time: new Date(send.timestamp).toISOString(),
            delay_hours: (diff / 3600000).toFixed(1),
            atom_sold: Math.round(send.atom),
            exchange: exchangeMap[send.to] || 'Unknown',
          });
          break; // count one sell per claim
        }
      }
    }

    totalClaims += whaleClaims;
    totalSells += whaleSells;
    totalClaimAtom += whaleClaimAtom;
    totalSellAtom += whaleSellAtom;

    const sellRate = whaleClaims > 0 ? (whaleSells / whaleClaims) : 0;
    console.log(`   🎯 ${whaleClaims} claims, ${whaleSells} followed by exchange send (${(sellRate * 100).toFixed(0)}%)`);

    results.push({
      address: addr,
      label: whale.label || null,
      staked: whale.staked,
      claims: whaleClaims,
      claim_atom: Math.round(whaleClaimAtom),
      sells: whaleSells,
      sell_atom: Math.round(whaleSellAtom),
      sell_rate: parseFloat(sellRate.toFixed(3)),
      sell_events: sellEvents.slice(0, 5), // keep last 5 for detail
    });
  }

  // ── BUILD OUTPUT ──
  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const dayOfWeek = dayNames.map((name, i) => ({
    day: name,
    day_short: name.slice(0, 3),
    claims: dayOfWeekClaims[i],
    sells: dayOfWeekSells[i],
    sell_rate: dayOfWeekClaims[i] > 0 ? parseFloat((dayOfWeekSells[i] / dayOfWeekClaims[i]).toFixed(3)) : 0,
  }));

  const hourOfDay = Array.from({ length: 24 }, (_, h) => ({
    hour: h,
    label: `${h.toString().padStart(2, '0')}:00`,
    claims: hourOfDayClaims[h],
    sells: hourOfDaySells[h],
    sell_rate: hourOfDayClaims[h] > 0 ? parseFloat((hourOfDaySells[h] / hourOfDayClaims[h]).toFixed(3)) : 0,
  }));

  // Top sellers (by sell rate, min 3 claims)
  const topSellers = results
    .filter(r => r.claims >= 3)
    .sort((a, b) => b.sell_rate - a.sell_rate);

  // Reward holders (claim but never sell)
  const holders = results
    .filter(r => r.claims >= 3 && r.sells === 0)
    .sort((a, b) => b.claim_atom - a.claim_atom);

  const overallSellRate = totalClaims > 0 ? totalSells / totalClaims : 0;

  const output = {
    generated_at: new Date().toISOString(),
    config: {
      whales_analyzed: whales.length,
      sell_window_hours: SELL_WINDOW_MS / 3600000,
      tx_limit_per_query: TX_LIMIT,
    },
    summary: {
      total_claims: totalClaims,
      total_sells: totalSells,
      overall_sell_rate: parseFloat(overallSellRate.toFixed(3)),
      total_claim_atom: Math.round(totalClaimAtom),
      total_sell_atom: Math.round(totalSellAtom),
      whales_who_sell: results.filter(r => r.sells > 0).length,
      whales_who_hold: results.filter(r => r.sells === 0 && r.claims > 0).length,
    },
    day_of_week: dayOfWeek,
    hour_of_day: hourOfDay,
    top_sellers: topSellers.slice(0, 15),
    diamond_hands: holders.slice(0, 10),
    all_whales: results.filter(r => r.claims > 0).sort((a, b) => b.claims - a.claims),
  };

  const outPath = resolve(DATA_DIR, 'whale-reward-patterns.json');
  writeFileSync(outPath, JSON.stringify(output, null, 2));

  console.log(`\n${'═'.repeat(50)}`);
  console.log(`✅ Analysis complete → ${outPath}`);
  console.log(`${'═'.repeat(50)}`);
  console.log(`   Claims analyzed: ${totalClaims}`);
  console.log(`   Sells detected:  ${totalSells} (${(overallSellRate * 100).toFixed(1)}%)`);
  console.log(`   Claim ATOM:      ${(totalClaimAtom).toLocaleString()}`);
  console.log(`   Sell ATOM:       ${(totalSellAtom).toLocaleString()}`);
  console.log(`   Sellers:         ${results.filter(r => r.sells > 0).length} whales`);
  console.log(`   Diamond hands:   ${holders.length} whales (never sell rewards)`);

  // Peak day
  const peakDay = dayOfWeek.reduce((max, d) => d.sells > max.sells ? d : max, dayOfWeek[0]);
  if (peakDay.sells > 0) {
    console.log(`   Peak sell day:   ${peakDay.day} (${peakDay.sells} sells, ${(peakDay.sell_rate * 100).toFixed(0)}% rate)`);
  }
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
