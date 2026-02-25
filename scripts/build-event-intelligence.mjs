// scripts/build-event-intelligence.mjs
// Builds a historical delegation/undelegation intelligence layer with
// volatility-adjusted forward outcomes.
//
// Output:
//   data/event-intelligence.json
//
// Env (optional):
//   MIN_EVENT_ATOM       default: 1
//   BASELINE_LOOKBACK_DAYS default: 30
//   HORIZONS_HOURS       default: "1,4,24,168"

import fs from "node:fs/promises";

const MIN_EVENT_ATOM = Number(process.env.MIN_EVENT_ATOM ?? "1");
const BASELINE_LOOKBACK_DAYS = Number(process.env.BASELINE_LOOKBACK_DAYS ?? "30");
const HORIZONS_HOURS = String(process.env.HORIZONS_HOURS ?? "1,4,24,168")
  .split(",")
  .map((s) => Number(s.trim()))
  .filter((n) => Number.isFinite(n) && n > 0)
  .sort((a, b) => a - b);

const OUT_FILE = "data/event-intelligence.json";
const KRAKEN_OHLC = "https://api.kraken.com/0/public/OHLC?pair=ATOMUSD&interval=60&since=";
const EDGE_MIN_SAMPLES = Number(process.env.EDGE_MIN_SAMPLES ?? "12");
const EDGE_MIN_EXACT_PCT = Number(process.env.EDGE_MIN_EXACT_PCT ?? "0.7");
const ICF_EXCLUDED_DELEGATORS = new Set([
  "cosmos1sufkm72dw7ua9crpfhhp0dqpyuggtlhdse98e7",
  "cosmos1z6czaavlk6kjd48rpf58kqqw9ssad2uaxnazgl",
]);

function median(arr) {
  if (!arr.length) return null;
  const s = arr.slice().sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? (s[m - 1] + s[m]) / 2 : s[m];
}

function mean(arr) {
  if (!arr.length) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function stdev(arr) {
  if (arr.length < 2) return null;
  const m = mean(arr);
  const v = mean(arr.map((x) => (x - m) ** 2));
  return Math.sqrt(v);
}

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function shortAtom(n) {
  if (!Number.isFinite(n)) return "—";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(Math.round(n));
}

async function readJson(path, fallback) {
  try {
    const txt = await fs.readFile(path, "utf8");
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}

async function fetchKrakenHourlyCandles(sinceSec) {
  const nowSec = Math.floor(Date.now() / 1000);
  const seen = new Set();
  const candles = [];
  let cursor = sinceSec;

  for (let i = 0; i < 80 && cursor < nowSec - 3600; i++) {
    const res = await fetch(`${KRAKEN_OHLC}${cursor}`);
    if (!res.ok) throw new Error(`Kraken HTTP ${res.status}`);
    const json = await res.json();
    if (json?.error?.length) throw new Error(`Kraken error: ${json.error.join(", ")}`);
    const rows = json?.result?.ATOMUSD || [];
    if (!rows.length) break;

    for (const r of rows) {
      const time = Number(r[0]);
      if (!Number.isFinite(time) || seen.has(time)) continue;
      seen.add(time);
      candles.push({
        time,
        close: Number(r[4]),
      });
    }

    const lastTime = Number(rows[rows.length - 1][0]);
    const nextCursor = Number(json?.result?.last || (lastTime + 1));
    if (!Number.isFinite(nextCursor) || nextCursor <= cursor) break;
    cursor = nextCursor;
    if (nextCursor >= nowSec - 3600) break;
  }

  candles.sort((a, b) => a.time - b.time);
  return candles.filter((c) => Number.isFinite(c.close) && c.close > 0);
}

function findIndexAtOrAfter(times, target) {
  let lo = 0;
  let hi = times.length - 1;
  if (!times.length) return -1;
  if (times[hi] < target) return -1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (times[mid] < target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function buildEventSet({ topDelegations, whaleEvents, whalePending, pendingUndelegations, rawEvents }) {
  const out = [];

  for (const e of rawEvents?.items || []) {
    out.push({
      category: e.type === "delegate" ? "delegation" : "undelegation_initiated",
      stage: "initiation",
      atom: Number(e.amount_atom || 0),
      timestamp: e.timestamp || null,
      delegator: e.delegator || "",
      estimated: false,
      txhash: e.txhash || "",
      source: "raw_archive",
    });
  }

  for (const d of topDelegations?.delegations || []) {
    out.push({
      category: "delegation",
      stage: "initiation",
      atom: Number(d.amount_atom || 0),
      timestamp: d.timestamp || null,
      delegator: d.delegator || "",
      estimated: false,
      txhash: d.txhash || "",
      source: "top_delegations",
    });
  }

  for (const e of whaleEvents?.events || []) {
    out.push({
      category: e.type === "delegate" ? "delegation" : "undelegation_initiated",
      stage: "initiation",
      atom: Number(e.atom || 0),
      timestamp: e.timestamp || null,
      delegator: e.delegator || "",
      estimated: false,
      txhash: e.txhash || "",
      source: "whale_events",
    });
  }

  for (const e of whalePending?.events || []) {
    out.push({
      category: "undelegation_completed",
      stage: "unlock",
      atom: Number(e.atom || 0),
      timestamp: e.timestamp || null,
      delegator: e.delegator || "",
      estimated: false,
      txhash: e.txhash || "",
      source: "whale_pending",
    });
  }

  const pendingSchedule = pendingUndelegations?.schedule_excluding_icf || pendingUndelegations?.schedule || [];
  for (const d of pendingSchedule) {
    out.push({
      category: "undelegation_completed",
      stage: "unlock",
      atom: Number(d.atom || 0),
      timestamp: d.date ? `${d.date}T12:00:00.000Z` : null,
      estimated: true,
      txhash: "",
      source: "pending_schedule_daily",
    });
  }

  const dedupe = new Set();
  const filtered = [];
  for (const e of out) {
    const delegator = String(e.delegator || "").toLowerCase();
    if (delegator && ICF_EXCLUDED_DELEGATORS.has(delegator)) continue;
    const tsMs = e.timestamp ? Date.parse(e.timestamp) : NaN;
    if (!Number.isFinite(tsMs)) continue;
    if (!Number.isFinite(e.atom) || e.atom < MIN_EVENT_ATOM) continue;
    const key = e.txhash
      ? `${e.category}:${e.txhash}`
      : `${e.category}:${Math.round(e.atom)}:${Math.floor(tsMs / 3600000)}`;
    if (dedupe.has(key)) continue;
    dedupe.add(key);
    filtered.push({
      ...e,
      ts: Math.floor(tsMs / 1000),
    });
  }

  filtered.sort((a, b) => a.ts - b.ts);
  const exactUnlockDays = new Set(
    filtered
      .filter((e) => e.category === "undelegation_completed" && !e.estimated)
      .map((e) => new Date(e.ts * 1000).toISOString().slice(0, 10))
  );

  // If we already have an exact unlock event for a day, drop estimated daily schedule
  // rows for the same day from edge/backtest inputs.
  return filtered.filter((e) => {
    if (e.category !== "undelegation_completed") return true;
    if (!e.estimated) return true;
    if (e.source !== "pending_schedule_daily") return true;
    const day = new Date(e.ts * 1000).toISOString().slice(0, 10);
    return !exactUnlockDays.has(day);
  });
}

function buildEventOutcomes(events, candles) {
  const times = candles.map((c) => c.time);
  const closes = candles.map((c) => c.close);
  const maxH = Math.max(...HORIZONS_HOURS) * 3600;
  const lookbackSec = BASELINE_LOOKBACK_DAYS * 86400;
  const firstCandle = times[0];
  const lastCandle = times[times.length - 1];

  const localVol24hAt = (ts) => {
    const baseIdx = findIndexAtOrAfter(times, ts);
    if (baseIdx <= 1) return null;
    const fromIdx = Math.max(1, baseIdx - 24);
    const rets = [];
    for (let i = fromIdx; i <= baseIdx; i++) {
      const prev = closes[i - 1];
      const curr = closes[i];
      if (!Number.isFinite(prev) || prev <= 0 || !Number.isFinite(curr) || curr <= 0) continue;
      rets.push((curr / prev) - 1);
    }
    if (rets.length < 12) return null;
    return stdev(rets);
  };

  const enriched = events.map((e) => {
    if (e.ts < firstCandle || e.ts > lastCandle) {
      return {
        ...e,
        outcomes: {},
        baseline_samples: 0,
        timestamp_quality: e.estimated ? "estimated" : "exact",
        price_at_event: null,
        price_time: null,
        local_vol_24h: null,
      };
    }
    const baseIdx = findIndexAtOrAfter(times, e.ts);
    if (baseIdx < 0) {
      return {
        ...e,
        outcomes: {},
        baseline_samples: 0,
        timestamp_quality: e.estimated ? "estimated" : "exact",
        price_at_event: null,
        price_time: null,
        local_vol_24h: null,
      };
    }
    const baseTime = times[baseIdx];
    const baseClose = closes[baseIdx];
    const outcomes = {};
    let baselineSamplesMax = 0;

    for (const h of HORIZONS_HOURS) {
      const hSec = h * 3600;
      const futureIdx = findIndexAtOrAfter(times, e.ts + hSec);
      if (futureIdx < 0) {
        outcomes[`h${h}`] = { return: null, alpha: null, z: null, samples: 0 };
        continue;
      }

      const actualReturn = (closes[futureIdx] / baseClose) - 1;

      const fromTs = e.ts - lookbackSec;
      const samples = [];
      for (let i = 0; i < times.length; i++) {
        const t = times[i];
        if (t >= e.ts) break;
        if (t < fromTs) continue;
        const fIdx = findIndexAtOrAfter(times, t + hSec);
        if (fIdx < 0) continue;
        if (times[fIdx] - (t + hSec) > 3 * 3600) continue;
        samples.push((closes[fIdx] / closes[i]) - 1);
      }

      const m = mean(samples);
      const s = stdev(samples);
      const alpha = m === null ? null : (actualReturn - m);
      const z = (alpha === null || !s || s <= 0) ? null : (alpha / s);
      baselineSamplesMax = Math.max(baselineSamplesMax, samples.length);

      outcomes[`h${h}`] = {
        return: actualReturn,
        alpha,
        z,
        samples: samples.length,
        base_time: baseTime,
        future_time: times[futureIdx],
      };
    }

    return {
      ...e,
      outcomes,
      baseline_samples: baselineSamplesMax,
      timestamp_quality: e.estimated ? "estimated" : "exact",
      price_at_event: baseClose,
      price_time: baseTime,
      local_vol_24h: localVol24hAt(e.ts),
    };
  });

  return {
    events: enriched,
    candles,
    horizon_max_sec: maxH,
  };
}

function summarizeByCategory(enrichedEvents) {
  const categories = ["delegation", "undelegation_initiated", "undelegation_completed"];
  const out = {};

  for (const cat of categories) {
    const evs = enrichedEvents.filter((e) => e.category === cat);
    const horizons = {};
    for (const h of HORIZONS_HOURS) {
      const key = `h${h}`;
      const rows = evs
        .map((e) => e.outcomes?.[key])
        .filter((x) => x && Number.isFinite(x.return));

      const rets = rows.map((r) => r.return);
      const alphas = rows.map((r) => r.alpha).filter((v) => Number.isFinite(v));
      const zs = rows.map((r) => r.z).filter((v) => Number.isFinite(v));
      const wins = rets.filter((r) => r > 0).length;

      horizons[key] = {
        count: rets.length,
        win_rate: rets.length ? wins / rets.length : null,
        median_return: rets.length ? median(rets) : null,
        median_alpha: alphas.length ? median(alphas) : null,
        median_z: zs.length ? median(zs) : null,
      };
    }

    out[cat] = {
      events: evs.length,
      exact_timestamps: evs.filter((e) => !e.estimated).length,
      estimated_timestamps: evs.filter((e) => e.estimated).length,
      horizons,
    };
  }

  return out;
}

function buildRegimeSplit(enrichedEvents) {
  const categories = ["delegation", "undelegation_completed"];
  const out = {};
  for (const cat of categories) {
    const rows = enrichedEvents
      .filter((e) => e.category === cat && !e.estimated)
      .map((e) => {
        const h24 = e.outcomes?.h24;
        const edge = Number.isFinite(h24?.alpha) ? h24.alpha : h24?.return;
        return {
          vol: e.local_vol_24h,
          ret: h24?.return,
          edge,
        };
      })
      .filter((r) => Number.isFinite(r.vol) && Number.isFinite(r.ret) && Number.isFinite(r.edge));

    if (!rows.length) {
      out[cat] = {
        threshold_vol_24h: null,
        low_vol: { count: 0, median_edge: null, win_rate: null },
        high_vol: { count: 0, median_edge: null, win_rate: null },
      };
      continue;
    }

    const threshold = median(rows.map((r) => r.vol));
    const low = rows.filter((r) => r.vol <= threshold);
    const high = rows.filter((r) => r.vol > threshold);
    const summarize = (arr) => ({
      count: arr.length,
      median_edge: arr.length ? median(arr.map((r) => r.edge)) : null,
      win_rate: arr.length ? (arr.filter((r) => r.ret > 0).length / arr.length) : null,
    });

    out[cat] = {
      threshold_vol_24h: threshold,
      low_vol: summarize(low),
      high_vol: summarize(high),
    };
  }
  return out;
}

function buildStability(enrichedEvents, category) {
  const rows = enrichedEvents
    .filter((e) => e.category === category && !e.estimated)
    .map((e) => {
      const h24 = e.outcomes?.h24;
      const edge = Number.isFinite(h24?.alpha) ? h24.alpha : h24?.return;
      return { ts: e.ts, edge };
    })
    .filter((r) => Number.isFinite(r.edge))
    .sort((a, b) => a.ts - b.ts);

  const n = rows.length;
  if (!n) return { label: "weak", n: 0, drift: null };

  const mid = Math.floor(n / 2);
  const firstHalf = rows.slice(0, mid).map((r) => r.edge);
  const secondHalf = rows.slice(mid).map((r) => r.edge);
  const m1 = median(firstHalf);
  const m2 = median(secondHalf);
  const drift = (Number.isFinite(m1) && Number.isFinite(m2)) ? Math.abs(m2 - m1) : null;

  let label = "weak";
  if (Number.isFinite(drift)) {
    if (n >= 30 && drift <= 0.005) label = "strong";
    else if (n >= 18 && drift <= 0.015) label = "medium";
  }

  return { label, n, drift };
}

function buildRecentBiasFromEvents(enrichedEvents) {
  const now = Math.floor(Date.now() / 1000);
  const since = now - 7 * 86400;
  const recent = enrichedEvents.filter((e) => e.ts >= since);

  let delegate = 0;
  let undelegateStart = 0;
  let undelegateComplete = 0;
  for (const e of recent) {
    if (e.category === "delegation") delegate += e.atom;
    else if (e.category === "undelegation_initiated") undelegateStart += e.atom;
    else if (e.category === "undelegation_completed") undelegateComplete += e.atom;
  }

  const net = delegate - undelegateStart - undelegateComplete;
  const score = clamp(Math.tanh(net / 2_000_000) * 100, -100, 100);
  const label = score >= 20 ? "Bullish" : score <= -20 ? "Bearish" : "Neutral";

  return {
    window: "7d",
    delegation_atom: delegate,
    undelegation_started_atom: undelegateStart,
    undelegation_completed_atom: undelegateComplete,
    net_atom: net,
    bias_score: score,
    bias_label: label,
  };
}

function pickEdgeCard(summary, coverage, key) {
  const h24 = summary?.[key]?.horizons?.h24;
  if (!h24 || !h24.count) {
    return {
      count: 0,
      edge: null,
      win_rate: null,
      confidence: "Low",
      qualified: false,
      gate_reason: "No 24h sample yet",
      mode: "exact_only",
      window: "24h",
    };
  }
  const edge = Number.isFinite(h24.median_alpha) ? h24.median_alpha : h24.median_return;
  const c = h24.count;
  const confidence = c >= 30 ? "High" : c >= 12 ? "Medium" : "Low";
  const exactPct = Number(coverage?.exact_pct || 0);
  const qualified = c >= EDGE_MIN_SAMPLES && exactPct >= EDGE_MIN_EXACT_PCT;
  const reasons = [];
  if (c < EDGE_MIN_SAMPLES) reasons.push(`n<${EDGE_MIN_SAMPLES}`);
  if (exactPct < EDGE_MIN_EXACT_PCT) reasons.push(`exact<${Math.round(EDGE_MIN_EXACT_PCT * 100)}%`);
  return {
    count: c,
    edge,
    win_rate: h24.win_rate,
    confidence,
    qualified,
    gate_reason: qualified ? "Qualified" : `Calibrating (${reasons.join(", ")})`,
    mode: "exact_only",
    window: "24h",
  };
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const topDelegations = await readJson("data/top-delegations.json", { delegations: [] });
  const whaleEvents = await readJson("data/whale-events.json", { events: [] });
  const whalePending = await readJson("data/whale-pending.json", { events: [] });
  const pendingUndelegations = await readJson("data/pending-undelegations.json", { schedule: [] });
  const rawEvents = await readJson("data/delegation-events-raw.json", { items: [] });
  const flowHourly = await readJson("data/delegation-flow-hourly.json", { items: [] });
  const flowDaily = await readJson("data/delegation-flow-daily.json", { items: [] });

  const events = buildEventSet({ topDelegations, whaleEvents, whalePending, pendingUndelegations, rawEvents });
  if (!events.length) {
    const out = {
      generated_at: new Date().toISOString(),
      timezone: "UTC",
      config: { min_event_atom: MIN_EVENT_ATOM, horizons_hours: HORIZONS_HOURS, baseline_lookback_days: BASELINE_LOOKBACK_DAYS },
      error: "No eligible events found",
      events_total: 0,
    };
    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
    console.log("⚠️ No eligible events found.");
    return;
  }

  const earliest = events[0].ts;
  const sinceSec = Math.max(0, earliest - (BASELINE_LOOKBACK_DAYS + 2) * 86400);
  const candles = await fetchKrakenHourlyCandles(sinceSec);
  if (!candles.length) throw new Error("No Kraken candles loaded");

  const built = buildEventOutcomes(events, candles);
  const summaryAll = summarizeByCategory(built.events);
  const exactEvents = built.events.filter((e) => !e.estimated);
  const summaryExact = summarizeByCategory(exactEvents);
  const eventBias = buildRecentBiasFromEvents(built.events);

  const now = Date.now();
  const h24Cutoff = now - 24 * 3600000;
  const d7Cutoff = now - 7 * 86400000;
  const flow24 = (flowHourly.items || [])
    .filter((b) => Date.parse(b.key) >= h24Cutoff)
    .reduce((acc, b) => {
      acc.delegate += Number(b.delegate_atom || 0);
      acc.undelegate += Number(b.undelegate_atom || 0);
      acc.net += Number(b.net_atom || 0);
      acc.count += Number(b.total_count || 0);
      acc.delegate_events += Number(b.delegates_count || 0);
      acc.undelegate_events += Number(b.undelegates_count || 0);
      return acc;
    }, { delegate: 0, undelegate: 0, net: 0, count: 0, delegate_events: 0, undelegate_events: 0 });
  const flow7 = (flowDaily.items || [])
    .filter((b) => {
      const ts = Date.parse(`${b.key}T00:00:00.000Z`);
      return Number.isFinite(ts) && ts >= d7Cutoff;
    })
    .reduce((acc, b) => {
      acc.delegate += Number(b.delegate_atom || 0);
      acc.undelegate += Number(b.undelegate_atom || 0);
      acc.net += Number(b.net_atom || 0);
      acc.count += Number(b.total_count || 0);
      acc.delegate_events += Number(b.delegates_count || 0);
      acc.undelegate_events += Number(b.undelegates_count || 0);
      return acc;
    }, { delegate: 0, undelegate: 0, net: 0, count: 0, delegate_events: 0, undelegate_events: 0 });

  const flowBiasScore = clamp(Math.tanh((flow7.net || 0) / 2_000_000) * 100, -100, 100);
  const flowBiasLabel = flowBiasScore >= 20 ? "Bullish" : flowBiasScore <= -20 ? "Bearish" : "Neutral";
  const flowBias = {
    window: "7d",
    delegation_atom: flow7.delegate,
    undelegation_started_atom: flow7.undelegate,
    undelegation_completed_atom: 0,
    net_atom: flow7.net,
    bias_score: flowBiasScore,
    bias_label: flowBiasLabel,
  };

  const coverage = {
    delegation: {
      exact_pct: (summaryAll?.delegation?.events || 0)
        ? (summaryAll.delegation.exact_timestamps / summaryAll.delegation.events)
        : 0,
    },
    undelegation_completed: {
      exact_pct: (summaryAll?.undelegation_completed?.events || 0)
        ? (summaryAll.undelegation_completed.exact_timestamps / summaryAll.undelegation_completed.events)
        : 0,
    },
  };
  const delegateCard = pickEdgeCard(summaryExact, coverage.delegation, "delegation");
  const unlockCard = pickEdgeCard(summaryExact, coverage.undelegation_completed, "undelegation_completed");
  const regimeSplit = buildRegimeSplit(built.events);
  const stability = {
    delegation_24h: buildStability(built.events, "delegation"),
    undelegation_completed_24h: buildStability(built.events, "undelegation_completed"),
  };

  const exactCount = built.events.filter((e) => !e.estimated).length;
  const estimatedCount = built.events.filter((e) => e.estimated).length;
  const totalCount = built.events.length;
  const exactPct = totalCount ? exactCount / totalCount : 0;

  const out = {
    generated_at: new Date().toISOString(),
    timezone: "UTC",
    config: {
      min_event_atom: MIN_EVENT_ATOM,
      horizons_hours: HORIZONS_HOURS,
      baseline_lookback_days: BASELINE_LOOKBACK_DAYS,
      price_source: "Kraken OHLC 1h",
      excluded_delegators: {
        icf: Array.from(ICF_EXCLUDED_DELEGATORS),
      },
    },
    data_quality: {
      events_total: totalCount,
      exact_timestamp_count: exactCount,
      estimated_timestamp_count: estimatedCount,
      exact_timestamp_pct: exactPct,
      candle_count: candles.length,
      candle_start: new Date(candles[0].time * 1000).toISOString(),
      candle_end: new Date(candles[candles.length - 1].time * 1000).toISOString(),
    },
    recent_bias: flow7.count ? flowBias : eventBias,
    historical_edge: summaryExact,
    historical_edge_all: summaryAll,
    regimes: regimeSplit,
    stability,
    cards: {
      delegation_24h: delegateCard,
      undelegation_completed_24h: unlockCard,
      flow_24h: {
        net_atom: flow24.net,
        delegation_atom: flow24.delegate,
        undelegation_atom: flow24.undelegate,
        total_events: flow24.count,
        delegation_events: flow24.delegate_events,
        undelegation_events: flow24.undelegate_events,
      },
      flow_7d: {
        net_atom: flow7.net,
        delegation_atom: flow7.delegate,
        undelegation_atom: flow7.undelegate,
        total_events: flow7.count,
        delegation_events: flow7.delegate_events,
        undelegation_events: flow7.undelegate_events,
      },
      bias_7d: {
        label: (flow7.count ? flowBias : eventBias).bias_label,
        score: (flow7.count ? flowBias : eventBias).bias_score,
        net_atom: (flow7.count ? flowBias : eventBias).net_atom,
        text: `${shortAtom(Math.abs((flow7.count ? flowBias : eventBias).net_atom))} ATOM ${(flow7.count ? flowBias : eventBias).net_atom >= 0 ? "net delegation" : "net undelegation"} (7d)`,
      },
      quality_gate: {
        min_samples: EDGE_MIN_SAMPLES,
        min_exact_pct: EDGE_MIN_EXACT_PCT,
      },
    },
    events: built.events
      .slice(-400)
      .map((e) => ({
        category: e.category,
        atom: e.atom,
        timestamp: new Date(e.ts * 1000).toISOString(),
        estimated: e.estimated,
        source: e.source,
        txhash: e.txhash || "",
        outcomes: e.outcomes,
      })),
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
  console.log(`✅ Event intelligence saved: ${OUT_FILE} (events=${totalCount}, candles=${candles.length})`);
}

main().catch((err) => {
  console.error("❌ Event intelligence build failed:", err?.message || err);
  process.exit(1);
});
