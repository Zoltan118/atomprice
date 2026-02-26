import fs from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const ROOT = process.cwd();
const LEDGER_DIR = path.join(ROOT, 'data', 'ledger');
const OUT_FEED = path.join(ROOT, 'data', 'delegation_feed.json');
const OUT_RAW = path.join(ROOT, 'data', 'delegation-events-raw.json');
const OUT_HOURLY = path.join(ROOT, 'data', 'delegation-flow-hourly.json');
const OUT_DAILY = path.join(ROOT, 'data', 'delegation-flow-daily.json');
const OUT_WHALE = path.join(ROOT, 'data', 'whale-events.json');
const SOURCE_STATUS = path.join(ROOT, 'data', 'source-status.json');
const VALIDATOR_CACHE_FILE = path.join(ROOT, 'data', 'validator_cache.json');
const REST_BASE = (process.env.REST_BASE || 'https://rest.cosmos.directory/cosmoshub').replace(/\/+$/, '');

const FEED_KEEP = Number(process.env.FEED_KEEP ?? '1000');
const WHALE_FEED_MIN = Number(process.env.WHALE_FEED_MIN ?? '50000');
const WHALE_FEED_DAYS = Number(process.env.WHALE_FEED_DAYS ?? '30');
const WHALE_EVENT_MIN = Number(process.env.WHALE_EVENT_MIN ?? '50000');
const RAW_KEEP_DAYS = Number(process.env.RAW_KEEP_DAYS ?? '365');
const HOURLY_KEEP_DAYS = Number(process.env.HOURLY_KEEP_DAYS ?? '370');
const RUN_PENDING = String(process.env.RUN_PENDING ?? 'true').toLowerCase() !== 'false';
const RUN_UNBONDING = String(process.env.RUN_UNBONDING ?? 'true').toLowerCase() !== 'false';

const validatorCache = {};

function toIsoHour(iso) {
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return null;
  d.setUTCMinutes(0, 0, 0);
  return d.toISOString();
}

function toIsoDay(iso) {
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

async function readJsonSafe(file, fallback) {
  try { return JSON.parse(await fs.readFile(file, 'utf8')); } catch { return fallback; }
}

async function loadValidatorCache() {
  const data = await readJsonSafe(VALIDATOR_CACHE_FILE, {});
  Object.assign(validatorCache, data || {});
}

async function saveValidatorCache() {
  await fs.writeFile(VALIDATOR_CACHE_FILE, JSON.stringify(validatorCache, null, 2));
}

async function resolveMoniker(valoperAddr) {
  if (!valoperAddr) return '';
  if (validatorCache[valoperAddr]) return validatorCache[valoperAddr];
  try {
    const res = await fetch(`${REST_BASE}/cosmos/staking/v1beta1/validators/${valoperAddr}`, { headers: { accept: 'application/json' } });
    if (!res.ok) return '';
    const d = await res.json();
    const moniker = d?.validator?.description?.moniker || '';
    if (moniker) validatorCache[valoperAddr] = moniker;
    return moniker;
  } catch {
    return '';
  }
}

async function ensureValidatorNames(items) {
  const unknown = new Set();
  for (const e of items) {
    const addr = e.validator_addr || '';
    if (!addr) continue;
    if (e.validator_name) {
      validatorCache[addr] = e.validator_name;
      continue;
    }
    if (validatorCache[addr]) {
      e.validator_name = validatorCache[addr];
      continue;
    }
    unknown.add(addr);
  }

  for (const addr of unknown) {
    const moniker = await resolveMoniker(addr);
    if (moniker) validatorCache[addr] = moniker;
  }

  for (const e of items) {
    if (!e.validator_name && e.validator_addr && validatorCache[e.validator_addr]) {
      e.validator_name = validatorCache[e.validator_addr];
    }
  }
}

async function listLedgerFiles() {
  if (!existsSync(LEDGER_DIR)) return [];
  const names = await fs.readdir(LEDGER_DIR);
  return names
    .filter((n) => /^events-\d{4}-\d{2}\.jsonl$/.test(n))
    .sort()
    .map((n) => path.join(LEDGER_DIR, n));
}

async function loadLedgerEvents() {
  const files = await listLedgerFiles();
  const dedup = new Map();
  for (const file of files) {
    const txt = await fs.readFile(file, 'utf8');
    for (const line of txt.split('\n')) {
      if (!line.trim()) continue;
      try {
        const row = JSON.parse(line);
        if (!row?.id) continue;
        dedup.set(row.id, row);
      } catch {
        // ignore bad line
      }
    }
  }
  return Array.from(dedup.values());
}

function aggregateByTime(items, keyFn, includeWindowDays = null) {
  const nowMs = Date.now();
  const cutoffMs = includeWindowDays ? nowMs - includeWindowDays * 86400000 : null;
  const buckets = new Map();

  for (const item of items) {
    const tsMs = Date.parse(item.timestamp || '');
    if (!Number.isFinite(tsMs)) continue;
    if (cutoffMs && tsMs < cutoffMs) continue;

    const key = keyFn(item.timestamp);
    if (!key) continue;

    if (!buckets.has(key)) {
      buckets.set(key, {
        key,
        delegate_atom: 0,
        undelegate_atom: 0,
        net_atom: 0,
        delegates_count: 0,
        undelegates_count: 0,
        total_count: 0,
      });
    }

    const b = buckets.get(key);
    const amt = Number(item.amount_atom || 0);
    if (!Number.isFinite(amt) || amt <= 0) continue;

    if (item.type === 'delegate') {
      b.delegate_atom += amt;
      b.delegates_count += 1;
      b.net_atom += amt;
    } else if (item.type === 'undelegate') {
      b.undelegate_atom += amt;
      b.undelegates_count += 1;
      b.net_atom -= amt;
    }
    b.total_count += 1;
  }

  return Array.from(buckets.values()).sort((a, b) => String(a.key).localeCompare(String(b.key)));
}

function normalizeFeedItem(e) {
  return {
    type: e.type,
    amount_atom: Number(e.amount_atom || 0),
    delegator: e.delegator || '',
    validator_addr: e.validator_addr || '',
    validator_name: e.validator_name || '',
    height: Number(e.height || 0),
    txhash: e.txhash || '',
    timestamp: e.timestamp || null,
  };
}

function runScript(file, env = {}) {
  const res = spawnSync('node', [file], {
    cwd: ROOT,
    env: { ...process.env, ...env },
    encoding: 'utf8'
  });
  if (res.status !== 0) {
    throw new Error(`${file} failed: ${res.stderr || res.stdout || `exit ${res.status}`}`);
  }
}

async function main() {
  await fs.mkdir(path.join(ROOT, 'data'), { recursive: true });
  await loadValidatorCache();

  const sourceStatus = await readJsonSafe(SOURCE_STATUS, {});
  let events = await loadLedgerEvents();

  const rawCutoffIso = new Date(Date.now() - RAW_KEEP_DAYS * 86400000).toISOString();
  events = events.filter((e) => !e.timestamp || e.timestamp >= rawCutoffIso);
  await ensureValidatorNames(events);
  events.sort((a, b) => {
    const ta = Date.parse(a.timestamp || 0) || 0;
    const tb = Date.parse(b.timestamp || 0) || 0;
    if (tb !== ta) return tb - ta;
    return Number(b.height || 0) - Number(a.height || 0);
  });

  const recentFeed = events.slice(0, FEED_KEEP);
  const whaleCutoffIso = new Date(Date.now() - WHALE_FEED_DAYS * 86400000).toISOString();
  const whaleFeed = events.filter((e) =>
    Number(e.amount_atom || 0) >= WHALE_FEED_MIN && (!e.timestamp || e.timestamp >= whaleCutoffIso)
  );

  const feedMap = new Map();
  for (const e of [...recentFeed, ...whaleFeed]) {
    feedMap.set(e.id, normalizeFeedItem(e));
  }

  const feedItems = Array.from(feedMap.values()).sort((a, b) => {
    const ta = Date.parse(a.timestamp || 0) || 0;
    const tb = Date.parse(b.timestamp || 0) || 0;
    if (tb !== ta) return tb - ta;
    return Number(b.height || 0) - Number(a.height || 0);
  });

  const dCount = feedItems.filter((i) => i.type === 'delegate').length;
  const uCount = feedItems.filter((i) => i.type === 'undelegate').length;

  await fs.writeFile(OUT_FEED, JSON.stringify({
    generated_at: new Date().toISOString(),
    min_atom: 1,
    total: feedItems.length,
    delegates: dCount,
    undelegates: uCount,
    ingestion_health: sourceStatus,
    items: feedItems,
  }, null, 2));

  await fs.writeFile(OUT_RAW, JSON.stringify({
    generated_at: new Date().toISOString(),
    timezone: 'UTC',
    min_atom: 1,
    retention_days: RAW_KEEP_DAYS,
    immutable: true,
    total: events.length,
    items: events.map(normalizeFeedItem),
  }, null, 2));

  const hourly = aggregateByTime(events, toIsoHour, HOURLY_KEEP_DAYS);
  const daily = aggregateByTime(events, toIsoDay, null);

  await fs.writeFile(OUT_HOURLY, JSON.stringify({
    generated_at: new Date().toISOString(),
    timezone: 'UTC',
    source: 'event-ledger-v2',
    retention_days: HOURLY_KEEP_DAYS,
    total: hourly.length,
    items: hourly,
  }, null, 2));

  await fs.writeFile(OUT_DAILY, JSON.stringify({
    generated_at: new Date().toISOString(),
    timezone: 'UTC',
    source: 'event-ledger-v2',
    total: daily.length,
    items: daily,
  }, null, 2));

  const yearAgo = new Date(Date.now() - 365 * 86400000).toISOString();
  const whales = events
    .filter((e) => Number(e.amount_atom || 0) >= WHALE_EVENT_MIN)
    .filter((e) => !e.timestamp || e.timestamp >= yearAgo)
    .map((e) => ({
      type: e.type,
      atom: Math.round(Number(e.amount_atom || 0)),
      timestamp: e.timestamp,
      txhash: e.txhash || '',
      validator_addr: e.validator_addr || '',
      validator_name: e.validator_name || '',
      delegator: e.delegator || '',
    }));

  await fs.writeFile(OUT_WHALE, JSON.stringify({
    generated_at: new Date().toISOString(),
    whale_min_atom: WHALE_EVENT_MIN,
    total: whales.length,
    events: whales,
  }, null, 2));

  if (RUN_PENDING) {
    runScript('scripts/fetch-pending-undelegations.mjs', { MIN_ATOM: process.env.PENDING_MIN_ATOM || '100' });
  }
  if (RUN_UNBONDING) {
    runScript('scripts/fetch-unbonding-flows.mjs', {});
  }

  await saveValidatorCache();
  console.log(`✅ v2 rebuild done: events=${events.length}, feed=${feedItems.length}, whales=${whales.length}`);
}

main().catch((err) => {
  console.error('❌ v2 rebuild failed:', err);
  process.exit(1);
});
