import fs from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';

const ROOT = process.cwd();
const LEDGER_DIR = path.join(ROOT, 'data', 'ledger');
const STATE_FILE = path.join(LEDGER_DIR, 'state.json');
const SOURCE_STATUS_FILE = path.join(ROOT, 'data', 'source-status.json');

const OVERLAP_HOURS = Number(process.env.OVERLAP_HOURS ?? '6');
const FEED_MIN = String(process.env.FEED_MIN ?? '1');
const FEED_KEEP = String(process.env.FEED_KEEP ?? '1000');
const WHALE_FEED_MIN = String(process.env.WHALE_FEED_MIN ?? '50000');
const LIMIT_PAGES = String(process.env.LIMIT_PAGES ?? '10');
const EXEC_LIMIT_PAGES = String(process.env.EXEC_LIMIT_PAGES ?? '2');
const REST_BASE = (process.env.REST_BASE || 'https://rest.cosmos.directory/cosmoshub').replace(/\/+$/, '');

const RPC_PROVIDERS = (process.env.RPC_PROVIDERS || process.env.RPC_BASES || [
  'https://cosmos-rpc.publicnode.com',
  'https://rpc.cosmos.directory/cosmoshub',
  'https://rpc.silknodes.io/cosmos'
].join(','))
  .split(',')
  .map((s) => s.trim().replace(/\/+$/, ''))
  .filter(Boolean);

const RPC_QUORUM_MIN = Math.max(1, Number(process.env.RPC_QUORUM_MIN ?? '2'));

const nowIso = new Date().toISOString();

function hashId(parts) {
  return crypto.createHash('sha1').update(parts.join('|')).digest('hex');
}

function normalizeType(v) {
  const t = String(v || '').toLowerCase();
  if (t.includes('undelegate') || t.includes('unbond')) return 'undelegate';
  if (t.includes('delegate')) return 'delegate';
  return null;
}

function normalizeEvent(ev, source) {
  const type = normalizeType(ev.type || ev.msg_type || ev.action || ev.event_type);
  if (!type) return null;

  const txhash = String(ev.txhash || ev.tx_hash || ev.hash || ev.transaction_hash || '').toUpperCase();
  const msgIndex = Number(ev.msg_index ?? ev.message_index ?? 0);
  const eventIndex = Number(ev.event_index ?? ev.log_index ?? 0);
  const amountAtom = Number(ev.amount_atom ?? ev.amount ?? ev.atom ?? 0);
  const timestampRaw = ev.timestamp || ev.block_time || ev.time || ev.completion_time;
  const timestamp = timestampRaw ? new Date(timestampRaw).toISOString() : null;
  const height = Number(ev.height || ev.block_height || 0) || 0;
  const delegator = String(ev.delegator || ev.delegator_address || ev.address || '').toLowerCase();
  const validatorAddr = String(ev.validator_addr || ev.validator || ev.validator_address || '');
  const validatorName = String(ev.validator_name || ev.validator_moniker || '');

  if (!Number.isFinite(amountAtom) || amountAtom <= 0) return null;
  if (!timestamp && !height) return null;

  const id = hashId([
    'cosmoshub-4',
    txhash,
    String(msgIndex),
    String(eventIndex),
    type,
    delegator,
    validatorAddr,
    amountAtom.toFixed(6),
    String(height)
  ]);

  return {
    id,
    chain_id: 'cosmoshub-4',
    source,
    type,
    txhash,
    msg_index: msgIndex,
    event_index: eventIndex,
    timestamp,
    height,
    delegator,
    validator_addr: validatorAddr,
    validator_name: validatorName,
    amount_atom: amountAtom,
    ingested_at: nowIso
  };
}

async function readJsonSafe(file, fallback) {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8'));
  } catch {
    return fallback;
  }
}

function getPartitionKey(iso) {
  const d = new Date(iso || nowIso);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

async function loadExistingIdsForPartitions(partitions) {
  const ids = new Set();
  for (const p of partitions) {
    const file = path.join(LEDGER_DIR, `events-${p}.jsonl`);
    if (!existsSync(file)) continue;
    const text = await fs.readFile(file, 'utf8');
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      try {
        const row = JSON.parse(line);
        if (row?.id) ids.add(row.id);
      } catch {
        // ignore bad lines
      }
    }
  }
  return ids;
}

function providerName(url) {
  try {
    return new URL(url).hostname;
  } catch {
    return url.replace(/^https?:\/\//, '');
  }
}

async function runFetcherForProvider(rpcBase) {
  const provider = providerName(rpcBase);
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), `atomprice-v2-${provider.replace(/[^a-z0-9.-]/gi, '_')}-`));
  try {
    const result = spawnSync('node', [path.join(ROOT, 'scripts', 'fetch-delegation-feed.mjs')], {
      cwd: tmpDir,
      env: {
        ...process.env,
        RPC_BASES: rpcBase,
        REST_BASE,
        FEED_MIN,
        FEED_KEEP,
        WHALE_FEED_MIN,
        LIMIT_PAGES,
        EXEC_LIMIT_PAGES,
        INCREMENTAL: 'false',
        RAW_IMMUTABLE: 'false',
        RAW_KEEP_DAYS: '2',
      },
      encoding: 'utf8'
    });

    if (result.status !== 0) {
      return {
        provider,
        rpc_base: rpcBase,
        ok: false,
        error: (result.stderr || result.stdout || `exit ${result.status}`).trim(),
        events: []
      };
    }

    const rawFile = path.join(tmpDir, 'data', 'delegation-events-raw.json');
    const raw = await readJsonSafe(rawFile, { items: [] });
    const events = (raw.items || []).map((r) => normalizeEvent(r, provider)).filter(Boolean);

    return {
      provider,
      rpc_base: rpcBase,
      ok: true,
      error: null,
      events
    };
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true });
  }
}

async function main() {
  await fs.mkdir(LEDGER_DIR, { recursive: true });

  const state = await readJsonSafe(STATE_FILE, {
    last_ingest_at: null,
    cursors: { since: null },
    stats: {}
  });

  const providerRuns = await Promise.allSettled(
    RPC_PROVIDERS.map((rpcBase) => runFetcherForProvider(rpcBase))
  ).then((results) =>
    results.map((r, idx) =>
      r.status === 'fulfilled'
        ? r.value
        : {
            provider: providerName(RPC_PROVIDERS[idx]),
            rpc_base: RPC_PROVIDERS[idx],
            ok: false,
            error: String(r.reason?.message || r.reason || 'unknown provider failure'),
            events: []
          }
    )
  );

  const okRuns = providerRuns.filter((r) => r.ok);
  const dynamicQuorum = okRuns.length >= RPC_QUORUM_MIN ? RPC_QUORUM_MIN : Math.max(1, okRuns.length);

  const evidence = new Map();
  const canonical = new Map();

  for (const run of okRuns) {
    for (const e of run.events) {
      if (!canonical.has(e.id)) canonical.set(e.id, e);
      if (!evidence.has(e.id)) evidence.set(e.id, new Set());
      evidence.get(e.id).add(run.provider);
    }
  }

  const quorumEvents = [];
  let droppedByQuorum = 0;
  for (const [id, ev] of canonical.entries()) {
    const supporters = evidence.get(id)?.size || 0;
    if (supporters >= dynamicQuorum) {
      quorumEvents.push(ev);
    } else {
      droppedByQuorum++;
    }
  }

  const partitions = [...new Set(quorumEvents.map((e) => getPartitionKey(e.timestamp)))];
  const existingIds = await loadExistingIdsForPartitions(partitions);

  const toAppendByPartition = new Map();
  let skippedExisting = 0;
  for (const e of quorumEvents) {
    if (existingIds.has(e.id)) {
      skippedExisting++;
      continue;
    }
    const p = getPartitionKey(e.timestamp);
    if (!toAppendByPartition.has(p)) toAppendByPartition.set(p, []);
    toAppendByPartition.get(p).push(e);
  }

  for (const [p, rows] of toAppendByPartition.entries()) {
    const file = path.join(LEDGER_DIR, `events-${p}.jsonl`);
    rows.sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));
    const lines = rows.map((r) => JSON.stringify(r)).join('\n') + '\n';
    await fs.appendFile(file, lines, 'utf8');
  }

  const appended = Array.from(toAppendByPartition.values()).reduce((s, rows) => s + rows.length, 0);

  const providerStats = Object.fromEntries(providerRuns.map((r) => [r.provider, {
    rpc_base: r.rpc_base,
    ok: r.ok,
    error: r.error,
    events: r.events.length
  }]));

  const supportCounts = Array.from(evidence.values()).map((s) => s.size);
  const agreementAvg = supportCounts.length
    ? Number((supportCounts.reduce((a, b) => a + b, 0) / supportCounts.length).toFixed(3))
    : 0;

  const nextState = {
    ...state,
    last_ingest_at: nowIso,
    cursors: {
      ...(state.cursors || {}),
      since: new Date(Date.now() - OVERLAP_HOURS * 3600 * 1000).toISOString()
    },
    stats: {
      providers_total: providerRuns.length,
      providers_ok: okRuns.length,
      quorum_required: dynamicQuorum,
      candidate_events: canonical.size,
      quorum_events: quorumEvents.length,
      dropped_by_quorum: droppedByQuorum,
      skipped_existing: skippedExisting,
      appended,
      partitions_touched: toAppendByPartition.size,
      agreement_avg_supporters: agreementAvg,
      provider_stats: providerStats
    }
  };

  await fs.writeFile(STATE_FILE, JSON.stringify(nextState, null, 2));

  const sourceStatus = {
    generated_at: nowIso,
    status: okRuns.length >= dynamicQuorum ? 'ok' : (okRuns.length > 0 ? 'degraded' : 'critical'),
    overlap_hours: OVERLAP_HOURS,
    quorum: {
      configured_min: RPC_QUORUM_MIN,
      active_required: dynamicQuorum,
      provider_count: providerRuns.length,
      provider_success: okRuns.length,
      agreement_avg_supporters: agreementAvg,
      dropped_by_quorum: droppedByQuorum
    },
    providers: providerStats,
    ledger: {
      appended,
      skipped_existing: skippedExisting,
      partitions_touched: toAppendByPartition.size
    }
  };

  await fs.writeFile(SOURCE_STATUS_FILE, JSON.stringify(sourceStatus, null, 2));

  console.log(`✅ v2 ingest done: providers_ok=${okRuns.length}/${providerRuns.length}, quorum=${dynamicQuorum}, candidate=${canonical.size}, quorum_events=${quorumEvents.length}, appended=${appended}`);
}

main().catch((err) => {
  console.error('❌ v2 ingest failed:', err);
  process.exit(1);
});
