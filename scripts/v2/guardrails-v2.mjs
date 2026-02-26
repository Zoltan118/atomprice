import fs from 'node:fs';
import path from 'node:path';

const ROOT = process.cwd();
const MODE = (process.argv.find(a => a.startsWith('--mode=')) || '--mode=preflight').split('=')[1];

const ICF = new Set([
  'cosmos1sufkm72dw7ua9crpfhhp0dqpyuggtlhdse98e7',
  'cosmos1z6czaavlk6kjd48rpf58kqqw9ssad2uaxnazgl'
]);

function readJson(p, fallback = null) {
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch { return fallback; }
}

function mustExist(rel, errors) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) errors.push(`Missing required file: ${rel}`);
}

function icfCount(rows, getDelegator) {
  if (!Array.isArray(rows)) return 0;
  return rows.filter(r => ICF.has(String(getDelegator(r) || '').toLowerCase())).length;
}

function runPreflight() {
  const errors = [];
  [
    'scripts/v2/ingest-events-v2.mjs',
    'scripts/v2/rebuild-derived-v2.mjs',
    'scripts/v2/reconcile-health-v2.mjs',
    'scripts/v2/backfill-repair-v2.mjs',
    'scripts/fetch-delegation-feed.mjs',
    'scripts/fetch-pending-undelegations.mjs',
    'scripts/fetch-unbonding-flows.mjs',
    'scripts/build-event-intelligence.mjs',
    'scripts/staking_ratio_updater.mjs',
    'scripts/fx_ecb_updater.mjs',
    'scripts/fetch-daily-metrics.js',
    'scripts/fetch-total-staked.js'
  ].forEach(f => mustExist(f, errors));

  if (errors.length) {
    console.error('❌ Guardrails preflight failed');
    for (const e of errors) console.error(' -', e);
    process.exit(1);
  }
  console.log('✅ Guardrails preflight passed');
}

function runDataChecks() {
  const errors = [];
  const warnings = [];

  const raw = readJson(path.join(ROOT, 'data/delegation-events-raw.json'), { items: [] });
  const feed = readJson(path.join(ROOT, 'data/delegation_feed.json'), { items: [] });
  const whales = readJson(path.join(ROOT, 'data/whale-events.json'), { events: [] });
  const pending = readJson(path.join(ROOT, 'data/pending-undelegations.json'), {});

  const rawIcf = icfCount(raw.items, r => r.delegator);
  const feedIcf = icfCount(feed.items, r => r.delegator);
  const whaleIcf = icfCount(whales.events, r => r.delegator);

  if (rawIcf > 0 && feedIcf === 0) errors.push('ICF exists in raw but not in delegation_feed');
  if (rawIcf > 0 && whaleIcf === 0) errors.push('ICF exists in raw but not in whale-events');

  const day = (pending.schedule || []).find(d => d.date === '2026-03-17');
  const dayEx = (pending.schedule_excluding_icf || []).find(d => d.date === '2026-03-17');
  if (day && dayEx && Number(dayEx.atom || 0) > Number(day.atom || 0)) {
    errors.push('schedule_excluding_icf is greater than schedule for 2026-03-17');
  }

  const quality = readJson(path.join(ROOT, 'data/event-intelligence.json'), {});
  if (!quality?.generated_at) warnings.push('event-intelligence.json missing or not generated');

  console.log(`ℹ️ ICF visibility: raw=${rawIcf}, feed=${feedIcf}, whale=${whaleIcf}`);
  if (warnings.length) for (const w of warnings) console.log('⚠️', w);

  if (errors.length) {
    console.error('❌ Guardrails data check failed');
    for (const e of errors) console.error(' -', e);
    process.exit(1);
  }
  console.log('✅ Guardrails data check passed');
}

if (MODE === 'preflight') runPreflight();
else if (MODE === 'data') runDataChecks();
else {
  console.error(`Unknown mode: ${MODE}`);
  process.exit(1);
}
