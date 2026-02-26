import fs from 'node:fs/promises';
import path from 'node:path';

const ROOT = process.cwd();

async function readJson(file, fallback = null) {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8'));
  } catch {
    return fallback;
  }
}

function minutesSince(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  return Math.round((Date.now() - t) / 60000);
}

function statusByFreshness(mins, okMax) {
  if (mins === null) return 'missing';
  if (mins <= okMax) return 'ok';
  return 'stale';
}

async function main() {
  const source = await readJson(path.join(ROOT, 'data', 'source-status.json'), {});
  const feed = await readJson(path.join(ROOT, 'data', 'delegation_feed.json'), {});
  const pending = await readJson(path.join(ROOT, 'data', 'pending-undelegations.json'), {});
  const unbonding = await readJson(path.join(ROOT, 'data', 'unbonding-flows.json'), {});
  const eventIntelligence = await readJson(path.join(ROOT, 'data', 'event-intelligence.json'), {});

  const freshness = {
    source_mins: minutesSince(source.generated_at),
    feed_mins: minutesSince(feed.generated_at),
    pending_mins: minutesSince(pending.generated_at),
    unbonding_mins: minutesSince(unbonding.generated_at),
    event_intelligence_mins: minutesSince(eventIntelligence.generated_at),
  };

  const checks = {
    source: statusByFreshness(freshness.source_mins, 30),
    feed: statusByFreshness(freshness.feed_mins, 30),
    pending: statusByFreshness(freshness.pending_mins, 90),
    unbonding: statusByFreshness(freshness.unbonding_mins, 90),
    event_intelligence: statusByFreshness(freshness.event_intelligence_mins, 30),
  };

  for (const [name, info] of Object.entries(source?.providers || {})) {
    checks[`provider_${name}`] = info?.ok ? 'ok' : 'degraded';
  }

  if (Number(source?.quorum?.provider_success || 0) === 0) {
    checks.quorum = 'critical';
  } else if (Number(source?.quorum?.provider_success || 0) < Number(source?.quorum?.active_required || 1)) {
    checks.quorum = 'degraded';
  } else {
    checks.quorum = 'ok';
  }

  const degradedCount = Object.values(checks).filter((v) => v !== 'ok').length;
  const overall = degradedCount === 0 ? 'ok' : degradedCount <= 2 ? 'degraded' : 'critical';

  const report = {
    generated_at: new Date().toISOString(),
    overall,
    freshness,
    checks,
    stats: {
      feed_total: Number(feed.total || 0),
      delegates: Number(feed.delegates || 0),
      undelegates: Number(feed.undelegates || 0),
      pending_days: Array.isArray(pending.schedule) ? pending.schedule.length : 0,
      unbonding_days: Array.isArray(unbonding.daily_flows) ? unbonding.daily_flows.length : 0,
      event_intelligence_points: Number(eventIntelligence.total_events || eventIntelligence.sample_size || 0),
    },
    notes: {
      source: 'v2 ledger pipeline',
      guidance: overall === 'ok' ? 'Data healthy' : 'At least one source/check degraded. Keep fallback enabled.'
    }
  };

  await fs.writeFile(path.join(ROOT, 'data', 'ingestion-health.json'), JSON.stringify(report, null, 2));
  console.log(`✅ v2 health: ${overall}`);
}

main().catch((err) => {
  console.error('❌ v2 reconcile failed:', err);
  process.exit(1);
});
