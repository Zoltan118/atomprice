import { spawnSync } from 'node:child_process';

function runNode(script, env = {}) {
  const res = spawnSync('node', [script], {
    cwd: process.cwd(),
    env: { ...process.env, ...env },
    encoding: 'utf8'
  });
  if (res.status !== 0) {
    throw new Error(`${script} failed:\n${res.stderr || res.stdout || `exit ${res.status}`}`);
  }
  if (res.stdout?.trim()) console.log(res.stdout.trim());
}

function main() {
  // Wider overlap and non-incremental sweep to repair missed windows
  runNode('scripts/v2/ingest-events-v2.mjs', {
    OVERLAP_HOURS: process.env.OVERLAP_HOURS || '168',
    INCREMENTAL: 'false',
    LIMIT_PAGES: process.env.LIMIT_PAGES || '20',
    EXEC_LIMIT_PAGES: process.env.EXEC_LIMIT_PAGES || '6'
  });

  // Rebuild all user-facing files
  runNode('scripts/v2/rebuild-derived-v2.mjs', {
    RUN_PENDING: process.env.RUN_PENDING || 'true',
    RUN_UNBONDING: process.env.RUN_UNBONDING || 'true'
  });

  // Recompute health summary
  runNode('scripts/v2/reconcile-health-v2.mjs');

  console.log('✅ v2 backfill repair completed');
}

try {
  main();
} catch (err) {
  console.error('❌ v2 backfill repair failed:', err);
  process.exit(1);
}
