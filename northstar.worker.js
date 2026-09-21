/**
 * NORTH STAR — VOLUME MINUTE WORKER v0.1
 *
 * Separate Railway service:
 *   node northstar.worker.js
 *
 * Required:
 *   DATABASE_URL
 *   northstar.minute.sql in the same directory
 *   setup.sql already executed
 *
 * SAFETY:
 * - Dry-run by default.
 * - No checkpoint changes in dry-run mode.
 * - Processes completed minutes only.
 * - Checkpoint and aggregation commit together.
 * - Reprocesses a bounded overlap for late events.
 * - Stops on errors rather than skipping minutes.
 *
 * Companion SQL contract:
 *   $1 = window_start (inclusive)
 *   $2 = window_end   (exclusive)
 *
 * SQL must return exactly one diagnostics row containing:
 *   qualifying_events
 *   minutes_written
 *
 * IMPORTANT:
 * Do not enable writes until northstar.minute.sql has been
 * created, reviewed, and tested against the database.
 */

'use strict';

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const JOB_NAME = 'volume_minutes';

const ENABLE_WRITES =
  process.env.NORTHSTAR_ENABLE_MINUTE_WRITES === 'true';

const POLL_MS = positiveInteger(
  'NORTHSTAR_POLL_MS',
  15000
);

const FINALITY_DELAY_MINUTES = positiveInteger(
  'NORTHSTAR_FINALITY_DELAY_MINUTES',
  2
);

const OVERLAP_MINUTES = nonnegativeInteger(
  'NORTHSTAR_OVERLAP_MINUTES',
  3
);

const MAX_MINUTES_PER_CYCLE = positiveInteger(
  'NORTHSTAR_MAX_MINUTES_PER_CYCLE',
  5
);

const STATEMENT_TIMEOUT_MS = positiveInteger(
  'NORTHSTAR_STATEMENT_TIMEOUT_MS',
  30000
);

const MINUTE_MS = 60 * 1000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 2,
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000,
  application_name: 'northstar-volume-minute-worker'
});

let shuttingDown = false;

function positiveInteger(name, fallback) {
  const raw = process.env[name];

  if (raw === undefined || raw === '') {
    return fallback;
  }

  const value = Number(raw);

  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return value;
}

function nonnegativeInteger(name, fallback) {
  const raw = process.env[name];

  if (raw === undefined || raw === '') {
    return fallback;
  }

  const value = Number(raw);

  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a nonnegative integer`);
  }

  return value;
}

function log(event, details = {}) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    service: 'northstar-volume-minute-worker',
    event,
    ...details
  }));
}

function floorToMinute(date) {
  return new Date(
    Math.floor(date.getTime() / MINUTE_MS) * MINUTE_MS
  );
}

function addMinutes(date, minutes) {
  return new Date(date.getTime() + minutes * MINUTE_MS);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function loadMinuteSQL() {
  const filename = path.join(
    __dirname,
    'northstar.minute.sql'
  );

  if (!fs.existsSync(filename)) {
    if (ENABLE_WRITES) {
      throw new Error(
        'Writes are enabled but northstar.minute.sql is missing'
      );
    }

    log('SQL_NOT_INSTALLED', {
      message: 'Dry-run can continue, but writes cannot be enabled.'
    });

    return null;
  }

  const sql = fs.readFileSync(filename, 'utf8').trim();

  if (!sql) {
    throw new Error('northstar.minute.sql is empty');
  }

  return sql;
}

async function readCheckpoint(client) {
  const result = await client.query(
    `
      SELECT
        job_name,
        bootstrap_at,
        next_minute_at
      FROM public.northstar_worker_checkpoint
      WHERE job_name = $1
    `,
    [JOB_NAME]
  );

  if (result.rowCount !== 1) {
    throw new Error(
      'Worker checkpoint missing. Run setup.sql first.'
    );
  }

  return result.rows[0];
}

function calculateWindow(checkpoint, databaseNow) {
  const bootstrap = new Date(checkpoint.bootstrap_at);
  const nextMinute = new Date(checkpoint.next_minute_at);

  if (
    !Number.isFinite(bootstrap.getTime()) ||
    !Number.isFinite(nextMinute.getTime())
  ) {
    throw new Error('Invalid checkpoint timestamp');
  }

  if (nextMinute.getTime() < bootstrap.getTime()) {
    throw new Error('Checkpoint precedes bootstrap');
  }

  // A minute becomes eligible only after the configured
  // finality delay has elapsed.
  const latestEligibleEnd = floorToMinute(
    addMinutes(databaseNow, -FINALITY_DELAY_MINUTES)
  );

  const windowEnd = addMinutes(nextMinute, 1);

  if (windowEnd.getTime() > latestEligibleEnd.getTime()) {
    return null;
  }

  // Reprocess prior minutes, but never cross bootstrap.
  const proposedStart = addMinutes(
    nextMinute,
    -OVERLAP_MINUTES
  );

  const windowStart = new Date(
    Math.max(
      proposedStart.getTime(),
      bootstrap.getTime()
    )
  );

  return {
    windowStart,
    windowEnd,
    nextMinute,
    nextCheckpoint: windowEnd
  };
}

async function getDatabaseNow(client) {
  const result = await client.query(
    'SELECT clock_timestamp() AS database_now'
  );

  return new Date(result.rows[0].database_now);
}

async function dryRunCycle() {
  const client = await pool.connect();

  try {
    const checkpoint = await readCheckpoint(client);
    const databaseNow = await getDatabaseNow(client);

    const window = calculateWindow(
      checkpoint,
      databaseNow
    );

    if (!window) {
      log('DRY_RUN_CAUGHT_UP', {
        checkpoint: checkpoint.next_minute_at,
        databaseNow: databaseNow.toISOString(),
        writesEnabled: false
      });

      return;
    }

    log('DRY_RUN_NO_WRITES', {
      bootstrap: checkpoint.bootstrap_at,
      checkpoint: checkpoint.next_minute_at,
      windowStart: window.windowStart.toISOString(),
      windowEnd: window.windowEnd.toISOString(),
      proposedNextCheckpoint:
        window.nextCheckpoint.toISOString(),
      overlapMinutes: OVERLAP_MINUTES,
      finalityDelayMinutes: FINALITY_DELAY_MINUTES,
      writesEnabled: false
    });

    // Deliberately no INSERT, UPDATE, or COMMIT here.
  } finally {
    client.release();
  }
}
async function processOneMinute(sql) {
  const client = await pool.connect();

  let transactionOpen = false;

  try {
    await client.query('BEGIN');
    transactionOpen = true;

    await client.query(
      `SET LOCAL statement_timeout = ${STATEMENT_TIMEOUT_MS}`
    );

    // RESTART-SAFE TEST GATE
    // Lock the gate before executing any aggregation SQL.
    // The lock is held until COMMIT or ROLLBACK.
    const gateResult = await client.query(
      `
        SELECT
          max_commits,
          commits_used
        FROM public.northstar_worker_test_gate
        WHERE job_name = $1
        FOR UPDATE
      `,
      [JOB_NAME]
    );

    if (gateResult.rowCount !== 1) {
      throw new Error(
        'TEST_GATE_MISSING: refusing to write'
      );
    }

    const gate = gateResult.rows[0];

    if (gate.commits_used >= gate.max_commits) {
      await client.query('ROLLBACK');
      transactionOpen = false;

      log('TEST_LIMIT_REACHED', {
        maxCommits: gate.max_commits,
        commitsUsed: gate.commits_used
      });

      return {
        processed: false,
        reason: 'TEST_LIMIT_REACHED'
      };
    }

    // Keep your existing checkpoint-locking code here.
    // It begins with:
    // const checkpointResult = await client.query(...);

    // Serialize worker instances through the checkpoint row.
    const checkpointResult = await client.query(
      `
        SELECT
          job_name,
          bootstrap_at,
          next_minute_at
        FROM public.northstar_worker_checkpoint
        WHERE job_name = $1
        FOR UPDATE
      `,
      [JOB_NAME]
    );

    if (checkpointResult.rowCount !== 1) {
      throw new Error('Checkpoint missing');
    }

    const checkpoint = checkpointResult.rows[0];
    const databaseNow = await getDatabaseNow(client);

    const window = calculateWindow(
      checkpoint,
      databaseNow
    );

    if (!window) {
      await client.query('COMMIT');
      transactionOpen = false;

      return {
        processed: false,
        reason: 'CAUGHT_UP'
      };
    }

    log('WINDOW_START', {
      windowStart: window.windowStart.toISOString(),
      windowEnd: window.windowEnd.toISOString(),
      checkpoint: window.nextMinute.toISOString()
    });

    // The companion SQL performs the minute upsert and
    // returns diagnostics in this same transaction.
    const aggregation = await client.query(sql, [
      window.windowStart.toISOString(),
      window.windowEnd.toISOString()
    ]);

    if (aggregation.rowCount !== 1) {
      throw new Error(
        `Expected one diagnostics row, received ${aggregation.rowCount}`
      );
    }

    const diagnostics = aggregation.rows[0];

 const qualifyingEvents = Number(
  diagnostics.qualifying_events
);

const minutesWritten = Number(
  diagnostics.minutes_written
);

// Validate aggregation diagnostics.
if (
  !Number.isSafeInteger(qualifyingEvents) ||
  qualifyingEvents < 0 ||
  !Number.isSafeInteger(minutesWritten) ||
  minutesWritten < 0
) {
  throw new Error(
    'Companion SQL returned invalid diagnostics'
  );
}

// SAFETY: Never advance through an unverified empty window.
// The surrounding transaction will roll back.
if (qualifyingEvents === 0) {
  throw new Error(
    'EMPTY_WINDOW_UNVERIFIED: refusing to advance checkpoint'
  );
}

// Verify that qualifying events produced valid minute records.
const minutesPrepared = Number(
  diagnostics.minutes_prepared
);

if (
  !Number.isSafeInteger(minutesPrepared) ||
  minutesPrepared <= 0 ||
  minutesWritten > minutesPrepared
) {
  throw new Error(
    'Companion SQL returned inconsistent minute diagnostics'
  );
}
const status = 'SUCCESS';

await client.query(
  `
    INSERT INTO public.northstar_worker_runs (
      job_name,
      started_at,
      finished_at,
      window_start,
      window_end,
      qualifying_events,
      minutes_written,
      status,
      diagnostics
    )
    VALUES (
      $1,
      clock_timestamp(),
      clock_timestamp(),
      $2,
      $3,
      $4,
      $5,
      $6,
      $7::jsonb
    )
  `,
  [
    JOB_NAME,
    window.windowStart,
    window.windowEnd,
    qualifyingEvents,
    minutesWritten,
    status,
    JSON.stringify(diagnostics)
  ]
);
     

    const advanced = await client.query(
      `
        UPDATE public.northstar_worker_checkpoint
        SET
          next_minute_at = $2,
          updated_at = clock_timestamp()
        WHERE
          job_name = $1
          AND next_minute_at = $3
      `,
      [
        JOB_NAME,
        window.nextCheckpoint,
        window.nextMinute
      ]
    );

    if (advanced.rowCount !== 1) {
      throw new Error(
        'Checkpoint advancement failed; rolling back'
      );
    }

    // RESTART-SAFE TEST GATE
    // Consume the allowance in the same transaction as
    // the minute upsert and checkpoint advancement.
    const gateUpdate = await client.query(
      `
        UPDATE public.northstar_worker_test_gate
        SET
          commits_used = commits_used + 1,
          updated_at = clock_timestamp()
        WHERE job_name = $1
          AND commits_used < max_commits
      `,
      [JOB_NAME]
    );

    if (gateUpdate.rowCount !== 1) {
      throw new Error(
        'TEST_GATE_UPDATE_FAILED: rolling back'
      );
    }

    // Aggregation, checkpoint and test gate commit together.
    await client.query('COMMIT');
    transactionOpen = false;

    log('WINDOW_COMMITTED', {
      status,
      windowStart: window.windowStart.toISOString(),
      windowEnd: window.windowEnd.toISOString(),
      qualifyingEvents,
      minutesWritten,
      nextCheckpoint:
        window.nextCheckpoint.toISOString()
    });

    return {
      processed: true,
      status
    };

  } catch (error) {
    if (transactionOpen) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        log('ROLLBACK_FAILED', {
          error: rollbackError.message
        });
      }
    }

    throw error;

  } finally {
    client.release();
  }
}

async function writeCycle(sql) {
  let processed = 0;

  while (
    !shuttingDown &&
    processed < MAX_MINUTES_PER_CYCLE
  ) {
    const result = await processOneMinute(sql);

    if (!result.processed) {
      // The database gate has already been exhausted.
      // Stop polling rather than reporting CAUGHT_UP.
      if (result.reason === 'TEST_LIMIT_REACHED') {
        log('TEST_GATE_EXHAUSTED', {
          message:
            'One-commit test complete. No further writes permitted.'
        });

        shuttingDown = true;
        break;
      }

      // Only report CAUGHT_UP when there genuinely
      // isn't an eligible minute to process.
      if (
        processed === 0 &&
        result.reason === 'CAUGHT_UP'
      ) {
        log('CAUGHT_UP');
      }

      break;
    }

    processed += 1;

    // Convenience stop for the controlled test.
    // The PostgreSQL gate provides restart safety.
    if (
      process.env.NORTHSTAR_STOP_AFTER_ONE_COMMIT === 'true'
    ) {
      log('ONE_COMMIT_TEST_COMPLETE', {
        minutesProcessed: processed
      });

      shuttingDown = true;
      break;
    }
  }

  if (processed > 0) {
    log('CYCLE_COMPLETE', {
      minutesProcessed: processed
    });
  }
}

async function main() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required');
  }

  const sql = loadMinuteSQL();

  log('WORKER_START', {
    mode: ENABLE_WRITES ? 'WRITE' : 'DRY_RUN',
    pollMs: POLL_MS,
    finalityDelayMinutes: FINALITY_DELAY_MINUTES,
    overlapMinutes: OVERLAP_MINUTES,
    maxMinutesPerCycle: MAX_MINUTES_PER_CYCLE,
    statementTimeoutMs: STATEMENT_TIMEOUT_MS
  });

  while (!shuttingDown) {
    try {
      if (ENABLE_WRITES) {
        await writeCycle(sql);
      } else {
        await dryRunCycle();
      }
    } catch (error) {
      log('WORKER_ERROR', {
        error: error.message,
        code: error.code || null,
        stack: error.stack
      });

      // Fail closed: don't skip or advance the checkpoint.
      // Keep the process alive for the next retry.
    }

    if (!shuttingDown) {
      await sleep(POLL_MS);
    }
  }

  await pool.end();

  log('WORKER_STOPPED');
}

function shutdown(signal) {
  log('SHUTDOWN_REQUESTED', { signal });
  shuttingDown = true;
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

main().catch(error => {
  log('FATAL_ERROR', {
    error: error.message,
    stack: error.stack
  });

  process.exitCode = 1;
});
