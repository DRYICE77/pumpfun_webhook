/**
 * NorthStar isolated PostgreSQL crash/recovery integration test.
 *
 * Requires DATABASE_URL and CREATE privilege on the database.
 * Uses a randomly named schema with copies of the three worker-control tables
 * and the minute table. Reads real source events but never writes production
 * worker state or production minute rows. Drops its schema in finally.
 *
 * Run while the normal minute worker is stopped:
 *   node northstar.recovery.test.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Client } = require('pg');

const START = '2026-09-20T23:42:00.000Z';
const END = '2026-09-20T23:43:00.000Z';
const JOB = 'volume_minutes_recovery_test';
const SCHEMA = `northstar_recovery_${crypto.randomBytes(6).toString('hex')}`;
const q = name => `"${name}"`;
const table = name => `${q(SCHEMA)}.${q(name)}`;
const minuteTable = table('northstar_token_volume_minutes');
const checkpointTable = table('northstar_worker_checkpoint');
const gateTable = table('northstar_worker_test_gate');
const runsTable = table('northstar_worker_runs');

if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');
const originalSql = fs.readFileSync(path.join(__dirname, 'northstar.minute.sql'), 'utf8');
const needle = 'INSERT INTO public.northstar_token_volume_minutes AS target';
if (originalSql.split(needle).length !== 2) {
  throw new Error('Unexpected minute SQL target; refusing to run');
}
const minuteSql = originalSql.replace(needle, `INSERT INTO ${minuteTable} AS target`);
const connect = async () => {
  const c = new Client({ connectionString: process.env.DATABASE_URL, connectionTimeoutMillis: 10000, application_name: 'northstar-recovery-test' });
  await c.connect();
  return c;
};
const assert = (condition, message) => { if (!condition) throw new Error(`ASSERTION_FAILED: ${message}`); };
const scalar = async (c, sql, params = []) => (await c.query(sql, params)).rows[0];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function state(c) {
  const [checkpoint, gate, runs, minutes] = await Promise.all([
    scalar(c, `SELECT next_minute_at FROM ${checkpointTable} WHERE job_name=$1`, [JOB]),
    scalar(c, `SELECT max_commits, commits_used FROM ${gateTable} WHERE job_name=$1`, [JOB]),
    scalar(c, `SELECT count(*)::int AS n FROM ${runsTable} WHERE job_name=$1`, [JOB]),
    scalar(c, `SELECT count(*)::int AS n FROM ${minuteTable}`),
  ]);
  return {
    checkpoint: new Date(checkpoint.next_minute_at).toISOString(),
    maxCommits: gate.max_commits,
    commitsUsed: gate.commits_used,
    runs: runs.n,
    minutes: minutes.n,
  };
}

async function workerTransaction(c) {
  await c.query('BEGIN');
  await c.query("SET LOCAL statement_timeout = '30000ms'");
  const gate = await scalar(c, `SELECT max_commits, commits_used FROM ${gateTable} WHERE job_name=$1 FOR UPDATE`, [JOB]);
  assert(gate.commits_used < gate.max_commits, 'test gate exhausted');
  const checkpoint = await scalar(c, `SELECT next_minute_at FROM ${checkpointTable} WHERE job_name=$1 FOR UPDATE`, [JOB]);
  assert(new Date(checkpoint.next_minute_at).toISOString() === START, 'unexpected checkpoint');
  const result = await c.query(minuteSql, [START, END]);
  assert(result.rowCount === 1, 'aggregation diagnostics missing');
  const diagnostics = result.rows[0];
  assert(Number(diagnostics.qualifying_events) > 0, 'no qualifying source events');
  assert(Number(diagnostics.minutes_prepared) > 0, 'no minutes prepared');
  await c.query(`INSERT INTO ${runsTable} (job_name, started_at, finished_at, window_start, window_end, qualifying_events, minutes_written, status, diagnostics)
    VALUES ($1, clock_timestamp(), clock_timestamp(), $2, $3, $4, $5, 'SUCCESS', $6::jsonb)`,
    [JOB, START, END, diagnostics.qualifying_events, diagnostics.minutes_written, JSON.stringify(diagnostics)]);
  const advanced = await c.query(`UPDATE ${checkpointTable} SET next_minute_at=$2, updated_at=clock_timestamp()
    WHERE job_name=$1 AND next_minute_at=$3`, [JOB, END, START]);
  assert(advanced.rowCount === 1, 'checkpoint update failed');
  const consumed = await c.query(`UPDATE ${gateTable} SET commits_used=commits_used+1, updated_at=clock_timestamp()
    WHERE job_name=$1 AND commits_used < max_commits`, [JOB]);
  assert(consumed.rowCount === 1, 'gate update failed');
  return diagnostics;
}

async function main() {
  let admin;
  let crashed;
  let retry;
  let schemaCreated = false;
  try {
    admin = await connect();
    await admin.query(`CREATE SCHEMA ${q(SCHEMA)}`);
    schemaCreated = true;
    for (const name of ['northstar_token_volume_minutes', 'northstar_worker_checkpoint', 'northstar_worker_test_gate', 'northstar_worker_runs']) {
      await admin.query(`CREATE TABLE ${table(name)} (LIKE public.${q(name)} INCLUDING ALL)`);
    }
    // LIKE INCLUDING DEFAULTS can copy the production minute ID sequence default.
    // Replace it with a sequence owned exclusively by the test schema.
    await admin.query(`CREATE SEQUENCE ${q(SCHEMA)}.minute_id_seq`);
    await admin.query(`ALTER TABLE ${minuteTable} ALTER COLUMN id SET DEFAULT nextval('${SCHEMA}.minute_id_seq'::regclass)`);
    await admin.query(`INSERT INTO ${checkpointTable} (job_name, bootstrap_at, next_minute_at)
      VALUES ($1,$2,$2)`, [JOB, START]);
    await admin.query(`INSERT INTO ${gateTable} (job_name, max_commits, commits_used) VALUES ($1,1,0)`, [JOB]);
    const before = await state(admin);
    assert(before.checkpoint === START && before.commitsUsed === 0 && before.runs === 0 && before.minutes === 0, 'invalid initial state');
    console.log('BASELINE', before);

    crashed = await connect();
    const pid = (await scalar(crashed, 'SELECT pg_backend_pid() AS pid')).pid;
    const attempted = await workerTransaction(crashed);
    console.log('UNCOMMITTED_ATTEMPT', { qualifying_events: attempted.qualifying_events, minutes_prepared: attempted.minutes_prepared, minutes_written: attempted.minutes_written });
    assert(Number(attempted.minutes_written) > 0, 'crash attempt did not write rows');

    // Abruptly sever the client socket: no COMMIT and no explicit ROLLBACK.
    // PostgreSQL must abort the open transaction when the backend disconnects.
    crashed.connection.stream.destroy();
    crashed.on('error', () => {});
    let disconnected = false;
    for (let i = 0; i < 40; i++) {
      const active = await scalar(admin, 'SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1) AS active', [pid]);
      if (!active.active) { disconnected = true; break; }
      await sleep(250);
    }
    assert(disconnected, 'crashed backend did not disconnect within 10 seconds');
    const afterCrash = await state(admin);
    assert(JSON.stringify(afterCrash) === JSON.stringify(before), 'partial state visible after crash');
    console.log('CRASH_ROLLBACK_PASS', afterCrash);

    retry = await connect();
    const retried = await workerTransaction(retry);
    await retry.query('COMMIT');
    const afterRetry = await state(admin);
    assert(afterRetry.checkpoint === END, 'checkpoint did not advance');
    assert(afterRetry.commitsUsed === 1 && afterRetry.runs === 1, 'gate/run history not committed exactly once');
    assert(afterRetry.minutes === Number(retried.minutes_prepared), 'unexpected committed minute row count');
    console.log('RECOVERY_RETRY_PASS', afterRetry);

    await retry.query('BEGIN');
    const replay = (await retry.query(minuteSql, [START, END])).rows[0];
    await retry.query('ROLLBACK');
    assert(Number(replay.minutes_written) === 0, 'replay unnecessarily wrote rows');
    console.log('REPLAY_PASS', { minutes_written: replay.minutes_written });
    console.log('TEST_RESULT', { passed: true, schema: SCHEMA, production_state_modified: false });
  } finally {
    if (retry) await retry.end().catch(() => {});
    if (crashed) await crashed.end().catch(() => {});
    if (admin) {
      if (schemaCreated) {
        await admin.query(`DROP SCHEMA ${q(SCHEMA)} CASCADE`);
        console.log('TEST_SCHEMA_DROPPED', SCHEMA);
      }
      await admin.end();
    }
  }
}

main().catch(error => {
  console.error('RECOVERY_TEST_FAILED', error);
  process.exitCode = 1;
});
