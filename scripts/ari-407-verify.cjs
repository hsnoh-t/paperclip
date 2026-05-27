#!/usr/bin/env node
/* eslint-disable no-console */
// ARI-407 운영 검증 스크립트
// - 최근 N시간 동안 발생한 context_length_exceeded fingerprint의 run을 추출
// - 같은 persistedSessionId 재사용 여부, sessionRotation 적용 여부, sliding-window/consecutive
//   가드 발화 여부를 보고
// - 동일 패턴(template)으로 ARI-103/228 metrics 스크립트와 호환되도록 작성
//
// 사용:
//   node scripts/ari-407-verify.cjs [--hours=24] [--company=<uuid>] [--agent=<uuid>]
//
// 패치 전 stuck session 재현은 동일 agent에서 errorCode!='context_length_exceeded'이면서
// resultJson.errorFingerprint가 없을 때(즉 detection 미적용 빌드) 다수의 같은 sessionIdBefore
// 가 관찰되는 형태로 나타난다. 패치 후에는 동일 agent의 sessionIdBefore가 매 run마다 달라지고
// usageJson.sessionRotationReason='context_length_exceeded'가 기록된다.

const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
  const out = { hours: 24, company: null, agent: null };
  for (const arg of argv.slice(2)) {
    const m = arg.match(/^--([a-zA-Z][\w-]*)=(.+)$/);
    if (!m) continue;
    const [, key, value] = m;
    if (key === 'hours') out.hours = Math.max(1, parseInt(value, 10) || 24);
    else if (key === 'company') out.company = value;
    else if (key === 'agent') out.agent = value;
  }
  return out;
}

function resolvePostgresClient() {
  const candidates = [
    path.resolve(__dirname, '..', 'node_modules', 'postgres'),
    '/Users/hsnoh/Workspace/Softronic/paperclip-ari103-6d1b4aea/node_modules/postgres',
  ];
  for (const candidate of candidates) {
    try {
      // eslint-disable-next-line global-require, import/no-dynamic-require
      return require(candidate);
    } catch {
      // try next
    }
  }
  throw new Error('postgres-npm client not found; install in paperclip fork tree');
}

function resolveEmbeddedPostgresPort() {
  const pidPath = `${process.env.HOME}/.paperclip/instances/default/db/postmaster.pid`;
  if (!fs.existsSync(pidPath)) {
    throw new Error(`embedded postmaster.pid not found at ${pidPath}; start the local instance first`);
  }
  const lines = fs.readFileSync(pidPath, 'utf8').split('\n');
  const port = parseInt(lines[3], 10);
  if (!Number.isFinite(port)) {
    throw new Error('failed to parse port from postmaster.pid line 4');
  }
  return port;
}

async function main() {
  const args = parseArgs(process.argv);
  const postgres = resolvePostgresClient();
  const port = resolveEmbeddedPostgresPort();
  const sql = postgres({
    host: '127.0.0.1',
    port,
    database: 'paperclip',
    username: 'paperclip',
    password: 'paperclip',
    max: 4,
  });
  const windowMinutes = args.hours * 60;
  const cutoff = new Date(Date.now() - windowMinutes * 60_000).toISOString();
  const out = { window_start_utc: cutoff, window_hours: args.hours };

  // 1) context_length_exceeded fingerprint를 가진 run을 가져온다.
  //    - errorCode 직접 매칭, error/usageJson/resultJson 내 어디라도 패턴이 있으면 포함.
  const matchedRuns = await sql`
    SELECT
      r.id,
      r.company_id,
      r.agent_id,
      LEFT(r.agent_id::text, 8) AS agent_short,
      a.adapter_type,
      r.status,
      r.error_code,
      r.error,
      r.session_id_before,
      r.session_id_after,
      r.usage_json,
      r.result_json,
      r.created_at,
      r.finished_at
    FROM heartbeat_runs r
    LEFT JOIN agents a ON a.id = r.agent_id
    WHERE r.created_at >= ${cutoff}
      AND r.status = 'failed'
      AND (
        r.error_code ~* 'context[_\\s-]?length[_\\s-]?exceeded'
        OR r.error ~* 'context[_\\s-]?length[_\\s-]?exceeded'
        OR r.error ~* 'maximum context length'
        OR r.error ~* 'context window'
        OR r.error ~* 'input is too long for'
        OR (r.result_json->>'errorFingerprint') = 'context_length_exceeded'
        OR (r.result_json->>'sessionRotationReason') = 'context_length_exceeded'
        OR (r.usage_json->>'sessionRotationReason') = 'context_length_exceeded'
      )
      ${args.company ? sql`AND r.company_id = ${args.company}` : sql``}
      ${args.agent ? sql`AND r.agent_id = ${args.agent}` : sql``}
    ORDER BY r.created_at ASC
  `;
  out.matched_runs_total = matchedRuns.length;

  // 2) agent별 집계
  const perAgent = new Map();
  for (const r of matchedRuns) {
    const key = r.agent_id;
    if (!perAgent.has(key)) {
      perAgent.set(key, {
        agent_short: r.agent_short,
        company_short: r.company_id ? r.company_id.slice(0, 8) : null,
        adapter: r.adapter_type,
        total: 0,
        with_rotation_reason: 0,
        with_error_code: 0,
        distinct_session_id_before: new Set(),
        repeated_session_id_before: new Set(),
        last_run_id: null,
        last_run_at: null,
        sample_session_ids: [],
      });
    }
    const acc = perAgent.get(key);
    acc.total += 1;
    if (r.error_code === 'context_length_exceeded') acc.with_error_code += 1;
    const usageJson = r.usage_json || {};
    const resultJson = r.result_json || {};
    if (
      usageJson.sessionRotationReason === 'context_length_exceeded' ||
      resultJson.sessionRotationReason === 'context_length_exceeded'
    ) {
      acc.with_rotation_reason += 1;
    }
    const sessionId = r.session_id_before;
    if (sessionId) {
      if (acc.distinct_session_id_before.has(sessionId)) {
        acc.repeated_session_id_before.add(sessionId);
      }
      acc.distinct_session_id_before.add(sessionId);
      if (acc.sample_session_ids.length < 4) acc.sample_session_ids.push(sessionId);
    }
    acc.last_run_id = r.id;
    acc.last_run_at = r.finished_at || r.created_at;
  }
  out.per_agent = Array.from(perAgent.values()).map((r) => ({
    agent_short: r.agent_short,
    company_short: r.company_short,
    adapter: r.adapter,
    total: r.total,
    with_error_code: r.with_error_code,
    with_rotation_reason: r.with_rotation_reason,
    distinct_session_ids: r.distinct_session_id_before.size,
    // ARI-407: stuck-session 신호. 정상 회전 동작이면 0이어야 한다.
    repeated_session_ids: r.repeated_session_id_before.size,
    sample_session_ids: r.sample_session_ids,
    last_run_id: r.last_run_id,
    last_run_at: r.last_run_at,
  }));

  // 3) 60분 sliding window 별 카운트(가장 최근 트리거 후보)
  const slidingWindowMs = 60 * 60_000;
  const slidingHotspots = [];
  for (const [agentId, acc] of perAgent.entries()) {
    const runs = matchedRuns.filter((r) => r.agent_id === agentId);
    let peak = 0;
    let peakAt = null;
    for (let i = 0; i < runs.length; i += 1) {
      const start = new Date(runs[i].created_at).getTime();
      let count = 0;
      for (let j = i; j < runs.length; j += 1) {
        if (new Date(runs[j].created_at).getTime() - start > slidingWindowMs) break;
        count += 1;
      }
      if (count > peak) {
        peak = count;
        peakAt = runs[i].created_at;
      }
    }
    if (peak > 0) {
      slidingHotspots.push({
        agent_short: acc.agent_short,
        company_short: acc.company_short,
        adapter: acc.adapter,
        peak_60min_window_count: peak,
        peak_window_started_at: peakAt,
        would_trigger_sliding_window_threshold_6: peak >= 6,
      });
    }
  }
  out.sliding_window_hotspots = slidingHotspots.sort(
    (a, b) => b.peak_60min_window_count - a.peak_60min_window_count,
  );

  // 4) 실제로 ARI-103/407 가드가 발화한 lifecycle warn 이벤트
  const guardEvents = await sql`
    SELECT
      e.created_at,
      e.run_id,
      e.payload->>'code' AS code,
      e.payload->>'adapter' AS adapter,
      e.payload->>'triggerKind' AS trigger_kind,
      e.payload->>'metricTag' AS metric_tag,
      (e.payload->>'consecutiveErrorCount')::int AS consecutive,
      (e.payload->>'slidingWindowCount')::int AS sliding_window_count,
      (e.payload->>'slidingWindowMinutes')::int AS sliding_window_minutes,
      (e.payload->>'slidingWindowThreshold')::int AS sliding_window_threshold,
      e.payload->>'lastRunId' AS last_run_id,
      e.payload->>'fingerprint' AS fingerprint,
      a.id AS agent_id,
      LEFT(a.id::text,8) AS agent_short,
      LEFT(a.company_id::text,8) AS company_short
    FROM heartbeat_run_events e
    LEFT JOIN heartbeat_runs r ON r.id = e.run_id
    LEFT JOIN agents a ON a.id = r.agent_id
    WHERE e.event_type='lifecycle' AND e.stream='system' AND e.level='warn'
      AND (e.payload->>'guardVersion') = 'heartbeat-error-autopause/v1'
      AND (e.payload->>'code') = 'context_length_exceeded'
      AND e.created_at >= ${cutoff}
    ORDER BY e.created_at ASC
  `;
  out.guard_events = guardEvents.map((r) => ({
    created_at: r.created_at,
    company_short: r.company_short,
    agent_short: r.agent_short,
    adapter: r.adapter,
    trigger_kind: r.trigger_kind,
    metric_tag: r.metric_tag,
    consecutive: r.consecutive,
    sliding_window_count: r.sliding_window_count,
    sliding_window_minutes: r.sliding_window_minutes,
    sliding_window_threshold: r.sliding_window_threshold,
    last_run_id: r.last_run_id,
    fingerprint: r.fingerprint,
  }));
  out.guard_event_total = guardEvents.length;
  out.guard_sliding_window_triggers = guardEvents.filter((r) => r.trigger_kind === 'sliding_window').length;
  out.guard_consecutive_triggers = guardEvents.filter((r) => r.trigger_kind === 'consecutive').length;

  // 5) 회귀 신호 요약
  const regression = {
    stuck_session_repeats: out.per_agent
      .filter((r) => r.repeated_session_ids > 0)
      .map((r) => ({ agent_short: r.agent_short, repeated_session_ids: r.repeated_session_ids })),
    agents_with_failures_but_no_rotation_marker: out.per_agent
      .filter((r) => r.total > 0 && r.with_rotation_reason === 0)
      .map((r) => ({ agent_short: r.agent_short, total: r.total, with_error_code: r.with_error_code })),
  };
  out.regression_signals = regression;

  console.log(JSON.stringify(out, null, 2));
  await sql.end({ timeout: 2 });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
