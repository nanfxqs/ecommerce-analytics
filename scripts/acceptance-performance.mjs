#!/usr/bin/env node
// Actual Kafka callback ACK -> first painted React result that covers its offset.
import { chromium } from '../frontend/node_modules/@playwright/test/index.mjs';
import { parseArgs } from 'node:util';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import fs from 'node:fs/promises';
import path from 'node:path';
const exec = promisify(execFile);
const { values: options } = parseArgs({ options: {
  api: { type: 'string', default: 'http://127.0.0.1:8007' },
  frontend: { type: 'string', default: 'http://127.0.0.1:5179' },
  cluster: { type: 'string' }, edge: { type: 'string' }, out: { type: 'string' },
  seconds: { type: 'string', default: '900' }, rate: { type: 'string', default: '5' },
} });
for (const key of ['cluster', 'edge', 'out']) if (!options[key]) throw new Error('--' + key + ' is required');
const duration = Number(options.seconds), rate = Number(options.rate);
if (!(duration > 0 && rate > 0)) throw new Error('Positive duration/rate required');
const out = path.resolve(options.out); await fs.mkdir(out, { recursive: true });
const cluster = JSON.parse(await fs.readFile(options.cluster)), edge = JSON.parse(await fs.readFile(options.edge));
const quote = value => "'" + String(value).replaceAll("'", "'\\''") + "'";
const now = () => process.hrtime.bigint();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function remotePython(code) {
  return (await exec('ssh', ['-o', 'BatchMode=yes', cluster.host, quote(cluster.python) + ' -c ' + quote(code)], { timeout: 15000, maxBuffer: 8 * 1024 * 1024 })).stdout;
}
const calibration = [];
async function calibrate(count = 3) {
  const group = [];
  for (let i = 0; i < count; i++) {
    const before = now(), wallBefore = BigInt(Date.now()) * 1000000n;
    const guest = JSON.parse(await remotePython('import time,json;print(json.dumps(dict(monotonic_ns=str(time.monotonic_ns()),wall_ns=str(time.time_ns()))))'));
    const after = now(); const midpoint = (before + after) / 2n;
    group.push({ host_before_ns: String(before), host_after_ns: String(after), host_wall_before_ns: String(wallBefore), guest_monotonic_ns: guest.monotonic_ns, guest_wall_ns: guest.wall_ns, offset_ns: String(midpoint - BigInt(guest.monotonic_ns)), uncertainty_ns: String((after - before) / 2n), rtt_ms: Number(after - before) / 1e6 });
  }
  group.sort((a, b) => a.rtt_ms - b.rtt_ms);
  calibration.push({ selected: group[0], candidates: group });
  await fs.writeFile(path.join(out, 'calibration.json'), JSON.stringify(calibration, null, 2));
  return group[0];
}
async function request(suffix, body) {
  const response = await fetch(options.api + '/api/replay/' + suffix, body === undefined ? undefined : { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  const result = await response.json(); if (!response.ok) throw new Error(JSON.stringify(result)); return result;
}
const frames = [], observations = [], polls = [];
let run, browser, page, firstAck, start, stopRequest, stoppedState, publishedBefore;
let failure;
try {
  const initial = await calibrate(7);
  browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || '/usr/bin/chromium', args: ['--no-sandbox'] });
  page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  await page.exposeFunction('__acceptanceVisible', data => { frames.push({ ...data, host_monotonic_ns: String(now()) }); });
  await page.addInitScript(() => {
    let previous = ''; const pending = new Set();
    const inspect = () => {
      const element = document.querySelector('[data-testid="live-result"]'); if (!element) return;
      const key = element.getAttribute('data-run-id') + ':' + element.getAttribute('data-result-offsets');
      if (key === previous || pending.has(key)) return;
      pending.add(key);
      requestAnimationFrame(() => requestAnimationFrame(() => {
        pending.delete(key);
        const current = document.querySelector('[data-testid="live-result"]'); if (!current) return;
        const run_id = current.getAttribute('data-run-id'), raw = current.getAttribute('data-result-offsets');
        const renderedKey = run_id + ':' + raw;
        if (renderedKey === previous) return;
        previous = renderedKey;
        window.__acceptanceVisible({ run_id, offsets: JSON.parse(raw), browser_performance_ms: performance.now(), valid_text: current.querySelector('[data-testid="live-valid"]')?.textContent });
      }));
    };
    const install = () => { new MutationObserver(inspect).observe(document.body, { childList: true, subtree: true, attributes: true }); inspect(); };
    if (document.body) install(); else addEventListener('DOMContentLoaded', install, { once: true });
  });
  page.on('request', request => { if (request.url().endsWith('/api/replay/current')) polls.push(String(now())); });
  await page.goto(options.frontend); await page.getByText('实时监控', { exact: true }).click();
  publishedBefore = (await (await fetch(options.api + '/api/history/overview')).json()).snapshot;
  run = await request('runs', { dataset: 'small', rate });
  await fs.writeFile(path.join(out, 'start.json'), JSON.stringify(run, null, 2));
  const ackFile = path.posix.join(edge.root, run.run_id, 'acknowledgements.jsonl');
  const readyDeadline = now() + 300000000000n;
  while (!firstAck) {
    const text = await remotePython('from pathlib import Path;p=Path(' + JSON.stringify(ackFile) + ');print(p.open().readline() if p.exists() else "")');
    if (text.trim()) firstAck = JSON.parse(text.trim());
    else { const state = await request('current'); if (state.phase === 'failed') throw new Error(JSON.stringify(state)); if (now() > readyDeadline) throw new Error('No actual ACK within readiness deadline'); await sleep(1000); }
  }
  start = BigInt(firstAck.ack_monotonic_ns) + BigInt(initial.offset_ns);
  const deadline = start + BigInt(Math.ceil((duration + 2) * 1e9));
  let nextCalibration = now() + 60000000000n;
  while (now() < deadline) {
    const state = await request('current');
    observations.push({ host_monotonic_ns: String(now()), phase: state.phase, confirmed: state.confirmed, lag: state.lag, committed: state.committed, result_offsets: state.result_offsets, observation_error: state.observation_error });
    if (state.phase === 'failed' || state.observation_error) throw new Error(JSON.stringify(state));
    if (state.run_id !== run.run_id) throw new Error('Replay identity changed during measurement');
    if (now() - start > 60000000000n && !frames.some(frame => frame.run_id === run.run_id && Object.keys(frame.offsets).length)) throw new Error('No painted committed result during first60seconds');
    if (now() >= nextCalibration) { await calibrate(); nextCalibration = now() + 60000000000n; }
    await sleep(1000);
  }
  stopRequest = now(); await request('current/stop', {});
  const drainDeadline = now() + 180000000000n;
  while (true) {
    const state = await request('current');
    if (state.phase === 'stopped' && state.stream_released) { stoppedState = state; await fs.writeFile(path.join(out, 'stopped.json'), JSON.stringify(state, null, 2)); break; }
    if (state.phase === 'failed' || now() > drainDeadline) throw new Error('Stop/drain failed: ' + JSON.stringify(state));
    await sleep(1000);
  }
  await sleep(4000); // Ensure browser's next2second poll paints final committed result.
  await page.screenshot({ path: path.join(out, 'browser.png'), fullPage: true });
  await calibrate(7);
  const ackText = await remotePython('from pathlib import Path;print(Path(' + JSON.stringify(ackFile) + ').read_text(),end="")');
  await fs.writeFile(path.join(out, 'acknowledgements.jsonl'), ackText);
  const acknowledgements = ackText.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  if (acknowledgements.length !== stoppedState.confirmed || new Set(acknowledgements.map(value => value.source_record_id)).size !== stoppedState.confirmed) throw new Error('ACK callback log does not cover every confirmed source record exactly once');
  const publishedAfter = (await (await fetch(options.api + '/api/history/overview')).json()).snapshot;
  if (stoppedState.complete || JSON.stringify(publishedBefore) !== JSON.stringify(publishedAfter)) throw new Error('Early stop changed complete historical publication');
  const selected = calibration.map(group => group.selected);
  const lowerOffset = selected.reduce((value, sample) => { const x = BigInt(sample.offset_ns) - BigInt(sample.uncertainty_ns); return value === null || x < value ? x : value; }, null);
  const upperOffset = selected.reduce((value, sample) => { const x = BigInt(sample.offset_ns) + BigInt(sample.uncertainty_ns); return value === null || x > value ? x : value; }, null);
  const painted = frames.filter(frame => frame.run_id === run.run_id);
  const samples = acknowledgements.map(ack => {
    const frame = painted.find(frame => (frame.offsets[ack.topic]?.[String(ack.partition)] ?? 0) > ack.offset);
    const low = frame ? Math.max(0, Number(BigInt(frame.host_monotonic_ns) - BigInt(ack.ack_monotonic_ns) - upperOffset) / 1e6) : null;
    const high = frame ? Math.max(0, Number(BigInt(frame.host_monotonic_ns) - BigInt(ack.ack_monotonic_ns) - lowerOffset) / 1e6) : null;
    return { ...ack, first_painted_host_ns: frame?.host_monotonic_ns ?? null, latency_lower_ms: low, latency_upper_ms: high };
  });
  const latencies = samples.map(sample => sample.latency_upper_ms).filter(value => value !== null).sort((a, b) => a - b);
  const percentile = p => latencies[Math.max(0, Math.ceil(latencies.length * p) - 1)];
  const inputSpan = Number(BigInt(acknowledgements.at(-1).ack_monotonic_ns) - BigInt(acknowledgements[0].ack_monotonic_ns)) / 1e9;
  const buckets = [];
  for (let minute = 0; minute < Math.floor(inputSpan / 60); minute++) {
    const lags = observations.filter(value => { const seconds = Number(BigInt(value.host_monotonic_ns) - start) / 1e9; return seconds >= minute * 60 && seconds < (minute + 1) * 60; }).map(value => value.lag).filter(Number.isFinite).sort((a, b) => a - b);
    if (lags.length) buckets.push({ minute, median: lags[Math.floor(lags.length / 2)], max: lags.at(-1), samples: lags.length });
  }
  const drift = buckets.length > 1 ? buckets.at(-1).median - buckets[0].median : Infinity;
  const meanMinute = buckets.reduce((sum, value) => sum + value.minute, 0) / buckets.length;
  const meanLag = buckets.reduce((sum, value) => sum + value.median, 0) / buckets.length;
  const denominator = buckets.reduce((sum, value) => sum + (value.minute - meanMinute) ** 2, 0);
  const slope = denominator ? buckets.reduce((sum, value) => sum + (value.minute - meanMinute) * (value.median - meanLag), 0) / denominator / 60 : Infinity;
  const normalPolls = polls.map(BigInt).filter(time => time >= start && time <= stopRequest);
  const intervals = normalPolls.slice(1).map((time, i) => Number(time - normalPolls[i]) / 1e9).sort((a, b) => a - b);
  const report = { accepted: duration >= 900 && inputSpan >= 900 && samples.length === latencies.length && percentile(.95) <= 10000 && drift <= rate * 2 && slope <= rate * .005,
    run_id: run.run_id, dataset: 'small', requested_rate: rate, duration_requested_seconds: duration, actual_ack_span_seconds: inputSpan,
    samples: samples.length, matched_samples: latencies.length, p50_upper_ms: percentile(.5), p95_upper_ms: percentile(.95), p99_upper_ms: percentile(.99), maximum_upper_ms: latencies.at(-1),
    calibration_offset_span_ms: Number(upperOffset - lowerOffset) / 1e6, lag_minute_buckets: buckets, lag_end_minus_start_median: drift, lag_median_slope_messages_per_second: slope, measured_ack_rate: (samples.length - 1) / inputSpan, historical_batch_preserved: publishedAfter?.batch_id,
    browser_poll_interval_median_seconds: intervals[Math.floor(intervals.length / 2)], browser_poll_interval_p95_seconds: intervals[Math.ceil(intervals.length * .95) - 1],
    protocol: 'All actual Future ACK callbacks after empty-topic readiness barrier, no latency/startup sample exclusions; first two-animation-frame painted DOM coverage by same run/topic/partition exclusive offset; upperbound includes measured clock-calibration uncertainty and host binding transit. Intentional stop/drain after normalwindow separately recorded.' };
  await fs.writeFile(path.join(out, 'latencies.json'), JSON.stringify(samples, null, 2));
  await fs.writeFile(path.join(out, 'summary.json'), JSON.stringify(report, null, 2));
  if (!report.accepted) throw new Error('Performance acceptance failed: ' + JSON.stringify(report));
  console.log(JSON.stringify(report));
} catch (error) {
  failure = String(error); await fs.writeFile(path.join(out, 'failure.json'), JSON.stringify({ failure, run_id: run?.run_id }, null, 2));
  if (run && !stopRequest) { try { await request('current/stop', {}); } catch {} }
} finally {
  await fs.writeFile(path.join(out, 'frames.json'), JSON.stringify(frames, null, 2));
  await fs.writeFile(path.join(out, 'observations.json'), JSON.stringify(observations, null, 2));
  await fs.writeFile(path.join(out, 'polls.json'), JSON.stringify(polls, null, 2));
  if (browser) await browser.close();
}
if (failure) { console.error(failure); process.exitCode = 1; }
