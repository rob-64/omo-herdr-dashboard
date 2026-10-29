import assert from 'node:assert/strict';
import net from 'node:net';
import { EventEmitter, once } from 'node:events';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { FixedDagPane, paneViewerProcess, canCleanupPane, parseLaunchCommand } from '../src/controller.mjs';
import { writeJson } from '../src/storage.mjs';
import { payload, sessionId } from './fixtures.mjs';
import { ipcPath } from '../../coordination/src/platform.mjs';

function control(type, state, pid) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(state.controlOffer.pipePath);
    const abort = AbortSignal.timeout(3000);
    abort.addEventListener('abort', () => { socket.destroy(); reject(abort.reason); }, { once: true });
    socket.once('connect', () => socket.write(`${JSON.stringify({ ...state.controlOffer, type, pid })}\n`));
    socket.once('data', data => { socket.end(); resolve(JSON.parse(data.toString())); });
    socket.once('error', reject);
  });
}
test('Windows shell-only report verifies exact native PID, parent and fixed role', async () => {
  if (process.platform !== 'win32') return;
  const record = { role: 'dag', paneId: 'qa:pane', statePath: 'C:\\QA space\\dag.json',
    viewerPath: 'C:\\QA space\\viewer.mjs', nodePath: 'C:\\Program Files\\nodejs\\node.exe',
    launchToken: 'launch', processId: 123, creationProcessId: 456, creationDate: '2026-10-03T00:00:00.000Z' };
  const info = { process_info: { shell_pid: 456, foreground_processes: [{ pid: 456, argv: ['powershell.exe'] }] } };
  const shell = { ProcessId: 456, ExecutablePath: 'powershell.exe', CommandLine: 'powershell.exe',
    CreationDate: record.creationDate };
  const native = { ProcessId: 123, ParentProcessId: 456, ExecutablePath: record.nodePath,
    CreationDate: '2026-10-03T00:00:01.000Z',
    CommandLine: `"${record.nodePath}" "${record.viewerPath}" --view dag --state "${record.statePath}" --close-pane qa:pane --launch-token launch` };
  assert.equal((await paneViewerProcess(info, record, 123, async () => [shell, native])).pid, 123);
  for (const child of [null, { ...native, ProcessId: 124 }, { ...native, ParentProcessId: 457 },
    { ...native, ExecutablePath: 'C:\\foreign.exe' },
    { ...native, CommandLine: native.CommandLine.replace('--view dag', '--view tasks') },
    { ...native, CommandLine: native.CommandLine.replace('--launch-token launch', '--launch-token stale') },
    { ...native, CommandLine: native.CommandLine.replace('qa:pane', 'qa:foreign') },
    { ...native, CommandLine: native.CommandLine.replace('dag.json', 'foreign.json') }])
    assert.equal(await paneViewerProcess(info, record, 123, async () => child ? [shell, child] : [shell]), null);
  let inspected = false;
  assert.equal(await paneViewerProcess({ process_info: { ...info.process_info, shell_pid: 457 } },
    record, 123, async () => { inspected = true; return native; }), null);
  assert.equal(inspected, false);
  for (const command of [native.CommandLine + ' --once', native.CommandLine + ' --view dag',
    native.CommandLine + ' extra', native.CommandLine.replace('token launch', 'token "launchx'),
    native.CommandLine.replace('"C:\\QA space\\viewer.mjs"', '"C:\\QA space\\"viewer.mjs')]) {
    const child = { ...native, CommandLine: command };
    assert.equal(await paneViewerProcess(info, record, 123, async () => [shell, child]), null);
    assert.equal(await canCleanupPane(info, record, async () => [shell, child]), false);
  }
  assert.equal(parseLaunchCommand('--launch-token "launchx'), null);
  assert.equal(parseLaunchCommand('"node\\" --state state'), null);
  assert.equal(await canCleanupPane(info, record, async () => [shell]), true);
  assert.equal(await canCleanupPane(info, record, async () => [shell, native]), true);
  for (const rows of [[], [native], [shell, { ...native, ExecutablePath: 'foreign.exe' }],
    [shell, native, { ...native, ProcessId: 789 }], [shell, { ...native, CreationDate: undefined }],
    [{ ...shell, CreationDate: '2026-10-03T00:00:02.000Z' }, native]])
    assert.equal(await canCleanupPane(info, record, async () => rows), false);
  assert.equal(await canCleanupPane(info, record, async () => { throw new Error('CIM failed'); }), false);
  assert.equal(await paneViewerProcess(info, record, 123, async () =>
    [{ ...shell, CreationDate: '2026-10-03T00:00:02.000Z' }, native]), null);
});
async function setup(t) {
  const directory = join(tmpdir(), `fixed-controller-${process.pid}-${randomUUID()}`);
  console.log(JSON.stringify({ event: 'resource-planned', pid: process.pid, directory }));
  await mkdir(directory);
  const calls = [], warnings = [], events = new EventEmitter();
  const panes = new Map([['qa:parent', { pane_id: 'qa:parent', tab_id: 'qa:tab' }]]);
  const processes = new Map();
  let number = 0;
  let focused = 'qa:parent';
  const herdr = async (...args) => {
    calls.push(args);
    if (args[0] === 'list') return { panes: [...panes.values()].map(pane => ({ ...pane, focused: pane.pane_id === focused })) };
    if (args[0] === 'layout') return { layout: { tab_id: 'qa:tab', focused_pane_id: focused,
      panes: [...panes.values()].map((pane, index) => ({ ...pane, rect: { x: index ? 100 : 0, y: index ? index * 10 : 0, width: 50, height: 10 } })) } };
    if (args[0] === 'swap') { focused = args[2]; return {}; }
    if (args[0] === 'neighbor') return { neighbor: { neighbor_pane_id: 'qa:parent', layout: { tab_id: 'qa:tab', focused_pane_id: focused } } };
    if (args[0] === 'focus') { focused = 'qa:parent'; return {}; }
    if (args[0] === 'split') {
      const pane_id = `qa:role${++number}`;
      panes.set(pane_id, { pane_id, tab_id: 'qa:tab' }); return { pane: { pane_id } };
    }
    if (args[0] === 'process-info') return { process_info: { shell_pid: 7000 + Number(args[2].replace('qa:role', '')),
      foreground_processes: processes.get(args[2]) ?? [{ pid: 7000 + Number(args[2].replace('qa:role', '')), argv: ['powershell.exe'] }] } };
    if (args[0] === 'close') { panes.delete(args[1]); events.emit(`closed:${args[1]}`); return {}; }
    if (args[0] === 'run') {
      const argv = [...args[2].matchAll(/'((?:''|[^'])*)'/g)].map(match => match[1].replaceAll("''", "'"));
      const file = argv[argv.indexOf('--state') + 1], state = JSON.parse(await readFile(file, 'utf8'));
      processes.set(args[1], [{ pid: 6000 + number, argv }]);
      assert.equal((await control('register', state, 6000 + number)).type, 'registered');
      return {};
    }
    return {};
  };
  const options = { sessionId, parentPane: 'qa:parent', socket: 'qa:socket', stateDir: directory,
    managedStateDir: join(directory, 'uncreated-managed-state'),
    cwd: directory, taskStateDir: join(directory, 'native-source'), node: 'node', viewer: join(directory, 'viewer.mjs'),
    herdr, notify: message => warnings.push(message),
    inspectShell: async pid => [{ ProcessId: pid, ExecutablePath: 'powershell.exe',
      CommandLine: 'powershell.exe', CreationDate: '2026-10-03T00:00:00.000Z' },
    ...(processes.get(`qa:role${pid - 7000}`) ?? []).map(row => ({
      ProcessId: row.pid, ParentProcessId: pid, ExecutablePath: row.argv[0],
      CommandLine: row.argv.map(value => `"${value}"`).join(' '), CreationDate: '2026-10-03T00:00:01.000Z',
    }))] };
  const controller = new FixedDagPane(options);
  console.log(JSON.stringify({ event: 'resource-planned', pid: process.pid, controlPipe: controller.controlPath }));
  const controllers = [controller];
  t.after(async () => {
    for (const owner of controllers) await owner.stop();
    await rm(directory, { recursive: true, force: true });
    console.log(JSON.stringify({ event: 'resource-cleaned', pid: process.pid, directory,
      controlPipes: controllers.map(owner => owner.controlPath) }));
  });
  return { directory, calls, warnings, events, panes, processes, options, controller, controllers,
    get focused() { return focused; }, set focused(value) { focused = value; } };
}
for (const selected of ['qa:parent', 'qa:role2']) test(`owned reopen preserves selected input pane ${selected}`, async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed; f.focused = selected;
  const original = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'neighbor') return { neighbor: { neighbor_pane_id: selected, layout: { tab_id: 'qa:tab', focused_pane_id: f.focused } } };
    if (args[0] === 'focus') { f.calls.push(args); f.focused = selected; return {}; }
    return original(...args);
  };
  await f.controller.openPair({ force: true });
  assert.equal(f.focused, selected, 'swap must not change foreground input target');
});
for (const moment of ['split', 'swap', 'neighbor']) test(`later manual selection after ${moment} is never overwritten`, async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed;
  const original = f.controller.herdr;
  f.controller.herdr = async (...args) => { const result = await original(...args); if (args[0] === moment) f.focused = 'qa:role2'; return result; };
  await f.controller.openPair({ force: true }).catch(error => assert.match(error.message, /layout-waiting/));
  assert.equal(f.focused, 'qa:role2', 'late manual selection is authoritative');
  assert.equal(f.calls.filter(args => args[0] === 'focus').length, 0);
  if (moment === 'split') assert.equal(f.calls.filter(args => args[0] === 'swap').length, 0);
});
test('inactive selected tab defers before any split or swap', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed;
  f.panes.set('foreign:selected', { pane_id: 'foreign:selected', tab_id: 'foreign:tab' }); f.focused = 'foreign:selected';
  const count = f.calls.filter(args => ['split', 'swap', 'focus'].includes(args[0])).length;
  await assert.rejects(f.controller.openPair({ force: true }),
    error => /^layout-waiting: /.test(error.message) && error.message.includes('/dashboard'));
  assert.equal(f.calls.filter(args => ['split', 'swap', 'focus'].includes(args[0])).length, count);
  assert.equal(f.focused, 'foreign:selected');
  f.focused = 'qa:parent'; await f.controller.openPair({ force: true });
  assert.equal(f.focused, 'qa:parent');
});

test('managed dashboard without a coordinator stays BLOCKED with the unchanged parsed prefixes', async t => {
  const f = await setup(t);
  f.controller.managedAvailable = true; f.controller.coordinator = null;
  for (const call of [() => f.controller.open(), () => f.controller.openPair(),
    () => f.controller.mutate('dag', 'arrange', 'qa:parent', ['focus', '--pane', 'qa:parent'])])
    await assert.rejects(call(), error => error.message.startsWith('BLOCKED: ') && error.inlineFallback === true);
  assert.equal(f.controller.status, 'BLOCKED');
  // The explicit open reports once; the identical automatic openPair failure is suppressed.
  assert.equal(f.warnings.length, 1);
  for (const warning of f.warnings) assert.ok(warning.startsWith('DAG pane: BLOCKED: '));
});
test('initial source save precedes launches; concurrent opens create exactly one fixed pair', async t => {
  const f = await setup(t);
  await f.controller.start();
  await f.controller.provider().sourceReady;
  assert.equal(f.calls.length, 0);
  assert.deepEqual(JSON.parse(await readFile(f.controller.stateFile, 'utf8')).runs, []);
  await Promise.all(Array.from({ length: 5 }, () => f.controller.openPair()));
  const splits = f.calls.filter(call => call[0] === 'split');
  assert.equal(splits.length, 2);
  assert.equal(splits[0][splits[0].indexOf('--direction') + 1], 'right');
  assert.equal(splits[1][splits[1].indexOf('--direction') + 1], 'down');
  assert.equal(splits[1][splits[1].indexOf('--ratio') + 1], '0.4');
  assert.ok(splits.every(call => call.includes('--no-focus')));
  assert.ok(f.calls.filter(call => call[0] === 'run').some(call => call[2].includes("'--view' 'dag'")));
  assert.ok(f.calls.filter(call => call[0] === 'run').some(call => call[2].includes("'--view' 'tasks'")));
  assert.equal(f.calls.filter(call => ['close', 'resize', 'focus'].includes(call[0])).length, 0);
});

test('late offers serialize existing role writes without launching or emitting source revisions', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const before = f.calls.filter(call => call[0] === 'run').length;
  const sourceEvents = [];
  const off = f.controller.provider().subscribeSource(source => sourceEvents.push(source));
  const offers = Object.fromEntries(['dag', 'workers'].map(role => [role, { pipePath: ipcPath('synthetic-presence'),
    v: 1, token: 'presence-token', ownerKey: f.controller.key, sessionId, scopeEpoch: 2, role, sequence: 0 }]));
  await f.controller.setOffers(offers);
  off();
  for (const role of ['dag', 'workers']) {
    const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
    assert.deepEqual(state.presenceOffer, offers[role]);
    assert.equal(state.scopeEpoch, 2);
  }
  assert.equal(sourceEvents.length, 0);
  assert.equal(f.calls.filter(call => call[0] === 'run').length, before);
  await assert.rejects(f.controller.setOffers({ ...offers, dag: { ...offers.dag, scopeEpoch: 1 } }), /Stale/);
});

test('explicit column reopen rebuilds only verified surviving roles when a fixed role was closed', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6002); await closed;
  const claim = { ...f.controller.binding, tabId: 'qa:tab', lifetimeId: f.controller.lifetimeId, layout: 'columns' };
  const pair = await f.controller.openPair({ force: true, claim });
  assert.notEqual(pair.dag.paneId, 'qa:role1');
  assert.notEqual(pair.workers.paneId, state.paneId);
  assert.ok(f.calls.some(call => call[0] === 'close' && call[1] === 'qa:role1'));
  assert.equal(f.panes.size, 3);
  assert.equal(f.focused, 'qa:parent');
});

test('reported task metrics and DAG topology survive fixed-role source updates', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  await f.controller.receive(payload());
  await f.controller.receiveTasks({ parent_session_id: sessionId, tasks: [
    { task_id: 'st_ordinary', description: 'Ordinary', status: 'running', updated_at: '2026-09-01T00:00:00Z',
      run_stats: { output_tokens: 28087, generation_ms: 72394, tokens_per_second: 388, cost_usd: 0, cost_status: 'reported' } },
  ] });
  for (const role of ['dag', 'workers']) {
    const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
    assert.equal(state.runs[0].nodes.length, payload().runs[0].nodes.length);
    assert.equal(state.runs[0].edges.length, payload().runs[0].edges.length);
    assert.equal(state.tasks.find(task => task.id === 'st_ordinary').metrics.tokens_per_second, 388);
    assert.equal(state.tasks.find(task => task.id === 'st_ordinary').metrics.cost_usd, 0);
  }
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
});

test('manual workers close is durable, source updates do not reopen, explicit pair open reuses DAG', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  assert.equal((await control('close', state, 6002)).type, 'closed');
  await closed;
  await f.controller.receive(payload());
  await f.controller.openPair();
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
  assert.equal(JSON.parse(await readFile(f.controller.paneFile('workers'), 'utf8')).manualClose, true);
  await f.controller.openPair({ force: true });
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 3);
  assert.equal((await f.controller.inspectPair()).dag.paneId, 'qa:role1');
});

test('workers reopen splits the verified surviving Todo region and places workers before Todo', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const todo = { ...f.controller.binding, role: 'todo', paneId: 'qa:todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true, manualClose: false };
  f.panes.set(todo.paneId, { pane_id: todo.paneId, tab_id: todo.tabId });
  f.controller.coordinator = { binding: f.controller.binding, lifetimeId: 'owned', inspectTodo: async () => todo };
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6002); await closed;
  await f.controller.openPair({ force: true });
  const split = f.calls.filter(call => call[0] === 'split').at(-1);
  assert.equal(split[split.indexOf('--pane') + 1], todo.paneId);
  assert.equal(split[split.indexOf('--ratio') + 1], '0.5');
  assert.deepEqual(f.calls.find(call => call[0] === 'swap'), ['swap', '--source-pane', 'qa:role3', '--target-pane', todo.paneId]);
});

test('DAG reopen splits surviving workers before them without a second right column', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed;
  await f.controller.openPair({ force: true });
  const split = f.calls.filter(call => call[0] === 'split').at(-1);
  assert.equal(split[split.indexOf('--pane') + 1], 'qa:role2');
  assert.equal(split[split.indexOf('--direction') + 1], 'down');
  assert.deepEqual(f.calls.find(call => call[0] === 'swap'), ['swap', '--source-pane', 'qa:role3', '--target-pane', 'qa:role2']);
});

test('both fixed roles reopen inside surviving Todo without changing its pane identity', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const todo = { ...f.controller.binding, role: 'todo', paneId: 'qa:todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true, manualClose: false };
  f.panes.set(todo.paneId, { pane_id: todo.paneId, tab_id: todo.tabId });
  f.controller.coordinator = { binding: f.controller.binding, lifetimeId: 'owned', inspectTodo: async () => todo };
  for (const [role, pid] of [['dag', 6001], ['workers', 6002]]) {
    const state = JSON.parse(await readFile(f.controller.roleFile(role), 'utf8'));
    const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
    await control('close', state, pid); await closed;
  }
  await f.controller.openPair({ force: true });
  const splits = f.calls.filter(call => call[0] === 'split').slice(2);
  assert.deepEqual(splits.map(call => call[call.indexOf('--pane') + 1]), [todo.paneId, todo.paneId]);
  assert.deepEqual(splits.map(call => call[call.indexOf('--ratio') + 1]), ['0.4', '0.5']);
  assert.equal(f.panes.has(todo.paneId), true);
});

test('DAG reopen restores the actually observed 23/23 collapsed owned region before splitting', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const todo = { ...f.controller.binding, role: 'todo', paneId: 'qa:todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true, manualClose: false };
  f.panes.set(todo.paneId, { pane_id: todo.paneId, tab_id: todo.tabId });
  f.controller.coordinator = { binding: f.controller.binding, lifetimeId: 'owned', inspectTodo: async () => todo };
  const state = JSON.parse(await readFile(f.controller.roleFile('dag'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(3000) });
  await control('close', state, 6001); await closed;
  // Exact public Herdr rects from the real 18/14/14 -> 23/23 collapse probe.
  const rects = [
    { pane_id: 'qa:parent', rect: { x: 0, y: 0, width: 112, height: 46 } },
    { pane_id: 'qa:role2', rect: { x: 112, y: 0, width: 60, height: 23 } },
    { pane_id: 'qa:todo', rect: { x: 112, y: 23, width: 60, height: 23 } },
  ];
  const originalHerdr = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'layout' && !f.panes.has('qa:role3')) { f.calls.push(args); return { layout: {
      tab_id: 'qa:tab', focused_pane_id: f.focused, panes: structuredClone(rects) } }; }
    if (args[0] === 'resize') {
      rects[1].rect.height = 32; rects[2].rect.y = 32; rects[2].rect.height = 14;
    }
    return originalHerdr(...args);
  };
  const start = f.calls.length;
  await f.controller.openPair({ force: true });
  const actions = f.calls.slice(start).filter(call => ['resize', 'split', 'swap'].includes(call[0]));
  assert.equal(actions[0]?.[0], 'resize', 'Collapsed 23/23 region must become 32/14 before inserting the DAG');
  assert.equal(actions[0][actions[0].indexOf('--pane') + 1], 'qa:role2');
  assert.equal(actions[0][actions[0].indexOf('--direction') + 1], 'down');
  assert.equal(Number(actions[0][actions[0].indexOf('--amount') + 1]), 9 / 46);
  assert.equal(actions[1][0], 'split'); assert.equal(actions[2][0], 'swap');
  assert.equal(f.panes.has(todo.paneId), true);
});

for (const mismatch of ['ownerKey', 'sessionId', 'scopeEpoch', 'role', 'tabId', 'ready']) test(`reopen rejects a ${mismatch} mismatch in the Todo anchor before pane changes`, async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const todo = { ...f.controller.binding, role: 'todo', paneId: 'qa:todo', tabId: 'qa:tab', scopeEpoch: 1, ready: true, manualClose: false, [mismatch]: 'foreign' };
  f.controller.coordinator = { binding: f.controller.binding, lifetimeId: 'owned', inspectTodo: async () => todo };
  const before = f.calls.filter(call => ['split', 'swap', 'close', 'run'].includes(call[0])).length;
  await assert.rejects(f.controller.openPair({ force: true }), /layout-waiting/);
  assert.equal(f.calls.filter(call => ['split', 'swap', 'close', 'run'].includes(call[0])).length, before);
});

test('a concurrent fixed-pair adoption cannot overwrite the workers manual close', { timeout: 7000 }, async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair();
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const entered = new EventEmitter();
  let resolve;
  const release = new Promise(done => { resolve = done; });
  const original = f.controller.herdr;
  let paused = false;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'process-info' && args[2] === state.paneId && !paused) {
      paused = true; entered.emit('adoption'); await release;
    }
    return original(...args);
  };
  const adopting = once(entered, 'adoption', { signal: AbortSignal.timeout(2000) });
  const updating = f.controller.openPair();
  await adopting;
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(2000) });
  try {
    assert.equal((await control('close', state, 6002)).type, 'closed');
    await closed;
  } finally { resolve(); }
  await updating;
  await f.controller.receive(payload());
  assert.equal(JSON.parse(await readFile(f.controller.paneFile('workers'), 'utf8')).manualClose, true);
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
});

for (const kind of ['legacy', 'foreign', 'ambiguous']) test(`${kind} ownership does not adopt, close or split`, async t => {
  const f = await setup(t); await f.controller.start();
  if (kind === 'legacy') await writeJson(f.controller.legacyRecordFile, { paneId: 'qa:legacy', ready: true });
  if (kind === 'foreign') await writeJson(f.controller.paneFile('dag'), { role: 'dag', ownerKey: 'foreign' });
  if (kind === 'ambiguous') f.panes.set('alias', { pane_id: 'qa:parent', tab_id: 'foreign:tab' });
  if (kind === 'ambiguous') await assert.rejects(f.controller.openPair(), /ambiguous/);
  else await f.controller.openPair();
  assert.equal(f.calls.filter(call => ['split', 'close'].includes(call[0])).length, 0);
});

test('failed launch records its exact attempt and does not spawn another orphan on source or open', async t => {
  const f = await setup(t);
  const original = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'run') throw new Error('launch failed');
    return original(...args);
  };
  await f.controller.start();
  await assert.rejects(f.controller.openPair(), /launch failed/);
  await f.controller.receive(payload());
  await f.controller.openPair();
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 1);
  assert.equal(f.calls.filter(call => call[0] === 'close').length, 1);
  assert.equal(f.panes.size, 1, 'failed launch must leave no owned orphan pane');
});

for (const kind of ['foreign-child', 'foreign-sibling', 'missing-shell', 'query-failure', 'owned-child'])
test(`failed launch ${kind} uses complete native observation for cleanup`, async t => {
  const f = await setup(t), original = f.controller.herdr, inspect = f.options.inspectShell;
  let launched = false, argv;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'run') {
      argv = [...args[2].matchAll(/'((?:''|[^'])*)'/g)].map(match => match[1].replaceAll("''", "'"));
      launched = true; throw new Error('launch failed');
    }
    return original(...args);
  };
  f.controller.inspectShell = async pid => {
    const rows = await inspect(pid);
    if (!launched) return rows;
    if (kind === 'query-failure') throw new Error('CIM failed');
    if (kind === 'missing-shell') return [];
    const owned = { ProcessId: 6001, ParentProcessId: pid, ExecutablePath: argv[0],
      CommandLine: argv.map(value => `"${value}"`).join(' '), CreationDate: '2026-10-03T00:00:01.000Z' };
    const foreign = { ...owned, ProcessId: 8100, ExecutablePath: 'foreign.exe', CommandLine: 'foreign.exe' };
    return [...rows, ...(kind === 'foreign-child' ? [foreign] : kind === 'foreign-sibling' ? [owned, foreign] : [owned])];
  };
  await f.controller.start();
  await assert.rejects(f.controller.openPair(), /launch failed/);
  assert.equal(f.calls.filter(call => call[0] === 'close').length, kind === 'owned-child' ? 1 : 0);
  assert.equal(f.panes.size, kind === 'owned-child' ? 1 : 2);
});

test('shell/native child registration supports pair reuse and authenticated manual close', async t => {
  const f = await setup(t), original = f.controller.herdr;
  f.controller.herdr = async (...args) => {
    if (args[0] === 'process-info') {
      const pid = 7000 + Number(args[2].replace('qa:role', ''));
      return { process_info: { shell_pid: pid, foreground_processes: [{ pid, argv: ['powershell.exe'] }] } };
    }
    return original(...args);
  };
  await f.controller.start(); await f.controller.openPair(); await f.controller.openPair();
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
  const state = JSON.parse(await readFile(f.controller.roleFile('workers'), 'utf8'));
  const closed = once(f.events, `closed:${state.paneId}`, { signal: AbortSignal.timeout(2000) });
  assert.equal((await control('close', state, 6002)).type, 'closed');
  await closed;
  assert.equal(JSON.parse(await readFile(f.controller.paneFile('workers'), 'utf8')).manualClose, true);
});

test('reload verifies the old process, refreshes lifetime offers and never changes existing dimensions', async t => {
  const f = await setup(t); await f.controller.start(); await f.controller.openPair(); await f.controller.stop();
  const next = new FixedDagPane({ ...f.options, scopeEpoch: 2 });
  console.log(JSON.stringify({ event: 'resource-planned', pid: process.pid, controlPipe: next.controlPath }));
  f.controllers.push(next);
  await next.start(); await next.openPair();
  assert.equal(f.calls.filter(call => call[0] === 'split').length, 2);
  assert.equal(f.calls.filter(call => call[0] === 'run').length, 2);
  assert.equal(f.calls.filter(call => ['resize', 'close', 'focus'].includes(call[0])).length, 0);
  assert.equal(JSON.parse(await readFile(next.roleFile('dag'), 'utf8')).scopeEpoch, 2);
  const before = await readFile(next.roleFile('dag'), 'utf8');
  await f.controller.receive(payload());
  assert.equal(await readFile(next.roleFile('dag'), 'utf8'), before);
});

test('failed initial source save rejects readiness and cannot authorize a fixed pair launch', async t => {
  const f = await setup(t);
  await writeFile(f.controller.directory, 'blocked directory');
  await assert.rejects(f.controller.start());
  await assert.rejects(f.controller.provider().sourceReady);
  await assert.rejects(f.controller.stop());
  assert.equal(f.controller.control.listening, false);
  assert.equal(f.calls.filter(call => ['split', 'run'].includes(call[0])).length, 0);
});
