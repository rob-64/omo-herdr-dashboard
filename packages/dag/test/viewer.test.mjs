import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { EventEmitter, once } from 'node:events';
import { createHash, randomUUID } from 'node:crypto';
import { watch } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { stripVTControlCharacters } from 'node:util';
import { messages } from '../src/i18n.mjs';
import { spinnerFrames, width } from '../src/render.mjs';
import { writeJson } from '../src/storage.mjs';
import { TASK_SCOPE } from '../src/view-state.mjs';

// Only the selected (or followed) title is bold: its SGR opens with 1; right before the status icon.
const selectedTitle = (raw, title) => new RegExp(`\\x1b\\[1;[\\d;]*m${title.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`).test(raw);
const includesRunning = (raw, suffix) => spinnerFrames.some(glyph => selectedTitle(raw, `${glyph} ${suffix}`));

for (const view of ['dag', 'tasks']) test(`real ${view} PTY pauses on mouse and resumes on blur`, { timeout: 25000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'mouse-pty-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const state = { sessionId: 'mouse-pty', language: 'en', connected: true,
    tasks: Array.from({ length: 20 }, (_, index) => ({ id: `task-${index}`,
      status: index === 19 ? 'running' : 'pending', description: `MOUSE_TASK_${index}` })),
    runs: view === 'dag' ? [{ id: 'run', name: 'MOUSE_RUN', status: 'running',
      nodes: Array.from({ length: 20 }, (_, index) => ({ id: `node-${index}`,
        state: index === 19 ? 'running' : 'pending', label: `MOUSE_NODE_${index}`, taskId: `task-${index}` })),
      edges: [] }] : [] };
  await writeJson(file, state);
  const viewer = openViewer(file, t, ['--view', view]);
  await viewer.frame(text => text.includes(messages.en.followOn));
  await viewer.frame(text => text.includes(messages.en.followOff),
    () => viewer.send({ keys: '\x1b[<0;4;8M' }));
  await viewer.frame(text => text.includes(messages.en.followOff),
    () => viewer.send({ keys: '\x1b[<32;4;12M' }));
  await viewer.frame(text => text.includes(messages.en.followOff),
    () => viewer.send({ keys: '\x1b[<64;4;12M' }));
  const following = await viewer.frame(text => text.includes(messages.en.followOn) &&
    text.includes(view === 'dag' ? 'MOUSE_NODE_19' : 'MOUSE_TASK_19'),
  () => viewer.send({ keys: '\x1b[<0;4;12m\x1b[O' }));
  assert.ok(following.includes(view === 'dag' ? 'MOUSE_NODE_19' : 'MOUSE_TASK_19'));
  await viewer.close();
});

test('real standalone PTY respects saved folds, preserves task selection, switches mixed views and persists', { timeout: 25000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'tasks-pty-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const state = { sessionId: 'tasks-pty', language: 'ko', connected: true, runs: [], tasks: [
    { id: 'st_b', status: 'completed', description: 'SECOND_DESCRIPTION' },
    { id: 'st_a', status: 'running', description: 'FIRST_DESCRIPTION 한글🙂', category: 'REAL_CATEGORY', model: 'REAL_MODEL', progress: 'FIRST_PROGRESS' },
    { id: 'st_child', parentTaskId: 'st_a', status: 'lost', description: 'EXPLICIT_CHILD' },
  ] };
  await writeJson(file, state);
  const preferences = { version: 1, sessionId: state.sessionId, expanded: { '[null,"st_a"]': false, '[null,"st_0"]': false } };
  await writeJson(`${file}.view.json`, preferences);
  let viewer = openViewer(file, t);
  const fresh = await viewer.frame((text, raw) => includesRunning(raw, '▸ FIRST_DESCRIPTION'));
  assert.equal(fresh.split('\n').filter(line => line.startsWith('│')).length, 1);
  assert.doesNotMatch(fresh, /FIRST_PROGRESS|REAL_MODEL|EXPLICIT_CHILD/);
  const pausedPreferences = await preferenceWritten(file, value => value.navigation?.follow === false,
    () => viewer.frame(text => text.includes('FIRST_PROGRESS'), () => viewer.send({ keys: 'd' })));
  const foldedAgain = await viewer.frame((text, raw) => includesRunning(raw, '▸ FIRST_DESCRIPTION'), () => viewer.send({ keys: 'd' }));
  assert.doesNotMatch(foldedAgain, /FIRST_PROGRESS|EXPLICIT_CHILD/);
  assert.deepEqual(pausedPreferences.expanded, preferences.expanded);
  assert.deepEqual(pausedPreferences.navigation, { follow: false });
  const initial = await viewer.frame((text, raw) => includesRunning(raw, 'FIRST_DESCRIPTION') && text.includes('REAL_MODEL'), () => viewer.send({ keys: ' ' }));
  for (const value of [messages.ko.tasks, 'REAL_CATEGORY', 'REAL_MODEL']) assert.ok(initial.includes(value), value);
  assert.ok(!initial.includes('FIRST_PROGRESS'));
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ SECOND_DESCRIPTION'), () => viewer.send({ keys: '\t' }));
  await viewer.frame((text, raw) => includesRunning(raw, 'FIRST_DESCRIPTION'), () => viewer.send({ keys: '\x1b[Z' }));
  const collapsed = await viewer.frame((text, raw) => includesRunning(raw, '▸ FIRST_DESCRIPTION'), () => viewer.send({ keys: ' ' }));
  assert.ok(!collapsed.includes('FIRST_PROGRESS'));
  assert.ok(!collapsed.includes('EXPLICIT_CHILD'));
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ SECOND_DESCRIPTION'), () => viewer.send({ keys: 'p' }));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ FIRST_DESCRIPTION'), () => viewer.send({ keys: 'n' }));
  state.tasks.reverse();
  state.tasks.push({ id: 'st_0', status: 'running', description: 'NEW_DESCRIPTION 새 작업 한글🙂', progress: 'NEW_PROGRESS' });
  await viewer.frame((text, raw) => includesRunning(raw, '▸ FIRST_DESCRIPTION') && text.includes(`${messages.ko.tasks} (3)`), () => writeJson(file, state));
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ SECOND_DESCRIPTION'), () => viewer.send({ keys: 'n' }));
  const arrival = await viewer.frame((text, raw) => includesRunning(raw, '▸ NEW_DESCRIPTION'), () => viewer.send({ keys: 'n' }));
  assert.doesNotMatch(arrival, /NEW_PROGRESS/);
  await viewer.frame((text, raw) => includesRunning(raw, '▸ FIRST_DESCRIPTION'), () => viewer.send({ keys: 'n' }));
  state.runs = [
    { id: 'r1', name: 'FIRST_DAG', status: 'running', nodes: [{ id: 'n', label: 'DAG_NODE', state: 'running', taskId: 'st_dag' }], edges: [] },
    { id: 'r2', name: 'SECOND_DAG', status: 'running', nodes: [{ id: 'n', label: 'OTHER_NODE', state: 'running', taskId: 'st_b' }], edges: [] },
  ];
  state.tasks.push({ id: 'st_dag', description: 'DAG_ONLY_DESCRIPTION' });
  const mixed = await viewer.frame((text, raw) => text.includes('DAG (2)') && includesRunning(raw, '▸ FIRST_DESCRIPTION'), () => writeJson(file, state));
  assert.ok(!mixed.includes('DAG_ONLY_DESCRIPTION'));
  assert.ok(!mixed.includes('SECOND_DESCRIPTION'));
  await viewer.frame(text => text.includes('FIRST_DAG') && text.includes(`${messages.ko.tasks} (2)`), () => viewer.send({ keys: 't' }));
  await viewer.frame(text => text.includes('SECOND_DAG'), () => viewer.send({ keys: '\x1b[C' }));
  await viewer.frame(text => text.includes('FIRST_DAG'), () => viewer.send({ keys: '\x1b[D' }));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ FIRST_DESCRIPTION') && !text.includes('FIRST_DAG'), () => viewer.send({ keys: 't' }));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ NEW_DESCRIPTION'), () => viewer.send({ keys: 'p' }));
  const narrow = await viewer.frame(text => text.split('\n').length === 18 && text.includes('NEW_DESCRIPTION'), () => viewer.send({ resize: [18, 35] }));
  assert.ok(narrow.split('\n').every(line => width(line) < 35));
  state.runs = [];
  await viewer.frame((text, raw) => !text.includes('t DAG') && includesRunning(raw, '▸ NEW_DESCRIPTION'), () => writeJson(file, state));
  await viewer.close();
  assert.equal(JSON.parse(await readFile(`${file}.view.json`, 'utf8')).expanded[JSON.stringify([TASK_SCOPE, 'st_a'])], false);
  assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), state);
  viewer = openViewer(file, t);
  await viewer.frame((text, raw) => includesRunning(raw, '▸ NEW_DESCRIPTION'));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ FIRST_DESCRIPTION') && !text.includes('FIRST_PROGRESS'), () => viewer.send({ keys: 'n' }));
  await viewer.frame((text, raw) => includesRunning(raw, 'FIRST_DESCRIPTION') && text.includes('REAL_MODEL'), () => viewer.send({ keys: '\r' }));
  await viewer.close();
  assert.equal(JSON.parse(await readFile(`${file}.view.json`, 'utf8')).expanded[JSON.stringify([TASK_SCOPE, 'st_a'])], true);
  viewer = openViewer(file, t);
  await viewer.frame((text, raw) => includesRunning(raw, 'FIRST_DESCRIPTION') && text.includes('REAL_MODEL'));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ NEW_DESCRIPTION'), () => viewer.send({ keys: 'p' }));
  const restored = await viewer.frame((text, raw) => includesRunning(raw, 'FIRST_DESCRIPTION') && text.includes('REAL_MODEL'), () => viewer.send({ keys: 'n' }));
  assert.ok(restored.includes('EXPLICIT_CHILD'));
  assert.ok(!restored.includes('FIRST_PROGRESS'));
  await viewer.close();
  console.log('Standalone PTY evidence: saved false folded, preference-preserving d peek, stored true restored, startup, Tab/Shift-Tab/n/p, Space/Enter, arrivals/reorder, mixed DAG toggle/all-run exclusion, CJK resize, restart persistence.');
});

test('real PTY compact cards expose full selected progress with d without persisting detail or changing folds', { timeout: 25000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'compact-pty-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const progress = `PROGRESS_HEAD ${'한글🙂 useful work '.repeat(12)} PROGRESS_TAIL`;
  const state = { sessionId: 'compact-pty', language: 'ko', connected: true, runs: [], tasks: [
    { id: 'a-full-id', status: 'completed', description: 'COMPACT_DESCRIPTION', agent: 'worker', category: 'visual',
      model: 'opencodex/gpt-6-astra (opencodex)', progress, turns: 20, toolCalls: 65,
      startedAt: '2026-09-06T00:00:00Z', completedAt: '2026-09-06T00:09:05Z' },
    { id: 'b-full-id', status: 'failed', description: 'SECOND_COMPACT', progress: 'SECOND_PROGRESS' },
    { id: 'c-full-id' },
  ] };
  await writeJson(file, state);
  const expanded = Object.fromEntries(state.tasks.map(task => [JSON.stringify([TASK_SCOPE, task.id]), true]));
  await writeJson(`${file}.view.json`, { version: 1, sessionId: state.sessionId, expanded });
  let viewer = openViewer(file, t);
  const initial = await viewer.frame((text, raw) => selectedTitle(raw, '✓ COMPACT_DESCRIPTION') && text.split('\n').length === 32,
    () => viewer.send({ resize: [32, 80] }));
  assert.equal(initial.split('\n').filter(line => line.startsWith('╭')).length, 3);
  const interiors = initial.split('\n').filter(line => line.startsWith('│'));
  assert.equal(interiors.length, 5);
  assert.match(interiors[1], /\[worker\/visual\] gpt-6-astra \(opencodex\)/);
  assert.match(interiors[2], /9분 5초 · 20턴 · 도구 65회/);
  assert.match(interiors[3], /× SECOND_COMPACT/);
  assert.match(interiors[4], /\? -/);
  assert.doesNotMatch(initial, /PROGRESS_HEAD|SECOND_PROGRESS/);
  assert.ok(interiors.every(line => line.replace(/[│ ]/g, '')));
  assert.doesNotMatch(initial, /a-full-id|b-full-id|c-full-id|PROGRESS_TAIL|2026-09-06|opencodex\//);
  await viewer.frame(text => text.split('\n').length === 50, () => viewer.send({ resize: [50, 80] }));
  const full = await viewer.frame(text => text.includes('PROGRESS_TAIL'), () => viewer.send({ keys: 'd' }));
  for (const token of ['a-full-id', '2026-09-06T00:00:00Z', '2026-09-06T00:09:05Z', 'gpt-6-astra']) assert.ok(full.includes(token), token);
  assert.doesNotMatch(full, /opencodex\//);
  assert.doesNotMatch(full, /b-full-id|c-full-id/);
  await viewer.frame((text, raw) => !text.includes('PROGRESS_TAIL') && !text.includes('PROGRESS_HEAD') && selectedTitle(raw, '✓ COMPACT_DESCRIPTION'), () => viewer.send({ keys: 'd' }));
  await viewer.frame(text => text.includes('PROGRESS_TAIL'), () => viewer.send({ keys: 'd' }));
  const next = await viewer.frame((text, raw) => selectedTitle(raw, '× SECOND_COMPACT'), () => viewer.send({ keys: 'n' }));
  assert.doesNotMatch(next, /a-full-id|b-full-id|PROGRESS_TAIL/);
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ COMPACT_DESCRIPTION'), () => viewer.send({ keys: 'p' }));
  const folded = await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ COMPACT_DESCRIPTION'), () => viewer.send({ keys: ' ' }));
  assert.doesNotMatch(folded, /PROGRESS_HEAD|PROGRESS_TAIL/);
  await viewer.frame(text => text.includes('PROGRESS_TAIL'), () => viewer.send({ keys: 'd' }));
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ COMPACT_DESCRIPTION') && !text.includes('PROGRESS_HEAD'), () => viewer.send({ keys: 'd' }));
  await viewer.frame(text => text.includes('PROGRESS_TAIL'), () => viewer.send({ keys: 'd' }));
  await viewer.close();
  const saved = JSON.parse(await readFile(`${file}.view.json`, 'utf8'));
  assert.equal(saved.version, 1);
  assert.equal(saved.sessionId, state.sessionId);
  assert.deepEqual(saved.expanded, { ...expanded, '[null,"a-full-id"]': false });
  assert.equal(saved.navigation.follow, false);
  assert.equal(saved.navigation.selectedTaskId, 'a-full-id');
  viewer = openViewer(file, t);
  const restarted = await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ COMPACT_DESCRIPTION'));
  assert.doesNotMatch(restarted, /PROGRESS_HEAD|PROGRESS_TAIL|a-full-id/);
  await viewer.frame(text => text.includes('9분 5초') && !text.includes('PROGRESS_HEAD') && !text.includes('▸ COMPACT_DESCRIPTION'), () => viewer.send({ keys: '\r' }));
  await viewer.close();
  assert.deepEqual(JSON.parse(await readFile(`${file}.view.json`, 'utf8')).expanded, { ...expanded, '[null,"a-full-id"]': true });
  assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), state);
  console.log('Compact PTY evidence: three content-height cards including metrics, description/status, provider suffix, Korean counts, ellipsis, full progress/times, d on/off, selection reset, collapsed peek, restart without verbose persistence.');
});

test('real PTY invalid preferences show folded cards without overwriting corrupt preferences', { timeout: 15000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'invalid-view-pty-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  await writeJson(file, { sessionId: 'invalid', connected: true, runs: [], tasks: [
    { id: 'task', status: 'completed', description: 'INVALID_PREF_TASK', progress: 'HIDDEN_PROGRESS' },
  ] });
  const invalid = { version: 1, sessionId: 'invalid', expanded: { '[null,"task"]': 'true' } };
  await writeJson(`${file}.view.json`, invalid);
  const viewer = openViewer(file, t);
  const initial = await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ INVALID_PREF_TASK'));
  assert.equal(initial.split('\n').filter(line => line.startsWith('│')).length, 0);
  assert.doesNotMatch(initial, /HIDDEN_PROGRESS/);
  await viewer.frame(text => text.includes('HIDDEN_PROGRESS'), () => viewer.send({ keys: 'd' }));
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ INVALID_PREF_TASK') && !text.includes('HIDDEN_PROGRESS'), () => viewer.send({ keys: 'd' }));
  await viewer.close();
  assert.deepEqual(JSON.parse(await readFile(`${file}.view.json`, 'utf8')), invalid);
});

for (const dag of [false, true]) test(`real ${dag ? 'DAG' : 'standalone'} PTY auto expansion follows live status and persists only manual overrides`, { timeout: 25000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'auto-expand-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const task = { id: 'task', status: dag ? 'completed' : 'pending', description: 'AUTO_CARD', progress: 'AUTO_PROGRESS' };
  const node = { id: 'node', label: 'AUTO_NODE', state: 'pending', taskId: task.id };
  const state = { sessionId: 'auto', connected: true, tasks: [task], runs: dag ? [
    { id: 'run', name: 'AUTO_RUN', status: 'running', nodes: [node], edges: [] },
  ] : [] };
  const preferenceKey = JSON.stringify([dag ? 'run' : TASK_SCOPE, dag ? node.id : task.id]);
  await writeJson(file, state);
  let viewer = openViewer(file, t);
  const matches = (expanded, verbose = false, includeGraph = true) => text => text.includes('AUTO_CARD') &&
    text.includes('AUTO_PROGRESS') === (dag ? expanded || verbose : verbose) &&
    text.includes(`Task: ${task.id}`) === verbose &&
    (verbose || text.includes('▸ AUTO_CARD') === !expanded) &&
    (!dag || !includeGraph || text.includes(`[${expanded ? '-' : '+'}] AUTO_NODE`));
  const verifiedFrame = async (expanded, verbose, action, current = () => true) => {
    const detail = await viewer.frame(text => matches(expanded, verbose, false)(text) && current(text), action);
    // Recorded metric details increased the verbose card height. Observe the
    // committed details first, then explicitly reveal the graph if offscreen.
    // The original complete graph/detail predicate still has to pass.
    const visible = matches(expanded, verbose)(detail) ? detail :
      await viewer.frame(text => matches(expanded, verbose)(text) && current(text),
        () => viewer.send({ keys: '\x1b[5~' }));
    assert.equal(matches(expanded, verbose)(visible), true);
    return visible;
  };
  const check = async (expanded, action, verbose = false) => {
    const text = await verifiedFrame(expanded, verbose, action);
    if (!verbose) assert.equal(text.split('\n').filter(line => line.startsWith('│')).length,
      expanded ? (dag ? 2 : 1) : !dag && task.status === 'completed' ? 0 : 1);
    return text;
  };
  const transition = async (status, expanded, verbose = false) => {
    if (dag) node.state = status;
    else task.status = status;
    task.progress = `AUTO_PROGRESS_${status}`;
    await verifiedFrame(expanded, verbose, () => writeJson(file, state),
      text => verbose || (dag && expanded) ? text.includes(task.progress) :
        (dag || !text.includes(task.progress)) && (expanded || text.includes(dag ? 'Completed' : '✓')));
  };
  await check(false);
  await transition('running', true);
  await viewer.frame(text => text.split('\n').length === 40 && matches(true)(text), () => viewer.send({ resize: [40, 80] }));
  await transition('completed', false);
  await transition('running', true);
  await check(true, () => viewer.send({ keys: 'd' }), true);
  await transition('completed', false, true);
  await check(false, () => viewer.send({ keys: 'd' }));
  await check(false, () => viewer.send({ keys: 'd' }), true);
  await transition('running', true, true);
  await check(true, () => viewer.send({ keys: 'd' }));
  await viewer.close();
  const manualOnly = JSON.parse(await readFile(`${file}.view.json`, 'utf8'));
  assert.deepEqual(manualOnly.expanded, {});
  assert.equal(manualOnly.navigation.follow, false);
  viewer = openViewer(file, t);
  await check(true);
  await check(false, () => viewer.send({ keys: ' ' }));
  await transition('completed', false);
  if (dag) { task.id = 'retry'; node.taskId = task.id; }
  if (dag) node.state = 'running'; else task.status = 'running';
  task.description = 'AUTO_CARD_RETRY';
  await viewer.frame(text => matches(false)(text) && text.includes('AUTO_CARD_RETRY'), () => writeJson(file, state));
  await viewer.close();
  const savedFalse = JSON.parse(await readFile(`${file}.view.json`, 'utf8'));
  assert.deepEqual(savedFalse.expanded, { [preferenceKey]: false });
  viewer = openViewer(file, t);
  await check(false);
  await transition('completed', false);
  await check(true, () => viewer.send({ keys: '\r' }));
  await transition('running', true);
  await transition('completed', true);
  await viewer.close();
  const savedTrue = JSON.parse(await readFile(`${file}.view.json`, 'utf8'));
  assert.deepEqual(savedTrue.expanded, { [preferenceKey]: true });
  viewer = openViewer(file, t);
  await check(true);
  await viewer.frame(text => text.split('\n').length === 40 && matches(true)(text), () => viewer.send({ resize: [40, 80] }));
  await check(true, () => viewer.send({ keys: 'd' }), true);
  await check(true, () => viewer.send({ keys: 'd' }));
  await viewer.close();
  assert.deepEqual(JSON.parse(await readFile(`${file}.view.json`, 'utf8')), savedTrue);
  assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), state);
});

// Standard-library PTY bridge: output and key actions are event-driven, with no
// dwell timers. A frame has one terminal erase token per terminal row.
const bridge = String.raw`
import os, sys, pty, subprocess, selectors, json, fcntl, termios, struct, signal
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 26, 80, 0, 0))
child = subprocess.Popen(sys.argv[1:], stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
os.close(slave)
selector = selectors.DefaultSelector()
selector.register(master, selectors.EVENT_READ)
selector.register(sys.stdin, selectors.EVENT_READ)
commands = b''
output = b''
height = 26
try:
  while True:
    for key, _ in selector.select():
      if key.fileobj == master:
        try: data = os.read(master, 65536)
        except OSError: data = b''
        if not data: sys.exit(child.wait())
        output += data
        start, end = b'\x1b[H', b'\x1b[K'
        while start in output:
          # A queued old-size frame must not consume rows from a resized frame.
          # Cursor-home starts a replacement screen, so keep the latest one.
          output = output[output.rindex(start):]
          if output.count(end) < height: break
          parts = output.split(end, height)
          frame = end.join(parts[:height]) + end
          output = parts[height]
          print(json.dumps({'frame': frame.decode('utf8')}), flush=True)
      else:
        data = os.read(sys.stdin.fileno(), 65536)
        if not data: sys.exit(0)
        commands += data
        while b'\n' in commands:
          command, commands = commands.split(b'\n', 1)
          value = json.loads(command)
          if 'keys' in value: os.write(master, value['keys'].encode('utf8'))
          if 'resize' in value:
            rows, columns = value['resize']
            height = rows
            fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack('HHHH', rows, columns, 0, 0))
            os.kill(child.pid, signal.SIGWINCH)
finally:
  if child.poll() is None:
    child.terminate()
    child.wait(timeout=5)
  os.close(master)
`;
import { ipcPath } from '../../coordination/src/platform.mjs';

async function preferenceWritten(file, predicate, action) {
  let resolve, reject;
  const changed = new Promise((done, fail) => { resolve = done; reject = fail; });
  const abort = AbortSignal.timeout(5000);
  const watcher = watch(join(file, '..'), (_event, name) => {
    if (String(name) !== file.split(/[\\/]/).at(-1) + '.view.json') return;
    void readFile(`${file}.view.json`, 'utf8').then(raw => {
      const value = JSON.parse(raw);
      if (predicate(value)) resolve(value);
    }, reject);
  });
  const failed = () => reject(new Error('Missing exact preference write'));
  abort.addEventListener('abort', failed, { once: true });
  watcher.on('error', reject);
  try { const [value] = await Promise.all([changed, action()]); return value; }
  finally { watcher.close(); abort.removeEventListener('abort', failed); }
}

function openViewer(file, t, options = []) {
  const events = new EventEmitter();
  const viewerArgs = [fileURLToPath(new URL('../src/viewer.mjs', import.meta.url)), '--state', file, ...options];
  const child = process.platform === 'win32'
    ? spawn(process.execPath, [fileURLToPath(new URL('./windows-pty.mjs', import.meta.url)), ...viewerArgs], { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, HERDR_ENV: '0' } })
    : spawn('python3', ['-u', '-c', bridge, process.execPath, ...viewerArgs], { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, HERDR_ENV: '0' } });
  console.log(`TASK8_PTY_RESOURCE ${JSON.stringify({ workerPid: child.pid, argv: viewerArgs, statePath: file })}`);
  let buffer = '', stderr = '', lastFrame = '';
  child.stderr.on('data', data => { stderr += data; });
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', data => {
    buffer += data;
    let end = buffer.indexOf('\n');
    while (end >= 0) {
      const value = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1);
      lastFrame = stripVTControlCharacters(value.frame).replaceAll('\r', '');
      // Waits also see the raw frame: selection is told apart only by SGR weight.
      events.emit('frame', lastFrame, value.frame.replaceAll('\r', ''));
      end = buffer.indexOf('\n');
    }
  });
  t.after(async () => {
    if (child.exitCode === null) {
      const exited = once(child, 'exit', { signal: AbortSignal.timeout(5000) });
      child.stdin.end(); await exited;
    }
    console.log(`TASK8_PTY_CLEANED ${JSON.stringify({ workerPid: child.pid, exitCode: child.exitCode, statePath: file })}`);
  });
  const send = value => child.stdin.write(`${JSON.stringify(value)}\n`);
  function frame(predicate, action = () => {}, { raw: keepRaw = false } = {}) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => finish(new Error(`PTY frame timeout\n${lastFrame}\n${stderr}`)), 5000);
      const onFrame = (text, raw) => { if (predicate(text, raw)) finish(null, keepRaw ? { text, raw } : text); };
      const onExit = code => finish(new Error(`Viewer exited ${code}: ${stderr}\n${lastFrame}`));
      function finish(error, text) {
        clearTimeout(timer); events.off('frame', onFrame); child.off('exit', onExit);
        if (error) reject(error); else resolve(text);
      }
      events.on('frame', onFrame); child.on('exit', onExit);
      Promise.resolve().then(action).catch(finish);
    });
  }
  return { frame, send, async close() {
    const exited = once(child, 'exit', { signal: AbortSignal.timeout(5000) });
    send({ keys: 'q' });
    assert.equal((await exited)[0], 0, stderr);
  } };
}

test('real viewer PTY selects, toggles, refreshes, switches runs and persists across restart', { timeout: 25000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dag-pty-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const state = { sessionId: 'pty-session', connected: true, runs: [
    { id: 'r1', name: 'FIRST_RUN', status: 'running', nodes: [
      { id: 'a', label: 'ALPHA', state: 'running', taskId: 'ta' },
      { id: 'b', label: 'BETA', state: 'pending', taskId: 'tb' },
    ], edges: [{ from: 'a', to: 'b' }] },
    { id: 'r2', name: 'SECOND_RUN', status: 'running', nodes: [
      { id: 'a', label: 'OTHER_ALPHA', state: 'running', taskId: 'other' },
    ], edges: [] },
  ], tasks: [
    { id: 'ta', description: 'ALPHA_DESCRIPTION', progress: 'ALPHA_PROGRESS' },
    { id: 'tb', description: 'BETA_DESCRIPTION', progress: 'BETA_PROGRESS' },
    { id: 'other', description: 'OTHER_DESCRIPTION' },
  ] };
  await writeJson(file, state);
  await writeJson(`${file}.view.json`, { version: 1, sessionId: state.sessionId,
    expanded: { '["r1","a"]': false, '["r1","c"]': false, '["r2","a"]': false } });
  let viewer = openViewer(file, t);
  const fresh = await viewer.frame(text => text.includes('FIRST_RUN'), undefined, { raw: true });
  assert.match(fresh.raw, /\x1b\[1;[\d;]*m\[[-+]\] ALPHA/);
  assert.ok(fresh.text.includes('▸ BETA'));
  await viewer.frame(text => text.split('\n').length === 60, () => viewer.send({ resize: [60, 80] }));
  await viewer.frame((text, raw) => includesRunning(raw, 'ALPHA_DESCRIPTION') && text.includes('ALPHA_PROGRESS'), () => viewer.send({ keys: ' ' }));
  await viewer.frame((text, raw) => selectedTitle(raw, '○ ▸ BETA_DESCRIPTION'), () => viewer.send({ keys: '\t' }));
  await viewer.frame((text, raw) => selectedTitle(raw, '○ BETA_DESCRIPTION') && text.includes('BETA_PROGRESS'), () => viewer.send({ keys: '\r' }));
  await viewer.frame((text, raw) => includesRunning(raw, 'ALPHA_DESCRIPTION'), () => viewer.send({ keys: '\x1b[Z' }));
  const selected = await viewer.frame((text, raw) => selectedTitle(raw, '○ BETA_DESCRIPTION'), () => viewer.send({ keys: '\t' }));
  assert.ok(selected.includes('BETA_PROGRESS'));
  await viewer.frame(text => text.includes(`${messages.en.task}: tb`) && text.includes('BETA_PROGRESS'), () => viewer.send({ keys: 'd' }));
  await viewer.frame(text => text.includes('BETA_PROGRESS') && !text.includes(`${messages.en.task}: tb`), () => viewer.send({ keys: 'd' }));
  await viewer.frame((text, raw) => includesRunning(raw, 'ALPHA_DESCRIPTION'), () => viewer.send({ keys: '\x1b[Z' }));
  const collapsed = await viewer.frame((text, raw) => includesRunning(raw, '▸ ALPHA_DESCRIPTION'), () => viewer.send({ keys: ' ' }));
  assert.ok(!collapsed.includes('ALPHA_PROGRESS'));
  await viewer.frame(text => text.includes(`${messages.en.task}: ta`) && text.includes('ALPHA_PROGRESS'), () => viewer.send({ keys: 'd' }));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ ALPHA_DESCRIPTION') && !text.includes('ALPHA_PROGRESS'), () => viewer.send({ keys: 'd' }));
  await viewer.frame(text => text.split('\n').length === 35, () => viewer.send({ resize: [35, 80] }));
  state.runs[0].nodes.reverse();
  state.runs[0].nodes.find(node => node.id === 'a').taskId = 'retry';
  state.runs[0].nodes.push({ id: 'c', label: 'NEW_NODE', state: 'running', taskId: 'tc' });
  state.tasks.push({ id: 'retry', description: 'RETRY_DESCRIPTION' }, { id: 'tc', description: 'NEW_DESCRIPTION' });
  state.runs[0].name = 'UPDATED_RUN';
  await viewer.frame((text, raw) => text.includes('UPDATED_RUN') && includesRunning(raw, '▸ RETRY_DESCRIPTION'), () => writeJson(file, state));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ NEW_DESCRIPTION'), () => viewer.send({ keys: 'n' }));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ RETRY_DESCRIPTION'), () => viewer.send({ keys: 'p' }));
  await viewer.frame((text, raw) => text.includes('SECOND_RUN') && selectedTitle(raw, '▸ OTHER_ALPHA'), () => viewer.send({ keys: '\x1b[C' }));
  await viewer.frame((text, raw) => text.includes('UPDATED_RUN') && selectedTitle(raw, '▸ ALPHA'), () => viewer.send({ keys: '\x1b[D' }));
  const top = await viewer.frame((text, raw) => selectedTitle(raw, '▸ ALPHA') && /1–/.test(text), () => viewer.send({ keys: '\x1b[5~' }));
  const down = await viewer.frame(text => /2–/.test(text), () => viewer.send({ keys: 'j' }));
  assert.notEqual(down, top);
  await viewer.frame(text => /1–/.test(text), () => viewer.send({ keys: 'k' }));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ NEW_DESCRIPTION'), () => viewer.send({ keys: 'n' }));
  const resized = await viewer.frame((text, raw) => text.split('\n').length === 18 && includesRunning(raw, '▸ NEW_DESCRIPTION'), () => viewer.send({ resize: [18, 54] }));
  assert.ok(resized.split('\n').every(line => width(line) < 54));
  await viewer.close();
  const saved = JSON.parse(await readFile(`${file}.view.json`, 'utf8'));
  assert.equal(saved.expanded['["r1","a"]'], false);
  assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), state);
  viewer = openViewer(file, t);
  await viewer.frame((text, raw) => selectedTitle(raw, '▸ NEW_NODE') && text.includes(messages.en.followOn));
  await viewer.frame((text, raw) => includesRunning(raw, '▸ RETRY_DESCRIPTION'), () => viewer.send({ keys: 'p' }));
  await viewer.frame((text, raw) => includesRunning(raw, 'RETRY_DESCRIPTION'), () => viewer.send({ keys: '\r' }));
  await viewer.close();
  assert.equal(JSON.parse(await readFile(`${file}.view.json`, 'utf8')).expanded['["r1","a"]'], true);
  viewer = openViewer(file, t);
  await viewer.frame(text => text.includes('UPDATED_RUN') && text.includes(messages.en.followOn));
  const restored = await viewer.frame(text => text.includes('UPDATED_RUN') && text.includes('[-] ALPHA'),
    () => viewer.send({ keys: '\x1b[5~'.repeat(8) }));
  assert.ok(restored.includes('[-] BETA'));
  assert.ok(restored.includes('▸ NEW_NODE'));
  await viewer.close();
  console.log('PTY evidence: saved false folded, saved true restored, Tab/Shift-Tab/n/p, Space/Enter, run switches, atomic refresh/reorder/retry/new node, quit/restart persistence passed.');
});

test('real viewer follows newly active runs and manual run selection survives later arrivals', { timeout: 25000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dag-follow-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const state = { sessionId: 'follow-session', connected: true, runs: [
    { id: 'r-old', name: 'OLD_RUN', status: 'completed', nodes: [
      { id: 'a', label: 'OLD_NODE', state: 'completed', taskId: 'st_old' },
    ], edges: [] },
  ], tasks: [{ id: 'st_old', status: 'completed', description: 'OLD_TASK' }] };
  await writeJson(file, state);
  const viewer = openViewer(file, t);
  await viewer.frame(text => text.includes('OLD_RUN'));
  state.runs.unshift({ id: 'r-new', name: 'NEW_RUN', status: 'running', nodes: [
    { id: 'b', label: 'NEW_NODE', state: 'running', taskId: 'st_new' },
  ], edges: [] });
  state.tasks.push({ id: 'st_new', status: 'running', description: 'NEW_TASK' });
  const followed = await viewer.frame(text => text.includes('Selected run: NEW_RUN') && text.includes(messages.en.followOn), () => writeJson(file, state));
  assert.ok(followed.includes('NEW_NODE'));
  await viewer.frame(text => text.includes('Selected run: NEW_RUN') && text.includes(messages.en.followOff), () => viewer.send({ keys: 'c' }));
  await viewer.frame(text => text.includes('Selected run: OLD_RUN') && text.includes(messages.en.followOff), () => viewer.send({ keys: '\x1b[C' }));
  state.runs.unshift({ id: 'r-later', name: 'LATER_RUN', status: 'running', nodes: [
    { id: 'c', label: 'LATER_NODE', state: 'running' },
  ], edges: [] });
  await viewer.frame(text => /Run 3\/3/.test(text) && text.includes('Selected run: OLD_RUN'), () => writeJson(file, state));
  await viewer.close();
  console.log('PTY evidence: new run auto-followed once; manual run selection pins the pane across later arrivals.');
});

test('real PTY follow reveals graph work, retains parallel targets, hands off and suspends for every inspection control', { timeout: 45000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dag-target-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const nodes = Array.from({ length: 9 }, (_, i) => ({
    id: `n${i}`, label: i === 6 ? 'RUN_TARGET_한글' : i === 7 ? 'NEXT_TARGET_中文' : `NODE_${i}`,
    state: i === 6 ? 'running' : 'completed', taskId: `t${i}`,
  }));
  const run = { id: 'r', name: 'GRAPH_TARGET', status: 'running', nodes,
    edges: nodes.slice(1).map((node, i) => ({ from: nodes[i].id, to: node.id })) };
  const state = { sessionId: 'target', connected: true, runs: [run,
    { id: 'other', name: 'OTHER_RUN', status: 'completed', nodes: [{ id: 'other', label: 'OTHER_NODE', state: 'completed' }], edges: [] }],
  tasks: [...nodes.map(node => ({ id: node.taskId, status: node.state, description: `DETAIL_${node.id}`, progress: 'INSPECT_PROGRESS' })),
    { id: 'root', status: 'running', description: 'ROOT_TASK' }] };
  await writeJson(file, state);
  const viewer = openViewer(file, t);
  await viewer.frame(text => text.includes('RUN_TARGET_') && text.includes(messages.en.followOn));
  // A queued old-width frame can have the new row count before resize completes.
  const narrow = await viewer.frame(text => text.split('\n').length === 18 && text.includes('RUN_TARGET_') &&
    text.split('\n').every(line => width(line) < 35), () => viewer.send({ resize: [18, 35] }), { raw: true });
  assert.ok(narrow.text.split('\n').every(line => width(line) < 35), narrow.text);
  assert.doesNotMatch(narrow.text, /DETAIL_n6/);
  assert.ok(!selectedTitle(narrow.raw, '[-] RUN_TARGET'), narrow.text);
  nodes[7].state = 'running';
  state.tasks[7].status = 'running';
  run.name = 'PARALLEL_UPDATE';
  const stable = await viewer.frame(text => text.includes('PARALLEL_UPDATE') && text.includes('RUN_TARGET_'), () => writeJson(file, state));
  assert.doesNotMatch(stable, /DETAIL_n6|DETAIL_n7/);
  state.tasks[6].progress = 'STREAM_UPDATE';
  run.name = 'TOKEN_UPDATE';
  const token = await viewer.frame(text => text.includes('TOKEN_UPDATE') && text.includes('RUN_TARGET_'), () => writeJson(file, state));
  assert.equal(token.split('\n').at(-1), stable.split('\n').at(-1));
  const beforeHandoff = await viewer.frame(text => text.split('\n').length === 12 &&
    text.includes('RUN_TARGET_'), () => viewer.send({ resize: [12, 35] }));
  assert.doesNotMatch(beforeHandoff, /NEXT_TARGET_/);
  nodes[6].state = 'completed'; state.tasks[6].status = 'completed'; run.name = 'COMPLETION_UPDATE';
  const handoff = await viewer.frame(text => text.includes('COMPLETION_UPDATE') && text.includes('NEXT_TARGET_'), () => writeJson(file, state));
  assert.doesNotMatch(handoff, /RUN_TARGET_|DETAIL_n7/);
  await viewer.frame(text => text.split('\n').length === 26, () => viewer.send({ resize: [26, 80] }));
  const controls = ['j', 'k', '\x1b[6~', '\x1b[5~', 'n', 'p', '\t', '\x1b[Z', ' ', '\r', 'd', '\x1b[C', '\x1b[D', 'c', 't'];
  for (const [index, keys] of controls.entries()) {
    await viewer.frame(text => text.includes(messages.en.followOff), () => viewer.send({ keys }));
    run.name = `INSPECTION_${index}`;
    state.runs[1].name = `OTHER_INSPECTION_${index}`;
    state.tasks.find(task => task.id === 'root').description = `ROOT_UPDATE_${index}`;
    const inspected = await viewer.frame(text => text.includes(messages.en.followOff) &&
      (text.includes(`Selected run: ${run.name}`) || text.includes(`Selected run: ${state.runs[1].name}`) ||
        text.includes(`ROOT_UPDATE_${index}`)), () => writeJson(file, state));
    assert.ok(inspected.includes(messages.en.followOff));
    const resumed = await viewer.frame(text => text.includes(messages.en.followOn) && text.includes('NEXT_TARGET_') &&
      !text.includes('DETAIL_n7'), () => viewer.send({ keys: 'f' }), { raw: true });
    assert.ok(!selectedTitle(resumed.raw, '[-] NEXT_TARGET'), resumed.text);
  }
  await viewer.frame(text => text.includes(messages.en.followOff), () => viewer.send({ keys: 'f' }));
  await viewer.frame(text => text.includes(messages.en.followOn) && text.includes('NEXT_TARGET_'), () => viewer.send({ keys: 'f' }));
  await viewer.close();
  assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), state);
});

test('real PTY follows empty/task/DAG transitions, keeps a running task on parallel arrivals and disables live follow when disconnected', { timeout: 30000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'follow-transition-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const state = { sessionId: 'transition', language: 'zh-cn', connected: true, runs: [], tasks: [] };
  await writeJson(file, state);
  const viewer = openViewer(file, t);
  await viewer.frame(text => text.includes(messages['zh-cn'].followOn));
  const task = { id: 'z', status: 'running', description: 'INITIAL_TASK' };
  state.tasks.push(task);
  await viewer.frame((text, raw) => includesRunning(raw, 'INITIAL_TASK') && text.includes(messages['zh-cn'].followOn), () => writeJson(file, state));
  state.tasks.unshift({ id: 'a', status: 'running', description: 'PARALLEL_TASK' });
  const kept = await viewer.frame((text, raw) => text.includes('PARALLEL_TASK') && includesRunning(raw, 'INITIAL_TASK'), () => writeJson(file, state), { raw: true });
  assert.ok(!includesRunning(kept.raw, 'PARALLEL_TASK'), kept.text);
  const linked = { id: 'r', name: 'LINKED_DAG', status: 'running', nodes: [
    { id: 'start', label: 'START_NODE', state: 'completed' },
    { id: 'linked', label: 'LINKED_TARGET', state: 'running', taskId: task.id },
  ], edges: [{ from: 'start', to: 'linked' }] };
  state.runs.push(linked);
  await viewer.frame(text => text.includes('LINKED_DAG') && text.includes('LINKED_TARGET'), () => writeJson(file, state));
  linked.nodes[1].state = 'completed'; linked.status = 'completed'; task.status = 'completed';
  await viewer.frame((text, raw) => includesRunning(raw, 'PARALLEL_TASK') && !text.includes('LINKED_TARGET'), () => writeJson(file, state));
  const next = { id: 'new', name: 'NEW_DAG', status: 'running', nodes: [{ id: 'next', label: 'NEW_TARGET', state: 'running' }], edges: [] };
  state.runs.unshift(next);
  const stillTasks = await viewer.frame((text, raw) => text.includes('DAG (2)') && includesRunning(raw, 'PARALLEL_TASK'), () => writeJson(file, state));
  assert.doesNotMatch(stillTasks, /NEW_TARGET/);
  state.tasks[0].status = 'completed';
  await viewer.frame(text => text.includes('NEW_DAG') && text.includes('NEW_TARGET'), () => writeJson(file, state));
  state.connected = false;
  await viewer.frame(text => text.includes(messages['zh-cn'].followOff) && text.includes(messages['zh-cn'].disconnected), () => writeJson(file, state));
  state.runs = [];
  state.tasks.push({ id: 'reappeared', status: 'running', description: 'RETURNED_TASK' });
  await viewer.frame(text => text.includes('RETURNED_TASK') && text.includes(messages['zh-cn'].followOff), () => writeJson(file, state));
  state.connected = true;
  await viewer.frame(text => text.includes('RETURNED_TASK') && text.includes(messages['zh-cn'].followOn), () => writeJson(file, state));
  await viewer.close();
});
for (const view of ['dag', 'tasks']) test(`fixed ${view} retains an active target across same-scope source read errors`, { timeout: 30000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'task8-read-error-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'role.json');
  const nodes = [{ id: 'a', label: 'EARLIER_GRAPH', state: 'completed', taskId: 'linked-a' },
    { id: 'z', label: 'RETAINED_GRAPH', state: 'running', taskId: 'linked-z' }];
  const state = { sessionId: 'same-scope-read-error', connected: true, language: 'en',
    runs: view === 'dag' ? [{ id: 'run', name: 'ERROR_RETENTION', status: 'running', nodes,
      edges: [{ from: 'a', to: 'z' }] }] : [],
    tasks: view === 'dag' ? nodes.map(node => ({ id: node.taskId, status: node.state, description: node.label })) :
      [{ id: 'a', status: 'completed', description: 'EARLIER_WORKER' },
        { id: 'z', status: 'running', description: 'RETAINED_WORKER' }] };
  await writeJson(file, state);
  const viewer = openViewer(file, t, ['--view', view]);
  const retained = view === 'dag' ? 'RETAINED_GRAPH' : 'RETAINED_WORKER';
  const hasRetained = (text, raw) => view === 'dag' ? text.includes(retained) : includesRunning(raw, retained);
  const narrowRows = 12;
  await viewer.frame((text, raw) => hasRetained(text, raw) && text.includes(messages.en.followOn));
  await viewer.frame((text, raw) => text.split('\n').length === narrowRows && hasRetained(text, raw),
    () => viewer.send({ resize: [narrowRows, 80] }));
  if (view === 'dag') { nodes[0].state = 'running'; state.runs[0].name = 'PARALLEL_ERROR_RETENTION'; }
  state.tasks[0].status = 'running';
  state.tasks.at(-1).description += '_PARALLEL';
  await viewer.frame((text, raw) => hasRetained(text, raw) && text.includes('PARALLEL'), () => writeJson(file, state));
  await viewer.frame(text => text.split('\n').length === 60 && text.includes('Read error:'), async () => {
    await writeFile(file, '{');
    viewer.send({ resize: [60, 80] });
  });
  await viewer.frame(text => text.split('\n').length === narrowRows && text.includes('Read error:'),
    () => viewer.send({ resize: [narrowRows, 80] }));
  const recovered = await viewer.frame((text, raw) => !text.includes('Read error:') &&
    hasRetained(text, raw) && text.includes(messages.en.followOn), () => writeJson(file, state), { raw: true });
  if (view === 'dag') assert.doesNotMatch(recovered.text, /EARLIER_GRAPH/);
  else assert.ok(!includesRunning(recovered.raw, 'EARLIER_WORKER'), recovered.text);
  await viewer.close();
  assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), state);
});


test('workers completed group toggles independently and selected completed details survive updates and restart', { timeout: 25000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(),'task10-group-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const file=join(dir,'role.json');
  const state={sessionId:'group',connected:true,runs:[],tasks:Array.from({length:9},(_,i)=>({id:'t'+i,status:i===8?'running':'completed',description:'GROUP_TASK_'+i,progress:'PROGRESS_'+i}))};
  await writeJson(file,state);let viewer=openViewer(file,t,['--view','tasks']);
  await viewer.frame(text=>text.includes('GROUP_TASK_8')&&text.includes('Follow ON'));
  const group=await viewer.frame(text=>text.includes('Completed tasks (8) ▾')&&text.includes('Follow OFF'),()=>viewer.send({keys:'c'}));
  assert.match(group,/GROUP_TASK_0/);
  await viewer.frame(text=>text.includes('Completed tasks (8) ▸'),()=>viewer.send({keys:'c'}));
  const selected=await viewer.frame((text, raw) =>selectedTitle(raw, '✓ ▸ GROUP_TASK_0'),()=>viewer.send({keys:'n'}));
  assert.match(selected,/Completed tasks \(8\) ▸/);
  await viewer.frame((text, raw)=>selectedTitle(raw, '✓ GROUP_TASK_0')&&!text.includes('PROGRESS_0'),()=>viewer.send({keys:' '}));
  // The compact card shows the speed (the cost stays in the d detail view).
  state.tasks[0].metrics={cost_usd:0,cost_status:'reported',tokens_per_second:7,source:'fixture-metrics'};
  const updated=await viewer.frame(text=>text.includes('7 tok/s')&&!text.includes('[reported]'),()=>writeJson(file,state));
  assert.doesNotMatch(updated,/PROGRESS_0/);
  assert.match(updated,/GROUP_TASK_0/);
  await viewer.frame(text=>text.split('\n').length===18&&text.includes('7 tok/s')&&!text.includes('PROGRESS_0'),()=>viewer.send({resize:[18,40]}));
  await viewer.close();const saved=JSON.parse(await readFile(file+'.view.json','utf8'));assert.equal(saved.expanded['[null,"t0"]'],true);assert.equal(saved.navigation.completedExpanded,false);
  viewer=openViewer(file,t,['--view','tasks']);await viewer.frame(text=>text.includes('GROUP_TASK_8')&&text.includes('Follow ON'));
  await viewer.frame(text=>text.includes('Task: t0'),()=>viewer.send({keys:'d'}));
  await viewer.frame(text=>text.includes('Metrics source: fixture-metrics'),()=>viewer.send({keys:'\x1b[6~'}));
  await viewer.frame(text=>text.includes('7 tok/s')&&!text.includes('PROGRESS_0')&&!text.includes('Metrics source:'),()=>viewer.send({keys:'d'}));
  state.tasks[1].status='running';state.tasks[1].progress='REOPENED';
  await viewer.frame(text=>text.includes('Completed tasks (7)')&&text.includes('GROUP_TASK_1')&&!text.includes('REOPENED'),()=>writeJson(file,state));
  await viewer.frame(text=>text.includes('Follow ON'),()=>viewer.send({keys:'f'}));await viewer.close();
  assert.deepEqual(JSON.parse(await readFile(file,'utf8')),state);
});

test('real viewer clock advances elapsed without new snapshots or input', { timeout: 15000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dag-clock-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.json');
  const startedAt = new Date(Date.now() - 65000).toISOString();
  const state = { sessionId: 'clock-session', connected: true,
    runs: [{ id: 'r', name: 'CLOCK_RUN', status: 'running',
      nodes: [{ id: 'a', label: 'CLOCK_NODE', state: 'running', taskId: 'task' }], edges: [] }],
    tasks: [{ id: 'task', status: 'running', startedAt }] };
  await writeJson(file, state);
  const viewer = openViewer(file, t);
  await viewer.frame(text => text.includes('CLOCK_RUN'));
  const seconds = text => {
    const duration = text.match(new RegExp(`${messages.en.elapsed}: (\\d+):(\\d{2}):(\\d{2})`));
    return duration ? Number(duration[1]) * 3600 + Number(duration[2]) * 60 + Number(duration[3]) : -1;
  };
  const initial = await viewer.frame(text => seconds(text) >= 65, () => viewer.send({ keys: 'd' }));
  const tick = await viewer.frame(text => seconds(text) > seconds(initial));
  const nextTick = await viewer.frame(text => seconds(text) > seconds(tick));
  assert.ok(seconds(nextTick) > seconds(initial));
  assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), state);
  state.tasks[0].status = 'completed';
  state.tasks[0].completedAt = new Date(Date.parse(startedAt) + 9000).toISOString();
  state.runs[0].nodes[0].state = 'completed';
  state.runs[0].status = 'completed';
  await viewer.frame(text => seconds(text) === 9, () => writeJson(file, state));
  await viewer.frame(text => seconds(text) === 9, () => viewer.send({ keys: 'n' }));
  await viewer.close();
  console.log('PTY clock evidence: two elapsed advances without snapshots/input; completed duration frozen at 00:00:09.');
});

for (const view of ['dag', 'tasks']) test(`fixed ${view} follow keeps auto reveal transient and restores scoped manual viewport/cursor/fold`, { timeout: 30000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), `task8-${view}-`));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'role.json');
  const nodes = Array.from({ length: 9 }, (_, index) => ({ id: `n${index}`, label: `GRAPH_${index}`,
    state: index === 8 ? 'running' : 'completed', taskId: `linked-${index}` }));
  const state = { schema: 'omo-herdr-dashboard/role/1', sessionId: 'task8-scope', socketPath: 'fixture',
    parentPaneId: 'fixture-parent', tabId: 'fixture-tab', paneId: 'fixture-owned', scopeEpoch: 1,
    role: view === 'dag' ? 'dag' : 'workers', launchToken: 'task8-test', connected: true, language: 'en',
    controlOffer: null, presenceOffer: null,
    runs: [{ id: 'run', name: 'TASK8_RUN', status: 'running', nodes,
      edges: nodes.slice(1).map((node, index) => ({ from: nodes[index].id, to: node.id })) }],
    tasks: [...nodes.map(node => ({ id: node.taskId, status: node.state, description: `LINKED_${node.id}` })),
      ...Array.from({ length: 10 }, (_, index) => ({ id: `a${index}`, status: 'completed', description: `ORDINARY_${index}` })),
      { id: 'z', status: 'running', description: 'RETAINED_WORKER' }] };
  state.ownerKey = createHash('sha256').update(JSON.stringify([state.socketPath, state.parentPaneId, state.sessionId]))
    .digest('hex').slice(0, 24);
  const options = ['--view', view, '--close-pane', state.paneId, '--launch-token', state.launchToken];
  await writeJson(file, state);
  let viewer = openViewer(file, t, options);
  const marker = view === 'dag' ? 'GRAPH_8' : 'RETAINED_WORKER';
  await viewer.frame(text => text.includes(marker) && text.includes(messages.en.followOn));
  state.tasks.filter(task => task.id.startsWith('a')).forEach(task => { task.status = 'running'; });
  state.tasks.find(task => task.id === 'z').description = 'RETAINED_WORKER_UPDATE';
  state.runs[0].name = 'PARALLEL_ARRIVAL';
  const automatic = await viewer.frame(text => text.includes(marker) && text.includes(messages.en.followOn) &&
    (view === 'dag' ? text.includes('PARALLEL_ARRIVAL') : text.includes('RETAINED_WORKER_UPDATE')), () => writeJson(file, state));
  assert.doesNotMatch(automatic, /LINKED_n8/);
  await viewer.frame(text => text.includes(messages.en.followOff), () => viewer.send({ keys: 'f' }));
  await viewer.close();
  const justPaused = JSON.parse(await readFile(`${file}.view.json`, 'utf8'));
  assert.deepEqual(justPaused.navigation, { follow: false }, 'f must not persist an automatic scroll or target cursor');
  viewer = openViewer(file, t, options);
  await viewer.frame(text => text.includes(view === 'dag' ? marker : 'ORDINARY_0') &&
    text.includes(messages.en.followOn));
  const selection = view === 'dag' ? 'LINKED_n8' : 'RETAINED_WORKER_UPDATE';
  await viewer.frame(text => text.includes(selection) && text.includes(messages.en.followOff), () => viewer.send({ keys: 'p' }));
  await viewer.frame(text => text.includes(`▸ ${selection}`), () => viewer.send({ keys: ' ' }));
  await viewer.frame(text => text.includes(selection) && text.includes(messages.en.followOff), () => viewer.send({ keys: 'k' }));
  await viewer.close();
  const saved = JSON.parse(await readFile(`${file}.view.json`, 'utf8'));
  assert.equal(saved.navigation.follow, false);
  assert.ok(saved.navigation.scroll > 0);
  assert.equal(saved.scopeEpoch, 1);
  if (view === 'dag') assert.equal(new Map(saved.navigation.selectedNodes).get('run'), 'n8');
  else assert.equal(saved.navigation.selectedTaskId, 'z');
  const source = await readFile(file, 'utf8');
  viewer = openViewer(file, t, options);
  await viewer.frame(text => text.includes(messages.en.followOn));
  await viewer.frame(text => text.includes(selection) && text.includes(messages.en.followOff),
    () => viewer.send({ keys: 'd' }));
  await viewer.frame(text => text.includes(`▸ ${selection}`) && text.includes(messages.en.followOff),
    () => viewer.send({ keys: 'd' }));
  await viewer.frame(text => text.includes(selection) && text.includes(messages.en.followOff), () => writeJson(file, state));
  const narrow = await viewer.frame(text => text.split('\n').length === 18 && text.includes(messages.en.followOff) &&
    text.split('\n').every(line => width(line) < 35), () => viewer.send({ resize: [18, 35] }));
  assert.ok(narrow.split('\n').every(line => width(line) < 35));
  await viewer.close();
  assert.deepEqual(JSON.parse(await readFile(`${file}.view.json`, 'utf8')), saved);
  assert.equal(await readFile(file, 'utf8'), source);
  state.scopeEpoch = 2;
  state.runs = [];
  state.tasks = [{ id: 'new', status: 'running', description: 'NEW_SCOPE_WORKER' }];
  await writeJson(file, state);
  viewer = openViewer(file, t, options);
  const rebased = await viewer.frame(text => text.includes(messages.en.followOn) &&
    text.includes(view === 'dag' ? 'DAG (0)' : 'NEW_SCOPE_WORKER'));
  assert.doesNotMatch(rebased, /GRAPH_8|RETAINED_WORKER|LINKED_n8|ORDINARY_9/);
  await viewer.close();
});

// The fixed DAG viewer on a terminal without a PTY: stdin and stdout report a
// TTY, so it binds presence and ACKs its painted frames as in a Herdr pane.
const terminalShim = `data:text/javascript,${encodeURIComponent('for (const stream of [process.stdin, process.stdout]) ' +
  "Object.defineProperty(stream, 'isTTY', { value: true }); process.stdin.setRawMode = () => process.stdin;")}`;

test('fixed dag viewer ACKs a painted waiting presence snapshot (no native Todo) with its exact binding', { timeout: 20000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dag-waiting-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'role.json');
  const state = { schema: 'omo-herdr-dashboard/role/1', sessionId: 'waiting-scope', socketPath: 'fixture',
    parentPaneId: 'fixture-parent', tabId: 'fixture-tab', paneId: 'fixture-owned', scopeEpoch: 1, role: 'dag',
    launchToken: randomUUID(), connected: true, language: 'en', controlOffer: null, runs: [], tasks: [] };
  state.ownerKey = createHash('sha256').update(JSON.stringify([state.socketPath, state.parentPaneId, state.sessionId]))
    .digest('hex').slice(0, 24);
  const binding = { v: 1, token: randomUUID(), ownerKey: state.ownerKey, sessionId: state.sessionId, scopeEpoch: 1, role: 'dag' };
  const pipePath = ipcPath(`dag-waiting-${process.pid}-${randomUUID()}`);
  state.presenceOffer = { pipePath, ...binding, sequence: 0 };
  // The presence end of the pipe: one waiting snapshot after the hello, then every ACK.
  const peer = new EventEmitter(), sockets = new Set();
  let hello = false;
  const server = net.createServer(socket => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.once('close', () => sockets.delete(socket));
    socket.setEncoding('utf8');
    let buffer = '';
    socket.on('data', chunk => {
      buffer += chunk;
      for (let cut = buffer.indexOf('\n'); cut >= 0; cut = buffer.indexOf('\n')) {
        const message = JSON.parse(buffer.slice(0, cut)); buffer = buffer.slice(cut + 1);
        if (message.type !== 'hello') { peer.emit('message', message); continue; }
        hello = true;
        socket.write(`${JSON.stringify({ type: 'snapshot', ...binding, sequence: 1, snapshot: { ownerKey: binding.ownerKey,
          sessionId: binding.sessionId, scopeEpoch: 1, sequence: 1, availability: 'waiting', phases: [] } })}\n`);
      }
    });
  });
  const listening = once(server, 'listening');
  server.listen(pipePath); await listening;
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
  });
  await writeJson(file, state);
  let frames = 0, stderr = '', child;
  // Subscribe before the viewer starts; bounded.
  const acked = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no ACK of the painted waiting snapshot: hello ${hello}, frames ${frames}, ` +
      `exit ${child.exitCode}\n${stderr}`)), 10000);
    peer.once('message', message => { clearTimeout(timer); resolve(message); });
  });
  child = spawn(process.execPath, ['--import', terminalShim, fileURLToPath(new URL('../src/viewer.mjs', import.meta.url)),
    '--view', 'dag', '--state', file, '--close-pane', state.paneId, '--launch-token', state.launchToken],
  { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, HERDR_ENV: '0' } });
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', data => { stderr += data; });
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', data => { frames += data.split('\x1b[H').length - 1; });
  t.after(async () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    const exited = once(child, 'exit', { signal: AbortSignal.timeout(5000) });
    child.stdin.write('q');
    await exited.catch(() => child.kill());
  });
  assert.deepEqual(await acked, { type: 'ack', ...binding, sequence: 1 });
  const exited = once(child, 'exit', { signal: AbortSignal.timeout(5000) });
  child.stdin.write('q');
  assert.equal((await exited)[0], 0, stderr);
});

// The Windows PTY bridge needs node-pty, which this worktree intentionally does
// not install. These cases still drive the real viewer key handler in a child
// process through a fake TTY; frames are bounded renderer output, not native panes.
const interactiveShim = (columns, rows) => `data:text/javascript,${encodeURIComponent(
  'for (const stream of [process.stdin, process.stdout]) ' +
  "Object.defineProperty(stream, 'isTTY', { value: true, configurable: true });" +
  `Object.defineProperty(process.stdout, 'columns', { value: ${columns}, configurable: true });` +
  `Object.defineProperty(process.stdout, 'rows', { value: ${rows}, configurable: true });` +
  'process.stdin.setRawMode = () => process.stdin;')}`;

function openInteractiveViewer(file, t, options = [], { rows = 26, columns = 80 } = {}) {
  const events = new EventEmitter();
  const viewerArgs = [fileURLToPath(new URL('../src/viewer.mjs', import.meta.url)), '--state', file, ...options];
  const child = spawn(process.execPath, ['--import', interactiveShim(columns, rows), ...viewerArgs],
    { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, HERDR_ENV: '0' } });
  let buffer = '', stderr = '', lastFrame = '', lastRaw = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', data => { stderr += data; });
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', data => {
    buffer += data;
    while (true) {
      const start = buffer.indexOf('\x1b[H');
      if (start < 0) { if (buffer.length > 2) buffer = buffer.slice(-2); return; }
      if (start > 0) buffer = buffer.slice(start);
      const body = buffer.slice(3), next = body.indexOf('\x1b[H');
      const rawFrame = next >= 0 ? body.slice(0, next) : body;
      if (next < 0 && !(rawFrame.endsWith('\x1b[K') && rawFrame.split('\x1b[K').length - 1 >= rows)) return;
      buffer = next >= 0 ? buffer.slice(3 + next) : '';
      if (!rawFrame) { if (next < 0) return; continue; }
      const text = stripVTControlCharacters(rawFrame.replaceAll('\x1b[K\r\n', '\n').replace(/\x1b\[K$/, ''));
      lastFrame = text;
      lastRaw = rawFrame;
      events.emit('frame', text, rawFrame);
      if (next < 0) return;
    }
  });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit', { signal: AbortSignal.timeout(5000) });
      child.stdin.end();
      try { await exited; } catch { child.kill(); await once(child, 'exit').catch(() => {}); }
    }
  });
  const send = keys => child.stdin.write(keys);
  function frame(predicate, action = () => {}, { raw: keepRaw = false, current = false } = {}) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => finish(new Error(`interactive frame timeout\n${lastFrame}\nRAW ${JSON.stringify(lastRaw.slice(0, 4000))}\n${stderr}`)), 8000);
      const onFrame = (text, raw) => { if (predicate(text, raw)) finish(null, keepRaw ? { text, raw } : text); };
      const onExit = code => finish(new Error(`Viewer exited ${code}: ${stderr}\n${lastFrame}`));
      function finish(error, text) {
        clearTimeout(timer); events.off('frame', onFrame); child.off('exit', onExit);
        if (error) reject(error); else resolve(text);
      }
      events.on('frame', onFrame); child.on('exit', onExit);
      if (current && lastFrame && predicate(lastFrame, lastRaw)) { finish(null, keepRaw ? { text: lastFrame, raw: lastRaw } : lastFrame); return; }
      Promise.resolve().then(action).catch(finish);
    });
  }
  return { frame, send, async close() {
    const exited = once(child, 'exit', { signal: AbortSignal.timeout(5000) });
    send('q');
    try { assert.equal((await exited)[0], 0, stderr); }
    catch (error) { child.kill(); throw error; }
  } };
}

test('Workers metrics-only refresh preserves the Follow OFF row window and a surviving anchor offset', { timeout: 35000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'workers-anchor-offset-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'role.json');
  const tasks = Array.from({ length: 40 }, (_, index) => historyTask(index));
  Object.assign(tasks[0], { agent: 'worker', model: 'MODEL_0', turns: 2 });
  Object.assign(tasks[6], { agent: 'worker', model: 'MODEL_6', turns: 2 });
  const state = { sessionId: 'workers-anchor-offset', connected: true, language: 'en', runs: [], tasks };
  await writeJson(file, state);
  await writeJson(`${file}.view.json`, { version: 1, sessionId: state.sessionId,
    expanded: { '[null,"st_h000"]': true, '[null,"st_h006"]': true },
    navigation: { follow: false, selectedTaskId: 'st_h000' } });
  const viewer = openInteractiveViewer(file, t, [], { rows: 12, columns: 80 });
  await viewer.frame(text => text.includes(messages.en.followOn), () => {}, { current: true });
  await viewer.frame(text => text.includes(messages.en.followOff), () => viewer.send('f'));
  await viewer.frame(text => /1–8\/46/.test(text) && text.includes(messages.en.followOff),
    () => viewer.send('\x1b[5~'.repeat(10)));
  await viewer.frame(text => /3–10\/46/.test(text) && text.includes(messages.en.followOff), () => viewer.send('jj'));

  // Identical IDs/order/card membership and card height: only a metric value changes.
  state.tasks[0].turns = 9;
  const updated = await viewer.frame(text => text.includes('9 turns') && text.includes(messages.en.followOff),
    () => writeJson(file, state));
  assert.match(updated, /3–10\/46/, 'metrics-only refresh must keep the exact visible row window');

  // Move to a row inside a later card, then remove an earlier compact card.
  const anchorSaved = preferenceWritten(file, value => value.navigation?.scrollTaskId === 'st_h006' &&
    value.navigation?.scrollOffset === 1, () => {});
  await viewer.frame(text => /12–19\/46/.test(text) && text.includes(messages.en.followOff),
    () => viewer.send('j'.repeat(9)));
  const saved = await anchorSaved;
  assert.equal(saved.navigation.scrollOffset, 1, 'the within-card offset is persisted with the anchor');
  state.tasks.splice(1, 1);
  const removed = await viewer.frame(text => text.includes('[39/39]') && text.includes(messages.en.followOff),
    () => writeJson(file, state));
  assert.match(removed, /11–18\/45/, 'the surviving anchor keeps its offset instead of jumping to the top');
  assert.doesNotMatch(removed, /1–8\//);
  state.tasks.splice(state.tasks.findIndex(task => task.id === 'st_h006'), 1);
  const fallback = await viewer.frame(text => text.includes('[38/38]') && text.includes(messages.en.followOff),
    () => writeJson(file, state));
  assert.match(fallback, /HISTORY_007/, 'the nearest surviving candidate becomes the anchor');
  assert.doesNotMatch(fallback, /1–8\//);
  await viewer.close();
});

const historyAt = index => new Date(Date.parse('2026-10-01T00:00:00Z') + index * 60000).toISOString();
const historyTask = index => ({ id: `st_h${String(index).padStart(3, '0')}`, status: 'completed',
  createdAt: historyAt(index), description: `HISTORY_${String(index).padStart(3, '0')}` });

// Renderer fixtures below are text frames produced by the real viewer process;
// they are not native live pane captures.
test('real Workers navigation reaches oldest, middle and newest of 127 roots and keeps the manual anchor across arrivals', { timeout: 40000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'workers-nav-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'role.json');
  const state = { sessionId: 'workers-navigation', connected: true, language: 'en', runs: [],
    tasks: Array.from({ length: 127 }, (_, index) => historyTask(index)) };
  await writeJson(file, state);
  const viewer = openInteractiveViewer(file, t);

  const followed = await viewer.frame(text => text.includes('HISTORY_126') && text.includes(messages.en.followOn),
    () => {}, { current: true });
  assert.match(followed, /\[127\/127\]/);
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ HISTORY_126') && text.includes(messages.en.followOff),
    () => viewer.send('p'));
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ HISTORY_000') && text.includes(messages.en.followOff),
    () => viewer.send('n'));
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ HISTORY_063') && text.includes(messages.en.followOff),
    () => viewer.send('n'.repeat(63)));
  const detail = await viewer.frame(text => text.includes('Completed tasks (127)') && text.includes(messages.en.followOff),
    () => viewer.send('c'));
  assert.match(detail, /\[127\/127\]/, 'detail depth must not change root membership');
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ HISTORY_063') && text.includes(messages.en.followOff),
    () => viewer.send('c'));

  await viewer.frame(text => text.includes('HISTORY_126') && text.includes(messages.en.followOn), () => viewer.send('f'));
  state.tasks.push({ ...historyTask(999), id: 'st_h999', description: 'HISTORY_999' });
  await viewer.frame(text => text.includes('HISTORY_999') && text.includes(messages.en.followOn),
    () => writeJson(file, state), { current: true });
  state.tasks.push({ id: 'st_run', status: 'running', createdAt: historyAt(1001),
    description: 'RUNNING_HANDOFF' });
  await viewer.frame((text) => spinnerFrames.some(glyph => text.includes(`${glyph} RUNNING_HANDOFF`)) &&
    text.includes(messages.en.followOn), () => writeJson(file, state), { current: true });
  state.tasks.find(task => task.id === 'st_run').status = 'completed';
  await viewer.frame(text => text.includes('✓ ▸ RUNNING_HANDOFF') && text.includes(messages.en.followOn),
    () => writeJson(file, state), { current: true });
  const paused = await viewer.frame(text => text.includes(messages.en.followOff), () => viewer.send('f'));
  const firstStart = Number(/(\d+)–(\d+)\/(\d+)/.exec(paused)?.[1]);
  assert.ok(Number.isSafeInteger(firstStart));
  const moved = await viewer.frame(text => {
    const match = /(\d+)–(\d+)\/(\d+)/.exec(text);
    return match && Number(match[1]) < firstStart;
  }, () => viewer.send('kk'));
  const anchor = /HISTORY_\d{3}/.exec(moved)?.[0];
  assert.ok(anchor);
  state.tasks.unshift({ id: 'st_newtop', status: 'running', createdAt: historyAt(1000), description: 'NEWTOP' });
  const anchored = await viewer.frame(text => text.includes(anchor) && !text.includes('NEWTOP') &&
    text.includes(messages.en.followOff), () => writeJson(file, state), { current: true });
  assert.ok(anchored.includes(anchor), 'the manually anchored history row stays at the viewport top');
  await viewer.close();
  const saved = JSON.parse(await readFile(`${file}.view.json`, 'utf8'));
  assert.equal(saved.navigation.follow, false);
  assert.equal(saved.navigation.selectedTaskId, 'st_h063');
  assert.equal(typeof saved.navigation.scrollTaskId, 'string');
  assert.ok(state.tasks.some(task => task.id === saved.navigation.scrollTaskId),
    'the saved anchor names a still-valid root, not a removed row');
});

test('Workers selection and one card survive a paused -> running -> completed group move', { timeout: 25000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'workers-move-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'role.json');
  const state = { sessionId: 'workers-move', connected: true, language: 'en', runs: [], tasks: [
    { id: 'st_old', status: 'completed', createdAt: historyAt(1), description: 'OLD_DONE' },
    { id: 'st_move', status: 'paused', createdAt: historyAt(2), description: 'MOVE_ME' },
  ] };
  await writeJson(file, state);
  const viewer = openInteractiveViewer(file, t);
  await viewer.frame((text, raw) => selectedTitle(raw, 'Ⅱ ▸ MOVE_ME') && text.includes(messages.en.followOff),
    () => viewer.send('p'));
  state.tasks[1].status = 'running';
  await viewer.frame((text, raw) => includesRunning(raw, 'MOVE_ME') && text.includes('History (1)') &&
    text.includes(messages.en.followOff) && (text.match(/MOVE_ME/g) ?? []).length === 1,
  () => writeJson(file, state), { current: true });
  state.tasks[1].status = 'completed';
  state.tasks[1].completedAt = historyAt(3);
  await viewer.frame((text, raw) => selectedTitle(raw, '✓ ▸ MOVE_ME') && text.includes('History (2)') &&
    text.includes(messages.en.followOff) && (text.match(/MOVE_ME/g) ?? []).length === 1,
  () => writeJson(file, state), { current: true });
  await viewer.close();
});

test('Workers replacement binding carries explicit detail state and rejects foreign scope or prefs path', { timeout: 35000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'workers-replace-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'role.json');
  const sessionId = 'replacement-session';
  const tasks = [
    { id: 'st_a', status: 'completed', createdAt: historyAt(1), description: 'EXPANDED_TARGET',
      agent: 'worker', category: 'visual', model: 'MODEL_A', turns: 2 },
    { id: 'st_b', status: 'completed', createdAt: historyAt(2), description: 'OTHER_COMPACT', model: 'MODEL_B' },
  ];
  const ownerKey = createHash('sha256').update(JSON.stringify(['fixture', 'fixture-parent', sessionId]))
    .digest('hex').slice(0, 24);
  const stateFor = ({ paneId, launchToken, scopeEpoch = 1, prefsPath }) => {
    const value = { schema: 'omo-herdr-dashboard/role/1', version: 1, sessionId, socketPath: 'fixture',
      parentPaneId: 'fixture-parent', tabId: 'fixture-tab', paneId, launchToken, scopeEpoch, role: 'workers',
      ownerKey, connected: true, language: 'en', controlOffer: null, presenceOffer: null, runs: [], tasks };
    if (prefsPath !== undefined) value.prefsPath = prefsPath;
    return value;
  };
  const view = { version: 1, sessionId, ownerKey, scopeEpoch: 1,
    expanded: { '[null,"st_a"]': true },
    navigation: { follow: false, scroll: 0, view: 'tasks', selectedTaskId: 'st_a', selectedNodes: [],
      completedExpanded: false, scrollTaskId: 'st_a' } };
  await writeJson(`${file}.view.json`, view);

  const first = stateFor({ paneId: 'pane-a', launchToken: 'launch-a', prefsPath: `${file}.view.json` });
  await writeJson(file, first);
  let viewer = openInteractiveViewer(file, t, ['--view', 'tasks', '--close-pane', 'pane-a', '--launch-token', 'launch-a']);
  await viewer.frame((text, raw) => text.includes('MODEL_A') && !text.includes('▸ EXPANDED_TARGET') &&
    selectedTitle(raw, '✓ EXPANDED_TARGET'), () => {}, { current: true });
  await viewer.close();

  const replacement = stateFor({ paneId: 'pane-b', launchToken: 'launch-b', prefsPath: `${file}.view.json` });
  await writeJson(file, replacement);
  viewer = openInteractiveViewer(file, t, ['--view', 'tasks', '--close-pane', 'pane-b', '--launch-token', 'launch-b']);
  await viewer.frame((text, raw) => text.includes('MODEL_A') && !text.includes('▸ EXPANDED_TARGET') &&
    selectedTitle(raw, '✓ EXPANDED_TARGET'), () => {}, { current: true });
  await viewer.close();
  assert.deepEqual(JSON.parse(await readFile(`${file}.view.json`, 'utf8')), view,
    'same-conversation replacement reuses the exact sidecar without a global rekey');

  const foreign = stateFor({ paneId: 'pane-c', launchToken: 'launch-c', scopeEpoch: 2, prefsPath: `${file}.view.json` });
  await writeJson(file, foreign);
  viewer = openInteractiveViewer(file, t, ['--view', 'tasks', '--close-pane', 'pane-c', '--launch-token', 'launch-c']);
  await viewer.frame(text => text.includes('▸ EXPANDED_TARGET') && !text.includes('MODEL_A'),
    () => {}, { current: true });
  await viewer.close();

  await writeJson(join(dir, 'foreign.view.json'), view);
  const pointed = stateFor({ paneId: 'pane-d', launchToken: 'launch-d', prefsPath: join(dir, 'foreign.view.json') });
  await writeJson(file, pointed);
  viewer = openInteractiveViewer(file, t, ['--view', 'tasks', '--close-pane', 'pane-d', '--launch-token', 'launch-d']);
  await viewer.frame(text => text.includes('▸ EXPANDED_TARGET') && !text.includes('MODEL_A'),
    () => {}, { current: true });
  await viewer.close();
});
