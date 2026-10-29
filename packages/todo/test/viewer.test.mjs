import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { watch } from 'node:fs';
import * as fs from 'node:fs/promises';
import net from 'node:net';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { stripVTControlCharacters } from 'node:util';
import { startViewer, parseViewerArgs, validateRoleBinding, readViewerCache } from '../src/viewer.mjs';
import { createPresence } from '../src/presence.mjs';
import { createSnapshotStorage } from '../src/storage.mjs';
import { displayOwnerKey } from '../src/display-bridge.mjs';
import { readRoleState } from '../src/controller.mjs';
import { projectNativeTodo } from '../src/native-todo.mjs';
import { ipcPath } from '../../coordination/src/platform.mjs';

console.log(`TASK7_TEST_PROCESS ${process.pid} ${process.version}`);
function signal(emitter, name, predicate = () => true) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      emitter.off(name, listener); reject(new Error(`Missing exact ${name} event`));
    }, 5000);
    function listener(value) {
      if (!predicate(value)) return;
      clearTimeout(timeout); emitter.off(name, listener); resolve(value);
    }
    emitter.on(name, listener);
  });
}
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
function clock() {
  let now = 0, id = 0;
  const timers = new Map(), intervals = new Map(), events = new EventEmitter();
  return { now: () => now, setTimeout(fn, ms) { const key = ++id; timers.set(key, { fn, at: now + ms });
      events.emit('timeout', ms); return key; },
    clearTimeout: key => timers.delete(key), setInterval(fn, ms) { const key = ++id; intervals.set(key, { fn, ms }); return key; },
    clearInterval: key => intervals.delete(key),
    advance(ms) { now += ms; for (const [key, timer] of [...timers]) if (timer.at <= now) { timers.delete(key); timer.fn(); } },
    repaint(ms) { for (const timer of intervals.values()) if (ms === undefined || ms === timer.ms) timer.fn(); },
    intervalCount(ms) { return [...intervals.values()].filter(timer => timer.ms === ms).length; },
    timeoutDelays() { return [...timers.values()].map(timer => timer.at - now); },
    async waitForTimeout(ms) {
      if ([...timers.values()].some(timer => timer.at - now === ms)) return;
      await signal(events, 'timeout', value => value === ms);
    },
    get size() { return timers.size + intervals.size; } };
}
class Output extends EventEmitter {
  constructor({ hold = false } = {}) {
    super(); this.columns = 80; this.rows = 24; this.isTTY = true; this.hold = hold; this.pending = []; this.writes = [];
  }
  write(raw, callback) {
    this.writes.push(raw);
    if (!callback) return true;
    // A painted live frame: the header names the pane and the footer source is exactly LIVE
    // (cached and offline frames end in 'NOT LIVE' and must not be held).
    if (this.hold && /Todo \[[\s\S]*· LIVE/.test(stripVTControlCharacters(raw))) {
      const entry = { raw, callback };
      this.pending.push(entry); this.emit('pending', entry);
    } else queueMicrotask(callback);
    return true;
  }
  release(cause) { this.pending.shift().callback(cause); }
  releaseAll() { this.hold = false; for (const entry of this.pending.splice(0)) entry.callback(); }
}
const native = content => projectNativeTodo([{ type: 'custom', customType: 'senpi.todo-state',
  data: { schema: 'v2', ask: { entryId: 'fixture-ask', capturedAt: 1, text: '가나다라마바사'.repeat(70) },
    phases: [{ name: 'Build', tasks: [{ content, status: 'in_progress' },
      ...Array.from({ length: 40 }, (_, index) => ({ content: `Queued task ${index}`, status: 'pending' }))] }] } }]);

async function fixture(t, { offers = true, hold = false } = {}) {
  const id = randomUUID();
  const prefix = join(tmpdir(), `todo-viewer-${process.pid}-${id}-`);
  const presencePipe = ipcPath(`todo-viewer-${process.pid}-${id}-presence`);
  const controlPipe = ipcPath(`todo-viewer-${process.pid}-${id}-control`);
  console.log(`TASK7_FIXTURE_DESCRIPTOR ${JSON.stringify({ pid: process.pid, prefix, presencePipe, controlPipe })}`);
  const directory = await fs.mkdtemp(prefix);
  console.log(`TASK7_FIXTURE_ALLOCATED ${JSON.stringify({ pid: process.pid, directory })}`);
  const statePath = join(directory, 'todo.json'), cachePath = join(directory, 'cache.json');
  const binding = { socketPath: 'fixture-socket', parentPaneId: 'fixture-parent', sessionId: 'fixture-session', scopeEpoch: 1 };
  binding.ownerKey = displayOwnerKey(binding.socketPath, binding.parentPaneId, binding.sessionId);
  const storage = createSnapshotStorage({ path: cachePath }), time = clock();
  const presence = createPresence({ ...binding, storage, clock: time, pipePath: presencePipe });
  const output = new Output({ hold }), input = new PassThrough();
  const controlEvents = new EventEmitter(), sockets = new Set(), requests = [], viewers = [];
  const registrationReplies = [], heldRegistrations = [];
  let state = { schema: 'omo-herdr-dashboard/role/1', ...binding, tabId: 'fixture-tab', role: 'todo',
    paneId: `fixture-pane-${id}`, launchToken: randomUUID(), connected: true, language: 'en',
    updatedAt: 1, availability: 'waiting', cachePath, prefsPath: `${statePath}.view.json`,
    presenceOffer: offers ? presence.offer('todo') : null, controlOffer: null };
  state.controlOffer = offers ? { ...presence.offer('todo'), pipePath: controlPipe, token: randomUUID(),
    paneId: state.paneId, launchToken: state.launchToken, statePath } : null;
  let acceptClose = true;
  const server = net.createServer(socket => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.once('close', () => sockets.delete(socket));
    let buffer = '';
    socket.setEncoding('utf8');
    socket.on('data', data => {
      buffer += data;
      const end = buffer.indexOf('\n');
      if (end < 0) return;
      const request = JSON.parse(buffer.slice(0, end));
      buffer = buffer.slice(end + 1);
      assert.equal(request.pid, process.pid);
      for (const key of ['v', 'token', 'ownerKey', 'sessionId', 'scopeEpoch', 'role', 'paneId', 'launchToken', 'statePath']) {
        assert.equal(request[key], state.controlOffer[key]);
      }
      requests.push(request); controlEvents.emit('request', request);
      if (request.type === 'register') {
        const action = registrationReplies.shift() ?? 'registered';
        if (action === 'hold') { heldRegistrations.push({ socket, offer: { ...state.controlOffer } }); return; }
        if (action === 'error') { socket.destroy(); return; }
        socket.end(`${JSON.stringify({ ...state.controlOffer,
          type: action === 'denied' ? 'denied' : 'registered' })}\n`);
        return;
      }
      socket.end(`${JSON.stringify({ ...state.controlOffer, type: acceptClose ? 'closed' : 'denied' })}\n`);
    });
  });
  t.after(async () => {
    output.releaseAll();
    for (const viewer of viewers) await viewer.close(false);
    input.destroy();
    await presence.close();
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve, reject) => server.close(cause => cause ? reject(cause) : resolve()));
    assert.equal(time.size, 0);
    await fs.rm(directory, { recursive: true, force: true });
    console.log(`TASK7_FIXTURE_REMOVED ${JSON.stringify({ directory, sockets: sockets.size, timers: time.size })}`);
  });
  const listening = signal(server, 'listening');
  server.listen(controlPipe); await listening;
  await presence.start();
  async function writeState(next = state) {
    state = next;
    const temporary = `${statePath}.${randomUUID()}.pending`;
    console.log(`TASK7_FILE_DESCRIPTOR ${JSON.stringify({ path: temporary, final: statePath })}`);
    await fs.writeFile(temporary, JSON.stringify(next)); await fs.rename(temporary, statePath);
  }
  await writeState();
  const options = { statePath, paneId: state.paneId, launchToken: state.launchToken };
  return { directory, statePath, cachePath, controlPipe, presence, output, input, time, requests, controlEvents, options, writeState,
    registrationReplies, heldRegistrations,
    releaseRegistration(type = 'denied') {
      const entry = heldRegistrations.shift();
      if (!entry) throw new Error('No held registration');
      entry.socket.end(`${JSON.stringify({ ...entry.offer, type })}\n`);
    },
    get state() { return state; }, set acceptClose(value) { acceptClose = value; },
    async publish(content) { return presence.publish({ ...native(content), ...presence.getOwner() }); },
    async start(overrides = {}) {
      const viewer = await startViewer(options, { input, output, interactive: true, clock: time, ...overrides });
      viewers.push(viewer); return viewer;
    },
  };
}

test('mouse inspection is local and blur reveals the running target without a key', async t => {
  const f = await fixture(t);
  await f.publish('Mouse target');
  const ack = signal(f.presence.events, 'event', value => value.type === 'ack');
  const viewer = await f.start(); await ack;
  const mouse = async (sequence, follow) => {
    const painted = signal(viewer.events, 'frame');
    f.input.write(sequence);
    await painted;
    assert.equal(viewer.state.follow, follow);
  };
  await mouse('\x1b[<65;4;12M', false);
  const wheelScroll = viewer.frame.scroll;
  assert.ok(wheelScroll > 0);
  await mouse('\x1b[<0;4;12M', false);
  await mouse('\x1b[<32;4;9M', false);
  assert.ok(viewer.frame.scroll > wheelScroll);
  f.input.write('\x1b[<0;4;9m');
  await mouse('\x1b[O', true);
  const range = viewer.frame.taskRanges[viewer.state.target.key];
  assert.ok(range.start >= viewer.frame.scroll &&
    range.start < viewer.frame.scroll + viewer.frame.bodyRows);
  assert.equal(viewer.state.target.content, 'Mouse target');
  assert.equal(f.requests.filter(value => value.type === 'close').length, 0);
  await viewer.close(false);
  assert.equal(f.input.listenerCount('data'), 0);
});

test('CLI and role/offer bindings reject foreign identity before any connection or close', async t => {
  const f = await fixture(t);
  assert.deepEqual(parseViewerArgs(['--state', f.statePath, '--close-pane', f.state.paneId,
    '--launch-token', f.state.launchToken, '--once']), { ...f.options, once: true });
  assert.throws(() => parseViewerArgs(['--state', 'relative.json']), /Absolute/);
  assert.throws(() => validateRoleBinding({ ...f.state, role: 'dag' }, f.options));
  assert.throws(() => validateRoleBinding({ ...f.state, launchToken: 'foreign' }, f.options));
  assert.throws(() => validateRoleBinding({ ...f.state, paneId: 'foreign' }, f.options));
  assert.throws(() => validateRoleBinding({ ...f.state, controlOffer: { ...f.state.controlOffer, scopeEpoch: 2 } }, f.options));
  await assert.rejects(startViewer({ ...f.options, launchToken: 'foreign' }, { interactive: false, output: f.output }));
  assert.equal(f.requests.length, 0);
});

test('--once and noninteractive cache rendering are readonly, labelled and never registered or ACKed', async t => {
  const f = await fixture(t);
  const receipt = await f.publish('Cached source');
  const beforeState = await fs.readFile(f.statePath, 'utf8'), beforeCache = await fs.readFile(f.cachePath, 'utf8');
  const once = await startViewer({ ...f.options, once: true }, { interactive: true, output: f.output });
  assert.equal(once.state.availability, 'last-known');
  assert.match(once.frame.text, /CACHED NOT LIVE/);
  assert.equal(f.requests.length, 0);
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), false);
  assert.equal(await fs.readFile(f.statePath, 'utf8'), beforeState);
  assert.equal(await fs.readFile(f.cachePath, 'utf8'), beforeCache);
  await assert.rejects(fs.stat(f.state.prefsPath), { code: 'ENOENT' });
  await once.close(true);
  assert.equal(f.requests.length, 0);
  const otherScope = { ...f.state, scopeEpoch: 2 };
  assert.deepEqual(await readViewerCache(otherScope), { availability: 'waiting', snapshot: null, error: '' });
});

test('actual helper registration and real pipe baseline ACK follow only the matching output callback', async t => {
  const f = await fixture(t, { hold: true });
  const receipt = await f.publish('Paint baseline');
  const pending = signal(f.output, 'pending');
  const registered = signal(f.controlEvents, 'request', request => request.type === 'register');
  const viewer = await f.start();
  await Promise.all([pending, registered]);
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), false);
  const ack = signal(f.presence.events, 'event', event => event.type === 'ack');
  const painted = signal(viewer.events, 'frame', frame => frame.sequence === receipt.sequence && !frame.stale);
  f.output.release();
  await Promise.all([ack, painted]);
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), true);
  assert.equal(f.requests[0].pid, process.pid);
});

test('failed stdout callback cannot ACK a received baseline', async t => {
  const f = await fixture(t, { hold: true });
  const receipt = await f.publish('Output failure');
  const pending = signal(f.output, 'pending');
  const viewer = await f.start(); await pending;
  const failure = signal(viewer.events, 'output-error');
  f.output.release(new Error('Synthetic output failure'));
  await failure;
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), false);
});

test('queued old message cannot ACK a newer sequence; the exact new frame can', async t => {
  const f = await fixture(t, { hold: true });
  const first = await f.publish('First sequence');
  const pending = signal(f.output, 'pending');
  const viewer = await f.start(); await pending;
  const snapshots = signal(viewer.events, 'snapshot', value => value.snapshot.sequence > first.sequence);
  const second = await f.publish('Second sequence'); await snapshots;
  const oldFrame = signal(viewer.events, 'frame', value => value.sequence === first.sequence && value.stale);
  const nextPending = signal(f.output, 'pending');
  f.output.release(); await Promise.all([oldFrame, nextPending]);
  assert.equal(f.presence.isReady({ ...second, role: 'todo' }), false);
  const acks = [];
  f.presence.events.on('event', event => { if (event.type === 'ack') acks.push(event.sequence); });
  const ack = signal(f.presence.events, 'event', event => event.type === 'ack' && event.sequence === second.sequence);
  f.output.release(); await ack;
  assert.deepEqual(acks, [second.sequence]);
});

test('new scope discards old client/cache/message and stale completion cannot ACK it', async t => {
  const f = await fixture(t, { hold: true });
  const first = await f.publish('Old branch');
  const pending = signal(f.output, 'pending');
  const viewer = await f.start(); await pending;
  const owner = { ...f.presence.getOwner(), scopeEpoch: 2 };
  f.presence.rebase(owner);
  await f.writeState({ ...f.state, scopeEpoch: 2, presenceOffer: f.presence.offer('todo'),
    controlOffer: { ...f.state.controlOffer, scopeEpoch: 2 } });
  await viewer.refresh();
  assert.equal(viewer.state.role.scopeEpoch, 2);
  assert.equal(viewer.state.snapshot, null);
  assert.equal(viewer.state.availability, 'waiting');
  const newSnapshot = signal(viewer.events, 'snapshot', value => value.snapshot.scopeEpoch === 2);
  const receipt = await f.publish('New branch'); await newSnapshot;
  const oldFrame = signal(viewer.events, 'frame', value => value.scope.scopeEpoch === 1 && value.sequence === first.sequence);
  const nextPending = signal(f.output, 'pending');
  f.output.release(); await Promise.all([oldFrame, nextPending]);
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), false);
  const ack = signal(f.presence.events, 'event', event => event.type === 'ack' && event.scopeEpoch === 2);
  f.output.release(); await ack;
  assert.equal(viewer.state.snapshot.target.content, 'New branch');
});

test('late offers connect without restart and same-scope observations preserve manual prefs', async t => {
  const f = await fixture(t, { offers: false });
  const viewer = await f.start();
  assert.equal(viewer.state.availability, 'waiting');
  // No control offer means a read-only fixture, not authority to close a pane.
  await f.publish('Late source');
  const snapshot = signal(viewer.events, 'snapshot');
  await f.writeState({ ...f.state, presenceOffer: f.presence.offer('todo') });
  await snapshot;
  const saved = signal(viewer.events, 'preferences', value => value.scroll > 0);
  viewer.key('j'); await saved;
  const preference = JSON.parse(await fs.readFile(f.state.prefsPath, 'utf8'));
  await f.writeState({ ...f.state, updatedAt: 2 });
  await viewer.refresh();
  assert.equal(viewer.state.scroll, preference.scroll);
  assert.ok(viewer.state.scroll > 0);
  assert.equal(f.requests.length, 0);
});

test('a changed control offer registers the current bound PID without resetting same-scope preferences', async t => {
  const f = await fixture(t);
  await f.publish('Rebound control');
  const ack = signal(f.presence.events, 'event', value => value.type === 'ack');
  const viewer = await f.start(); await ack;
  const saved = signal(viewer.events, 'preferences', value => value.scroll > 0);
  viewer.key('j'); await saved;
  const token = randomUUID();
  const registered = signal(f.controlEvents, 'request', value => value.type === 'register' && value.token === token);
  const rebound = signal(viewer.events, 'snapshot', value => value.snapshot.target.content === 'Rebound control');
  await f.writeState({ ...f.state, controlOffer: { ...f.state.controlOffer, token } });
  const request = await registered; await rebound;
  assert.equal(request.pid, process.pid);
  assert.ok(viewer.state.scroll > 0, 'the manual scroll survives the control rebind');
  assert.equal(f.requests.filter(value => value.type === 'register').length, 2);
});

test('watch subscribes before initial read; an atomic replacement cannot install the stale read', async t => {
  const f = await fixture(t, { offers: false });
  await f.publish('Old cache');
  const reads = new EventEmitter(), gate = deferred();
  let first = true, subscribed = false;
  const initial = signal(reads, 'initial');
  const starting = f.start({
    watchState(path, callback) {
      subscribed = true;
      return watch(path, (event, name) => { callback(event, name); reads.emit('watch', name); });
    },
    async readState(path) {
      assert.equal(subscribed, true);
      const value = await readRoleState(path);
      if (first) { first = false; reads.emit('initial'); await gate.promise; }
      return value;
    },
  });
  await initial;
  const changed = signal(reads, 'watch', name => String(name) === 'todo.json');
  await f.writeState({ ...f.state, scopeEpoch: 2 });
  await changed; gate.resolve();
  const viewer = await starting;
  assert.equal(viewer.state.role.scopeEpoch, 2);
  assert.equal(viewer.state.snapshot, null);
  assert.equal(viewer.state.availability, 'waiting');
});

test('denied registration retries the identical offer on the injected 100ms clock and succeeds once', async t => {
  const f = await fixture(t);
  await f.publish('Retry target');
  f.registrationReplies.push('denied');
  const first = signal(f.controlEvents, 'request', value => value.type === 'register');
  const viewer = await f.start(); await first;
  const registers = () => f.requests.filter(value => value.type === 'register').length;
  assert.equal(registers(), 1);
  await f.time.waitForTimeout(100);
  // A re-read of identical valid state is not a reconnect and must not cancel the retry.
  await viewer.refresh();
  assert.equal(registers(), 1);
  f.time.advance(99);
  assert.equal(registers(), 1, 'no retry before 100ms');
  const second = signal(f.controlEvents, 'request', value => value.type === 'register');
  const painted = signal(viewer.events, 'snapshot', value => value.snapshot.target.content === 'Retry target');
  f.time.advance(1);
  await Promise.all([second, painted]);
  assert.equal(registers(), 2, 'exactly one retry attempt');
  assert.equal(viewer.state.availability, 'current');
  assert.equal(viewer.state.error, '');
});

test('a registration error then a rejection use the next backoff steps without overlapping attempts', async t => {
  const f = await fixture(t);
  await f.publish('Backoff target');
  f.registrationReplies.push('error', 'denied');
  const first = signal(f.controlEvents, 'request', value => value.type === 'register');
  const viewer = await f.start(); await first;
  const registers = () => f.requests.filter(value => value.type === 'register').length;
  await f.time.waitForTimeout(100);
  f.time.advance(99);
  assert.equal(registers(), 1);
  const second = signal(f.controlEvents, 'request', value => value.type === 'register');
  f.time.advance(1);
  await second;
  assert.equal(registers(), 2);
  await f.time.waitForTimeout(200);
  f.time.advance(199);
  assert.equal(registers(), 2);
  const third = signal(f.controlEvents, 'request', value => value.type === 'register');
  const painted = signal(viewer.events, 'snapshot', value => value.snapshot.target.content === 'Backoff target');
  f.time.advance(1);
  await Promise.all([third, painted]);
  assert.equal(registers(), 3);
  assert.equal(viewer.state.availability, 'current');
});

test('a late denial from the previous offer cannot demote the newer registration success', async t => {
  const f = await fixture(t);
  await f.publish('Current offer');
  f.registrationReplies.push('hold');
  const first = signal(f.controlEvents, 'request', value => value.type === 'register');
  const viewer = await f.start(); await first;
  assert.equal(f.heldRegistrations.length, 1);
  const token = randomUUID();
  const accepted = signal(viewer.events, 'registered', value => value.accepted && value.scope?.controlOffer?.token === token);
  const painted = signal(viewer.events, 'snapshot', value => value.snapshot.target.content === 'Current offer');
  await f.writeState({ ...f.state, controlOffer: { ...f.state.controlOffer, token } });
  await accepted; await painted;
  assert.equal(viewer.state.availability, 'current');
  let controlErrors = 0;
  viewer.events.on('control-error', () => controlErrors++);
  const stale = signal(viewer.events, 'registration-stale');
  f.releaseRegistration('denied');
  const settlement = await stale;
  assert.equal(settlement.accepted, false);
  assert.equal(controlErrors, 0);
  assert.equal(viewer.state.availability, 'current');
  assert.equal(viewer.state.error, '');
  assert.equal(viewer.state.snapshot.target.content, 'Current offer');
  assert.equal(f.requests.filter(value => value.type === 'register').length, 2);
  f.time.advance(5000);
  assert.equal(f.requests.filter(value => value.type === 'register').length, 2, 'the obsolete reply starts no retry');
  assert.equal(viewer.state.availability, 'current');
});

test('a changed control offer retires the pending retry of its previous epoch', async t => {
  const f = await fixture(t);
  await f.publish('Epoch target');
  f.registrationReplies.push('denied');
  const first = signal(f.controlEvents, 'request', value => value.type === 'register');
  const viewer = await f.start(); await first;
  await f.time.waitForTimeout(100);
  assert.ok(f.time.timeoutDelays().includes(100));
  const token = randomUUID();
  const accepted = signal(viewer.events, 'registered', value => value.accepted && value.scope?.controlOffer?.token === token);
  const painted = signal(viewer.events, 'snapshot', value => value.snapshot.target.content === 'Epoch target');
  await f.writeState({ ...f.state, controlOffer: { ...f.state.controlOffer, token } });
  await accepted; await painted;
  assert.equal(f.requests.filter(value => value.type === 'register').length, 2);
  assert.ok(!f.time.timeoutDelays().includes(100), 'the old epoch retry timer is cleared by retire');
  f.time.advance(5000);
  assert.equal(f.requests.filter(value => value.type === 'register').length, 2, 'the old epoch cannot reconnect');
  assert.equal(viewer.state.availability, 'current');
  assert.equal(viewer.state.error, '');
});

test('close during a pending registration retry cancels the timer without a stale error', async t => {
  const f = await fixture(t);
  f.registrationReplies.push('denied');
  const first = signal(f.controlEvents, 'request', value => value.type === 'register');
  const viewer = await f.start(); await first;
  await f.time.waitForTimeout(100);
  assert.ok(f.time.timeoutDelays().includes(100));
  await viewer.close(false);
  assert.ok(!f.time.timeoutDelays().includes(100));
  const registers = f.requests.filter(value => value.type === 'register').length;
  f.time.advance(5000);
  assert.equal(f.requests.filter(value => value.type === 'register').length, registers);
  assert.equal(viewer.state.availability, 'waiting');
  assert.equal(viewer.state.error, '');
});

test('source failure and disconnect keep only labelled same-scope last-known; read errors never imply empty', async t => {
  const f = await fixture(t);
  await f.publish('Last known');
  const registered = signal(f.controlEvents, 'request', value => value.type === 'register');
  const viewer = await f.start(); await registered;
  if (!viewer.state.snapshot || viewer.state.availability !== 'current') await signal(viewer.events, 'snapshot');
  const failedFrame = signal(viewer.events, 'frame', value => value.text.includes('CACHED NOT LIVE'));
  const pending = signal(viewer.events, 'fallback-pending', value => value.channel === 'presence');
  const diagnostic = signal(viewer.events, 'diagnostic', value => value.branch === 'presence-status');
  f.presence.sourceFailed(new Error('Source read failed'), f.presence.getOwner());
  await pending;
  assert.equal((await diagnostic).code, 'SOURCE_ERROR');
  assert.equal(viewer.state.availability, 'current');
  f.time.advance(2000);
  await failedFrame;
  assert.equal(viewer.state.availability, 'last-known');
  assert.equal(viewer.state.snapshot.target.content, 'Last known');
  const disconnected = signal(viewer.events, 'disconnected');
  await f.presence.close(); await disconnected;
  assert.equal(viewer.state.availability, 'disconnected');
  const errored = signal(viewer.events, 'state-error', cause => cause.name === 'SyntaxError');
  await fs.writeFile(f.statePath, '{');
  await errored;
  // A new state-read failure gets its own grace, retaining the disconnected view.
  assert.equal(viewer.state.availability, 'disconnected');
  f.time.advance(2000);
  assert.equal(viewer.state.availability, 'last-known');
  assert.notEqual(viewer.state.availability, 'empty');
});

test('idle actual repaint renews the lease, while data receipt and cache do not', async t => {
  const f = await fixture(t);
  const receipt = await f.publish('Idle target');
  const ack = signal(f.presence.events, 'event', value => value.type === 'ack');
  const viewer = await f.start(); await ack;
  f.time.advance(9999);
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), true);
  const before = f.output.writes.length;
  const repaint = signal(viewer.events, 'frame', value => value.sequence === receipt.sequence && !value.stale);
  const renewal = signal(f.presence.events, 'event', value => value.type === 'ack');
  f.time.repaint(); await Promise.all([repaint, renewal]);
  assert.ok(f.output.writes.length > before);
  f.time.advance(1);
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), true);
});

for (const sequence of ['q', '\x03', '\x04']) {
  test(`manual quit ${JSON.stringify(sequence)} uses actual close helper then local cleanup`, async t => {
    const f = await fixture(t);
    await f.publish('Close target');
    const ack = signal(f.presence.events, 'event', value => value.type === 'ack');
    const viewer = await f.start(); await ack;
    const request = signal(f.controlEvents, 'request', value => value.type === 'close');
    const result = viewer.key(sequence);
    await request;
    assert.deepEqual(await result, { manual: true, accepted: true });
    assert.equal(f.requests.filter(value => value.type === 'close').length, 1);
  });
}

test('rejected close has no direct fallback; termination does not request manual close', async t => {
  const f = await fixture(t);
  await f.publish('Denied close');
  const ack = signal(f.presence.events, 'event', value => value.type === 'ack');
  const viewer = await f.start(); await ack;
  f.acceptClose = false;
  assert.deepEqual(await viewer.close(true), { manual: true, accepted: false });
  assert.equal(f.requests.filter(value => value.type === 'close').length, 1);
  const terminated = await f.start();
  await terminated.close(false);
  assert.equal(f.requests.filter(value => value.type === 'close').length, 1);
});

test('follow keeps reveal transient, retains navigation but resumes automatically on restart', async t => {
  const f = await fixture(t);
  const phases = [{ name: 'Long phase', tasks: Array.from({ length: 35 }, (_, index) => ({
    content: `FOLLOW_NATIVE_${index}`, status: index === 31 ? 'in_progress' : 'completed',
  })) }];
  const publish = () => f.presence.publish({ ...projectNativeTodo([{ type: 'custom',
    customType: 'senpi.todo-state', data: { schema: 'v2', phases,
      ask: { entryId: 'ask', capturedAt: 1, text: 'Ask line\n'.repeat(30) } } }]), ...f.presence.getOwner() });
  await publish();
  const ack = signal(f.presence.events, 'event', event => event.type === 'ack');
  let viewer = await f.start(); await ack;
  assert.equal(viewer.state.follow, true);
  assert.ok(viewer.frame.scroll > 0);
  assert.equal(viewer.state.scroll, 0);
  await assert.rejects(fs.readFile(f.state.prefsPath), { code: 'ENOENT' });
  const source = await fs.readFile(f.statePath, 'utf8');
  const paused = signal(viewer.events, 'preferences', value => !value.follow);
  viewer.key('f'); await paused;
  assert.equal(viewer.state.scroll, 0, 'pausing follow must not save the automatic body reveal');
  const resumed = signal(viewer.events, 'preferences', value => value.follow);
  viewer.key('f'); await resumed;
  const moving = signal(viewer.events, 'frame', value => value.text.includes('Follow OFF'));
  const manual = signal(viewer.events, 'preferences', value => !value.follow && value.scroll > 0);
  viewer.key('k'); await moving;
  const saved = await manual, manualScroll = saved.scroll;
  const duplicate = signal(viewer.events, 'frame', value => value.text.includes('Follow OFF'));
  await f.writeState(); await duplicate;
  const resized = signal(viewer.events, 'frame', value => value.lines.length === 18);
  f.output.columns = 40; f.output.rows = 18; f.output.emit('resize'); await resized;
  assert.equal(viewer.state.scroll, manualScroll);
  assert.deepEqual(JSON.parse(await fs.readFile(f.state.prefsPath, 'utf8')), saved);
  await viewer.close(false);
  const restarted = signal(f.presence.events, 'event', event => event.type === 'ack');
  viewer = await f.start(); await restarted;
  assert.equal(viewer.state.follow, true);
  assert.equal(viewer.state.scroll, manualScroll);
  assert.equal(await fs.readFile(f.statePath, 'utf8'), source);
  const following = signal(viewer.events, 'frame', value => value.text.includes('Follow ON'));
  await viewer.draw(); await following;
  phases.unshift({ name: 'Parallel phase', tasks: [{ content: 'EARLY_ARRIVAL', status: 'in_progress' }] });
  const retained = signal(viewer.events, 'frame', value => value.sequence === 2 && value.text.includes('FOLLOW_NATIVE_31'));
  await publish(); await retained;
  assert.equal(viewer.state.target.content, 'FOLLOW_NATIVE_31');
  phases[1].tasks[31].status = 'completed';
  const handoff = signal(viewer.events, 'frame', value => value.sequence === 3 && value.text.includes('[•] EARLY_ARRIVAL'));
  await publish(); await handoff;
  assert.equal(viewer.state.target.content, 'EARLY_ARRIVAL');
  phases.shift();
  const idle = signal(viewer.events, 'frame', value => value.sequence === 4 && !value.text.includes('[•]'));
  await publish(); await idle;
  assert.equal(viewer.state.target, null);
  const owner = f.presence.getOwner();
  await f.presence.rebase({ ...owner, scopeEpoch: 2 });
  const rebound = signal(viewer.events, 'registered', value => value.scope.scopeEpoch === 2);
  await f.writeState({ ...f.state, scopeEpoch: 2, presenceOffer: f.presence.offer('todo'),
    controlOffer: { ...f.state.controlOffer, scopeEpoch: 2 } });
  await rebound;
  assert.equal(viewer.state.follow, true);
  assert.equal(viewer.state.scroll, 0);
  assert.equal(viewer.state.target, null);
});


test('closed phase c persists manual expansion only in its validated scope', async t => {
  const f=await fixture(t);const phases=[{name:'Closed',tasks:[{content:'COMPLETED_DETAIL',status:'completed'},{content:'DROPPED_DETAIL',status:'abandoned'}]},{name:'Empty',tasks:[]}];
  const publish=()=>f.presence.publish({...projectNativeTodo([{type:'custom',customType:'senpi.todo-state',data:{schema:'v2',phases}}]),...f.presence.getOwner()});
  await publish();const ack=signal(f.presence.events,'event',v=>v.type==='ack');let viewer=await f.start();await ack;
  // Every phase is listed by default; c folds the closed ones and that fold is what persists.
  assert.match(viewer.frame.text,/COMPLETED_DETAIL/);assert.match(viewer.frame.text,/DROPPED_DETAIL/);
  const prefs=signal(viewer.events,'preferences',v=>!v.completedExpanded&&v.closedFolded&&!v.follow);viewer.key('c');await prefs;
  assert.doesNotMatch(viewer.frame.text,/COMPLETED_DETAIL|DROPPED_DETAIL/);
  const saved=await fs.readFile(f.state.prefsPath,'utf8');
  const updated=signal(viewer.events,'frame',v=>v.sequence===2);await publish();await updated;
  f.output.columns=24;f.output.rows=8;await viewer.draw();assert.equal(viewer.state.completedExpanded,false);
  assert.equal(await fs.readFile(f.state.prefsPath,'utf8'),saved);await viewer.close(false);
  const restarted=signal(f.presence.events,'event',v=>v.type==='ack');viewer=await f.start();await restarted;
  assert.equal(viewer.state.completedExpanded,false);assert.equal(viewer.state.follow,true);
  const owner=f.presence.getOwner();await f.presence.rebase({...owner,scopeEpoch:2});
  const rebound=signal(viewer.events,'registered',v=>v.scope.scopeEpoch===2);
  await f.writeState({...f.state,scopeEpoch:2,presenceOffer:f.presence.offer('todo'),controlOffer:{...f.state.controlOffer,scopeEpoch:2}});await rebound;
  assert.equal(viewer.state.completedExpanded,true);assert.equal(viewer.state.follow,true);
});

test('same-scope source error, disconnect and reattach retain an active target without revealing it offline', async t => {
  const f = await fixture(t);
  const phases = [{ name: 'Current', tasks: [{ content: 'TODO_PAR', status: 'in_progress' }] }];
  const projection = () => projectNativeTodo([{ type: 'custom', customType: 'senpi.todo-state',
    data: { schema: 'v2', phases } }]);
  await f.presence.publish({ ...projection(), ...f.presence.getOwner() });
  const initialAck = signal(f.presence.events, 'event', event => event.type === 'ack');
  const viewer = await f.start(); await initialAck;
  const targetKey = viewer.state.target.key;
  phases.unshift({ name: 'Earlier', tasks: [{ content: 'EARLY_RUN', status: 'in_progress' },
    ...Array.from({ length: 35 }, (_, index) => ({ content: `History ${index}`, status: 'completed' }))] });
  const retained = signal(viewer.events, 'frame', value => value.sequence === 2);
  await f.presence.publish({ ...projection(), ...f.presence.getOwner() }); await retained;
  assert.equal(viewer.state.target.key, targetKey);
  const paused = signal(viewer.events, 'preferences', value => !value.follow);
  viewer.key('k'); const saved = await paused;
  const failed = signal(viewer.events, 'frame', () => viewer.state.availability === 'last-known');
  const pending = signal(viewer.events, 'fallback-pending', value => value.channel === 'presence');
  f.presence.sourceFailed(new Error('Owned source unavailable'), f.presence.getOwner());
  await pending;
  f.time.advance(2000);
  await failed;
  assert.equal(viewer.state.target.key, targetKey);
  assert.ok(viewer.frame.bodyLines.some(line => line.startsWith('\x1b[36m[•] ')), 'offline in-progress rows keep their colour');
  assert.ok(!viewer.frame.bodyLines.some(line => line.startsWith('\x1b[1;36m[•] ')), 'no row is drawn as the target offline');
  const disconnected = signal(viewer.events, 'disconnected');
  await f.presence.close(); await disconnected;
  assert.equal(viewer.state.availability, 'disconnected');
  assert.equal(viewer.state.target.key, targetKey);
  const owner = f.presence.getOwner();
  const replacement = createPresence({ ...owner,
    storage: createSnapshotStorage({ path: f.cachePath }),
    pipePath: ipcPath(`todo-viewer-${process.pid}-${randomUUID()}-reattach`) });
  t.after(() => replacement.close());
  await replacement.start();
  await replacement.publish({ ...projection(), ...replacement.getOwner() });
  const reattached = signal(viewer.events, 'snapshot', value => value.snapshot.sequence === 1);
  await f.writeState({ ...f.state, presenceOffer: replacement.offer('todo') });
  await reattached;
  assert.equal(viewer.state.role.ownerKey, owner.ownerKey);
  assert.equal(viewer.state.role.sessionId, owner.sessionId);
  assert.equal(viewer.state.role.scopeEpoch, owner.scopeEpoch);
  assert.equal(viewer.state.target.key, targetKey);
  assert.equal(viewer.state.follow, false);
  assert.equal(viewer.state.scroll, saved.scroll);
  const resumed = signal(viewer.events, 'preferences', value => value.follow);
  viewer.key('f'); await resumed;
  assert.equal(viewer.state.target.key, targetKey);
  assert.equal(viewer.state.scroll, saved.scroll);
  assert.equal(JSON.parse(await fs.readFile(f.state.prefsPath, 'utf8')).scroll, saved.scroll);
  phases.length = 0;
  const emptied = signal(viewer.events, 'snapshot', value => value.snapshot.availability === 'empty');
  await replacement.publish({ ...projection(), ...replacement.getOwner() }); await emptied;
  assert.equal(viewer.state.target, null);
  phases.push({ name: 'Earlier', tasks: [{ content: 'EARLY_RUN', status: 'in_progress' }] },
    { name: 'Current', tasks: [{ content: 'TODO_PAR', status: 'in_progress' }] });
  const repopulated = signal(viewer.events, 'snapshot', value => value.snapshot.sequence === 3);
  await replacement.publish({ ...projection(), ...replacement.getOwner() }); await repopulated;
  assert.equal(viewer.state.target.content, 'EARLY_RUN');
});

test('completion transition animates once and stops its 65ms timer without removing the ACK heartbeat', async t => {
  const f = await fixture(t);
  await f.publish('Completion fixture');
  const ack = signal(f.presence.events, 'event', event => event.type === 'ack');
  const viewer = await f.start(); await ack;
  assert.equal(f.time.intervalCount(65), 0);
  const phases = [{ name: 'Build', tasks: [{ content: 'Completion fixture', status: 'completed' },
    { content: 'Remaining fixture', status: 'pending' }] }];
  const publish = () => f.presence.publish({ ...projectNativeTodo([{ type: 'custom', customType: 'senpi.todo-state',
    data: { schema: 'v2', phases } }]), ...f.presence.getOwner() });
  const snapshot = signal(viewer.events, 'snapshot', value => value.snapshot.sequence === 2);
  await publish(); await snapshot;
  assert.equal(f.time.intervalCount(65), 1);
  for (let frame = 0; frame < 14; frame++) {
    const painted = signal(viewer.events, 'frame'); f.time.repaint(65); await painted;
  }
  assert.equal(f.time.intervalCount(65), 0);
  assert.equal(f.time.intervalCount(1000), 1);
  const repeated = signal(viewer.events, 'snapshot', value => value.snapshot.sequence === 3);
  await publish(); await repeated;
  assert.equal(f.time.intervalCount(65), 0);
});

test('a waiting snapshot (no native Todo) is ACKed after its output callback; the source error that follows is not', async t => {
  const f = await fixture(t);
  const receipt = await f.presence.publish({ ...projectNativeTodo([]), ...f.presence.getOwner() });
  assert.equal(f.presence.state.availability, 'waiting');
  const ack = signal(f.presence.events, 'event', event => event.type === 'ack' && event.role === 'todo' &&
    event.sequence === receipt.sequence);
  const viewer = await f.start(); await ack;
  assert.equal(viewer.state.availability, 'waiting');
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), true);
  const stale = signal(viewer.events, 'frame', frame => frame.stale);
  f.presence.sourceFailed(Object.assign(new Error('read failed'), { code: 'EPERM' }), f.presence.getOwner());
  await stale;
  assert.equal(f.presence.isReady({ ...receipt, role: 'todo' }), false);
});
