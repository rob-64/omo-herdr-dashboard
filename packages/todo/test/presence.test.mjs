import { test } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import * as fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { createPresence, createRoleClient } from '../src/presence.mjs';
import { createSnapshotStorage } from '../src/storage.mjs';
import { installDisplayBridge } from '../src/display-bridge.mjs';
import { inlineRecoveryLines } from '../src/inline-recovery.mjs';
import { projectNativeTodo } from '../src/native-todo.mjs';
import { ipcPath } from '../../coordination/src/platform.mjs';

const pipe = name => ipcPath(`omo-herdr-todo-task5-${name}`);
console.log(`TASK5_TEST_PROCESS ${process.pid} ${process.version}`);
const owner = { socketPath: 'test-herdr-socket', parentPaneId: 'test-parent', sessionId: 'test-session', scopeEpoch: 1 };
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
function signal(emitter, event, predicate = () => true) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { emitter.off(event, listener); reject(new Error(`Missing exact event ${event}`)); }, 3000);
    const listener = value => {
      if (!predicate(value)) return;
      clearTimeout(timer);
      emitter.off(event, listener);
      resolve(value);
    };
    emitter.on(event, listener);
  });
}
function fakeClock() {
  let now = 0;
  const timers = new Map();
  let id = 0;
  return { now: () => now, setTimeout(fn, ms) { const key = ++id; timers.set(key, { fn, at: now + ms }); return key; },
    clearTimeout: key => timers.delete(key),
    advance(ms) {
      now += ms;
      for (const [key, timer] of [...timers]) if (timer.at <= now) { timers.delete(key); timer.fn(); }
    },
    get size() { return timers.size; },
  };
}
function memoryStorage() {
  let binding;
  const committed = [];
  return { committed, rebase(next) { binding = next; },
    async write(snapshot) {
      assert.equal(snapshot.scopeEpoch, binding.scopeEpoch);
      committed.push(structuredClone(snapshot));
      return snapshot;
    },
  };
}
function snapshot(presence, phases = [{ name: 'Build', tasks: [{ content: '작업', status: 'in_progress' }] }]) {
  return { ...projectNativeTodo([{ type: 'custom', customType: 'senpi.todo-state',
    data: { schema: 'v2', phases } }]), ...presence.getOwner() };
}
async function paint(client, message, ledger) {
  const lines = message.role === 'todo' ? inlineRecoveryLines(message.snapshot) :
    [`ROLE ${message.role}`, JSON.stringify(message.snapshot.counts)];
  const marker = `TASK5_FRAME_${message.role}_${message.scopeEpoch}_${message.sequence}`;
  const frame = `\x1b[2J\x1b[H${lines.join('\r\n')}\r\n${marker}\r\n`;
  await new Promise((resolve, reject) => process.stdout.write(frame, error => error ? reject(error) : resolve()));
  ledger.push({ type: 'output-completed', role: message.role, sequence: message.sequence, marker });
  assert.equal(client.painted(message), true);
}
function attachClient(t, presence, role) {
  const messages = [];
  const client = createRoleClient({ offer: presence.offer(role), onSnapshot: (value, message) => messages.push(message) });
  t.after(() => client.close());
  return { client, messages };
}

test('real named pipe: three painted role baselines, bridge coverage, renewals, updates and closure', async t => {
  const prefix = fileURLToPath(new URL(`../.task5-presence-${process.pid}-`, import.meta.url));
  console.log(`TASK5_FIXTURE_DESCRIPTOR ${JSON.stringify({ pid: process.pid, prefix })}`);
  const cacheDirectory = await fs.mkdtemp(prefix);
  console.log(`TASK5_FIXTURE_ALLOCATED ${JSON.stringify({ pid: process.pid, directory: cacheDirectory })}`);
  const cachePath = join(cacheDirectory, 'snapshot.json');
  const storage = createSnapshotStorage({ path: cachePath });
  const clock = fakeClock();
  let dagOwned = true;
  const presence = createPresence({ ...owner, storage, clock, pipePath: pipe('main'),
    hasDagOwnedTasks: () => dagOwned });
  const ledger = [];
  presence.events.on('event', event => ledger.push(event));
  const widgets = new Map();
  const nativeTodo = ['native todo'];
  widgets.set('todo-sidebar', nativeTodo);
  let phases = [{ name: 'Build', tasks: [{ content: '작업', status: 'in_progress' }] }];
  const branch = () => [{ type: 'custom', id: 'test-entry', customType: 'senpi.todo-state',
    data: { schema: 'v2', phases } }];
  const ctx = { hasUI: true, mode: 'tui',
    sessionManager: { getSessionFile: () => 'test-parent/session.jsonl', getSessionId: () => owner.sessionId,
      getLeafId: () => 'test-entry', getBranch: branch },
    ui: { setWidget(key, content) { if (content === undefined) widgets.delete(key); else widgets.set(key, content); } } };
  let bridge;
  let unsubscribe;
  t.after(async () => {
    unsubscribe?.();
    bridge?.remove();
    await presence.close();
    assert.equal(clock.size, 0);
    await fs.rm(cachePath, { force: true });
    await fs.rm(`${cachePath}.pending`, { force: true });
    await fs.rmdir(cacheDirectory);
    console.log(`TASK5_FIXTURE_REMOVED ${JSON.stringify({ pid: process.pid, directory: cacheDirectory })}`);
  });
  await presence.start();
  bridge = installDisplayBridge(ctx, presence, {
    environment: { HERDR_SOCKET_PATH: owner.socketPath, HERDR_PANE_ID: owner.parentPaneId },
  });
  unsubscribe = presence.subscribeReadiness(() => bridge.readinessChanged());
  await presence.idle();
  ctx.ui.setWidget('todo-sidebar', nativeTodo);
  ctx.ui.setWidget('omo-task', ['native workers']);
  ctx.ui.setWidget('omo-dag', ['native dag']);
  assert.deepEqual(bridge.status.masked, []);
  const clients = {};
  for (const role of ['todo', 'workers', 'dag']) {
    const attached = attachClient(t, presence, role);
    clients[role] = attached;
    const message = await signal(attached.client.events, 'snapshot');
    assert.equal(presence.isReady({ ...bridge.status.publication, role }), false, 'receipt/cache cannot authorize readiness');
    const ack = signal(presence.events, 'event', event => event.type === 'ack' && event.role === role);
    await paint(attached.client, message, ledger);
    await ack;
  }
  assert.deepEqual(new Set(bridge.status.masked), new Set(['todo-sidebar', 'omo-task', 'omo-dag']));
  for (const role of ['todo', 'workers', 'dag']) {
    const ackIndex = ledger.findIndex(event => event.type === 'ack' && event.role === role);
    const paintIndex = ledger.findIndex(event => event.type === 'output-completed' && event.role === role);
    assert.ok(paintIndex >= 0 && ackIndex > paintIndex);
    assert.ok(ledger.findIndex(event => event.type === 'committed') <
      ledger.findIndex(event => event.type === 'delivery' && event.role === role));
  }
  clock.advance(9999);
  assert.equal(presence.isReady({ ...bridge.status.publication, role: 'todo' }), true);
  const renewal = signal(presence.events, 'event', event => event.type === 'ack' && event.role === 'todo');
  await paint(clients.todo.client, clients.todo.messages.at(-1), ledger);
  await renewal;
  clock.advance(1);
  assert.equal(presence.isReady({ ...bridge.status.publication, role: 'todo' }), true);
  assert.deepEqual(bridge.status.masked, ['todo-sidebar'], 'DAG expiry also restores DAG-owned worker coverage');
  for (const role of ['workers', 'dag']) {
    const ack = signal(presence.events, 'event', event => event.type === 'ack' && event.role === role);
    await paint(clients[role].client, clients[role].messages.at(-1), ledger);
    await ack;
  }
  const frames = Object.values(clients).map(({ client }) => signal(client.events, 'snapshot'));
  phases = [{ name: 'Build', tasks: [{ content: '다음 작업', status: 'pending' }] }];
  bridge.observe();
  const updates = await Promise.all(frames);
  await presence.idle();
  const currentReceipt = bridge.status.publication;
  assert.ok(currentReceipt.sequence > 1);
  assert.equal(clients.todo.client.painted(clients.todo.messages[0]), false, 'stale asynchronous paint cannot ACK a newer frame');
  for (const [index, role] of ['todo', 'workers', 'dag'].entries()) {
    assert.equal(presence.isReady({ ...currentReceipt, role }), false);
    const ack = signal(presence.events, 'event', event => event.type === 'ack' && event.role === role);
    await paint(clients[role].client, updates[index], ledger);
    await ack;
  }
  assert.equal(bridge.status.masked.length, 3);
  const disconnected = signal(presence.events, 'event', event => event.type === 'disconnected' && event.role === 'dag');
  const clientDisconnected = signal(clients.dag.client.events, 'disconnected');
  clients.dag.client.close();
  await Promise.all([disconnected, clientDisconnected]);
  assert.equal(clients.dag.client.state.availability, 'last-known');
  assert.match(clients.dag.client.state.label, /not live/);
  assert.deepEqual(bridge.status.masked, ['todo-sidebar']);
  assert.deepEqual(widgets.get('omo-dag'), ['native dag']);
  dagOwned = false;
  bridge.readinessChanged();
  assert.deepEqual(new Set(bridge.status.masked), new Set(['todo-sidebar', 'omo-task']));
  const todoClosed = signal(presence.events, 'event', event => event.type === 'disconnected' && event.role === 'todo');
  clients.todo.client.close();
  await todoClosed;
  assert.ok(widgets.has('todo-sidebar'));
  assert.equal(bridge.status.masked.includes('omo-task'), true);
  const workerClosed = signal(presence.events, 'event', event => event.type === 'disconnected' && event.role === 'workers');
  clients.workers.client.close();
  await workerClosed;
  assert.deepEqual(bridge.status.masked, []);
  console.log(`TASK5_IPC_LEDGER ${JSON.stringify(ledger)}`);
});

test('wrong bindings, replayed sequence and duplicate observers cannot grant readiness', async t => {
  const presence = createPresence({ ...owner, storage: memoryStorage(), pipePath: pipe('negative') });
  t.after(() => presence.close());
  await presence.start();
  const receipt = await presence.publish(snapshot(presence));
  const attached = attachClient(t, presence, 'todo');
  const message = await signal(attached.client.events, 'snapshot');
  const offer = presence.offer('todo');
  for (const [field, value] of [['sequence', receipt.sequence - 1], ['role', 'workers']]) {
    const rejected = signal(presence.events, 'event', event => event.type === 'rejected');
    attached.client.socket.write(`${JSON.stringify({ ...offer, type: 'ack', sequence: receipt.sequence, [field]: value })}\n`);
    await rejected;
    assert.equal(presence.isReady({ ...receipt, role: 'todo' }), false);
  }
  for (const [field, value] of [['token', 'wrong'], ['ownerKey', 'wrong'], ['scopeEpoch', 0], ['sessionId', 'wrong'], ['role', 'unknown'], ['v', 2]]) {
    const socket = net.createConnection(pipe('negative'));
    t.after(() => socket.destroy());
    const disconnected = signal(socket, 'close');
    const rejected = signal(presence.events, 'event', event => event.type === 'rejected');
    socket.once('connect', () => socket.write(`${JSON.stringify({ ...offer, type: 'hello', [field]: value })}\n`));
    await rejected;
    await disconnected;
    assert.equal(presence.isReady({ ...receipt, role: 'todo' }), false);
  }
  const duplicate = attachClient(t, presence, 'todo');
  const duplicateClosed = signal(duplicate.client.events, 'disconnected');
  await duplicateClosed;
  const ack = signal(presence.events, 'event', event => event.type === 'ack');
  await paint(attached.client, message, []);
  await ack;
  assert.equal(presence.isReady({ ...receipt, role: 'todo' }), true);
  assert.equal(presence.isReady({ ...receipt, role: 'workers' }), false);
  const fresh = createPresence({ ...owner, storage: memoryStorage(), pipePath: pipe('fresh-token') });
  t.after(() => fresh.close());
  assert.notEqual(fresh.offer('todo').token, offer.token);
});

test('scope rebasing rejects async writes and callbacks; waiting/empty/error/last-known are distinct', async t => {
  const storage = memoryStorage();
  const entered = deferred();
  const release = deferred();
  const write = storage.write;
  let block = true;
  storage.write = async value => {
    if (block) { block = false; entered.resolve(); await release.promise; }
    return write.call(storage, value);
  };
  const presence = createPresence({ ...owner, storage, pipePath: pipe('rebase') });
  t.after(() => presence.close());
  await presence.start();
  assert.equal(presence.state.availability, 'waiting');
  const staleWrite = presence.publish(snapshot(presence));
  const staleRejection = assert.rejects(staleWrite);
  await entered.promise;
  presence.rebase({ ...owner, scopeEpoch: 2 });
  const fresh = presence.publish(snapshot(presence, []));
  release.resolve();
  await staleRejection;
  const receipt = await fresh;
  assert.equal(presence.state.availability, 'empty');
  assert.equal(storage.committed.length, 1);
  const attached = attachClient(t, presence, 'todo');
  const message = await signal(attached.client.events, 'snapshot');
  const ack = signal(presence.events, 'event', event => event.type === 'ack');
  await paint(attached.client, message, []);
  await ack;
  assert.equal(presence.isReady({ ...receipt, role: 'todo' }), true);
  presence.sourceFailed(Object.assign(new Error('read failed'), { code: 'EPERM' }), presence.getOwner());
  assert.equal(presence.state.availability, 'last-known');
  assert.match(presence.state.label, /not live/);
  assert.equal(presence.isReady({ ...receipt, role: 'todo' }), false);
  const disconnected = signal(attached.client.events, 'disconnected');
  presence.rebase({ ...owner, scopeEpoch: 3 });
  await disconnected;
  assert.equal(attached.client.painted(message), false);
  assert.deepEqual(presence.state, { availability: 'waiting', snapshot: null });
  assert.equal(attached.client.state.snapshot, null, 'scope retirement cannot retain an old branch');
  assert.equal(presence.sourceFailed(new Error('stale read callback'), { ...presence.getOwner(), scopeEpoch: 2 }), false);
  presence.sourceFailed(new Error('new scope read failed'), presence.getOwner());
  assert.equal(presence.state.availability, 'error');
  assert.equal(presence.state.snapshot, null);
  await presence.publish({ ...snapshot(presence), ...projectNativeTodo([]) });
  assert.equal(presence.state.availability, 'waiting');
  await assert.rejects(presence.publish({ ...snapshot(presence), scopeEpoch: 2 }));
  assert.throws(() => presence.rebase({ ...owner, scopeEpoch: 3 }));
});

test('serialized delivery follows cache commit; ENOENT/EPERM publication failure restores actual bridge inline', async t => {
  for (const code of ['ENOENT', 'EPERM']) {
    const storage = memoryStorage();
    let fail = false;
    const realWrite = storage.write;
    storage.write = async value => {
      if (fail) throw Object.assign(new Error('cache publication failed'), { code });
      return realWrite.call(storage, value);
    };
    const presence = createPresence({ ...owner, storage, pipePath: pipe(`failure-${code}`) });
    t.after(() => presence.close());
    await presence.start();
    const widgets = new Map([['todo-sidebar', ['inline']]]);
    const ctx = { hasUI: true, mode: 'tui', ui: {
      setWidget(key, content) { if (content === undefined) widgets.delete(key); else widgets.set(key, content); },
    }, sessionManager: { getSessionFile: () => 'test-parent/session.jsonl',
      getSessionId: () => owner.sessionId, getLeafId: () => 'test-entry',
      getBranch: () => [{ type: 'custom', customType: 'senpi.todo-state',
        data: { schema: 'v2', phases: snapshot(presence).phases } }] } };
    const bridge = installDisplayBridge(ctx, presence, { environment: {
      HERDR_SOCKET_PATH: owner.socketPath, HERDR_PANE_ID: owner.parentPaneId,
    } });
    const unsubscribe = presence.subscribeReadiness(() => bridge.readinessChanged());
    t.after(() => { unsubscribe(); bridge.remove(); });
    await presence.idle();
    assert.ok(widgets.has('todo-sidebar'));
    const attached = attachClient(t, presence, 'todo');
    const message = await signal(attached.client.events, 'snapshot');
    const ready = signal(presence.events, 'event', event => event.type === 'ack');
    await paint(attached.client, message, []);
    await ready;
    assert.deepEqual(bridge.status.masked, ['todo-sidebar']);
    assert.equal(widgets.has('todo-sidebar'), false);
    fail = true;
    const error = signal(presence.events, 'event', event => event.type === 'source-error');
    await assert.rejects(presence.publish(snapshot(presence)), { code });
    assert.equal((await error).code, code);
    assert.equal(presence.state.availability, 'last-known');
    assert.deepEqual(bridge.status.masked, []);
    assert.ok(widgets.has('todo-sidebar'));
  }
});

test('a painted waiting snapshot (no native Todo) is ACKed and leased; wrong role or sequence, expiry and a source error still fail closed', async t => {
  const clock = fakeClock();
  const presence = createPresence({ ...owner, storage: memoryStorage(), clock, pipePath: pipe('waiting') });
  t.after(() => presence.close());
  await presence.start();
  const receipt = await presence.publish({ ...snapshot(presence), ...projectNativeTodo([]) });
  assert.equal(presence.state.availability, 'waiting');
  const attached = attachClient(t, presence, 'dag');
  const message = await signal(attached.client.events, 'snapshot');
  assert.equal(message.snapshot.availability, 'waiting');
  assert.equal(presence.isReady({ ...receipt, role: 'dag' }), false, 'delivery is not a paint');
  const offer = presence.offer('dag');
  for (const [field, value] of [['sequence', receipt.sequence + 1], ['role', 'workers']]) {
    const rejected = signal(presence.events, 'event', event => event.type === 'rejected');
    attached.client.socket.write(`${JSON.stringify({ ...offer, type: 'ack', sequence: receipt.sequence, [field]: value })}\n`);
    await rejected;
    assert.equal(presence.isReady({ ...receipt, role: 'dag' }), false);
  }
  const ack = signal(presence.events, 'event', event => event.type === 'ack' && event.role === 'dag');
  await paint(attached.client, message, []);
  await ack;
  assert.equal(presence.isReady({ ...receipt, role: 'dag' }), true);
  assert.equal(presence.isReady({ ...receipt, role: 'todo' }), false, 'only the painted role');
  assert.equal(presence.isReady({ ...receipt, scopeEpoch: 2, role: 'dag' }), false);
  clock.advance(10000);
  assert.equal(presence.isReady({ ...receipt, role: 'dag' }), false, 'the paint lease expires');
  const renewal = signal(presence.events, 'event', event => event.type === 'ack' && event.role === 'dag');
  await paint(attached.client, message, []);
  await renewal;
  assert.equal(presence.isReady({ ...receipt, role: 'dag' }), true);
  const status = signal(attached.client.events, 'status');
  presence.sourceFailed(Object.assign(new Error('read failed'), { code: 'EPERM' }), presence.getOwner());
  await status;
  assert.equal(presence.state.availability, 'error', 'a waiting snapshot is never last-known');
  assert.equal(presence.isReady({ ...receipt, role: 'dag' }), false);
  const rejected = signal(presence.events, 'event', event => event.type === 'rejected');
  attached.client.socket.write(`${JSON.stringify({ ...offer, type: 'ack', sequence: receipt.sequence })}\n`);
  await rejected;
  assert.equal(presence.isReady({ ...receipt, role: 'dag' }), false);
});
