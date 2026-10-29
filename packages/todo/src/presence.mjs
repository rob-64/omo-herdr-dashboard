import net from 'node:net';
import { randomBytes, randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { displayOwnerKey } from './display-bridge.mjs';
import { sameSnapshotScope, validSnapshot } from './storage.mjs';
import { clearStaleIpc, ipcPath, isIpcPath } from '../../coordination/src/platform.mjs';

const roles = new Set(['todo', 'workers', 'dag']);
const leaseMs = 10000;
const realClock = { now: () => Date.now(), setTimeout, clearTimeout };
const send = (socket, value) => socket.write(`${JSON.stringify(value)}\n`);

// Newline-framed, bounded local messages; no TCP endpoints or daemon.
function receive(socket, callback) {
  socket.setEncoding('utf8');
  let buffer = '';
  const data = chunk => {
    buffer += chunk;
    if (buffer.length > 4 * 1024 * 1024) { socket.destroy(); return; }
    let cut;
    while ((cut = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, cut);
      buffer = buffer.slice(cut + 1);
      let message;
      try { message = JSON.parse(line); }
      catch { socket.destroy(); return; }
      callback(message);
    }
  };
  socket.on('data', data);
  return () => socket.off('data', data);
}

export function createPresence({ socketPath, parentPaneId, sessionId, scopeEpoch = 1,
  storage, pipePath = ipcPath(`omo-herdr-todo-${randomUUID()}`),
  clock = realClock, settings = {}, hasDagOwnedTasks = () => false }) {
  if (![socketPath, parentPaneId, sessionId].every(value => typeof value === 'string' && value.length > 0) ||
    !Number.isSafeInteger(scopeEpoch) || scopeEpoch < 1 ||
    !isIpcPath(pipePath)) throw new TypeError('Valid owner and local IPC path required');
  let owner = { socketPath, parentPaneId, sessionId, scopeEpoch,
    ownerKey: displayOwnerKey(socketPath, parentPaneId, sessionId) };
  const tokens = new Map([...roles].map(role => [role, randomBytes(32).toString('hex')]));
  const events = new EventEmitter();
  const sockets = new Set();
  const observers = new Map();
  let sequence = 0;
  let revision = 0;
  let baseline = null;
  let lastKnown = null;
  let state = { availability: 'waiting', snapshot: null };
  let tail = Promise.resolve();
  let closed = false;
  let started = false;
  let starting = null;
  storage.rebase(owner);
  const emit = (type, fields = {}) => events.emit('event', { type, ...fields });
  const changed = () => events.emit('readiness');
  const binding = role => ({ v: 1, token: tokens.get(role), ownerKey: owner.ownerKey, sessionId: owner.sessionId,
    scopeEpoch: owner.scopeEpoch, role });
  const authenticated = (message, role = message?.role) => message?.v === 1 && message.token === tokens.get(role) &&
    sameSnapshotScope(message, owner) && roles.has(role);
  function clearLease(observer) {
    if (observer.timer !== null) clock.clearTimeout(observer.timer);
    observer.timer = null;
    observer.expires = 0;
    // A fresh delivery/ack gives the viewer a new paint opportunity. The
    // expiry callback sets this flag again after clearing the timer.
    observer.expired = false;
  }
  function deliver(observer) {
    if (!baseline) {
      send(observer.socket, { type: 'status', ...binding(observer.role), ...state });
      return;
    }
    clearLease(observer);
    observer.sequence = baseline.sequence;
    send(observer.socket, { type: 'snapshot', ...binding(observer.role), sequence: baseline.sequence,
      snapshot: baseline });
    emit('delivery', { role: observer.role, sequence: baseline.sequence, scopeEpoch: owner.scopeEpoch });
  }
  const server = net.createServer(socket => {
    sockets.add(socket);
    let observer = null;
    const stopReceiving = receive(socket, message => {
      emit('packet', { message: structuredClone(message) });
      if (closed || !authenticated(message, observer?.role ?? message?.role)) {
        emit('rejected', { reason: 'binding' }); socket.destroy(); return;
      }
      if (!observer) {
        if (message.type !== 'hello' || message.sequence !== 0 || observers.has(message.role)) {
          emit('rejected', { reason: 'duplicate-or-hello', role: message.role }); socket.destroy(); return;
        }
        observer = { socket, role: message.role, sequence: 0, expires: 0, expired: false, timer: null };
        observers.set(observer.role, observer);
        emit('connected', { role: observer.role });
        deliver(observer);
        return;
      }
      // A committed waiting snapshot (a readable branch without a native Todo) is
      // painted and acknowledged like current or empty; an error or last-known is
      // not. The display bridge alone keeps todo-sidebar unmasked without a Todo.
      if (message.type !== 'ack' || message.role !== observer.role || !baseline ||
        message.sequence !== baseline.sequence || message.sequence !== observer.sequence ||
        !['current', 'empty', 'waiting'].includes(state.availability)) {
        clearLease(observer);
        emit('rejected', { reason: 'ack', role: observer.role });
        changed();
        return;
      }
      clearLease(observer);
      observer.expires = clock.now() + leaseMs;
      const epoch = revision;
      observer.timer = clock.setTimeout(() => {
        if (epoch !== revision || observers.get(observer.role) !== observer) return;
        clearLease(observer);
        observer.expired = true;
        emit('expired', { role: observer.role });
        changed();
      }, leaseMs);
      observer.timer?.unref?.();
      emit('ack', { role: observer.role, sequence: message.sequence, scopeEpoch: owner.scopeEpoch });
      changed();
    });
    socket.on('error', error => emit('socket-error', { code: error.code }));
    socket.once('close', () => {
      stopReceiving();
      sockets.delete(socket);
      if (observer && observers.get(observer.role) === observer) {
        clearLease(observer);
        observers.delete(observer.role);
        emit('disconnected', { role: observer.role });
        changed();
      }
    });
  });
  server.on('error', error => emit('pipe-error', { code: error.code }));
  const presence = {
    settings,
    events,
    getOwner: () => ({ ...owner }),
    hasDagOwnedTasks,
    get state() { return structuredClone(state); },
    offer(role) {
      if (!roles.has(role)) throw new TypeError('Unknown viewer role');
      return { pipePath, ...binding(role), sequence: 0 };
    },
    rotateRole(role) {
      if (closed || !roles.has(role)) throw new TypeError('Current viewer role required');
      const observer = observers.get(role);
      if (observer) {
        clearLease(observer);
        observers.delete(role);
        observer.socket.end(JSON.stringify({ type: 'status', ...binding(role),
          availability: 'last-known', snapshot: baseline, label: 'disconnected; source changed; not live' }) + '\n');
      }
      tokens.set(role, randomBytes(32).toString('hex'));
      changed();
      return presence.offer(role);
    },
    subscribeReadiness(callback) {
      events.on('readiness', callback);
      return () => events.off('readiness', callback);
    },
    async idle() { await tail; },
    start() {
      if (closed) return Promise.reject(new Error('Presence closed'));
      if (starting) return starting;
      starting = clearStaleIpc(pipePath).then(() => new Promise((resolve, reject) => {
        const failed = error => { server.off('listening', listening); reject(error); };
        const listening = () => { server.off('error', failed); started = true; emit('listening'); resolve(); };
        server.once('error', failed);
        server.once('listening', listening);
        server.listen(pipePath);
      }));
      return starting;
    },
    publish(snapshot) {
      if (closed || !sameSnapshotScope(snapshot, owner)) return Promise.reject(new Error('Stale snapshot publication'));
      const candidate = { ...structuredClone(snapshot), sequence: ++sequence };
      if (!validSnapshot(candidate)) {
        const error = new TypeError('Invalid derived snapshot');
        presence.sourceFailed(error, owner);
        return Promise.reject(error);
      }
      const epoch = revision;
      const job = tail.then(async () => {
        if (closed || epoch !== revision) throw new Error('Stale snapshot publication');
        await storage.write(candidate);
        if (closed || epoch !== revision) throw new Error('Stale snapshot publication');
        baseline = candidate;
        lastKnown = candidate.availability === 'waiting' ? null : candidate;
        state = { availability: candidate.availability, snapshot: candidate };
        emit('committed', { sequence: candidate.sequence, scopeEpoch: candidate.scopeEpoch });
        for (const observer of observers.values()) deliver(observer);
        changed();
        return { ownerKey: candidate.ownerKey, sessionId: candidate.sessionId,
          scopeEpoch: candidate.scopeEpoch, sequence: candidate.sequence };
      }).catch(error => {
        if (!closed && epoch === revision) presence.sourceFailed(error, candidate);
        throw error;
      });
      tail = job.catch(() => {});
      return job;
    },
    // The boolean stays for compatibility. Callers that retain a mask need to
    // distinguish a healthy current-baseline ack lag from authorization that is
    // actually gone: expiry, a missing/disconnected observer, a closed server,
    // source error/last-known, or an invalid scope/baseline.
    readiness(receipt) {
      if (closed) return 'presence-closed';
      if (!sameSnapshotScope(receipt, owner)) return 'presence-scope';
      if (!['current', 'empty', 'waiting'].includes(state.availability)) return 'presence-error';
      if (!Number.isSafeInteger(receipt?.sequence) || receipt.sequence < 1) return 'presence-invalid';
      const observer = observers.get(receipt.role);
      if (!observer) return 'presence-missing';
      if (!baseline) return 'presence-baseline';
      if (observer.expired) return 'presence-expired';
      if (receipt.sequence !== baseline.sequence || observer.sequence !== receipt.sequence ||
        !(observer.expires > clock.now())) return 'presence-ack-lag';
      return 'ready';
    },
    isReady(receipt) {
      return this.readiness(receipt) === 'ready';
    },
    sourceFailed(error, sourceScope) {
      if (closed || !sameSnapshotScope(sourceScope, owner)) return false;
      revision++;
      baseline = null;
      for (const observer of observers.values()) clearLease(observer);
      state = { availability: lastKnown ? 'last-known' : 'error',
        label: lastKnown ? 'last-known; source unavailable; not live' : 'source unavailable',
        snapshot: lastKnown, error: { code: error?.code ?? 'SOURCE_ERROR', message: String(error?.message ?? error) } };
      for (const observer of observers.values()) deliver(observer);
      emit('source-error', { code: state.error.code });
      changed();
      return true;
    },
    rebase(next) {
      if (closed || !Number.isSafeInteger(next.scopeEpoch) || next.scopeEpoch <= owner.scopeEpoch) {
        throw new TypeError('Scope epoch must advance');
      }
      if (![next.socketPath, next.parentPaneId, next.sessionId].every(value => typeof value === 'string' && value)) {
        throw new TypeError('Valid rebased owner required');
      }
      // Retire the old scope explicitly before ending its sockets. A viewer must
      // not label an old branch as last-known data for a newly selected branch.
      for (const observer of observers.values()) {
        observer.socket.end(`${JSON.stringify({ type: 'status', ...binding(observer.role),
          availability: 'waiting', snapshot: null, label: 'scope changed; baseline unavailable' })}\n`);
      }
      revision++;
      owner = { ...next, ownerKey: displayOwnerKey(next.socketPath, next.parentPaneId, next.sessionId) };
      sequence = 0;
      baseline = null;
      lastKnown = null;
      state = { availability: 'waiting', snapshot: null };
      storage.rebase(owner);
      for (const observer of observers.values()) clearLease(observer);
      observers.clear();
      for (const socket of sockets) if (!socket.writableEnded) socket.destroy();
      emit('rebase', { scopeEpoch: owner.scopeEpoch });
      changed();
    },
    async close() {
      if (closed) return;
      closed = true;
      revision++;
      for (const observer of observers.values()) clearLease(observer);
      observers.clear();
      for (const socket of sockets) socket.destroy();
      changed();
      if (starting) await starting.catch(() => {});
      if (started) await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
      await tail;
      emit('closed', { sockets: sockets.size });
      events.removeAllListeners();
      server.removeAllListeners();
    },
  };
  return presence;
}

// Protocol helper only. The viewer's render clock calls painted AFTER output
// completion; receiving a snapshot, reading a file or constructing UI never ACKs.
export function createRoleClient({ offer, onSnapshot, onStatus = () => {} }) {
  if (!isIpcPath(offer?.pipePath) || !roles.has(offer.role)) throw new TypeError('Role pipe offer required');
  const events = new EventEmitter();
  const socket = net.createConnection(offer.pipePath);
  let current = null;
  let state = { availability: 'waiting', snapshot: null };
  let alive = true;
  const messageBinding = message => message.v === offer.v && message.token === offer.token &&
    sameSnapshotScope(message, offer) && message.role === offer.role;
  const stopReceiving = receive(socket, message => {
    if (!messageBinding(message)) { socket.destroy(); return; }
    if (message.type === 'snapshot') {
      if (!validSnapshot(message.snapshot) || message.sequence !== message.snapshot.sequence ||
        !sameSnapshotScope(message.snapshot, offer) || current && message.sequence <= current.sequence) {
        socket.destroy(); return;
      }
      current = message;
      state = { availability: message.snapshot.availability, snapshot: message.snapshot };
      events.emit('snapshot', message);
      onSnapshot?.(structuredClone(message.snapshot), message);
    } else if (message.type === 'status') {
      current = null;
      state = { availability: message.availability, snapshot: message.snapshot, label: message.label, error: message.error };
      onStatus(message);
      events.emit('status', message);
    }
  });
  socket.once('connect', () => send(socket, { ...offer, type: 'hello' }));
  socket.on('error', error => events.emit('failure', error));
  socket.once('close', () => {
    alive = false;
    current = null;
    stopReceiving();
    if (state.snapshot) {
      state = { ...state, availability: 'last-known', label: 'last-known; disconnected; not live' };
      onStatus(state);
    }
    events.emit('disconnected');
  });
  return {
    events, socket,
    get state() { return structuredClone(state); },
    painted(message) {
      if (!alive || current !== message || !messageBinding(message)) return false;
      send(socket, { type: 'ack', v: offer.v, token: offer.token, ownerKey: offer.ownerKey,
        sessionId: offer.sessionId, scopeEpoch: offer.scopeEpoch, role: offer.role, sequence: message.sequence });
      return true;
    },
    close() { socket.destroy(); },
  };
}
