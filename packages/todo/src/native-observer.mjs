// POSIX stand-in for the PowerShell observers in controller.mjs. It speaks the
// same stdout line protocol (OBSERVING/ARMED/CAPTURE/CHECK/DEAD/RELEASE/EFFECT)
// so the callers stay platform-neutral. Configuration arrives as the first
// stdin line, never argv, so tokens are not visible to other users through ps.
//
// POSIX has no process handle to hold or duplicate: a process is the pair
// (pid, start time), and the Handle field carries the peer pid. A non-child's
// exit status is unreadable, so DEAD reports UNOBSERVABLE_EXIT_CODE.
import net from 'node:net';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { ipcServerPids, processAlive, queryPosixProcesses, UNOBSERVABLE_EXIT_CODE,
  waitForProcessExit } from '../../coordination/src/platform.mjs';

const digest = value => createHash('sha256').update(value).digest('hex');

// One queue of newline-framed lines from a stream; next() resolves to null at EOF.
export function lineQueue(stream) {
  const lines = [], waiters = [];
  let ended = false, buffer = '';
  const push = value => { const waiter = waiters.shift(); if (waiter) waiter(value); else lines.push(value); };
  stream.setEncoding?.('utf8');
  stream.on('data', chunk => {
    buffer += chunk;
    let at;
    while ((at = buffer.indexOf('\n')) >= 0) { push(buffer.slice(0, at).replace(/\r$/, '')); buffer = buffer.slice(at + 1); }
  });
  const end = () => { if (ended) return; ended = true; if (buffer) push(buffer); buffer = ''; while (waiters.length) waiters.shift()(null); };
  stream.once('end', end); stream.once('close', end); stream.once('error', end);
  return {
    next(timeoutMs) {
      if (lines.length) return Promise.resolve(lines.shift());
      if (ended) return Promise.resolve(null);
      return new Promise((resolve, reject) => {
        let timer;
        const waiter = value => { clearTimeout(timer); resolve(value); };
        waiters.push(waiter);
        if (timeoutMs) timer = setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index >= 0) waiters.splice(index, 1);
          reject(new Error('line deadline'));
        }, timeoutMs);
      });
    },
  };
}

export function connectIpc(path, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(path);
    const timer = setTimeout(() => { socket.destroy(); reject(new Error('IPC connect deadline')); }, timeoutMs);
    socket.on('error', error => { clearTimeout(timer); reject(error); });
    socket.once('connect', () => { clearTimeout(timer); resolve(socket); });
  });
}

// The single server process behind a connected Unix socket path.
export async function ipcPeer(path) {
  const pids = await ipcServerPids(path);
  if (pids.length !== 1) throw new Error('native IPC peer unavailable');
  return pids[0];
}

export async function processRow(pid) {
  const row = (await queryPosixProcesses(pid)).find(item => item.ProcessId === pid);
  if (!row?.Birth || !row.ExecutablePath || !row.CommandLine) throw new Error('native process identity unavailable');
  return row;
}

// The exact (pid, birth, argv0, argv digest) identity recorded for a process.
export async function verifyProcess(identity, message) {
  const row = await processRow(identity.pid);
  if (row.Birth !== identity.birth || row.ExecutablePath !== identity.executable ||
    digest(row.CommandLine) !== identity.argvDigest) throw new Error(message);
  return row;
}

const write = (socket, value) => new Promise((resolve, reject) =>
  socket.write(`${typeof value === 'string' ? value : JSON.stringify(value)}\n`, error => error ? reject(error) : resolve()));
const print = line => process.stdout.write(`${line}\n`);

async function capture(config, input) {
  const { pipePath, request, nonce, token, custody } = config;
  const socket = await connectIpc(pipePath);
  const pipe = lineQueue(socket);
  const peer = await ipcPeer(pipePath);
  const row = await processRow(peer);
  const producer = { pid: peer, birth: row.Birth };
  await write(socket, request);
  if (custody) {
    const welcome = await pipe.next(15000).catch(() => null);
    if (!welcome) throw new Error('live custody binding unavailable');
    print(`OBSERVING ${welcome}`);
  }
  print(`ARMED ${JSON.stringify({ ProcessId: peer, Birth: row.Birth, ExecutablePath: row.ExecutablePath,
    CommandLine: row.CommandLine, ParentProcessId: row.ParentProcessId, Handle: String(peer) })}`);
  const death = async () => {
    await waitForProcessExit(producer, 15000).catch(() => { throw new Error('producer death deadline'); });
    print(`DEAD ${UNOBSERVABLE_EXIT_CODE}`);
  };
  if (!custody) {
    for (;;) {
      const mode = await input.next();
      if (mode === 'death') await death();
      else if (mode === 'release') {
        let raw, message;
        do {
          raw = await pipe.next(15000).catch(() => { throw new Error('producer release deadline'); });
          if (!raw) throw new Error('producer release unknown');
          message = JSON.parse(raw);
        } while (message.type === 'observing');
        print(`RELEASE ${raw}`);
      } else if (mode === 'dispose' || mode === null) break;
      else throw new Error('invalid observer action');
    }
    socket.destroy();
    return;
  }
  let invalid = false, producerDead = false, disposing = false, pipeOpen = true;
  const diagnostic = (name, extra) => print(`${name} ${JSON.stringify({ helperPid: process.pid, producerPid: peer, nonce, ...extra })}`);
  const exited = async () => !await processAlive(producer);
  let pipeRead = pipe.next(), inputRead = input.next();
  let deathWatch = waitForProcessExit(producer, 2 ** 31 - 1).then(() => 'dead');
  for (;;) {
    const waits = [deathWatch, pipeRead?.then(value => ({ pipe: value })), disposing ? null : inputRead.then(value => ({ input: value }))]
      .filter(Boolean);
    const event = await Promise.race(waits);
    if (event === 'dead') {
      producerDead = true; deathWatch = new Promise(() => {});
      diagnostic('CUSTODY_PRODUCER_DEAD', { producerBirth: row.Birth, exitCode: UNOBSERVABLE_EXIT_CODE, custodyInvalid: invalid });
      continue;
    }
    if ('pipe' in event) {
      const raw = event.pipe;
      if (raw === null) {
        pipeOpen = false;
        diagnostic('CUSTODY_PIPE_EOF', { disposing, producerDead, custodyInvalid: invalid });
        if (disposing) { if (!await exited()) throw new Error('unacknowledged custody disposal'); break; }
        if (!await exited()) invalid = true;
        pipeRead = null;
        continue;
      }
      const packet = JSON.parse(raw);
      if (packet.type === 'custody-effect' && packet.nonce === nonce && packet.token === token) {
        invalid = true;
        print(`EFFECT ${raw}`);
        await write(socket, { ...packet, type: 'custody-invalidated' });
      } else if (disposing && packet.type === 'custody-disposed' && packet.nonce === nonce && packet.token === token) {
        break;
      } else if (packet.type !== 'observing' && packet.type !== 'released') invalid = true;
      pipeRead = pipe.next();
      continue;
    }
    const mode = event.input;
    diagnostic('CUSTODY_INPUT', { mode, disposing, producerDead, custodyInvalid: invalid });
    if (mode === 'custody-check') print(`CHECK ${!invalid}`);
    else if (mode === 'death') await death();
    else if (mode === 'dispose' || mode === null) {
      invalid = true; disposing = true;
      if (pipeRead && pipeOpen) {
        diagnostic('CUSTODY_DISPOSE_WRITE_ENTER', { disposing, pipeConnected: pipeOpen });
        try { await write(socket, { ...request, type: 'custody-disposed' }); }
        catch (error) {
          const originalProducerDead = await exited();
          diagnostic('DISPOSE_PIPE_ERROR', { producerBirth: row.Birth, originalProducerDead,
            at: new Date().toISOString(), error: error.message });
          if (!originalProducerDead) throw error;
          break;
        }
      } else { if (!await exited()) throw new Error('unacknowledged custody disposal'); break; }
    } else throw new Error('invalid custody observer action');
    if (!disposing) inputRead = input.next();
  }
  socket.destroy();
}

async function custodied(config, input) {
  const { pipePath, viewer, shellPid, request, nonce, launchToken, producer } = config;
  const socket = await connectIpc(pipePath);
  const pipe = lineQueue(socket);
  const peer = await ipcPeer(pipePath);
  if (peer !== viewer.pid) throw new Error('custodian native peer changed');
  const row = await verifyProcess(viewer, 'live custody peer changed');
  if (row.ParentProcessId !== shellPid) throw new Error('custodian native shell changed');
  await write(socket, request);
  const raw = await pipe.next(15000).catch(() => null);
  if (!raw) throw new Error('live custody response unavailable');
  const captured = JSON.parse(raw);
  if (captured.type !== 'custody' || captured.nonce !== nonce || captured.launchToken !== launchToken)
    throw new Error('live custody response changed');
  const holderRow = await verifyProcess(captured.holder, 'live custody holder changed');
  if (holderRow.ParentProcessId !== peer) throw new Error('original holder no longer belongs to verified custodian');
  // The custodian's Handle is the producer pid; the producer is either still
  // the recorded instance or already gone (absent or pid reused).
  if (captured.nativeHandle !== String(producer.pid)) throw new Error('original handle duplication unavailable');
  print(`CAPTURE ${raw}`);
  for (;;) {
    const mode = await input.next();
    if (mode === 'death') {
      await waitForProcessExit(producer, 15000).catch(() => { throw new Error('original producer death unavailable'); });
      await write(socket, { ...request, type: 'custody-check', holder: captured.holder, nativeHandle: captured.nativeHandle });
      const valid = await pipe.next(15000).catch(() => null);
      if (!valid) throw new Error('original custody effect validity unavailable');
      const proof = JSON.parse(valid);
      if (proof.type !== 'custody-valid' || proof.nonce !== nonce || proof.launchToken !== launchToken)
        throw new Error('original custody effect validity changed');
      print(`DEAD ${UNOBSERVABLE_EXIT_CODE}`);
    } else if (mode === 'dispose' || mode === null) break;
    else throw new Error('invalid custody observer action');
  }
  socket.destroy();
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const input = lineQueue(process.stdin);
  try {
    const config = JSON.parse(await input.next() ?? 'null');
    if (config?.mode === 'capture') await capture(config, input);
    else if (config?.mode === 'custodied') await custodied(config, input);
    else throw new Error('invalid native observer mode');
    process.exit(0);
  } catch (error) {
    process.stderr.write(`${error?.message ?? error}\n`);
    process.exit(1);
  }
}
