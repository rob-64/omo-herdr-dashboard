import net from 'node:net';
import { execFile, spawn } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { EventEmitter, once } from 'node:events';
import { mkdir, readFile, readdir, realpath, rename, rm, writeFile } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, resolve } from 'node:path';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { AsyncLocalStorage } from 'node:async_hooks';
import { displayOwnerKey, installDisplayBridge } from './display-bridge.mjs';
import { createPresence } from './presence.mjs';
import { createSnapshotStorage, sameSnapshotScope } from './storage.mjs';
import { claimParentSlot, cooperativelyReleasedParentSlot, parentSlotPaths, validParentSlot } from './parent-slot.mjs';
import { assertResult, bindingRevision, CORE_ORDER, createRecoveryCoordinator, deriveLowerBand, protocolError,
  queryIssues, RECOVERY_SCOPE_KEYS, RECOVERY_V1, sameScope, validScope } from '../../coordination/src/recovery.mjs';
import { inspectViewerProof, readViewerProof, VIEWER_HEARTBEAT_LIMIT_MS } from '../../coordination/src/proof.mjs';
import { clearStaleIpc, ipcPath, isIpcPath, processAlive, queryPosixProcesses, UNOBSERVABLE_EXIT_CODE,
  waitForProcessExit } from '../../coordination/src/platform.mjs';
import { connectIpc, ipcPeer, lineQueue, verifyProcess } from './native-observer.mjs';

// The automatic or explicit Todo pass a managed mutation belongs to.
const passes = new AsyncLocalStorage();

const execute = promisify(execFile);
export const dashboardChannels = Object.freeze({
  query: 'omo.herdr.dashboard.query', reply: 'omo.herdr.dashboard.reply',
  provider: 'omo.herdr.dashboard.provider', retired: 'omo.herdr.dashboard.retired',
});
export const sameDashboardOwner = (a, b) => a && b &&
  ['socketPath', 'parentPaneId', 'sessionId', 'ownerKey'].every(key => a[key] === b[key]);
const sameRect = (a, b) => Boolean(a && b) &&
  ['x', 'y', 'width', 'height'].every(key => a[key] === b[key]);
// The coordination record's explicit geometry digest is SHA-256 over
// [paneId, rect], truncated to 16 hex characters. It is recomputed over the
// record's own rect and accepted only when that rect is the settled cell.
const collabGeometryDigest = (paneId, rect) => createHash('sha256')
  .update(JSON.stringify([paneId, rect])).digest('hex').slice(0, 16);
const lifecycles = globalThis[Symbol.for('omo.herdr.dashboard.lifecycle/2')] ??= new Map();
export function registerManagedDashboard(pi, { stateDir, environment = process.env, createController }) {
  const root = resolve(stateDir);
  const key = JSON.stringify([process.platform === 'win32' ? root.replaceAll('\\', '/').toLowerCase() : root,
    environment.HERDR_SOCKET_PATH, environment.HERDR_PANE_ID]);
  const retained = lifecycles.get(key);
  if (retained) {
    retained.configure(pi, createController);
    retained.announce();
    return retained;
  }
  let controller, revision = 0, tail = Promise.resolve();
  const starts = new WeakMap();
  const binding = { socketPath: environment.HERDR_SOCKET_PATH, parentPaneId: environment.HERDR_PANE_ID };
  const service = {
    stateDir, binding,
    configure(nextPi, nextFactory) {
      this.offAvailability?.();
      pi = nextPi; createController = nextFactory;
      this.offAvailability = pi.events?.on(dashboardChannels.query, event => {
        if (event?.v === 2 && event.kind === 'managed-availability' &&
          event.socketPath === binding.socketPath && event.parentPaneId === binding.parentPaneId) announce(event.requestId);
      });
    },
    announce: requestId => announce(requestId),
    get controller() { return controller; },
    start(event, ctx) {
      const previous = starts.get(event);
      if (previous) return previous;
      const current = ++revision, old = controller;
      controller = undefined;
      // stop() invalidates synchronously, even while the old start is pending.
      const drained = old?.stop();
      const result = tail.then(async () => {
        await drained;
        if (current !== revision) return;
        const next = createController(ctx);
        controller = next;
        try {
          await next.start();
          if (current !== revision || controller !== next) { await next.stop(); return; }
          next.bind(pi);
          void next.schedule().catch(() => {});
          return next;
        } catch (error) {
          if (controller === next) controller = undefined;
          await next.stop();
          ctx.ui.notify(error.message, 'warning');
        }
      });
      starts.set(event, result);
      tail = result.catch(() => {});
      return result;
    },
    stop() {
      revision++;
      const old = controller;
      controller = undefined;
      const drained = old?.stop();
      const result = tail.then(() => drained);
      tail = result.catch(() => {});
      return result;
    },
  };
  const announce = requestId => pi.events?.emit(dashboardChannels.provider, {
    v: 2, kind: 'managed-availability', ...binding, stateDir, service, requestId,
  });
  service.configure(pi, createController);
  lifecycles.set(key, service);
  announce();
  return service;
}
const roles = new Set(['todo', 'dag', 'workers']);
const send = (socket, message) => socket.write(`${JSON.stringify(message)}\n`);
const pathKey = path => process.platform === 'win32' ? String(path).replaceAll('\\', '/').toLowerCase() : String(path);
// A journaled effect that has no observed postcondition is unresolved, whatever
// its kind: it may still land after this verdict, so it can never count as current.
const effectDetail = effect => `pending ${effect?.kind ?? 'unknown'} effect (EFFECT_UNKNOWN)`;
// A v1 coordination record predates launch identity. Only the native shell/viewer
// proof taken immediately before a legacy yield can make it current, and only
// while that proof is fresh.
const legacyProofFresh = (record, now) => {
  const proof = record?.legacyProof;
  if (!proof || typeof proof !== 'object') return false;
  if (proof.paneId !== record.paneId || proof.sessionId !== record.sessionId) return false;
  if (!Number.isSafeInteger(proof.shellPid) || proof.shellPid < 1) return false;
  if (!Number.isSafeInteger(proof.viewerPid) || proof.viewerPid < 1) return false;
  if (!Array.isArray(proof.argv) || !proof.argv.length) return false;
  const at = Date.parse(proof.at ?? '');
  return Number.isFinite(at) && now - at <= VIEWER_HEARTBEAT_LIMIT_MS && at - now <= VIEWER_HEARTBEAT_LIMIT_MS;
};
export const authoritativePaneNotFound = error => error?.code === 'pane_not_found';
export async function discoverLegacyRecords(root, binding) {
  const directory = await realpath(resolve(root)), paths = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.isFile() && entry.name.endsWith('.pane.json')) paths.push(join(directory, entry.name));
    if (entry.isDirectory()) for (const file of await readdir(join(directory, entry.name), { withFileTypes: true })) {
      if (file.isFile() && file.name.endsWith('.pane.json')) paths.push(join(directory, entry.name, file.name));
    }
  }
  const records = [];
  for (const path of paths.sort()) {
    const actual = await realpath(path);
    if (!pathKey(actual).startsWith(pathKey(directory) + '/')) throw blocked('legacy locator leaves configured root');
    const record = await readJson(actual);
    if (record?.parentPaneId === binding.parentPaneId && pathKey(record.socketPath) === pathKey(binding.socketPath))
      records.push({ path, record });
  }
  return records;
}
const controlKeys = ['token', 'ownerKey', 'sessionId', 'scopeEpoch', 'role', 'launchToken', 'paneId', 'statePath'];
const sameControl = (a, b) => a?.v === 1 && b?.v === 1 && controlKeys.every(key => a[key] === b[key]);

function receive(socket, callback) {
  socket.setEncoding('utf8');
  let buffer = '';
  socket.on('data', chunk => {
    buffer += chunk;
    if (buffer.length > 65536) return socket.destroy();
    let cut;
    while ((cut = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, cut); buffer = buffer.slice(cut + 1);
      try { callback(JSON.parse(line)); } catch { socket.destroy(); }
    }
  });
}

async function readJson(path) {
  try { return JSON.parse(await readFile(path, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}
async function writeJson(path, value) {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = `${path}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, JSON.stringify(value), { mode: 0o600 });
    // Windows denies replacing a file while a reader (a viewer or this
    // controller) holds it open. Retry only those codes, briefly and bounded
    // (as parent-slot publish and dag storage); anything else escapes.
    for (let attempt = 0; ; attempt++) {
      try { await rename(temporary, path); break; }
      catch (error) {
        if (process.platform !== 'win32' || !['EPERM', 'EACCES', 'EBUSY'].includes(error.code) || attempt === 5) throw error;
        await delay(10 * 2 ** attempt);
      }
    }
  } catch (error) { await rm(temporary, { force: true }); throw error; }
}
export async function readRoleState(path) {
  const state = await readJson(path);
  if (state === null) return null;
  if (state.schema !== 'omo-herdr-dashboard/role/1' || !roles.has(state.role) ||
    !['socketPath', 'parentPaneId', 'tabId', 'sessionId', 'ownerKey'].every(key =>
      typeof state[key] === 'string' && state[key]) ||
    state.ownerKey !== displayOwnerKey(state.socketPath, state.parentPaneId, state.sessionId) ||
    !Number.isSafeInteger(state.scopeEpoch) || state.scopeEpoch < 1 ||
    typeof state.launchToken !== 'string' || !state.launchToken ||
    typeof state.paneId !== 'string' || !state.paneId) throw new TypeError('Invalid managed role state');
  return state;
}
function requestControl(type, { state, paneId, launchToken, pid = process.pid }) {
  const offer = state?.controlOffer;
  if (!isIpcPath(offer?.pipePath) || state.paneId !== paneId ||
    state.launchToken !== launchToken || offer.paneId !== paneId ||
    offer.launchToken !== launchToken || !sameSnapshotScope(offer, state) ||
    offer.role !== state.role) return Promise.resolve(false);
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(offer.pipePath);
    const timer = setTimeout(() => { socket.destroy(); reject(new Error('Role control response timed out')); }, 10000);
    socket.once('connect', () => send(socket, { ...offer, type, pid }));
    receive(socket, message => {
      const accepted = sameControl(message, offer) && message.type === (type === 'register' ? 'registered' : 'closed');
      clearTimeout(timer); socket.end(); resolve(accepted);
    });
    socket.once('error', error => { clearTimeout(timer); reject(error); });
    socket.once('close', () => { clearTimeout(timer); resolve(false); });
  });
}
export const registerRoleViewer = options => requestControl('register', options);
export const requestRoleClose = options => requestControl('close', options);

export const viewerRetirementPipe = launchToken =>
  ipcPath(`omo-herdr-viewer-retirement-${createHash('sha256').update(launchToken).digest('hex')}`);

// POSIX form of the PowerShell retirement below: the same peer, birth, argv0,
// argv digest and parent checks, then a wait for the exact process to vanish.
async function retirePosixViewer(record, identity, request, nonce) {
  const expected = identity.viewer, path = viewerRetirementPipe(record.launchToken);
  const socket = await connectIpc(path);
  try {
    const replies = lineQueue(socket);
    if (await ipcPeer(path) !== expected.pid) throw blocked('retirement peer changed');
    const row = await verifyProcess(expected, 'retirement birth, argv or image changed');
    if (row.ParentProcessId !== identity.shell.pid) throw blocked('retirement argv or parent changed');
    socket.write(`${JSON.stringify(request)}\n`);
    const raw = await replies.next(10000).catch(() => null);
    if (!raw) throw blocked('retirement acknowledgement unavailable');
    const reply = JSON.parse(raw);
    if (reply.type !== 'retiring' || reply.nonce !== nonce || reply.launchToken !== record.launchToken)
      throw blocked('retirement acknowledgement changed');
    await waitForProcessExit(expected, 15000).catch(() => { throw blocked('original viewer exit unavailable'); });
    const receipt = { pid: expected.pid, birth: expected.birth, exitCode: UNOBSERVABLE_EXIT_CODE,
      manual: Boolean(reply.manual), nonce: reply.nonce, custodyClosure: reply.custodyClosure };
    return { ...receipt, stdout: `${JSON.stringify(receipt)}\n`, stderr: '' };
  } finally { socket.destroy(); }
}

export async function retireVerifiedViewer(record, identity) {
  if (!identity?.viewer ||
    identity.viewer.pid !== record.processId || identity.viewer.birth !== record.viewerBirth)
    throw blocked('viewer retirement requires exact original native identity');
  const expected = identity.viewer, nonce = randomUUID();
  const request = { type: 'retire', nonce, role: record.role, sessionId: record.sessionId,
    ownerKey: record.ownerKey, scopeEpoch: record.scopeEpoch, paneId: record.paneId,
    statePath: record.statePath, launchToken: record.launchToken };
  if (process.platform !== 'win32') return retirePosixViewer(record, identity, request, nonce);
  const literal = value => "'" + String(value).replaceAll("'", "''") + "'";
  const script = `$ErrorActionPreference='Stop'
Add-Type -TypeDefinition 'using System;using System.Text;using System.Runtime.InteropServices;public static class RetirePeer {
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool GetNamedPipeServerProcessId(IntPtr h,out uint id);
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool GetProcessTimes(IntPtr h,out long born,out long exited,out long kernel,out long user);
[DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] public static extern bool QueryFullProcessImageName(IntPtr h,uint flags,StringBuilder b,ref uint n);}'
$pipe=[IO.Pipes.NamedPipeClientStream]::new('.',${literal(viewerRetirementPipe(record.launchToken).slice(9))},[IO.Pipes.PipeDirection]::InOut)
$p=$null
try {
$pipe.Connect(5000);[uint32]$peer=0
if(![RetirePeer]::GetNamedPipeServerProcessId($pipe.SafePipeHandle.DangerousGetHandle(),[ref]$peer) -or $peer -ne ${expected.pid}){throw 'retirement peer changed'}
$p=[Diagnostics.Process]::GetProcessById($peer);$handle=$p.Handle
[long]$born=0;[long]$exited=0;[long]$kernel=0;[long]$user=0
if(![RetirePeer]::GetProcessTimes($handle,[ref]$born,[ref]$exited,[ref]$kernel,[ref]$user) -or $born.ToString() -cne ${literal(expected.birth)}){throw 'retirement birth changed'}
$image=[Text.StringBuilder]::new(32768);[uint32]$size=32768
if(![RetirePeer]::QueryFullProcessImageName($handle,0,$image,[ref]$size) -or $image.ToString() -ine ${literal(expected.executable)}){throw 'retirement image changed'}
$row=Get-CimInstance Win32_Process -Filter "ProcessId=$peer"
$hash=[Security.Cryptography.SHA256]::Create()
try{$digest=([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($row.CommandLine)))).Replace('-','').ToLowerInvariant()}finally{$hash.Dispose()}
if($row.ParentProcessId -ne ${identity.shell.pid} -or $digest -cne ${literal(expected.argvDigest)}){throw 'retirement argv or parent changed'}
$writer=[IO.StreamWriter]::new($pipe,[Text.UTF8Encoding]::new($false),4096,$true);$writer.AutoFlush=$true
$reader=[IO.StreamReader]::new($pipe,[Text.UTF8Encoding]::new($false),$false,4096,$true)
$writer.WriteLine(${literal(JSON.stringify(request))})
$line=$reader.ReadLineAsync();if(!$line.Wait(10000) -or !$line.Result){throw 'retirement acknowledgement unavailable'}
$reply=ConvertFrom-Json $line.Result
if($reply.type -cne 'retiring' -or $reply.nonce -cne ${literal(nonce)} -or $reply.launchToken -cne ${literal(record.launchToken)}){throw 'retirement acknowledgement changed'}
if(!$p.WaitForExit(15000)){throw 'original viewer exit unavailable'}
[Console]::WriteLine((ConvertTo-Json -InputObject @{pid=[int]$peer;birth=$born.ToString();exitCode=$p.ExitCode;manual=[bool]$reply.manual;nonce=$reply.nonce;custodyClosure=$reply.custodyClosure} -Compress -Depth 8))
}finally{if($p){$p.Dispose()};$pipe.Dispose()}`;
  const { stdout, stderr } = await execute('powershell.exe',
    ['-NoProfile', '-NonInteractive', '-EncodedCommand', Buffer.from(script, 'utf16le').toString('base64')],
    { windowsHide: true, timeout: 35000, maxBuffer: 65536 });
  const receipt = JSON.parse(stdout);
  if (receipt.pid !== expected.pid || receipt.birth !== expected.birth ||
    receipt.nonce !== nonce || !Number.isInteger(receipt.exitCode))
    throw blocked('original viewer retirement receipt changed');
  return { ...receipt, stdout, stderr };
}

export async function createViewerRetirementServer(options, getState, retire, beforeRetire) {
  const sockets = new Set();
  const server = net.createServer(socket => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.once('close', () => sockets.delete(socket));
    receive(socket, message => {
      const state = getState();
      if (message?.type !== 'retire' || typeof message.nonce !== 'string' || !message.nonce ||
        message.statePath !== options.statePath || message.launchToken !== options.launchToken ||
        message.paneId !== options.paneId || !state ||
        !['role', 'sessionId', 'ownerKey', 'scopeEpoch', 'paneId', 'launchToken']
          .every(key => message[key] === state[key])) return socket.destroy();
      void (async () => {
        const custodyClosure = await beforeRetire?.();
        if (getState() !== state) return socket.destroy();
        socket.end(`${JSON.stringify({ type: 'retiring', nonce: message.nonce,
          launchToken: options.launchToken, manual: false, custodyClosure })}\n`, () => { void retire(); });
      })().catch(() => socket.destroy());
    });
  });
  await clearStaleIpc(viewerRetirementPipe(options.launchToken));
  await new Promise((resolve, reject) => {
    server.once('error', reject); server.once('listening', resolve);
    server.listen(viewerRetirementPipe(options.launchToken));
  });
  return async () => {
    for (const socket of sockets) socket.destroy();
    if (server.listening) await new Promise(resolve => server.close(resolve));
  };
}

export function createDashboardHerdr(environment = process.env) {
  return async (...args) => {
    let stdout;
    try {
      ({ stdout } = await execute(environment.HERDR_BIN_PATH || (process.platform === 'win32' ? 'herdr.exe' : 'herdr'),
        ['pane', ...args], { env: environment, timeout: 10000, maxBuffer: 2 * 1024 * 1024 }));
    } catch (error) {
      // Only the structured native RPC code is absence authority.
      let reply;
      try { reply = JSON.parse(error.stdout || error.stderr); } catch { throw error; }
      if (!reply.error) throw error;
      throw Object.assign(new Error(reply.error.message, { cause: error }), { code: reply.error.code });
    }
    if (!stdout.trim()) return {};
    const reply = JSON.parse(stdout);
    if (reply.error) throw Object.assign(new Error(reply.error.message), { code: reply.error.code });
    return reply.result;
  };
}
export async function resolveDashboardNode(environment = process.env) {
  const candidates = environment.OMO_HERDR_TODO_NODE ? [environment.OMO_HERDR_TODO_NODE] :
    [...(/^node(?:js)?(?:\.exe)?$/i.test(basename(process.execPath)) ? [process.execPath] : []), 'node'];
  for (const candidate of new Set(candidates)) {
    try {
      const { stdout } = await execute(candidate, ['--eval',
        'console.log(JSON.stringify({version:process.versions.node,bun:!!process.versions.bun,path:process.execPath}))'],
      { env: environment, timeout: 5000, maxBuffer: 16384 });
      const result = JSON.parse(stdout.trim());
      if (!result.bun && Number(result.version?.split('.')[0]) >= 24 && isAbsolute(result.path)) return result.path;
    } catch { /* Probe the next declared interpreter. */ }
  }
  throw new Error('Todo pane requires a Node >= 24 interpreter');
}
function shellCommand(args) {
  return process.platform === 'win32'
    ? `& ${args.map(value => `'${String(value).replaceAll("'", "''")}'`).join(' ')}`
    : args.map(value => `'${String(value).replaceAll("'", "'\\''")}'`).join(' ');
}
export function viewerProcess(info, record, expectedPid) {
  const matches = (info?.process_info?.foreground_processes ?? []).filter(process => {
    const argv = process.argv ?? [];
    return argv.length === 8 && pathKey(argv[0]) === pathKey(record.nodePath) && pathKey(argv[1]) === pathKey(record.viewerPath) &&
      argv[2] === '--state' && pathKey(argv[3]) === pathKey(record.statePath) &&
      argv[4] === '--close-pane' && argv[5] === record.paneId &&
      argv[6] === '--launch-token' && argv[7] === record.launchToken &&
      (expectedPid === undefined || process.pid === expectedPid) &&
      (record.processId === undefined || process.pid === record.processId);
  });
  return matches.length === 1 ? matches[0] : null;
}
export function parseLaunchCommand(command) {
  if (typeof command !== 'string') return null;
  const argv = [];
  let at = 0;
  while (at < command.length) {
    while (/[ \t]/.test(command[at] ?? '') && at < command.length) at++;
    if (at === command.length) break;
    let value;
    if (command[at] === '"') {
      const end = command.indexOf('"', at + 1);
      if (end < 0 || command[end - 1] === '\\') return null;
      value = command.slice(at + 1, end); at = end + 1;
      if (at < command.length && !/[ \t]/.test(command[at])) return null;
    } else {
      const start = at;
      while (at < command.length && !/[ \t]/.test(command[at])) at++;
      value = command.slice(start, at);
      if (/["\r\n]/.test(value)) return null;
    }
    if (!value || /[\r\n]/.test(value)) return null;
    argv.push(value);
  }
  return argv;
}
export async function queryShellProcesses(pid, run = execute) {
  // Fail as a query failure without starting PowerShell: an empty row list would be read by
  // observePreviousProducer as proof that the producer died.
  if (!Number.isSafeInteger(pid) || pid < 1) throw Object.assign(new Error('process query skipped: invalid pid'), { code: 'SHELL_QUERY_INVALID_PID' });
  for (let attempt = 0; attempt < 2; attempt++) {
  const { stdout } = await run('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command',
    `[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); $ErrorActionPreference = 'Stop'; $rows = @(Get-CimInstance Win32_Process -Filter "ProcessId = ${pid} OR ParentProcessId = ${pid}" | Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine,@{Name='CreationDate';Expression={$_.CreationDate.ToUniversalTime().ToString('o')}},@{Name='Birth';Expression={$h=[Diagnostics.Process]::GetProcessById($_.ProcessId);try{$null=$h.Handle;$h.StartTime.ToUniversalTime().ToFileTimeUtc().ToString()}finally{$h.Dispose()}}}); ConvertTo-Json -InputObject $rows -Compress`],
  { timeout: 30000, maxBuffer: 65536 }); // was 5 s: killed (SIGTERM) while OmO was starting, so the claim failed
  if (stdout.trim()) return JSON.parse(stdout);
  }
  // Empty output is a failed query, not an empty list.
  throw Object.assign(new Error('process query returned no output'), { code: 'SHELL_QUERY_EMPTY' });
}
// Process identity rows, cached per pid. Layout passes ask about the same pane shells, this
// process and its ancestors every few seconds, and each uncached ask starts a PowerShell + WMI
// query; at several per second that made the whole terminal stutter. A row set is reused for
// SHELL_QUERY_TTL_MS unless a pane mutation or a viewer register/close happened since. The epoch
// is shared through globalThis so the Todo and DAG controllers in one OmO process clear each other.
const PROCESS_QUERY_EPOCH = Symbol.for('omo-herdr-dashboard.processQueryEpoch');
export const SHELL_QUERY_TTL_MS = 10000;
export function bumpProcessQueryEpoch() {
  globalThis[PROCESS_QUERY_EPOCH] = (globalThis[PROCESS_QUERY_EPOCH] ?? 0) + 1;
}
export function cachedProcessQuery(query, { ttlMs = SHELL_QUERY_TTL_MS, now = Date.now } = {}) {
  const entries = new Map();
  return async pid => {
    const at = now(), epoch = globalThis[PROCESS_QUERY_EPOCH] ?? 0;
    let entry = entries.get(pid);
    if (!entry || entry.epoch !== epoch || at - entry.at >= ttlMs) {
      for (const [key, old] of entries) if (old.epoch !== epoch || at - old.at >= ttlMs) entries.delete(key);
      const created = entry = { at, epoch, promise: query(pid) };
      entries.set(pid, created);
      created.promise.catch(() => { if (entries.get(pid) === created) entries.delete(pid); });
    }
    return structuredClone(await entry.promise);
  };
}
const nativeShellProcesses = cachedProcessQuery(process.platform === 'win32' ? queryShellProcesses : queryPosixProcesses);
// Herdr list/process-info reads are repeated by layout passes a few times per
// second, and every uncached call synchronously spawns herdr on Windows. The
// 1000 ms cache is cleared by the shared process epoch (any pane mutation, a
// viewer register/close, or a DAG-side bump). Placement guards and final
// authorizations run inside freshReads so their evidence is always new.
const HERDR_READ_TTL_MS = 1000;
const herdrReadVerbs = new Set(['list', 'process-info']);
const herdrReadOnlyVerbs = new Set(['layout', 'get', 'neighbor']);
let freshDepth = 0;
export async function freshReads(job) {
  freshDepth++;
  try { return await job(); }
  finally { freshDepth--; }
}
export function herdrRead(query, { ttlMs = HERDR_READ_TTL_MS, now = Date.now } = {}) {
  const entries = new Map();
  return async (...args) => {
    if (!herdrReadVerbs.has(args[0])) {
      try { return await query(...args); }
      finally { if (!herdrReadOnlyVerbs.has(args[0])) bumpProcessQueryEpoch(); }
    }
    if (freshDepth > 0) return query(...args);
    const key = args.join('\u0000'), at = now(), epoch = globalThis[PROCESS_QUERY_EPOCH] ?? 0;
    let entry = entries.get(key);
    if (!entry || entry.epoch !== epoch || at - entry.at >= ttlMs) {
      for (const [older, value] of entries) if (value.epoch !== epoch || at - value.at >= ttlMs) entries.delete(older);
      const created = entry = { at, epoch, promise: query(...args) };
      entries.set(key, created);
      created.promise.catch(() => { if (entries.get(key) === created) entries.delete(key); });
    }
    return structuredClone(await entry.promise);
  };
}
// An explicit /dashboard only reconciles unresolved operations; it cannot
// rebuild a blocked actor. Restarting the OmO window releases and re-claims it.
const managedUnavailable = 'managed controller or lease unavailable; restart this OmO window, then run /dashboard in this tab if the dashboard does not return';
const blocked = (cause, code) => Object.assign(new Error(`BLOCKED: ${cause?.message ?? cause}`), { inlineFallback: true, cause },
  code ? { code } : {});
const nativeIdentity = row => {
  if (!Number.isSafeInteger(row?.ProcessId) || !row.Birth || !row.ExecutablePath || !row.CommandLine) throw blocked('native identity unavailable');
  return { pid: row.ProcessId, birth: row.Birth, executable: row.ExecutablePath,
    argvDigest: createHash('sha256').update(row.CommandLine).digest('hex') };
};
const retirementCaptures = new WeakMap();
// POSIX observers run as a Node child (OmO itself may be a Bun binary). The
// child is detached so it never joins a pane's foreground process group.
const nativeObserverPath = fileURLToPath(new URL('./native-observer.mjs', import.meta.url));
let observerNode;
async function spawnNativeObserver(config) {
  observerNode ??= !process.versions.bun && /^node(?:js)?$/i.test(basename(process.execPath))
    ? Promise.resolve(process.execPath) : resolveDashboardNode();
  const node = await observerNode.catch(error => { observerNode = undefined; throw error; });
  const child = spawn(node, [nativeObserverPath], { detached: true, stdio: ['pipe', 'pipe', 'pipe'] });
  child.stdin.on('error', () => {});
  child.stdin.write(`${JSON.stringify(config)}\n`);
  return child;
}
export async function captureLegacyProducer({ stateDir, recordPath, parent, herdr, slotPath, slotStateDir = stateDir,
  custody = false }) {
  const root = pathKey(await realpath(resolve(stateDir))), path = await realpath(recordPath);
  if (!pathKey(path).startsWith(root + '/')) throw blocked('legacy record outside configured root');
  const recordBytes = await readFile(path), record = JSON.parse(recordBytes);
  const statePath = await realpath(record.statePath);
  if (!pathKey(statePath).startsWith(root + '/')) throw blocked('legacy state outside configured root');
  const stateBytes = await readFile(statePath), state = JSON.parse(stateBytes), offer = state.controlOffer;
  if (record.parentPaneId !== parent.paneId || record.tabId !== parent.tabId ||
    !sameDashboardOwner(record, state) || record.role !== state.role || record.paneId !== state.paneId ||
    record.launchToken !== state.launchToken || !sameControl(offer, { ...offer, ...state, statePath: record.statePath }) ||
    !isIpcPath(offer?.pipePath)) throw blocked('legacy control binding unavailable');
  const members = (await herdr('list')).panes, parents = members?.filter(pane => pane.pane_id === parent.paneId);
  if (parents?.length !== 1 || parents[0].tab_id !== parent.tabId) throw blocked('legacy parent membership changed');
  const info = await herdr('process-info', '--pane', parent.paneId);
  if (info.process_info?.shell_pid !== parent.shell.pid) throw blocked('legacy parent shell changed');
  const parentRow = (await nativeShellProcesses(parent.shell.pid)).find(row => row.ProcessId === parent.shell.pid);
  if (nativeIdentity(parentRow).birth !== parent.shell.birth) throw blocked('legacy parent birth changed');
  const roleIdentity = await legacyPaneIdentity(await herdr('process-info', '--pane', record.paneId), record);
  if (!roleIdentity?.viewer) throw blocked('legacy native viewer unavailable');
  const nonce = randomUUID(), request = { ...offer, type: 'observe-retirement', nonce,
    ...(custody ? { custody: true } : {}) };
  const script = `$ErrorActionPreference='Stop'
Add-Type -TypeDefinition 'using System;using System.Text;using System.Runtime.InteropServices;public static class LegacyPeer {
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool GetNamedPipeServerProcessId(IntPtr h,out uint id);
[DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] public static extern bool QueryFullProcessImageName(IntPtr h,uint flags,StringBuilder b,ref uint n);
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool GetProcessTimes(IntPtr h,out long born,out long exited,out long kernel,out long user);}'
$pipe=[IO.Pipes.NamedPipeClientStream]::new('.','${offer.pipePath.slice(9).replaceAll("'", "''")}',[IO.Pipes.PipeDirection]::InOut,[IO.Pipes.PipeOptions]::Asynchronous)
$p=$null;$processWait=$null;$idle=$null;$inputReader=$null
try {
$pipe.Connect(5000);[uint32]$peer=0
if(![LegacyPeer]::GetNamedPipeServerProcessId($pipe.SafePipeHandle.DangerousGetHandle(),[ref]$peer)){throw 'native pipe peer unavailable'}
$p=[Diagnostics.Process]::GetProcessById($peer);$handle=$p.Handle
[long]$born=0;[long]$exited=0;[long]$kernel=0;[long]$user=0
if(![LegacyPeer]::GetProcessTimes($handle,[ref]$born,[ref]$exited,[ref]$kernel,[ref]$user)){throw 'native birth unavailable'}
$image=[Text.StringBuilder]::new(32768);[uint32]$size=32768
if(![LegacyPeer]::QueryFullProcessImageName($handle,0,$image,[ref]$size)){throw 'native image unavailable'}
$row=Get-CimInstance Win32_Process -Filter "ProcessId=$peer"
if(!$row.CommandLine -or $p.StartTime.ToUniversalTime().ToFileTimeUtc() -ne $born){throw 'native argv or birth changed'}
$writer=[IO.StreamWriter]::new($pipe,[Text.UTF8Encoding]::new($false),4096,$true);$writer.AutoFlush=$true
$reader=[IO.StreamReader]::new($pipe,[Text.UTF8Encoding]::new($false),$false,4096,$true)
$writer.WriteLine('${JSON.stringify(request).replaceAll("'", "''")}')
$custodyMode=${custody ? '$true' : '$false'}
if($custodyMode){
$welcome=$reader.ReadLineAsync();if(!$welcome.Wait(15000) -or !$welcome.Result){throw 'live custody binding unavailable'}
[Console]::WriteLine('OBSERVING '+$welcome.Result)
}
$identity=@{ProcessId=[int]$peer;Birth=$born.ToString();ExecutablePath=$image.ToString();CommandLine=$row.CommandLine;ParentProcessId=[int]$row.ParentProcessId;Handle=$handle.ToInt64().ToString()}
[Console]::WriteLine('ARMED '+(ConvertTo-Json -InputObject $identity -Compress))
if($custodyMode){
$custodyInvalid=$false;$producerDead=$false;$disposing=$false
$processWait=[Threading.ManualResetEvent]::new($false)
$processWait.SafeWaitHandle=[Microsoft.Win32.SafeHandles.SafeWaitHandle]::new($handle,$false)
$idle=[Threading.ManualResetEvent]::new($false)
$inputReader=[IO.StreamReader]::new([Console]::OpenStandardInput(),[Text.UTF8Encoding]::new($false),$false,4096,$true)
$pipeRead=$reader.ReadLineAsync();$inputRead=$inputReader.ReadLineAsync()
while($true){
$pipeHandle=if($pipeRead){([IAsyncResult]$pipeRead).AsyncWaitHandle}else{$idle}
$deathHandle=if($producerDead){$idle}else{$processWait}
$inputHandle=if($disposing){$idle}else{([IAsyncResult]$inputRead).AsyncWaitHandle}
$handles=[Threading.WaitHandle[]]@($pipeHandle,$inputHandle,$deathHandle)
$deadline=if(!$pipeRead -and !$producerDead -and !$custodyInvalid){15000}else{-1}
$selected=[Threading.WaitHandle]::WaitAny($handles,$deadline)
if($selected -eq 258){$custodyInvalid=$true;continue}
if($selected -eq 2){
$producerDead=$true
[Console]::WriteLine('CUSTODY_PRODUCER_DEAD '+(ConvertTo-Json -InputObject @{helperPid=$PID;producerPid=[int]$peer;producerBirth=$born.ToString();nonce='${nonce}';exitCode=$p.ExitCode;custodyInvalid=$custodyInvalid} -Compress))
continue
}
if($selected -eq 0){
$raw=$pipeRead.Result
if(!$raw){
[Console]::WriteLine('CUSTODY_PIPE_EOF '+(ConvertTo-Json -InputObject @{helperPid=$PID;producerPid=[int]$peer;nonce='${nonce}';disposing=$disposing;producerDead=$producerDead;custodyInvalid=$custodyInvalid;pipeConnected=$pipe.IsConnected;inputStatus=[string]$inputRead.Status} -Compress))
if($disposing){if(!$processWait.WaitOne(0)){throw 'unacknowledged custody disposal'};break}
if(!$processWait.WaitOne(0)){$custodyInvalid=$true}
$pipeRead=$null;continue
}
$packet=ConvertFrom-Json $raw
if($packet.type -eq 'custody-effect' -and $packet.nonce -ceq '${nonce}' -and $packet.token -ceq '${offer.token.replaceAll("'", "''")}'){
$custodyInvalid=$true
[Console]::WriteLine('EFFECT '+$raw)
$packet.type='custody-invalidated'
$writer.WriteLine((ConvertTo-Json -InputObject $packet -Compress -Depth 32))
}elseif($disposing -and $packet.type -eq 'custody-disposed' -and $packet.nonce -ceq '${nonce}' -and $packet.token -ceq '${offer.token.replaceAll("'", "''")}'){
break
}elseif($packet.type -ne 'observing' -and $packet.type -ne 'released'){$custodyInvalid=$true}
$pipeRead=$reader.ReadLineAsync();continue
}
$mode=$inputRead.Result
[Console]::WriteLine('CUSTODY_INPUT '+(ConvertTo-Json -InputObject @{helperPid=$PID;producerPid=[int]$peer;nonce='${nonce}';mode=$mode;disposing=$disposing;producerDead=$producerDead;custodyInvalid=$custodyInvalid;inputStatus=[string]$inputRead.Status} -Compress))
if($mode -eq 'custody-check'){
[Console]::WriteLine('CHECK '+(!$custodyInvalid).ToString().ToLowerInvariant())
}
elseif($mode -eq 'death'){if(!$p.WaitForExit(15000)){throw 'producer death deadline'};[Console]::WriteLine('DEAD '+$p.ExitCode)}
elseif($mode -eq 'dispose' -or $null -eq $mode){
$disposed=ConvertFrom-Json '${JSON.stringify(request).replaceAll("'", "''")}';$disposed.type='custody-disposed'
$custodyInvalid=$true;$disposing=$true
if($pipeRead -and $pipe.IsConnected){
[Console]::WriteLine('CUSTODY_DISPOSE_WRITE_ENTER '+(ConvertTo-Json -InputObject @{helperPid=$PID;producerPid=[int]$peer;nonce='${nonce}';disposing=$disposing;pipeConnected=$pipe.IsConnected} -Compress))
if([Environment]::GetEnvironmentVariable('OMO_RESTART_QA_DISPOSE_GATE') -ceq '1'){
$gate=[Threading.EventWaitHandle]::OpenExisting('Local\\omo-restart-qa-dispose-${nonce}')
$gateHandle=$gate.SafeWaitHandle
try{if(!$gate.WaitOne(3000)){throw 'owned disposal gate deadline'}}finally{
$gate.Dispose()
[Console]::WriteLine('CUSTODY_DISPOSE_GATE_CLOSED '+(ConvertTo-Json -InputObject @{helperPid=$PID;nonce='${nonce}';closed=$gateHandle.IsClosed} -Compress))
}
}
try{$writer.WriteLine((ConvertTo-Json -InputObject $disposed -Compress))}
catch [IO.IOException]{
$originalProducerDead=$processWait.WaitOne(0)
[Console]::WriteLine('DISPOSE_PIPE_ERROR '+(ConvertTo-Json -InputObject @{helperPid=$PID;producerPid=[int]$peer;producerBirth=$born.ToString();nonce='${nonce}';originalProducerDead=$originalProducerDead;at=[DateTime]::UtcNow.ToString('o');error=$_.Exception.Message} -Compress))
if(!$originalProducerDead){throw};break
}
}else{if(!$processWait.WaitOne(0)){throw 'unacknowledged custody disposal'};break}
}else{throw 'invalid custody observer action'}
if(!$disposing){$inputRead=$inputReader.ReadLineAsync()}
}
return
}
while($true){
$mode=[Console]::ReadLine()
if($mode -eq 'death'){if(!$p.WaitForExit(15000)){throw 'producer death deadline'};[Console]::WriteLine('DEAD '+$p.ExitCode)}
elseif($mode -eq 'release'){
do {$line=$reader.ReadLineAsync();if(!$line.Wait(15000)){throw 'producer release deadline'};$raw=$line.Result;if(!$raw){throw 'producer release unknown'};$message=ConvertFrom-Json $raw}while($message.type -eq 'observing')
[Console]::WriteLine('RELEASE '+$raw)
}elseif($mode -eq 'dispose' -or $null -eq $mode){break}else{throw 'invalid observer action'}
}
}finally{if($inputReader){$inputReader.Dispose()};if($processWait){$processWait.Dispose()};if($idle){$idle.Dispose()};if($p){$p.Dispose()};$pipe.Dispose()}`;
  const child = process.platform === 'win32' ? spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-EncodedCommand',
    Buffer.from(script, 'utf16le').toString('base64')], { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] })
    : await spawnNativeObserver({ mode: 'capture', pipePath: offer.pipePath, request, nonce, token: offer.token, custody });
  const events = new EventEmitter();
  let stdout = '', stderr = '', buffer = '', alive = true, captured, invalidated = false, disposing;
  const terminal = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.stdout.on('data', chunk => {
      stdout += chunk; buffer += chunk;
      let at;
      while ((at = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, at).trim(); buffer = buffer.slice(at + 1);
        if (line.startsWith('EFFECT ')) { invalidated = true; if (captured) captured.invalid = true; }
        events.emit('line', line);
      }
    });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.once('close', (code, signal) => {
      alive = false;
      const receipt = { code, signal, stdout, stderr, stdoutEOF: child.stdout.readableEnded, stderrEOF: child.stderr.readableEnded };
      events.emit('closed', receipt); resolve(receipt);
    });
  });
  const line = prefix => new Promise((resolve, reject) => {
    const signal = AbortSignal.timeout(20000);
    const accept = value => { if (value.startsWith(prefix + ' ')) { clear(); resolve(value.slice(prefix.length + 1)); } };
    const close = value => { clear(); reject(blocked(`native capture ${value.code}: ${value.stderr}`)); };
    const abort = () => { clear(); reject(blocked('native capture deadline')); };
    const clear = () => { events.off('line', accept); events.off('closed', close); signal.removeEventListener('abort', abort); };
    events.on('line', accept); events.once('closed', close); signal.addEventListener('abort', abort, { once: true });
  });
  const observation = { dispose() {
    if (!disposing) {
      if (alive) child.stdin.end('dispose\n');
      disposing = terminal.then(receipt => ({ ...receipt, pid: child.pid, holder: captured?.holder }));
    }
    return disposing;
  } };
  try {
    const row = JSON.parse(await line('ARMED')), identity = nativeIdentity(row);
    let ancestor = row.ParentProcessId;
    for (let depth = 0; depth < 5 && ancestor !== parent.shell.pid; depth++) {
      const next = (await nativeShellProcesses(ancestor)).find(row => row.ProcessId === ancestor);
      if (!next || next.ParentProcessId === ancestor) throw blocked('legacy producer ancestry unavailable');
      ancestor = next.ParentProcessId;
    }
    if (ancestor !== parent.shell.pid) throw blocked('legacy producer outside exact parent');
    let initialSlot, leaseRoot;
    if (slotPath) {
      leaseRoot = pathKey(await realpath(resolve(slotStateDir)));
      const pipe = record.socketPath.replaceAll('/', '\\');
      const socket = /^\\\\[.]\\pipe\\/i.test(pipe) ? pathKey(pipe) : pathKey(await realpath(resolve(record.socketPath)));
      const key = createHash('sha256').update(JSON.stringify([socket, parent.paneId])).digest('hex');
      if (pathKey(await realpath(slotPath)) !== pathKey(join(leaseRoot, 'parent-slots', key, 'slot.json')))
        throw blocked('legacy effect journal locator mismatch');
      initialSlot = await readJson(slotPath);
      if (!validParentSlot(initialSlot) || initialSlot.canonicalStateDir !== leaseRoot ||
        initialSlot.canonicalSocketPath !== socket || JSON.stringify(initialSlot.parent) !== JSON.stringify(parent) ||
        JSON.stringify(initialSlot.producer) !== JSON.stringify(identity))
        throw blocked('legacy effect journal producer mismatch');
      const role = initialSlot.roles[record.role];
      if (!role || role.paneId !== record.paneId || role.statePath !== record.statePath ||
        role.sessionId !== record.sessionId || role.launchToken !== record.launchToken || role.controlToken !== offer.token ||
        role.shell.pid !== roleIdentity.shell.pid || role.shell.birth !== roleIdentity.shell.birth ||
        JSON.stringify(role.viewer) !== JSON.stringify(roleIdentity.viewer)) initialSlot = null;
    }
    if (!/^[1-9][0-9]*$/.test(row.Handle)) throw blocked('original native handle unavailable');
    const holderRow = (await nativeShellProcesses(child.pid)).find(row => row.ProcessId === child.pid);
    if (holderRow?.ParentProcessId !== process.pid) throw blocked('original native holder parent changed');
    if (custody) {
      const raw = stdout.split(/\r?\n/).find(value => value.startsWith('OBSERVING '));
      const witnessed = raw && JSON.parse(raw.slice('OBSERVING '.length));
      if (!sameControl(witnessed, offer) || witnessed.type !== 'observing' || witnessed.nonce !== nonce ||
        !initialSlot || JSON.stringify(witnessed.slot) !== JSON.stringify(initialSlot) ||
        initialSlot.operations.some(operation => operation.status !== 'SETTLED'))
        throw blocked('live original custody journal binding changed');
    }
    captured = { root, path, statePath, recordBytes, stateBytes, parent: structuredClone(parent), herdr, roleIdentity,
      identity, slotPath, initialSlot, leaseRoot, nonce, offer, alive: () => alive, released: null, death: null, child, terminal,
      nativeHandle: row.Handle, holder: nativeIdentity(holderRow), invalid: invalidated };
    observation.recordPath = path;
    observation.identity = structuredClone(identity);
    // Diagnostic copies cannot construct or transfer the private observation.
    observation.bindings = structuredClone({ root, path, statePath, parent, roleIdentity, state, initialSlot });
    if (custody) observation.custodyValid = async () => {
      if (!alive || captured.invalid) return false;
      const received = line('CHECK'); child.stdin.write('custody-check\n');
      return await received === 'true';
    };
    observation.death = async () => {
      const received = line('DEAD');
      child.stdin.write('death\n');
      captured.death = { identity, exitCode: Number(await received) };
      if (!Number.isInteger(captured.death.exitCode)) throw blocked('native numeric exit unavailable');
      return structuredClone(captured.death);
    };
    observation.release = async () => {
      const received = line('RELEASE');
      child.stdin.write('release\n');
      const receipt = JSON.parse(await received);
      if (receipt.type !== 'released' || receipt.nonce !== nonce || !sameControl(receipt, offer) ||
        !validParentSlot(receipt.slot) || receipt.slot.phase !== 'RELEASED' ||
        JSON.stringify(receipt.slot.producer) !== JSON.stringify(identity) ||
        receipt.recordSHA256 !== createHash('sha256').update(recordBytes).digest('hex'))
        throw blocked('producer release binding mismatch');
      captured.released = receipt;
      return receipt;
    };
    retirementCaptures.set(observation, captured);
    return observation;
  } catch (error) { await observation.dispose(); throw error; }
}
export async function observeLegacyRetirement(observation, { stateDir, recordPath, parent }) {
  const captured = retirementCaptures.get(observation);
  if (!captured?.alive() || captured.invalid) return null;
  const invalid = () => { captured.invalid = true; return null; };
  if (pathKey(await realpath(stateDir)) !== captured.root ||
    pathKey(await realpath(recordPath)) !== pathKey(captured.path) ||
    JSON.stringify(parent) !== JSON.stringify(captured.parent) ||
    !Buffer.from(await readFile(captured.path)).equals(captured.recordBytes)) return invalid();
  const record = JSON.parse(captured.recordBytes);
  const members = (await captured.herdr('list')).panes?.filter(pane => pane.pane_id === parent.paneId);
  if (members?.length !== 1 || members[0].tab_id !== parent.tabId) return invalid();
  const parentInfo = await captured.herdr('process-info', '--pane', parent.paneId);
  if (parentInfo.process_info?.shell_pid !== parent.shell.pid) return invalid();
  const parentRow = (await nativeShellProcesses(parent.shell.pid)).find(row => row.ProcessId === parent.shell.pid);
  if (nativeIdentity(parentRow).birth !== parent.shell.birth) return invalid();
  const role = await legacyPaneIdentity(await captured.herdr('process-info', '--pane', record.paneId), record);
  if (JSON.stringify(role) !== JSON.stringify(captured.roleIdentity)) return invalid();
  if (captured.released) {
    const bytes = await readFile(captured.statePath);
    if (createHash('sha256').update(bytes).digest('hex') !== captured.released.stateSHA256) return invalid();
    return { identity: structuredClone(captured.identity), state: 'released', effects: 'settled' };
  }
  if (!captured.death || !captured.initialSlot) return null;
  const beforeState = JSON.parse(captured.stateBytes), afterState = JSON.parse(await readFile(captured.statePath));
  delete beforeState.updatedAt; delete afterState.updatedAt;
  if (JSON.stringify(afterState) !== JSON.stringify(beforeState)) return invalid();
  const slot = await readJson(captured.slotPath);
  if (!validParentSlot(slot) || JSON.stringify(slot.producer) !== JSON.stringify(captured.identity) ||
    JSON.stringify(slot.parent) !== JSON.stringify(parent) || slot.canonicalStateDir !== captured.leaseRoot ||
    slot.canonicalSocketPath !== captured.initialSlot.canonicalSocketPath ||
    slot.ownerNonce !== captured.initialSlot.ownerNonce || slot.generation !== captured.initialSlot.generation ||
    slot.revision < captured.initialSlot.revision ||
    JSON.stringify(slot.locatorOwners.slice(0, captured.initialSlot.locatorOwners.length)) !== JSON.stringify(captured.initialSlot.locatorOwners) ||
    captured.initialSlot.operations.some((op, index) =>
      JSON.stringify({ ...op, status: 'SETTLED' }) !== JSON.stringify(slot.operations[index])) ||
    slot.operations.some(op => op.status !== 'SETTLED')) return null;
  return { identity: structuredClone(captured.identity), state: 'dead', effects: 'settled',
    exitCode: captured.death.exitCode };
}
const custodyPipe = launchToken =>
  ipcPath(`omo-herdr-producer-custody-${createHash('sha256').update(launchToken).digest('hex')}`);

export async function createProducerCustodyServer(options, getObservation) {
  const sockets = new Set();
  const server = net.createServer(socket => {
    sockets.add(socket); socket.on('error', () => {});
    socket.once('close', () => sockets.delete(socket));
    receive(socket, message => {
      void (async () => {
        if (!['custody', 'custody-check'].includes(message?.type) || typeof message.nonce !== 'string' || !message.nonce ||
          message.launchToken !== options.launchToken || message.paneId !== options.paneId ||
          message.statePath !== options.statePath) throw blocked('custody request binding changed');
        const captured = retirementCaptures.get(getObservation());
        if (!captured?.alive() || captured.invalid || !captured.initialSlot ||
          !await getObservation().custodyValid?.() || captured.initialSlot.operations.some(op => op.status !== 'SETTLED'))
          throw blocked('original custody or settled effects unavailable');
        const slot = await readJson(captured.slotPath);
        if (!validParentSlot(slot) || slot.operations.some(op => op.status !== 'SETTLED') ||
          slot.ownerNonce !== captured.initialSlot.ownerNonce || slot.generation !== captured.initialSlot.generation ||
          JSON.stringify(slot.producer) !== JSON.stringify(captured.identity) ||
          ['parent', 'roles', 'operations', 'closures', 'locatorOwners'].some(key =>
            JSON.stringify(slot[key]) !== JSON.stringify(captured.initialSlot[key])))
          throw blocked('effects changed after original custody capture');
        if (!Buffer.from(await readFile(captured.path)).equals(captured.recordBytes))
          throw blocked('custodian role record changed');
        const state = JSON.parse(await readFile(captured.statePath)), original = JSON.parse(captured.stateBytes);
        delete state.updatedAt; delete original.updatedAt;
        if (JSON.stringify(state) !== JSON.stringify(original)) throw blocked('custodian role state changed');
        const rows = await nativeShellProcesses(captured.child.pid);
        const holder = rows.find(row => row.ProcessId === captured.child.pid);
        if (holder?.ParentProcessId !== process.pid || JSON.stringify(nativeIdentity(holder)) !== JSON.stringify(captured.holder))
          throw blocked('original custodian holder changed');
        if (message.type === 'custody-check') {
          if (message.nativeHandle !== captured.nativeHandle ||
            JSON.stringify(message.holder) !== JSON.stringify(captured.holder))
            throw blocked('original custody holder changed after transfer');
          send(socket, { type: 'custody-valid', nonce: message.nonce, launchToken: options.launchToken });
          return;
        }
        socket.write(`${JSON.stringify({ type: 'custody', nonce: message.nonce, launchToken: options.launchToken,
          identity: captured.identity, parent: captured.parent, initialSlot: captured.initialSlot,
          recordBase64: captured.recordBytes.toString('base64'), stateBase64: captured.stateBytes.toString('base64'),
          root: captured.root, path: captured.path, statePath: captured.statePath, roleIdentity: captured.roleIdentity,
          slotPath: captured.slotPath, leaseRoot: captured.leaseRoot,
          holder: captured.holder, nativeHandle: captured.nativeHandle })}\n`);
      })().catch(() => socket.destroy());
    });
  });
  await clearStaleIpc(custodyPipe(options.launchToken));
  await new Promise((resolve, reject) => {
    server.once('error', reject); server.once('listening', resolve);
    server.listen(custodyPipe(options.launchToken));
  });
  return async () => {
    for (const socket of sockets) socket.destroy();
    if (server.listening) await new Promise(resolve => server.close(resolve));
  };
}

export async function captureCustodiedProducer({ stateDir, recordPath, parent, herdr, slotPath, previous }) {
  const root = pathKey(await realpath(resolve(stateDir))), path = await realpath(recordPath);
  if (!pathKey(path).startsWith(root + '/')) throw blocked('custodian locator leaves configured root');
  const recordBytes = await readFile(path), record = JSON.parse(recordBytes);
  const statePath = await realpath(record.statePath), stateBytes = await readFile(statePath);
  if (!pathKey(statePath).startsWith(root + '/') || !validParentSlot(previous) ||
    JSON.stringify(previous.parent) !== JSON.stringify(parent)) throw blocked('custodian parent or state root changed');
  const roleIdentity = await legacyPaneIdentity(await herdr('process-info', '--pane', record.paneId), record);
  const role = previous.roles.todo;
  if (!roleIdentity?.viewer || record.role !== 'todo' || record.manualClose ||
    role?.paneId !== record.paneId || role.statePath !== record.statePath || role.launchToken !== record.launchToken ||
    role.shell.pid !== roleIdentity.shell.pid || role.shell.birth !== roleIdentity.shell.birth ||
    JSON.stringify(role.viewer) !== JSON.stringify(roleIdentity.viewer))
    throw blocked('surviving original custodian native identity unavailable');
  const nonce = randomUUID(), literal = value => "'" + String(value).replaceAll("'", "''") + "'";
  const request = { type: 'custody', nonce, launchToken: record.launchToken,
    paneId: record.paneId, statePath: record.statePath };
  const viewer = roleIdentity.viewer, producer = previous.producer;
  const script = `$ErrorActionPreference='Stop'
Add-Type -TypeDefinition 'using System;using System.Text;using System.Runtime.InteropServices;public static class CustodyPeer {
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool GetNamedPipeServerProcessId(IntPtr h,out uint id);
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool GetProcessTimes(IntPtr h,out long born,out long exited,out long kernel,out long user);
[DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)] public static extern bool QueryFullProcessImageName(IntPtr h,uint flags,StringBuilder b,ref uint n);
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool DuplicateHandle(IntPtr source,IntPtr handle,IntPtr target,out IntPtr duplicate,uint access,bool inherit,uint options);
[DllImport("kernel32.dll")] public static extern IntPtr GetCurrentProcess();
[DllImport("kernel32.dll",SetLastError=true)] public static extern uint GetProcessId(IntPtr h);
[DllImport("kernel32.dll",SetLastError=true)] public static extern uint WaitForSingleObject(IntPtr h,uint ms);
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool GetExitCodeProcess(IntPtr h,out uint code);
[DllImport("kernel32.dll",SetLastError=true)] public static extern bool CloseHandle(IntPtr h);}'
function Verify([Diagnostics.Process]$p,[string]$birth,[string]$image,[string]$argv) {
[long]$born=0;[long]$exited=0;[long]$kernel=0;[long]$user=0
if(![CustodyPeer]::GetProcessTimes($p.Handle,[ref]$born,[ref]$exited,[ref]$kernel,[ref]$user) -or $born.ToString() -cne $birth){throw 'live custody peer birth changed'}
$actual=[Text.StringBuilder]::new(32768);[uint32]$size=32768
if(![CustodyPeer]::QueryFullProcessImageName($p.Handle,0,$actual,[ref]$size) -or $actual.ToString() -ine $image){throw 'live custody peer image changed'}
$row=Get-CimInstance Win32_Process -Filter "ProcessId=$($p.Id)"
$hash=[Security.Cryptography.SHA256]::Create()
try{$digest=([BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($row.CommandLine)))).Replace('-','').ToLowerInvariant()}finally{$hash.Dispose()}
if($digest -cne $argv){throw 'live custody peer argv changed'}
return $row
}
$pipe=[IO.Pipes.NamedPipeClientStream]::new('.',${literal(custodyPipe(record.launchToken).slice(9))},[IO.Pipes.PipeDirection]::InOut)
$v=$null;$holder=$null;$duplicate=[IntPtr]::Zero
try{
$pipe.Connect(5000);[uint32]$peer=0
if(![CustodyPeer]::GetNamedPipeServerProcessId($pipe.SafePipeHandle.DangerousGetHandle(),[ref]$peer) -or $peer -ne ${viewer.pid}){throw 'custodian native peer changed'}
$v=[Diagnostics.Process]::GetProcessById($peer)
$row=Verify $v ${literal(viewer.birth)} ${literal(viewer.executable)} ${literal(viewer.argvDigest)}
if($row.ParentProcessId -ne ${roleIdentity.shell.pid}){throw 'custodian native shell changed'}
$writer=[IO.StreamWriter]::new($pipe,[Text.UTF8Encoding]::new($false),4096,$true);$writer.AutoFlush=$true
$reader=[IO.StreamReader]::new($pipe,[Text.UTF8Encoding]::new($false),$false,4096,$true)
$writer.WriteLine(${literal(JSON.stringify(request))})
$line=$reader.ReadLineAsync();if(!$line.Wait(15000) -or !$line.Result){throw 'live custody response unavailable'}
$capture=ConvertFrom-Json $line.Result
if($capture.type -cne 'custody' -or $capture.nonce -cne ${literal(nonce)} -or $capture.launchToken -cne ${literal(record.launchToken)}){throw 'live custody response changed'}
$holder=[Diagnostics.Process]::GetProcessById([int]$capture.holder.pid)
$holderRow=Verify $holder $capture.holder.birth $capture.holder.executable $capture.holder.argvDigest
if($holderRow.ParentProcessId -ne $peer){throw 'original holder no longer belongs to verified custodian'}
if($capture.nativeHandle -notmatch '^[1-9][0-9]*$' -or ![CustodyPeer]::DuplicateHandle($holder.Handle,[IntPtr]::new([long]$capture.nativeHandle),[CustodyPeer]::GetCurrentProcess(),[ref]$duplicate,0x101000,$false,0)){throw 'original handle duplication unavailable'}
[long]$born=0;[long]$exited=0;[long]$kernel=0;[long]$user=0
if([CustodyPeer]::GetProcessId($duplicate) -ne ${producer.pid} -or ![CustodyPeer]::GetProcessTimes($duplicate,[ref]$born,[ref]$exited,[ref]$kernel,[ref]$user) -or $born.ToString() -cne ${literal(producer.birth)}){throw 'duplicated original producer identity changed'}
[Console]::WriteLine('CAPTURE '+$line.Result)
while($true){
$mode=[Console]::ReadLine()
if($mode -eq 'death'){
if([CustodyPeer]::WaitForSingleObject($duplicate,15000) -ne 0){throw 'original producer death unavailable'}
[uint32]$code=0;if(![CustodyPeer]::GetExitCodeProcess($duplicate,[ref]$code)){throw 'original producer numeric exit unavailable'}
$check=ConvertFrom-Json ${literal(JSON.stringify(request))};$check.type='custody-check'
$check | Add-Member -NotePropertyName holder -NotePropertyValue $capture.holder
$check | Add-Member -NotePropertyName nativeHandle -NotePropertyValue $capture.nativeHandle
$writer.WriteLine((ConvertTo-Json -InputObject $check -Compress -Depth 8))
$valid=$reader.ReadLineAsync();if(!$valid.Wait(15000) -or !$valid.Result){throw 'original custody effect validity unavailable'}
$proof=ConvertFrom-Json $valid.Result
if($proof.type -cne 'custody-valid' -or $proof.nonce -cne ${literal(nonce)} -or $proof.launchToken -cne ${literal(record.launchToken)}){throw 'original custody effect validity changed'}
[Console]::WriteLine('DEAD '+$code)
}elseif($mode -eq 'dispose' -or $null -eq $mode){break}else{throw 'invalid custody observer action'}
}
}finally{if($duplicate -ne [IntPtr]::Zero){$null=[CustodyPeer]::CloseHandle($duplicate)};if($holder){$holder.Dispose()};if($v){$v.Dispose()};$pipe.Dispose()}`;
  const child = process.platform === 'win32' ? spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-EncodedCommand',
    Buffer.from(script, 'utf16le').toString('base64')], { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] })
    : await spawnNativeObserver({ mode: 'custodied', pipePath: custodyPipe(record.launchToken), viewer,
      shellPid: roleIdentity.shell.pid, request, nonce, launchToken: record.launchToken, producer });
  const events = new EventEmitter();
  let stdout = '', stderr = '', buffer = '', alive = true;
  const terminal = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.stdout.on('data', chunk => {
      stdout += chunk; buffer += chunk;
      let at;
      while ((at = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, at).trim(); buffer = buffer.slice(at + 1); events.emit('line', line);
      }
    });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.once('close', (code, signal) => {
      alive = false;
      const receipt = { code, signal, stdout, stderr, stdoutEOF: child.stdout.readableEnded, stderrEOF: child.stderr.readableEnded };
      events.emit('closed', receipt); resolve(receipt);
    });
  });
  const line = prefix => new Promise((resolve, reject) => {
    const signal = AbortSignal.timeout(20000);
    const clear = () => { events.off('line', accept); events.off('closed', close); signal.removeEventListener('abort', abort); };
    const accept = value => { if (value.startsWith(prefix + ' ')) { clear(); resolve(value.slice(prefix.length + 1)); } };
    const close = receipt => { clear(); reject(blocked(`native custody ${receipt.code}: ${receipt.stderr}`)); };
    const abort = () => { clear(); reject(blocked('native custody deadline')); };
    events.on('line', accept); events.once('closed', close); signal.addEventListener('abort', abort, { once: true });
  });
  const observation = { async dispose() { if (alive) child.stdin.end('dispose\n'); return terminal; } };
  try {
    const capture = JSON.parse(await line('CAPTURE'));
    if (capture.root !== root || pathKey(capture.path) !== pathKey(path) ||
      pathKey(capture.statePath) !== pathKey(statePath) || pathKey(capture.slotPath) !== pathKey(slotPath) ||
      JSON.stringify(capture.parent) !== JSON.stringify(parent) ||
      JSON.stringify(capture.identity) !== JSON.stringify(producer) ||
      JSON.stringify(capture.roleIdentity) !== JSON.stringify(roleIdentity) ||
      !validParentSlot(capture.initialSlot) ||
      capture.initialSlot.ownerNonce !== previous.ownerNonce || capture.initialSlot.generation !== previous.generation ||
      JSON.stringify(capture.initialSlot.operations) !== JSON.stringify(previous.operations) ||
      capture.initialSlot.operations.some(op => op.status !== 'SETTLED') ||
      !Buffer.from(capture.recordBase64, 'base64').equals(recordBytes) ||
      !Buffer.from(capture.stateBase64, 'base64').equals(stateBytes))
      throw blocked('original live custody provenance changed');
    const captured = { root, path, statePath, recordBytes, stateBytes, parent: structuredClone(parent), herdr,
      roleIdentity, identity: producer, slotPath, initialSlot: capture.initialSlot, leaseRoot: capture.leaseRoot,
      alive: () => alive, released: null, death: null, child, terminal };
    observation.recordPath = path;
    observation.identity = structuredClone(producer);
    observation.death = async () => {
      const received = line('DEAD'); child.stdin.write('death\n');
      const exitCode = Number(await received);
      if (!Number.isInteger(exitCode)) throw blocked('original native numeric exit unavailable');
      captured.death = { identity: producer, exitCode };
      return structuredClone(captured.death);
    };
    retirementCaptures.set(observation, captured);
    return observation;
  } catch (error) { await observation.dispose(); throw error; }
}
// POSIX form of the observer below. Without a process handle the exact
// (pid, birth) instance is armed by a live check and polled until it vanishes.
async function observePosixExit(identity, held) {
  if (!await processAlive(identity)) throw blocked('native exit observer: birth changed');
  let requested, choose;
  const chosen = held ? new Promise(resolve => { choose = resolve; }) : Promise.resolve('death');
  const receipt = (stdout, exitCode) => ({ exitCode, disposed: exitCode === null, stdout, stderr: '', code: 0,
    pid: process.pid, stdoutEOF: true, stderrEOF: true });
  const effects = chosen.then(async mode => {
    if (mode === 'dispose') return receipt('ARMED\nDISPOSED\n', null);
    await waitForProcessExit(identity, 15000).catch(() => { throw blocked('native exit observer 1: native effect deadline'); });
    return receipt(`ARMED\n${UNOBSERVABLE_EXIT_CODE}\n`, UNOBSERVABLE_EXIT_CODE);
  });
  effects.catch(() => {});
  return { effects,
    ...(held ? {
      death() { if (!requested) { requested = 'death'; choose('death'); } return effects; },
      dispose() { if (!requested) { requested = 'dispose'; choose('dispose'); } return effects; },
    } : {}) };
}
async function observeNativeExit(identity, held = false) {
  if (process.platform !== 'win32') return observePosixExit(identity, held);
  const script = `$ErrorActionPreference='Stop';$p=[Diagnostics.Process]::GetProcessById(${identity.pid});try{
    $null=$p.Handle;if($p.StartTime.ToUniversalTime().ToFileTimeUtc().ToString() -cne '${identity.birth.replaceAll("'", "''")}'){throw 'birth changed'}
    [Console]::WriteLine('ARMED')
    ${held ? "$mode=[Console]::ReadLine();if($mode -eq 'dispose' -or $null -eq $mode){[Console]::WriteLine('DISPOSED');return};if($mode -ne 'death'){throw 'invalid held observer action'}" : ''}
    if(!$p.WaitForExit(15000)){throw 'native effect deadline'}
    [Console]::WriteLine($p.ExitCode)
  }finally{$p.Dispose()}`;
  const child = spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-EncodedCommand',
    Buffer.from(script, 'utf16le').toString('base64')], { windowsHide: true, stdio: [held ? 'pipe' : 'ignore', 'pipe', 'pipe'] });
  let stdout = '', stderr = '', armed;
  const subscribed = new Promise((resolve, reject) => { armed = { resolve, reject }; });
  const effects = new Promise((resolve, reject) => {
    child.stdout.on('data', chunk => { stdout += chunk; if (/^ARMED\r?$/m.test(stdout)) armed.resolve(); });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.once('error', error => { armed.reject(error); reject(error); });
    child.once('close', code => {
      if (code !== 0 || !child.stdout.readableEnded || !child.stderr.readableEnded) {
        const error = blocked(`native exit observer ${code}: ${stdout}\n${stderr}`);
        armed.reject(error); reject(error);
      } else {
        const last = stdout.trim().split(/\r?\n/).at(-1);
        resolve({ exitCode: last === 'DISPOSED' ? null : Number(last), disposed: last === 'DISPOSED',
          stdout, stderr, code, pid: child.pid, stdoutEOF: true, stderrEOF: true });
      }
    });
  });
  effects.catch(() => {});
  await subscribed;
  let requested;
  return { effects,
    ...(held ? {
      death() { if (!requested) { requested = 'death'; child.stdin.end('death\n'); } return effects; },
      dispose() { if (!requested) { requested = 'dispose'; child.stdin.end('dispose\n'); } return effects; },
    } : {}) };
}
export async function observePaneShell(info, record, inspect = nativeShellProcesses) {
  const pid = info?.process_info?.shell_pid;
  if (!Number.isSafeInteger(pid) || pid < 1 ||
    pid !== record.creationProcessId || info.process_info.pane_id && info.process_info.pane_id !== record.paneId) return null;
  let rows;
  try { rows = await inspect(pid); } catch { return null; }
  if (!Array.isArray(rows) || rows.filter(row => row.ProcessId === pid).length !== 1 ||
    new Set(rows.map(row => row.ProcessId)).size !== rows.length) return null;
  const shell = rows.find(row => row.ProcessId === pid), born = Date.parse(shell.CreationDate);
  if (!Number.isFinite(born) || !shell.ExecutablePath || !shell.CommandLine ||
    record.creationDate && shell.CreationDate !== record.creationDate) return null;
  const reported = info.process_info.foreground_processes?.find(row => row.pid === pid);
  if (reported && pathKey(reported.argv?.[0]) !== pathKey(shell.ExecutablePath)) return null;
  const children = rows.filter(row => row !== shell);
  if (children.some(child => !Number.isSafeInteger(child.ProcessId) || child.ProcessId < 1 ||
    child.ParentProcessId !== pid || !child.ExecutablePath || !child.CommandLine ||
    !Number.isFinite(Date.parse(child.CreationDate)) || Date.parse(child.CreationDate) < born)) return null;
  return { shell, children };
}
// POSIX Herdr lists the whole foreground process group, which includes the
// viewer's own short-lived children (ps, herdr). Only the shell and its direct
// foreground job are pane identity; their reported argv is exact, unlike a
// space-joined command line.
function paneForeground(info, observation) {
  const foreground = info.process_info.foreground_processes ?? [];
  if (process.platform === 'win32') return foreground;
  const own = new Set([observation.shell.ProcessId, ...observation.children.map(child => child.ProcessId)]);
  return foreground.filter(row => own.has(row.pid));
}
function childArgv(child, foreground) {
  const reported = process.platform !== 'win32' && foreground.find(row => row.pid === child.ProcessId)?.argv;
  return Array.isArray(reported) && reported.length && reported.join(' ') === child.CommandLine
    ? [...reported] : parseLaunchCommand(child.CommandLine);
}
export async function legacyPaneIdentity(info, record, inspect) {
  if (!record.creationDate) return null;
  const observation = await observePaneShell(info, record, inspect);
  if (!observation || !observation.shell.Birth || record.creationBirth && observation.shell.Birth !== record.creationBirth ||
    observation.children.length > 1) return null;
  const foreground = paneForeground(info, observation);
  if (foreground.length !== 1 || ![record.creationProcessId, record.processId].includes(foreground[0].pid)) return null;
  const actual = { shell: nativeIdentity(observation.shell) };
  if (!observation.children.length) return record.prepared === true && !record.processId ? actual : null;
  const child = observation.children[0], argv = childArgv(child, foreground);
  const expected = [record.nodePath, record.viewerPath,
    ...(record.role && record.role !== 'todo' ? ['--view', record.role === 'dag' ? 'dag' : 'tasks'] : []),
    '--state', record.statePath, '--close-pane', record.paneId, '--launch-token', record.launchToken];
  if (!child.Birth || record.viewerBirth && child.Birth !== record.viewerBirth ||
    child.ProcessId !== record.processId || pathKey(child.ExecutablePath) !== pathKey(record.nodePath) ||
    !argv || argv.length !== expected.length || argv.some((value, index) =>
      [0, 1, expected.indexOf('--state') + 1].includes(index) ? pathKey(value) !== pathKey(expected[index]) : value !== expected[index])) return null;
  actual.viewer = nativeIdentity(child);
  return actual;
}
export async function paneViewerProcess(info, record, expectedPid, inspect) {
  const foreground = viewerProcess(info, record, expectedPid);
  if (foreground) return foreground;
  const shell = info?.process_info?.shell_pid, pid = expectedPid ?? record.processId;
  // Windows Herdr reports the PowerShell shell, not its foreground Node child
  // (POSIX Herdr can lag the same way). Only the shell returned for this exact
  // split can anchor a native PID query.
  if (!Number.isSafeInteger(pid) || pid < 1 ||
    !record.creationDate || !Number.isSafeInteger(record.creationProcessId) || shell !== record.creationProcessId ||
    info.process_info.foreground_processes?.length !== 1 ||
    info.process_info.foreground_processes[0].pid !== shell) return null;
  const observation = await observePaneShell(info, record, inspect);
  if (!observation || observation.children.length !== 1) return null;
  const child = observation.children[0];
  if (child.ProcessId !== pid || record.viewerBirth && child.Birth !== record.viewerBirth ||
    pathKey(child.ExecutablePath) !== pathKey(record.nodePath)) return null;
  const argv = parseLaunchCommand(child.CommandLine);
  if (!argv) return null;
  return viewerProcess({ process_info: { foreground_processes: [{ pid, argv }] } }, record, pid);
}
export async function canCleanupPane(info, record, inspect) {
  if (info?.process_info?.shell_pid !== record.creationProcessId) return false;
  if (!record.creationDate) return false;
  const observation = await observePaneShell(info, record, inspect);
  if (!observation) return false;
  if (!observation.children.length) return true;
  if (observation.children.length !== 1) return false;
  const child = observation.children[0], argv = childArgv(child, paneForeground(info, observation));
  return pathKey(child.ExecutablePath) === pathKey(record.nodePath) && argv !== null &&
    Boolean(viewerProcess({ process_info: { foreground_processes: [{ pid: child.ProcessId, argv }] } }, record));
}

export class TodoPane {
  constructor({ ctx, environment = process.env, stateDir, cwd, node = process.execPath,
    viewer, herdr = createDashboardHerdr(environment), inspectShell, language = 'en', notify = () => {}, managed = false,
    retirementObservations = new Map(), retireViewer = retireVerifiedViewer, cacheReads = false, clock,
    readCoordinationRecord, readCoordinationProof }) {
    Object.assign(this, { ctx, environment, stateDir, cwd, node, viewer, herdr: cacheReads ? herdrRead(herdr) : herdr, inspectShell, language, notify, managed, retireViewer, clock });
    this.binding = { socketPath: environment.HERDR_SOCKET_PATH, parentPaneId: environment.HERDR_PANE_ID,
      sessionId: ctx.sessionManager.getSessionId() };
    this.binding.ownerKey = displayOwnerKey(this.binding.socketPath, this.binding.parentPaneId, this.binding.sessionId);
    this.directory = join(stateDir, this.binding.ownerKey);
    this.stateFile = join(this.directory, 'todo.json');
    this.recordFile = join(this.directory, 'todo.pane.json');
    this.lifetimeId = randomUUID();
    this.events = new EventEmitter();
    this.tail = Promise.resolve();
    this.roleTail = Promise.resolve();
    this.controlJobs = new Set();
    this.stopSignal = new AbortController();
    this.stopped = false;
    this.provider = null;
    this.source = null;
    this.ownedRoles = new Map();
    this.unsubscribers = [];
    this.controlToken = randomBytes(32).toString('hex');
    this.controlPath = ipcPath(`omo-herdr-todo-control-${randomUUID()}`);
    this.presencePath = ipcPath(`omo-herdr-todo-${randomUUID()}`);
    this.controlSockets = new Set();
    this.status = 'waiting';
    this.managedTail = Promise.resolve();
    this.pendingLaunches = new Set();
    this.launchRegistrations = new Map();
    this.effectObservations = new Map();
    this.viewerHolders = new Map();
    this.viewerDeaths = new Map();
    this.legacyObservations = retirementObservations;
    this.retirementSubscribers = new Map();
    this.custodyLost = false;
    this.warned = new Map();
    this.unavailableDiagnosed = false;
    this.retainedTargets = new Map();
    this.closureVersions = new Map();
    this.lastPanes = null;
    this.readCoordinationRecord = readCoordinationRecord;
    this.readCoordinationProof = readCoordinationProof;
    this.recovery = createRecoveryCoordinator({
      publish: message => this.pi?.events?.emit(RECOVERY_V1.layout, message),
    });
  }
  assertManaged() {
    if (this.stopped || this.managedBlocked || this.custodyLost || !this.actor?.leaseHeld || this.actor.draining || this.actor.blocked) {
      this.status = 'BLOCKED';
      this.bridge?.readinessChanged();
      throw blocked(managedUnavailable, 'MANAGED_UNAVAILABLE');
    }
  }
  managedWrite(job) {
    const actor = this.actor, lifetime = this.lifetimeId;
    const result = this.managedTail.then(async () => {
      this.assertManaged();
      this.unavailableDiagnosed = false;
      if (actor !== this.actor || lifetime !== this.lifetimeId) throw blocked('stale publisher');
      return job(actor);
    }).catch(error => {
      // A layout wait made no mutation and is not a recovery block (IS-10, G4).
      if (error?.code === 'LAYOUT_WAIT') throw error;
      // Only real unavailability latches recovery. A rejected stale ACK wrote
      // nothing, and a healthy actor's write error belongs to its caller.
      const unavailable = error?.code === 'MANAGED_UNAVAILABLE' || !actor?.leaseHeld || actor.blocked || actor.draining;
      if (this.actor === actor) {
        if (unavailable && error?.code !== 'ACK_MISMATCH') {
          this.status = 'BLOCKED';
          this.recoveryBlocked = true;
          void this.watchRecoveryMembership().catch(() => {});
        }
        this.bridge?.readinessChanged();
      }
      // One unavailable actor, one line: writes queued behind it add nothing.
      if (!unavailable || !this.unavailableDiagnosed) void actor?.diagnose('controller-write', error);
      if (unavailable) this.unavailableDiagnosed = true;
      throw error;
    });
    this.managedTail = result.catch(() => {});
    return result;
  }
  // 11-i: a READY phase is not authority. Completion must also observe, at
  // verdict time, that the publisher lease and custody are still live; a
  // snapshot held across an awaited read cannot outlive the authority it
  // described. Returns the named failure or null when authority is current.
  completionAuthority() {
    if (!this.managed) return null;
    if (this.stopped) return { condition: 'stopped', detail: 'stopped: the controller is stopped' };
    if (this.managedBlocked)
      return { condition: 'managed-blocked', detail: 'managed-blocked: managed recovery is blocked' };
    if (this.custodyLost)
      return { condition: 'custody-lost', detail: 'custody-lost: the publisher lost original custody' };
    if (!this.actor?.leaseHeld)
      return { condition: 'lease-lost', detail: 'lease-lost: the publisher lease is not held' };
    if (this.actor.draining)
      return { condition: 'lease-draining', detail: 'lease-draining: the publisher lease is draining' };
    if (this.actor.blocked)
      return { condition: 'lease-blocked', detail: 'lease-blocked: the publisher lease is blocked' };
    return null;
  }
  async managedIdle() { await this.managedTail; }
  async nativePane(paneId, record) {
    const member = await this.membership(paneId);
    const info = await this.herdr('process-info', '--pane', paneId);
    const pid = info?.process_info?.shell_pid;
    const rows = await (this.inspectShell ?? nativeShellProcesses)(pid);
    const shell = rows.filter(row => row.ProcessId === pid);
    if (shell.length !== 1 || record && (record.creationProcessId !== pid ||
      record.creationDate !== shell[0].CreationDate || record.creationBirth && record.creationBirth !== shell[0].Birth)) {
      throw blocked('parent or role shell changed');
    }
    const actual = { paneId: member.pane.pane_id, tabId: member.tabId,
      shell: { pid, birth: nativeIdentity(shell[0]).birth } };
    if (!record && paneId !== this.binding.parentPaneId && rows.some(row => row.ParentProcessId === pid)) {
      throw blocked('unregistered target has a foreground child');
    }
    if (record) {
      const state = await readRoleState(this.stateFile);
      if (!state || state.scopeEpoch !== this.scopeEpoch || state.launchToken !== record.launchToken ||
        !sameDashboardOwner(state, this.binding)) throw blocked('role source changed');
      Object.assign(actual, { statePath: this.stateFile, sourcePath: this.ctx.sessionManager.getSessionFile(),
        cachePath: state.cachePath, prefsPath: state.prefsPath, launchToken: state.launchToken,
        controlToken: state.controlOffer?.token ?? this.controlToken });
      if (record.processId) {
        const children = rows.filter(row => row.ParentProcessId === pid);
        if (children.length !== 1 || children[0].ProcessId !== record.processId ||
          record.viewerBirth && children[0].Birth !== record.viewerBirth ||
          !await paneViewerProcess(info, record, record.processId, async () => rows)) throw blocked('role viewer changed');
        actual.viewer = nativeIdentity(children[0]);
      } else if (rows.some(row => row.ParentProcessId === pid)) throw blocked('launch target is occupied');
    }
    return actual;
  }
  async currentTarget(paneId) {
    const retained = this.retainedTargets.get(paneId);
    if (retained) return this.observeRetainedTarget(retained);
    const role = Object.values(this.actor.snapshot.roles).find(role => role.paneId === paneId);
    if (!role) return this.nativePane(paneId);
    return role.role === 'todo' ? this.nativePane(paneId, await readJson(this.recordFile)) : this.provider.observeTarget(role);
  }
  async observePreviousProducer(identity) {
    // A successful exact native process inventory is authoritative: absence or
    // a different immutable kernel birth proves this recorded instance is gone.
    // Query failures, malformed rows and a live matching birth are not death.
    const rows = await (this.inspectShell ?? nativeShellProcesses)(identity.pid);
    if (!Array.isArray(rows)) throw blocked('previous native process inventory unavailable');
    const matches = rows.filter(row => row.ProcessId === identity.pid);
    if (matches.length > 1 || matches.length === 1 && !matches[0].Birth)
      throw blocked('previous native process identity unavailable');
    if (!matches.length || matches[0].Birth !== identity.birth)
      return { identity: structuredClone(identity), state: 'dead' };
    return null;
  }
  async observeManaged({ target, operation, locator }) {
    const parent = await this.nativePane(this.binding.parentPaneId);
    const rows = await (this.inspectShell ?? nativeShellProcesses)(process.pid);
    const producerRow = rows.find(row => row.ProcessId === process.pid);
    const producer = nativeIdentity(producerRow);
    let ancestor = producerRow.ParentProcessId;
    for (let depth = 0; depth < 5 && ancestor !== parent.shell.pid; depth++) {
      const row = (await (this.inspectShell ?? nativeShellProcesses)(ancestor)).find(item => item.ProcessId === ancestor);
      if (!row || row.ParentProcessId === ancestor) throw blocked('producer parent membership unavailable');
      ancestor = row.ParentProcessId;
    }
    if (ancestor !== parent.shell.pid) throw blocked('producer is outside native parent');
    let missing;
    if (locator) {
      try { await this.herdr('get', locator.paneId); }
      catch (error) {
        if (!authoritativePaneNotFound(error)) throw error;
        missing = { code: error.code, paneId: locator.paneId };
      }
    }
    // Missing target authority is observed before awaiting a historical effect
    // promise or reading its role source. Closed panes cannot paint or preserve
    // the geometry an old placement operation was waiting for.
    if (target && operation) {
      try { await this.herdr('get', target.paneId); }
      catch (error) {
        if (!authoritativePaneNotFound(error)) throw error;
        return { parent, producer, missing: { code: error.code, paneId: target.paneId } };
      }
    }
    const retained = operation && this.effectObservations.get(operation.id);
    if (operation?.type === 'close' && retained?.settled && retained.retiredTarget) {
      try { await this.herdr('get', target.paneId); }
      catch (error) {
        if (!authoritativePaneNotFound(error)) throw error;
        return { parent, producer, missing: { code: error.code, paneId: target.paneId },
          retiredTarget: retained.retiredTarget, effect: { id: operation.id, settled: true } };
      }
      throw blocked('retired close target reappeared');
    }
    let observed;
    if (target) {
      if (this.retainedTargets.has(target.paneId)) {
        observed = await this.observeRetainedTarget(this.retainedTargets.get(target.paneId));
      } else if (target.statePath === this.stateFile) {
        observed = await this.nativePane(target.paneId, await readJson(this.recordFile));
      } else if (target.statePath) {
        observed = await this.provider?.observeTarget(target);
      } else observed = await this.nativePane(target.paneId);
      if (!observed) throw blocked('target participant unavailable');
    }
    // The actor's equality contract retains the requested key order. Values
    // above come from current membership, native rows and bound role files.
    const effect = operation && this.effectObservations.get(operation.id);
    return { parent, producer, missing,
      target: target && Object.fromEntries(Object.keys(target).map(key => [key, observed[key]])),
      effect: effect?.settled && effect.ownerNonce === this.actor.snapshot.ownerNonce &&
        effect.generation === this.actor.snapshot.generation && effect.lifetimeId === operation.lifetimeId
        ? { id: operation.id, settled: true } : undefined };
  }
  // The single mutation gate (attempt 5, verify-task-6-r4.md B2/B3). Every managed
  // native mutation passes here inside the held writer, immediately before the
  // actor dispatches it. An automatic pass re-gathers all region evidence and
  // decides it fresh: FRESH authorizes only Todo's first chat split, INHERITED
  // (the pass's claim) only a role split beside the chat; every swap, resize,
  // focus, move or close, and every other outcome, waits. An explicit /dashboard
  // pass keeps its behavior; a viewer's own q/X close runs outside any pass; a
  // launch or retirement runs in its own verified pane.
  // Phase (a) of the gate: every await (all region evidence and the current
  // geometry, gathered into one snapshot). It returns phase (b): a synchronous
  // decision over that snapshot, which awaits nothing.
  async layoutEvidence(role, type, target, pass) {
    // An unmanaged dashboard has no parent slot or region; its placement is unchanged.
    if (!this.managed || type === 'run' || type === 'retire' || type === 'close' && !pass || pass?.explicit) return () => undefined;
    const changed = 'layout-waiting: the dashboard panes changed during this pass; run /dashboard';
    if (type !== 'split' || !this.provider?.authorityEvidence) return () => changed;
    const evidence = await freshReads(() => this.provider.authorityEvidence());
    const chat = target?.paneId === this.binding.parentPaneId;
    return () => this.provider.decideAuthority(evidence, { role, chat, claim: this.claim?.layout }) ?? undefined;
  }
  async layoutGate(role, type, target, pass) { return (await this.layoutEvidence(role, type, target, pass))(); }
  perform(role, type, target, dispatch, check) {
    const pass = passes.getStore();
    // The authority (r5/r6 A1): inside the parent-slot dispatch the dispatcher
    // (a) awaits authorize(), the gathering, as its LAST await, then in one
    // synchronous run (b) calls the returned decision and (c) issues herdr. A
    // refusal is a known no-effect wait. A placement step brings its own
    // conditions (todo 7 A2: selection, chat, role identities, the tree it was
    // decided on): gathered here as well, after every actor preparation, and
    // decided in the same synchronous run.
    const authorize = () => freshReads(async () => {
      let decide, step;
      try { decide = await this.layoutEvidence(role, type, target, pass); step = await check?.(); }
      catch (error) { decide = () => `layout-waiting: the dashboard could not be verified (${error.message}); run /dashboard`; }
      return () => {
        let refusal;
        try { refusal = decide() || step?.(); }
        catch (error) { refusal = `layout-waiting: the dashboard could not be verified (${error.message}); run /dashboard`; }
        if (refusal) throw Object.assign(new Error(refusal), { code: 'LAYOUT_WAIT', noEffect: true });
      };
    });
    const result = this.managedWrite(async actor => {
      // Fast path only: the same gate, before the actor prepares anything.
      const refusal = await this.layoutGate(role, type, target, pass);
      if (refusal) throw Object.assign(new Error(refusal), { code: 'LAYOUT_WAIT', noEffect: true });
      this.layoutActive = true;
      if (this.arrangement) this.arrangement.moving = true;
      this.bridge?.readinessChanged();
      try {
        let result;
        try {
          result = await actor.perform(actor.capability(role === 'todo' ? 'todo' : 'dag', role), { type, target },
            ({ operation }) => dispatch({ operation, authorize }), ({ operation }) => this.invalidateCustodyEffect(operation));
        } catch (error) {
          if (error?.code !== 'LAYOUT_WAIT' || error.noEffect !== true) throw error;
          // Settled without effect: repaint what PREPARED marked unpainted.
          this.bridge?.invalidate();
          await this.presence?.idle();
          throw error;
        }
        if (type === 'arrange' || type === 'close' ||
          (type === 'split' || type === 'run') && actor.snapshot.roles[role]?.ack?.painted === false) {
          // PREPARED keeps the paint sequence floor. Repaint the current source
          // through the real publisher; an old same-sequence ACK cannot restore READY.
          this.bridge?.invalidate();
          await this.presence?.idle();
        }
        return result;
      } finally {
        this.layoutActive = false;
        this.bridge?.readinessChanged();
      }
    });
    return result.then(value => {
      void this.publishCustodyBinding().catch(error => { if (!this.stopped) this.notify(error.message); });
      return value;
    });
  }
  publishCustodyBinding() {
    return this.roleWrite(async () => {
      const record = await readJson(this.recordFile), slot = this.actor?.snapshot;
      if (!this.stopped && !this.actor?.blocked && !this.actor?.draining &&
        slot?.operations.every(operation => operation.status === 'SETTLED') &&
        this.recordMatches(record) && record.scopeEpoch === this.scopeEpoch &&
        !record.manualClose && slot.roles.todo?.viewer?.pid === record.processId)
        await writeJson(this.stateFile, this.roleState(record));
    });
  }
  async invalidateCustodyEffect(operation) {
    if (this.custodyLost) throw blocked('original custody channel lost before dispatch');
    const pending = [];
    for (const [socket, subscriber] of this.retirementSubscribers) {
      if (!subscriber.custody || subscriber.invalidated) continue;
      const signal = AbortSignal.any([this.stopSignal.signal, AbortSignal.timeout(10000)]);
      const result = new Promise((resolve, reject) => {
        const clear = () => {
          subscriber.pending.delete(operation.id);
          socket.off('close', close); signal.removeEventListener('abort', abort);
        };
        const close = () => { clear(); reject(blocked('original custody effect channel closed')); };
        const abort = () => { clear(); reject(signal.reason); };
        subscriber.pending.set(operation.id, { operation, accept: () => {
          subscriber.invalidated = true; clear(); resolve();
        } });
        socket.once('close', close); signal.addEventListener('abort', abort, { once: true });
        if (signal.aborted || socket.destroyed) return abort();
        send(socket, { ...subscriber.offer, type: 'custody-effect', nonce: subscriber.nonce, operation });
      });
      pending.push(result);
    }
    await Promise.all(pending);
  }
  async observeArrange(target, args, expected) {
    const kind = args[0], option = name => args[args.indexOf(name) + 1];
    const before = (await this.herdr('layout', '--pane', target.paneId))?.layout;
    const rect = (layout, id) => {
      const rows = layout?.panes?.filter(pane => pane.pane_id === id);
      if (rows?.length !== 1 || !rows[0].rect ||
        !['x', 'y', 'width', 'height'].every(key => Number.isFinite(rows[0].rect[key])))
        throw blocked('arrange target geometry unavailable');
      return rows[0].rect;
    };
    if (before?.tab_id !== target.tabId || !['move', 'swap', 'resize', 'focus'].includes(kind))
      throw blocked('arrange intent unavailable');
    rect(before, target.paneId);
    const otherId = kind === 'swap' || kind === 'move' ? option('--target-pane') :
      kind === 'focus' ? (await this.herdr('neighbor', '--pane', target.paneId,
        '--direction', option('--direction')))?.neighbor?.neighbor_pane_id : null;
    if ((kind === 'swap' ? option('--source-pane') : kind === 'move' ? args[1] : option('--pane')) !== target.paneId ||
      kind === 'move' && option('--tab') !== target.tabId ||
      ['move', 'swap', 'focus'].includes(kind) && (!otherId || otherId === target.paneId))
      throw blocked('arrange intent target mismatch');
    if (otherId) rect(before, otherId);
    if (kind === 'resize' && (expected?.paneId !== target.paneId || !expected.rect ||
      !['x', 'y', 'width', 'height'].every(key => Number.isFinite(expected.rect[key]))))
      throw blocked('resize expected geometry unavailable');
    const identities = new Map();
    for (const pane of before.panes) identities.set(pane.pane_id, await this.currentTarget(pane.pane_id));
    const actor = this.actor, lifetime = this.lifetimeId, provider = this.provider;
    const generation = actor.snapshot.generation, ownerNonce = actor.snapshot.ownerNonce;
    const participants = JSON.stringify(actor.snapshot.participants);
    const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
    const sameSplits = (a, b) => a?.length === b?.length && a?.every((split, index) =>
      split.id === b[index].id && split.direction === b[index].direction &&
      Math.fround(split.ratio) === Math.fround(b[index].ratio) && same(split.rect, b[index].rect));
    const matches = layout => {
      if (layout?.tab_id !== before.tab_id || layout.workspace_id !== before.workspace_id ||
        !same(layout.area, before.area) || layout.zoomed !== before.zoomed ||
        layout.panes?.length !== before.panes.length) return false;
      if (kind === 'move') {
        const a = rect(layout, target.paneId), b = rect(layout, otherId);
        return option('--split') === 'right'
          ? a.x === b.x + b.width && a.y === b.y && a.height === b.height
          : a.y === b.y + b.height && a.x === b.x && a.width === b.width;
      }
      if (kind === 'resize') {
        if (layout.focused_pane_id !== before.focused_pane_id ||
          !same(rect(layout, target.paneId), expected.rect) ||
          layout.splits?.length !== before.splits?.length || before.splits.length !== before.panes.length - 1)
          return false;
        const equalRect = (a, b) => ['x', 'y', 'width', 'height'].every(key => a?.[key] === b?.[key]);
        // Native split rectangles are subtree areas, not divider rectangles.
        // Retain each leaf and split's original first/second-child attachment.
        const children = split => {
          const axis = split.direction === 'right' ? 'width' : 'height';
          const coordinate = split.direction === 'right' ? 'x' : 'y';
          const size = Math.round(Math.fround(split.rect[axis] * Math.fround(split.ratio)));
          return [{ ...split.rect, [axis]: size }, { ...split.rect,
            [coordinate]: split.rect[coordinate] + size, [axis]: split.rect[axis] - size }];
        };
        const originalChildren = before.splits.map(children), currentChildren = layout.splits.map(children);
        const attachment = (oldRect, newRect) => {
          const parent = originalChildren.findIndex(parts => parts.some(part => equalRect(part, oldRect)));
          return parent !== -1 && equalRect(newRect,
            currentChildren[parent][originalChildren[parent].findIndex(part => equalRect(part, oldRect))]);
        };
        const changed = before.splits.filter((split, index) =>
          Math.fround(split.ratio) !== Math.fround(layout.splits[index].ratio));
        if (changed.length !== 1 || changed[0].direction !==
          (['left', 'right'].includes(option('--direction')) ? 'right' : 'down')) return false;
        const targetBefore = rect(before, target.paneId), area = changed[0].rect;
        const horizontal = changed[0].direction === 'right';
        const coordinate = horizontal ? 'x' : 'y', axis = horizontal ? 'width' : 'height';
        const cross = horizontal ? 'y' : 'x', crossSize = horizontal ? 'height' : 'width';
        const nearest = edge => before.splits.map((split, index) => ({ split,
          distance: Math.abs(originalChildren[index][0][coordinate] + originalChildren[index][0][axis] - edge) }))
          .filter(({ split, distance }) => split.direction === changed[0].direction && distance <= 1 &&
            split.rect[cross] < targetBefore[cross] + targetBefore[crossSize] &&
            split.rect[cross] + split.rect[crossSize] > targetBefore[cross])
          .sort((a, b) => a.distance - b.distance)[0]?.split;
        const nearEdge = targetBefore[coordinate], farEdge = nearEdge + targetBefore[axis];
        const forward = ['right', 'down'].includes(option('--direction'));
        if (changed[0] !== (nearest(forward ? farEdge : nearEdge) ?? nearest(forward ? nearEdge : farEdge)))
          return false;
        if (targetBefore.x < area.x || targetBefore.y < area.y ||
          targetBefore.x + targetBefore.width > area.x + area.width ||
          targetBefore.y + targetBefore.height > area.y + area.height) return false;
        return before.splits.every((split, index) => split.id === layout.splits[index].id &&
          split.direction === layout.splits[index].direction &&
          Number.isFinite(layout.splits[index].ratio) && layout.splits[index].ratio > 0 && layout.splits[index].ratio < 1 &&
          (index === 0 ? equalRect(split.rect, before.area) && equalRect(layout.splits[index].rect, before.area) :
            attachment(split.rect, layout.splits[index].rect))) &&
          before.panes.every(pane => attachment(pane.rect, rect(layout, pane.pane_id)));
      }
      return layout.focused_pane_id === (kind === 'swap' ? target.paneId : otherId) &&
        sameSplits(layout.splits, before.splits) && before.panes.every(pane => same(rect(layout, pane.pane_id),
          rect(before, kind === 'swap' && pane.pane_id === target.paneId ? otherId :
            kind === 'swap' && pane.pane_id === otherId ? target.paneId : pane.pane_id)));
    };
    let started, failed, completed, effectsReject, submitted = false;
    const ready = new Promise((resolve, reject) => { started = resolve; failed = reject; });
    const effects = new Promise((resolve, reject) => { completed = resolve; effectsReject = reject; });
    ready.catch(() => {}); effects.catch(() => {});
    const socket = net.createConnection(process.platform === 'win32'
      ? `\\\\.\\pipe\\${this.binding.socketPath}` : this.binding.socketPath);
    const signal = AbortSignal.any([this.stopSignal.signal, AbortSignal.timeout(10000)]);
    const id = randomUUID();
    const fail = error => { failed(error); effectsReject(error); socket.destroy(); };
    const abort = () => fail(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) fail(signal.reason);
    socket.once('error', fail);
    socket.once('close', () => fail(blocked('arrange completion connection lost')));
    socket.once('connect', () => send(socket, { id, method: 'events.subscribe', params: {
      subscriptions: [{ type: kind === 'focus' ? 'pane.focused' : kind === 'move' ? 'pane.moved' : 'layout.updated' }],
    } }));
    receive(socket, message => {
      try {
        if (message.id === id && message.error) return fail(blocked(message.error.code));
        if (message.id === id && message.result?.type === 'subscription_started') return started();
        if (!submitted) return;
        if (kind === 'focus' && message.event === 'pane_focused' &&
          message.data?.pane_id === otherId && message.data.workspace_id === before.workspace_id) completed(message);
        else if (kind === 'move' && message.event === 'pane_moved' &&
          message.data?.previous_pane_id === target.paneId && message.data.previous_tab_id === target.tabId) completed(message);
        else if (message.event === 'layout_updated' && matches(message.data?.layout)) completed(message);
      } catch (error) { fail(error); }
    });
    const close = () => { signal.removeEventListener('abort', abort); socket.destroy(); };
    try { await ready; }
    catch (error) { close(); throw error; }
    return { effects, submit: () => {
      this.assertManaged();
      if (signal.aborted || actor !== this.actor || lifetime !== this.lifetimeId || provider !== this.provider ||
        participants !== JSON.stringify(actor.snapshot.participants)) throw blocked('arrange source lifetime changed');
      submitted = true;
    }, close,
      complete: async result => {
        const receipt = result?.[kind === 'move' ? 'move_result' : kind];
        if (!receipt?.changed || result.type !== (kind === 'focus' ? 'pane_focus_direction' : 'pane_' + kind) ||
          (kind === 'swap' || kind === 'focus' ? receipt.source_pane_id : kind === 'move' ? receipt.previous_pane_id : receipt.pane_id) !== target.paneId ||
          kind === 'swap' && receipt.target_pane_id !== otherId ||
          !matches(kind === 'move' ? receipt.target_layout : receipt.layout))
          throw blocked('arrange operation receipt mismatch');
        await effects;
        this.assertManaged();
        if (actor !== this.actor || lifetime !== this.lifetimeId || provider !== this.provider ||
          generation !== actor.snapshot.generation || ownerNonce !== actor.snapshot.ownerNonce ||
          participants !== JSON.stringify(actor.snapshot.participants))
          throw blocked('arrange source lifetime changed');
        for (const [paneId, identity] of identities) {
          if (!same(await this.currentTarget(paneId), identity)) throw blocked('arrange native target changed');
        }
        const current = (await this.herdr('layout', '--pane', target.paneId))?.layout;
        if (!matches(current) || !same(current, kind === 'move' ? receipt.target_layout : receipt.layout))
          throw blocked('arrange current effect mismatch');
        if (!same((await this.observeManaged({ target })).target, target)) throw blocked('arrange source target changed');
        this.assertManaged();
        if (actor !== this.actor || lifetime !== this.lifetimeId || provider !== this.provider ||
          participants !== JSON.stringify(actor.snapshot.participants)) throw blocked('arrange source lifetime changed');
      } };
  }
  // A pane mutation changes which processes run where: cached identity rows are dropped before and after.
  async mutate(role, type, target, args, observedEffect, beforeSubmit, check) {
    bumpProcessQueryEpoch();
    try {
    if (!this.managed) return await this.herdr(...args);
    return await this.perform(role, type, target, async ({ operation, authorize }) => {
      let resolve, reject;
      const effects = new Promise((done, fail) => { resolve = done; reject = fail; });
      effects.catch(() => {});
      let arrange, submitted = false;
      const actor = this.actor, lifetime = this.lifetimeId;
      const observation = { settled: false, ownerNonce: operation.ownerNonce,
        generation: operation.generation, lifetimeId: operation.lifetimeId };
      this.effectObservations.set(operation.id, observation);
      try {
        const exit = type === 'close' ? await observeNativeExit(target.shell) : null;
        if (type === 'arrange') arrange = await this.observeArrange(target, args, observedEffect);
        if (type === 'run' && observedEffect || type === 'close') {
          observation.promise = (type === 'run' ? observedEffect : exit.effects).then(async () => {
            if (!submitted || this.stopped || this.actor !== actor || this.lifetimeId !== lifetime ||
              !actor.leaseHeld || actor.draining || actor.snapshot.ownerNonce !== operation.ownerNonce ||
              actor.snapshot.generation !== operation.generation) throw blocked('late effect lifetime changed');
            if (type === 'close') {
              try { await this.herdr('get', target.paneId); throw blocked('close target remains'); }
              catch (error) { if (!authoritativePaneNotFound(error)) throw error; }
              observation.retiredTarget = structuredClone(target);
            } else {
              const actual = await this.observeManaged({ target });
              if (JSON.stringify(actual.target) !== JSON.stringify(target)) throw blocked('late run source changed');
            }
            observation.settled = true;
          }).catch(error => { observation.error = error; });
        }
        await beforeSubmit?.();
        // (a) The last await before the native command: all gathering.
        const decide = await authorize();
        // (b) The decision and (c) the herdr invocation run in one synchronous
        // run: nothing is awaited between them.
        decide();
        arrange?.submit();
        submitted = true;
        const issued = this.herdr(...args);
        const result = await issued;
        if (type === 'split') await this.nativePane(result?.pane?.pane_id);
        else if (type === 'close') {
          await exit.effects;
          if ((await this.herdr('list')).panes.some(pane => pane.pane_id === target.paneId)) throw blocked('close effect not settled');
        } else if (type === 'arrange') await arrange.complete(result);
        if (observedEffect && type !== 'arrange') await observedEffect;
        resolve();
        observation.settled = true;
        return { result, effects };
      } catch (error) {
        // A no-effect refusal leaves no effect to observe (r6 C).
        if (error?.noEffect === true) this.effectObservations.delete(operation.id);
        reject(error); throw error;
      }
      finally { arrange?.close(); }
    }, check);
    } finally { bumpProcessQueryEpoch(); }
  }
  registerManagedRole(role, readTarget) {
    return this.managedWrite(async actor => {
      const target = await readTarget();
      if (target === null) return false;
      const old = actor.snapshot.roles[role];
      if (old?.lifetimeId === actor.snapshot.participants[role === 'todo' ? 'todo' : 'dag']?.lifetimeId &&
        ['paneId', 'tabId', 'shell', 'viewer', 'statePath', 'sourcePath', 'cachePath', 'prefsPath', 'launchToken', 'controlToken']
          .every(key => JSON.stringify(old[key]) === JSON.stringify(target[key]))) return;
      await actor.registerRole(actor.capability(role === 'todo' ? 'todo' : 'dag', role), target);
      this.bridge?.readinessChanged();
    });
  }
  paintManaged(event) {
    if (this.stopped || !this.actor?.leaseHeld) { this.bridge?.readinessChanged(); return; }
    const old = this.actor.snapshot.roles[event.role];
    if (this.actor.snapshot.phase === 'READY' && old?.ack?.painted &&
      old.ack.sequence === event.sequence && old.generation === this.actor.snapshot.generation) return;
    void this.managedReady(async actor => {
      const record = actor.snapshot.roles[event.role];
      const receipt = { ...this.presence.getOwner(), role: event.role, sequence: event.sequence };
      if (!record || !this.presence.isReady(receipt)) return;
      if (record.ack && event.sequence <= record.ack.sequence &&
        !(record.ack.painted && event.sequence === record.ack.sequence)) return;
      if (!record.ack?.painted || record.ack.sequence !== event.sequence)
        await actor.ackRole({ ...actor.capability(event.role === 'todo' ? 'todo' : 'dag', event.role),
          paneId: record.paneId, statePath: record.statePath, launchToken: record.launchToken,
          controlToken: record.controlToken, painted: true, sequence: event.sequence });
      return event.sequence;
    });
  }
  // READY needs every role's accepted paint of the current publication and no
  // arrangement that has begun moving panes: between its steps the paints are
  // fresh but the geometry is not complete (todo 9). accept(actor) accepts a paint
  // and returns its sequence; a finished arrangement decides again here.
  managedReady(accept) {
    return this.managedWrite(async actor => {
      const sequence = await accept(actor);
      if (sequence === undefined) return;
      const snapshot = actor.snapshot, receipt = { ...this.presence.getOwner(), sequence };
      if (!this.arrangement?.moving && snapshot.phase !== 'READY' && ['todo', 'dag', 'workers'].every(role =>
        snapshot.roles[role]?.ack?.painted && this.presence.isReady({ ...receipt, role }) &&
        snapshot.roles[role].ack.sequence === sequence)) {
        await actor.ready();
        return true;
      }
      this.bridge?.readinessChanged();
    }).then(async becameReady => {
      if (becameReady && !this.stopped) await this.roleWrite(async () => {
        const record = await readJson(this.recordFile);
        if (this.actor?.snapshot.phase === 'READY' && this.recordMatches(record) &&
          record.scopeEpoch === this.scopeEpoch && !record.manualClose)
          await writeJson(this.stateFile, this.roleState(record));
      });
      this.bridge?.readinessChanged();
    }).catch(error => { if (error?.code !== 'ACK_MISMATCH') this.warnOnce(error); });
  }
  // Each distinct warning is shown once until a scheduled pass succeeds. A
  // latch shows one warning: restating its unavailability adds nothing.
  warnOnce(error) {
    const restates = ['MANAGED_UNAVAILABLE', 'RECOVERY_BLOCKED'].includes(error?.code);
    // While the actor is blocked with nothing left to reconcile, /dashboard
    // cannot rebuild it: the latch's one warning, whether the blocking error or
    // the first 'recovery awaits', names the restart action instead.
    const message = error?.code !== 'MANAGED_UNAVAILABLE' && this.actor?.blocked &&
      this.actor.snapshot.operations.every(operation => operation.status === 'SETTLED')
      ? `BLOCKED: ${managedUnavailable}` : error?.message;
    if (this.stopped || this.warned.has(message) || restates && [...this.warned.values()].includes(true)) return;
    this.warned.set(message, this.recoveryBlocked === true);
    this.notify(message);
  }
  enqueue(job) {
    const lifetime = this.lifetimeId;
    const result = this.tail.then(() => {
      if (this.stopped || lifetime !== this.lifetimeId) return;
      return job();
    });
    this.tail = result.then(() => { if (!this.recoveryBlocked) this.warned.clear(); }, error => { if (!this.stopped) {
      this.status = this.managed || error.inlineFallback ? 'BLOCKED' : 'layout-waiting';
      if (!this.managed) { this.notify(error.message); return; }
      this.recoveryBlocked = true;
      this.warnOnce(error);
      this.bridge?.readinessChanged();
      void this.watchRecoveryMembership().catch(() => {});
    } });
    return result;
  }
  roleWrite(job) {
    const result = this.roleTail.then(job);
    this.roleTail = result.catch(() => {});
    return result;
  }
  async start() {
    const old = await readJson(this.recordFile);
    this.scopeEpoch = Math.max(0, old?.scopeEpoch ?? 0) + 1;
    let cache;
    const directory = this.directory;
    this.storage = {
      get path() { return cache.path; },
      rebase(next) {
        cache?.rebase(next); // Invalidate an old in-flight write before changing its path.
        cache = createSnapshotStorage({ path: join(directory, `scope-${next.scopeEpoch}`, 'todo.cache.json') });
        cache.rebase(next);
      },
      write(snapshot) { return cache.write(snapshot); },
    };
    this.presence = createPresence({ ...this.binding, scopeEpoch: this.scopeEpoch, storage: this.storage, pipePath: this.presencePath,
      clock: this.clock, hasDagOwnedTasks: () => this.source !== null && this.source.runs.some(run => run.nodes.some(node => node.taskId)) });
    await this.startControl();
    await this.presence.start();
    if (this.stopped) return;
    const readiness = receipt => {
      const record = this.ownedRoles.get(receipt?.role);
      const snapshot = this.actor?.snapshot;
      const result = (reason, diagnostic = reason) => {
        this.readinessReasons ??= new Map();
        if (this.readinessReasons.get(receipt.role) !== reason) {
          this.readinessReasons.set(receipt.role, reason);
          const diagnosticReason = diagnostic.startsWith('presence-') && diagnostic !== 'presence-not-ready'
            ? 'presence-not-ready' : diagnostic;
          void this.actor?.diagnose('display', { code: 'NONE', reason: diagnosticReason, sequence: receipt.sequence,
            ackSequence: snapshot?.roles[receipt.role]?.ack?.sequence, phase: snapshot?.phase },
            { role: receipt.role });
        }
        return reason;
      };
      if (this.managed) {
        for (const [reason, denied] of [
          ['stopped', this.stopped], ['managed-blocked', this.managedBlocked],
          ['recovery-blocked', this.recoveryBlocked], ['layout-active', this.layoutActive],
          ['arrangement-moving', this.arrangement?.moving], ['source-refreshing', this.sourceRefreshing],
          ['actor-fallback', this.actor?.inlineFallback !== false],
          ['scope-mismatch', snapshot?.sessionId !== this.binding.sessionId],
          ['participant-mismatch', snapshot?.participants.todo?.lifetimeId !== this.lifetimeId ||
            snapshot?.participants.dag?.lifetimeId !== this.provider?.lifetimeId],
        ]) if (denied) return result(reason);
        const required = ['todo', 'dag', 'workers'];
        // Hard gates for every required role precede any transient ack lag. A
        // Todo sequence mismatch must not hide a later role's loss.
        for (const role of required) {
          const owned = this.ownedRoles.get(role);
          if (owned?.ready !== true || !Number.isSafeInteger(owned.processId)) return result('role-not-ready');
          if (owned.manualClose === true) return result('role-closed');
        }
        for (const role of required) {
          if (snapshot.roles[role]?.generation !== snapshot.generation) return result('role-generation');
          if (!snapshot.roles[role]?.ack?.painted) return result('role-paint');
        }
        const presenceReasons = new Map();
        for (const role of required) {
          const reason = this.presence.readiness({ ...receipt, role });
          presenceReasons.set(role, reason);
          // Expiry, a missing observer, a closed presence or source/baseline
          // failure outranks any ack lag, even on a different role.
          if (reason !== 'ready' && reason !== 'presence-ack-lag') return result(reason);
        }
        // Aggregate transient pending only after every hard gate passed.
        for (const role of required)
          if (snapshot.roles[role]?.ack?.sequence !== receipt.sequence) return result('role-sequence');
        for (const role of required)
          if (presenceReasons.get(role) !== 'ready') return result(presenceReasons.get(role));
        return result('ready');
      }
      if (record?.scopeEpoch !== this.scopeEpoch) return result('scope-mismatch');
      if (record.ready !== true || !Number.isSafeInteger(record.processId)) return result('role-not-ready');
      if (record.manualClose === true) return result('role-closed');
      return result(this.presence.readiness(receipt));
    };
    this.adapter = {
      getOwner: () => this.presence.getOwner(),
      publish: snapshot => this.presence.publish(snapshot),
      settings: this.presence.settings,
      hasDagOwnedTasks: () => this.presence.hasDagOwnedTasks(),
      isReady: receipt => readiness(receipt) === 'ready',
      readiness,
    };
    this.onPresenceEvent = event => {
      if (event.type === 'disconnected') {
        this.ownedRoles.delete(event.role);
        if (this.managed && !this.stopped) void this.observeViewerDeath(event.role).catch(() => {});
      }
      if (this.managed && event.type === 'ack') this.paintManaged(event);
    };
    this.presence.events.on('event', this.onPresenceEvent);
    this.bridge = installDisplayBridge(this.ctx, this.adapter, { environment: this.environment });
    if (!this.bridge) throw new Error('Native display bridge is unavailable for this owner');
    this.offReadiness = this.presence.subscribeReadiness(() => {
      if (!this.stopped) this.bridge?.readinessChanged();
    });
    await this.presence.idle();
    if (!this.presence.state.snapshot) throw new Error('Initial native source save failed');
    if (this.managed) {
      const observation = await this.observeManaged({});
      const paths = await parentSlotPaths({ stateDir: this.stateDir, socketPath: this.binding.socketPath,
        parent: observation.parent });
      const previous = await readJson(paths.slotPath);
      if (validParentSlot(previous) && previous.phase !== 'RELEASED' &&
        !cooperativelyReleasedParentSlot(previous, observation.producer) &&
        !(await this.observePreviousProducer(previous.producer))) {
        let capture = [...this.legacyObservations.values()].find(value =>
          JSON.stringify(value.identity) === JSON.stringify(previous.producer));
        if (!capture) {
          const records = await discoverLegacyRecords(this.stateDir, this.binding);
          const custodian = records.find(entry => entry.record.role === 'todo' &&
            entry.record.paneId === previous.roles.todo?.paneId &&
            entry.record.statePath === previous.roles.todo?.statePath &&
            entry.record.launchToken === previous.roles.todo?.launchToken && !entry.record.manualClose);
          if (!custodian) throw blocked('surviving original producer custody unavailable; restart this OmO window');
          capture = await captureCustodiedProducer({ stateDir: this.stateDir, recordPath: custodian.path,
            parent: observation.parent, herdr: this.herdr, slotPath: paths.slotPath, previous });
          this.legacyObservations.set(capture.recordPath, capture);
        }
        const retired = await observeLegacyRetirement(capture, { stateDir: this.stateDir,
          recordPath: capture.recordPath, parent: observation.parent });
        if (retired?.state !== 'dead') await capture.death();
      }
      this.actor = await claimParentSlot({ stateDir: this.stateDir, socketPath: this.binding.socketPath,
        parent: observation.parent, producer: observation.producer, sessionId: this.binding.sessionId,
        observe: request => this.observeManaged(request),
        observePrevious: async identity => {
          const native = await this.observePreviousProducer(identity);
          if (native) return native;
          for (const capture of this.legacyObservations.values()) {
            if (JSON.stringify(capture.identity) !== JSON.stringify(identity)) continue;
            const retired = await observeLegacyRetirement(capture, { stateDir: this.stateDir,
              recordPath: capture.recordPath, parent: observation.parent });
            if (retired?.state === 'dead') return retired;
          }
          return null;
        } });
      await this.actor.join('todo', this.lifetimeId);
      this.publisherScope = Object.freeze({
        ownerNonce: this.actor.snapshot.ownerNonce, generation: this.actor.snapshot.generation,
        sessionId: this.binding.sessionId, lifetimeId: this.lifetimeId,
        producer: this.actor.snapshot.producer, parent: this.actor.snapshot.parent,
      });
      this.claimRetired = this.actor.snapshot.generation > 1;
    }
    this.status = 'layout-waiting';
  }
  async startControl() {
    this.control = net.createServer(socket => {
      this.controlSockets.add(socket);
      socket.once('close', () => this.controlSockets.delete(socket));
      socket.on('error', () => {});
      receive(socket, message => {
        const job = this.handleControl(message, socket);
        this.controlJobs.add(job);
        void job.catch(() => socket.destroy()).finally(() => this.controlJobs.delete(job));
      });
    });
    await new Promise((resolve, reject) => {
      this.control.once('error', reject); this.control.once('listening', resolve); this.control.listen(this.controlPath);
    });
  }
  async membership(paneId) {
    const panes = (await this.herdr('list'))?.panes ?? [];
    this.lastPanes = panes;
    const parents = panes.filter(pane => pane.pane_id === this.binding.parentPaneId);
    const matches = panes.filter(pane => pane.pane_id === paneId);
    if (parents.length !== 1 || matches.length !== 1 || !parents[0].tab_id ||
      matches[0].tab_id !== parents[0].tab_id) throw new Error('layout-waiting: missing or ambiguous owned anchor');
    return { pane: matches[0], tabId: parents[0].tab_id };
  }
  // The shared scope is the only identity a layout publication or replay may
  // use. generation is the current slot generation when managed.
  recoveryScope(tabId = this.claim?.tabId) {
    return { ownerKey: this.binding.ownerKey, sessionId: this.binding.sessionId,
      lifetimeId: this.lifetimeId, scopeEpoch: this.scopeEpoch,
      generation: this.actor?.snapshot?.generation ?? this.publisherScope?.generation ?? 1, tabId };
  }
  coordinationRecordPath() {
    const directory = this.environment.OMO_HERDR_COORDINATION_STATE_DIR;
    if (!directory) return null;
    const pane = String(this.binding.parentPaneId).replace(/[^A-Za-z0-9_-]/g, '_');
    return join(directory, `${pane}.pane.json`);
  }
  async scopedCoordinationRecord(tabId) {
    const { record, proof } = await this.coordinationSnapshot();
    if (!record?.paneId || record.manualClose === true) return null;
    if (record.parentPaneId !== this.binding.parentPaneId || record.sessionId !== this.binding.sessionId ||
      record.tabId && record.tabId !== tabId) return null;
    // A v2 record is current only while its viewer sidecar proves the exact
    // launch first paint and a live heartbeat. A stale record is not published
    // as a Collab cell; coordination may instead assert provenCurrent after it
    // revalidated the same identity. v1 has no launch identity and remains
    // legacy-waiting in the coordination controller; its scoped pane is still
    // the band's leftmost cell until the core proves and yields it.
    if (record.v === 2) {
      const now = typeof this.clock === 'function' ? this.clock() : Date.now();
      const current = inspectViewerProof(record, proof, { now }).ready === true;
      const asserted = record.provenCurrent === true && record.phase === 'running' && !record.pendingEffect;
      if (!current && !asserted) return null;
    }
    return record;
  }
  // The coordination record and its viewer proof are read through the same
  // injectable readers the layout pass uses. A missing or corrupt read is
  // absence, never evidence.
  async coordinationSnapshot() {
    let record = null, path = null, proof = null;
    try {
      path = this.coordinationRecordPath();
      record = typeof this.readCoordinationRecord === 'function'
        ? await this.readCoordinationRecord() : path ? await readJson(path) : null;
    } catch { record = null; } // A corrupt satellite record is absence, never a cell.
    if (record?.v === 2) {
      try {
        if (typeof this.readCoordinationProof === 'function') proof = await this.readCoordinationProof(record, path);
        else if (path) proof = readViewerProof(path);
      } catch { proof = null; }
    }
    return { record, path, proof };
  }
  // 11-d/11-g: 4/4 is the conjunction of four pane verdicts. A pane counts only
  // while its record still names the settled pane, belongs to this exact scope
  // and binding, proves the exact settled geometry, journals no unresolved
  // effect, and carries a proven identity. A v1 coordination record has no
  // launch identity, so it stays unproven unless it also carries the fresh
  // native legacy proof taken before a legacy yield.
  //
  // Every external read is awaited before the verdict runs. The verdict itself
  // is one synchronous pass over the live scope, ownership and actor phase, so
  // a READY->WAITING or ownership transition during a read cannot be rounded up.
  // The durable Todo record is read only after the coordination read: a pane
  // replacement that lands while that read is in flight must not be counted
  // from an earlier snapshot.
  async completionEvidence({ scope, layout }) {
    const tabId = layout?.scope?.tabId ?? scope?.tabId;
    const { record, proof } = await this.coordinationSnapshot();
    const todoRecord = await readJson(this.recordFile).catch(() => null);
    const verdict = () => this.completionVerdict({ layout, tabId, todoRecord, record, proof });
    return { ...verdict(), recheck: verdict };
  }
  completionVerdict({ layout, tabId, todoRecord, record, proof }) {
    const now = typeof this.clock === 'function' ? this.clock() : Date.now();
    const currentScope = this.recoveryScope(tabId);
    // The live owned Todo is the owner at verdict time. The durable record and
    // the settled layout must both still name that same pane.
    const ownedTodo = this.ownedRoles?.get?.('todo') ?? null;
    const panes = {}, waiting = [];
    const current = (role, paneId) => {
      panes[role] = { current: true, paneId: paneId ?? null, condition: 'current', detail: null };
      return true;
    };
    const fail = (role, paneId, condition, detail) => {
      panes[role] = { current: false, paneId: paneId ?? null, condition, detail };
      waiting.push({ role, paneId: paneId ?? null, condition, detail });
      return false;
    };
    const scopeKey = candidate => validScope(currentScope) && candidate
      ? RECOVERY_SCOPE_KEYS.find(key => String(candidate[key] ?? null) !== String(currentScope[key])) ?? null
      : 'scope';
    // The settled layout is one frozen identity; its scope and binding must still
    // be the live four-pane identity before any pane can count.
    let layoutCurrent = true;
    if (!validScope(currentScope) || !validScope(layout?.scope) || !sameScope(layout.scope, currentScope))
      layoutCurrent = fail('layout', null, 'scope-mismatch',
        `layout scope is no longer current (scope-mismatch: ${scopeKey(layout?.scope) ?? 'scope'})`);
    else {
      let live = null;
      try {
        live = bindingRevision({ scope: currentScope, todo: ownedTodo?.paneId ?? layout.todo,
          workers: layout.workers, dag: layout.dag, order: CORE_ORDER });
      } catch { live = null; }
      if (live !== layout.bindingRevision)
        layoutCurrent = fail('layout', null, 'binding-mismatch', 'layout binding revision is no longer current');
    }
    // Todo: its durable record is the proof of its pane and viewer identity, and
    // the live owned role must still name the same pane in the same scope.
    if (!ownedTodo)
      fail('todo', layout.todo, 'pane-mismatch', 'no live todo owner names the settled pane');
    else if (ownedTodo.paneId !== layout.todo)
      fail('todo', layout.todo, 'pane-mismatch', `the live todo owner names ${ownedTodo.paneId}`);
    else if (!this.recordMatches(todoRecord) || todoRecord.paneId !== layout.todo)
      fail('todo', layout.todo, 'pane-mismatch', 'the todo record does not name the settled pane');
    else if (!sameDashboardOwner(ownedTodo, this.binding) || ownedTodo.tabId !== tabId ||
      ownedTodo.scopeEpoch !== currentScope.scopeEpoch)
      fail('todo', layout.todo, 'scope-mismatch', !sameDashboardOwner(ownedTodo, this.binding)
        ? 'scope-mismatch: owner' : ownedTodo.tabId !== tabId ? 'scope-mismatch: tabId'
          : 'scope-mismatch: scopeEpoch');
    else if (todoRecord.scopeEpoch !== currentScope.scopeEpoch || todoRecord.tabId !== tabId)
      fail('todo', layout.todo, 'scope-mismatch', todoRecord.tabId !== tabId ? 'scope-mismatch: tabId'
        : 'scope-mismatch: scopeEpoch');
    else if (todoRecord.pendingEffect || ownedTodo.pendingEffect)
      fail('todo', layout.todo, 'unresolved-effect',
        effectDetail(todoRecord.pendingEffect ?? ownedTodo.pendingEffect));
    else if (todoRecord.manualClose === true || ownedTodo.manualClose === true)
      fail('todo', layout.todo, 'role-closed', 'the todo pane is closed');
    else if (todoRecord.ready !== true || !Number.isSafeInteger(todoRecord.processId) ||
      ownedTodo.ready !== true || !Number.isSafeInteger(ownedTodo.processId))
      fail('todo', layout.todo, 'unproven', 'the todo viewer identity is not proven');
    else current('todo', layout.todo);
    // Workers and DAG are owned role records from the same scope. They are read
    // from the live map at verdict time, never from a snapshot taken before a read.
    for (const role of ['workers', 'dag']) {
      const owned = this.ownedRoles.get(role), paneId = layout[role];
      if (!owned || owned.paneId !== paneId)
        fail(role, paneId, 'pane-mismatch', `the ${role} record does not name the settled pane`);
      else if (!sameDashboardOwner(owned, this.binding) || owned.tabId !== tabId ||
        owned.scopeEpoch !== currentScope.scopeEpoch)
        fail(role, paneId, 'scope-mismatch', !sameDashboardOwner(owned, this.binding) ? 'scope-mismatch: owner'
          : owned.tabId !== tabId ? 'scope-mismatch: tabId' : 'scope-mismatch: scopeEpoch');
      else if (owned.pendingEffect) fail(role, paneId, 'unresolved-effect', effectDetail(owned.pendingEffect));
      else if (owned.manualClose === true) fail(role, paneId, 'role-closed', `the ${role} pane is closed`);
      else if (owned.ready !== true || !Number.isSafeInteger(owned.processId))
        fail(role, paneId, 'unproven', `the ${role} viewer identity is not proven`);
      else current(role, paneId);
    }
    // Collab: the fourth pane is its coordination record, never the cell alone.
    // The record's proof must carry the exact settled geometry as well as the
    // binding: a width-only re-settle keeps the binding but changes the proof.
    const collabCell = layout.cells?.find(cell => cell.role === 'collab') ?? null;
    const collabPane = collabCell?.paneId ?? null;
    if (!collabCell) fail('collab', null, 'missing', 'the settled layout has no current collab pane');
    else if (!record?.paneId)
      fail('collab', collabPane, 'missing', 'no coordination record exists');
    else if (record.paneId !== collabPane)
      fail('collab', collabPane, 'pane-mismatch', `the coordination record names ${record.paneId}`);
    else if (record.manualClose === true)
      fail('collab', collabPane, 'role-closed', 'the coordination record is closed');
    else if (record.parentPaneId !== this.binding.parentPaneId || record.sessionId !== this.binding.sessionId ||
      record.tabId !== tabId)
      fail('collab', collabPane, 'scope-mismatch', 'scope-mismatch: pinned identity');
    else if (record.v === 2) {
      // Both published geometry identities are checked; either one that is
      // current is enough, because coordination refreshes the explicit digest
      // on resize while the layout revision may still describe the prior pass.
      const proofRevision = typeof record.layout?.geometryRevision === 'string' &&
        record.layout.geometryRevision ? record.layout.geometryRevision : null;
      const explicitGeometry = typeof record.revisions?.geometry === 'string' &&
        record.revisions.geometry ? record.revisions.geometry : null;
      const revisionCurrent = proofRevision !== null && proofRevision === layout.geometryRevision;
      const digestCurrent = explicitGeometry !== null && Boolean(record.rect && collabPane) &&
        explicitGeometry === collabGeometryDigest(collabPane, record.rect);
      if (!sameScope(record.scope, currentScope))
        fail('collab', collabPane, 'scope-mismatch', `scope-mismatch: ${scopeKey(record.scope) ?? 'scope'}`);
      else if (record.workers !== layout.workers || record.dag !== layout.dag)
        fail('collab', collabPane, 'binding-mismatch', 'binding-mismatch: workers/dag');
      else if (record.layout?.bindingRevision !== layout.bindingRevision)
        fail('collab', collabPane, 'binding-mismatch', 'binding-mismatch: revision');
      else if (!record.rect)
        fail('collab', collabPane, 'geometry-mismatch', 'geometry-mismatch: no current rect');
      else if (!proofRevision && !explicitGeometry)
        fail('collab', collabPane, 'geometry-mismatch', 'geometry-mismatch: no current geometry identity');
      else if (!revisionCurrent && !digestCurrent)
        fail('collab', collabPane, 'geometry-mismatch', proofRevision
          ? 'geometry-mismatch: revision' : 'geometry-mismatch: digest');
      else if (!sameRect(record.rect, collabCell.rect))
        fail('collab', collabPane, 'geometry-mismatch', 'geometry-mismatch: rect');
      else if (record.pendingEffect)
        fail('collab', collabPane, 'unresolved-effect', effectDetail(record.pendingEffect));
      else {
        const verdict = inspectViewerProof(record, proof, { now });
        const asserted = record.provenCurrent === true && record.phase === 'running' && !record.pendingEffect;
        if (!verdict.ready && !asserted)
          fail('collab', collabPane, 'unproven', `viewer proof: ${verdict.state}`);
        else current('collab', collabPane);
      }
    } else if (record.v === 1 || record.v === undefined) {
      if (!legacyProofFresh(record, now))
        fail('collab', collabPane, 'unproven', 'legacy-unproven: no fresh legacy proof');
      else current('collab', collabPane);
    } else fail('collab', collabPane, 'unproven', 'unproven: unknown coordination record version');
    const authority = this.completionAuthority();
    const coreReady = this.managed && authority === null && this.actor?.snapshot?.phase === 'READY';
    return { current: layoutCurrent && panes.todo?.current === true && panes.workers?.current === true &&
      panes.dag?.current === true && panes.collab?.current === true, panes, waiting, coreReady,
      coreFailure: authority };
  }
  async layoutPanes(tabId) {
    const layout = (await this.herdr('layout', '--pane', this.binding.parentPaneId))?.layout;
    if (Array.isArray(layout?.panes)) return !tabId || layout.tab_id === tabId ? layout.panes : [];
    return Array.isArray(this.lastPanes) ? this.lastPanes : [];
  }
  // A settled layout is published only after owned Workers/DAG records are
  // ready/written and no arrangement is moving. Every successful pass publishes,
  // including an explicit no-op, so passSeq advances exactly once per settle.
  async settleRecoveryLayout({ pair, tabId, explicit, commandId }) {
    if (this.stopped || !tabId || this.layoutActive || this.arrangement?.moving) return null;
    const lifetime = this.lifetimeId;
    const scope = this.recoveryScope(tabId);
    if (!validScope(scope)) return null;
    const actor = this.managed ? this.actor : null;
    const validOwned = owned => Boolean(owned) && sameDashboardOwner(owned, this.binding) &&
      owned.tabId === tabId && owned.manualClose !== true && owned.ready === true &&
      Number.isSafeInteger(owned.processId);
    const authorityCurrent = () => {
      if (!this.managed) return true;
      if (this.actor !== actor) return false;
      if (!this.actor?.leaseHeld || this.actor.blocked || this.actor.draining) return false;
      const snapshot = this.actor.snapshot;
      return snapshot?.sessionId === this.binding.sessionId &&
        snapshot.participants?.todo?.lifetimeId === lifetime;
    };
    if (!authorityCurrent()) return null;
    const record = await readJson(this.recordFile);
    const todo = this.recordMatches(record) && !record.manualClose ? record.paneId : this.ownedRoles.get('todo')?.paneId;
    const workers = this.managed ? this.ownedRoles.get('workers') : pair?.workers;
    const dag = this.managed ? this.ownedRoles.get('dag') : pair?.dag;
    const ownedTodo = this.ownedRoles.get('todo');
    const nonManagedPair = this.managed ? null : { workers: Boolean(pair?.workers), dag: Boolean(pair?.dag) };
    if (!todo || !workers?.paneId || !dag?.paneId) return null;
    if (!validOwned(workers) || !validOwned(dag)) return null;
    // Freeze the exact identity strings for the layout about to be settled. An
    // in-place record mutation must not be able to change what we compare later.
    const todoId = todo, workersId = workers.paneId, dagId = dag.paneId;
    const binding = bindingRevision({ scope, todo: todoId, workers: workersId, dag: dagId });
    const panes = await this.layoutPanes(tabId);
    if (!panes.some(pane => pane?.pane_id === todoId)) return null;
    const coordination = await this.scopedCoordinationRecord(tabId);
    const collab = coordination && (!coordination.workers || coordination.workers === workersId) &&
      (!coordination.dag || coordination.dag === dagId) ? coordination.paneId : null;
    const lower = deriveLowerBand({ panes, todo: todoId, workers: workersId, dag: dagId, collab });
    if (!lower) return null;
    // The reads above can yield. Revalidate everything mutable immediately before
    // settle, with no await between this check and publication. A reply that
    // arrives after invalidation is abandoned, not resurrected as settled.
    const finalRecord = await readJson(this.recordFile);
    if (this.stopped || this.layoutActive || this.arrangement?.moving ||
      this.lifetimeId !== lifetime || this.claim?.tabId !== tabId) return null;
    if (!sameScope(scope, this.recoveryScope(tabId)) || !authorityCurrent()) return null;
    // Current ownership is the map's current core records, not the pair snapshot
    // captured before the awaits. A replaced Todo id, a new pane id, an unowned
    // role, a removed pair entry, a pair/map mismatch or any new binding revision
    // abandons the old layout entirely.
    const currentTodo = this.ownedRoles.get('todo');
    if (currentTodo) { if (currentTodo.paneId !== todoId) return null; }
    else if (ownedTodo) return null;
    const currentWorkers = this.ownedRoles.get('workers');
    const currentDag = this.ownedRoles.get('dag');
    if (!validOwned(currentWorkers) || !validOwned(currentDag)) return null;
    if (currentWorkers.paneId !== workersId || currentDag.paneId !== dagId) return null;
    if (!this.managed) {
      if (nonManagedPair.workers && !pair?.workers) return null;
      if (nonManagedPair.dag && !pair?.dag) return null;
      if (pair?.workers && pair.workers.paneId !== currentWorkers.paneId) return null;
      if (pair?.dag && pair.dag.paneId !== currentDag.paneId) return null;
    }
    if (bindingRevision({ scope: this.recoveryScope(tabId), todo: todoId,
      workers: currentWorkers.paneId, dag: currentDag.paneId }) !== binding) return null;
    if (!this.recordMatches(finalRecord) || finalRecord.manualClose === true || finalRecord.paneId !== todoId)
      return null;
    if (!panes.some(pane => pane?.pane_id === currentWorkers.paneId) ||
      !panes.some(pane => pane?.pane_id === currentDag.paneId)) return null;
    return this.recovery.settle(scope, { explicit, todo: todoId, workers: workersId, dag: dagId,
      band: lower.band, cells: lower.cells }, { commandId });
  }
  async recoveryResult({ scope, commandId, layout }) {
    const evidence = layout ? await this.completionEvidence({ scope, layout }) : null;
    // Final gate after every awaited read: re-run the synchronous verdict over
    // the live state. A core or pane transition during a read therefore yields a
    // truthful waiting result, never a stale 4/4.
    const finalEvidence = evidence?.recheck ? evidence.recheck() : evidence;
    // The phase alone is not authority: re-read the live lease/custody at the
    // same synchronous point as the pane verdict so a loss during any awaited
    // read is reported instead of rounded up.
    const coreFailure = finalEvidence ? finalEvidence.coreFailure ?? null : this.completionAuthority();
    const coreReady = finalEvidence ? finalEvidence.coreReady === true :
      (this.managed ? this.actor?.snapshot?.phase === 'READY' : false);
    const collabCell = layout?.cells?.find(cell => cell.role === 'collab') ?? null;
    const complete = Boolean(layout && coreReady && finalEvidence?.current === true);
    const result = { v: 1, kind: 'result', scope, commandId,
      status: layout ? 'settled' : 'waiting', fourPaneComplete: complete,
      core: { ready: Boolean(coreReady), workers: layout?.workers ?? null, dag: layout?.dag ?? null },
      collab: { current: finalEvidence?.panes.collab?.current === true, paneId: collabCell?.paneId ?? null } };
    // A fully proven four-pane state keeps the exact settled shape. Anything
    // less names the failing pane and condition instead of rounding up to 4/4.
    if (layout && !complete) {
      const waiting = [...(finalEvidence?.waiting ?? [])];
      if (!coreReady) waiting.unshift({ role: 'core', paneId: null,
        condition: coreFailure?.condition ?? 'core-not-ready',
        detail: coreFailure?.detail ?? (this.managed ? 'the core phase is not READY' : 'the core is not managed') });
      result.waiting = waiting;
      result.reason = waiting.map(entry => `${entry.role}${entry.paneId ? ` ${entry.paneId}` : ''}: ` +
        `${entry.detail ?? entry.condition}`).join('; ');
    }
    return result;
  }
  // A caller-supplied command id is an explicit intent. Re-delivery joins the
  // existing waiter; the shared coordinator serializes passes and keeps at most
  // one coalesced rerun queued for fresh ids.
  requestRecovery({ commandId, explicit = true, forceRoles } = {}) {
    const id = commandId ?? randomUUID(), scope = this.recoveryScope();
    if (!validScope(scope)) return Promise.reject(protocolError('RECOVERY_SCOPE_INVALID',
      'the current recovery scope is not settled yet'));
    return this.recovery.request({ commandId: id, scope, reconcile: async ({ commandId: passCommandId }) => {
      const before = this.recovery.inspect().passSeq;
      const roles = forceRoles ?? (explicit ? [...CORE_ORDER] : []);
      await this.schedule({ forceRoles: roles, commandId: passCommandId });
      const after = this.recovery.inspect();
      const layout = after.passSeq > before && after.last?.commandId === passCommandId
        ? this.recovery.replay(this.recoveryScope()) : null;
      const result = await this.recoveryResult({ scope: this.recoveryScope(), commandId: passCommandId, layout });
      try { assertResult(result); this.pi?.events?.emit(RECOVERY_V1.result, result); }
      catch (error) { this.warnOnce(error); }
      return result;
    } });
  }
  recordMatches(record) {
    return record?.schema === 'omo-herdr-dashboard/pane/1' && record.role === 'todo' &&
      sameDashboardOwner(record, this.binding) && pathKey(record.statePath) === pathKey(this.stateFile) &&
      pathKey(record.viewerPath) === pathKey(this.viewer) && typeof record.launchToken === 'string';
  }
  async verify(record, pid) {
    if (!this.recordMatches(record) || !record.paneId) return null;
    const member = await this.membership(record.paneId);
    if (record.tabId !== member.tabId) return null;
    const state = await readRoleState(this.stateFile);
    if (!state || !sameDashboardOwner(state, record) || state.launchToken !== record.launchToken ||
      state.paneId !== record.paneId || state.role !== record.role) return null;
    const info = await this.herdr('process-info', '--pane', record.paneId);
    if (this.managed) {
      const identity = await legacyPaneIdentity(info, { ...record, processId: pid ?? record.processId }, this.inspectShell);
      return identity?.viewer ? { pid: identity.viewer.pid } : null;
    }
    return paneViewerProcess(info, record, pid, this.inspectShell);
  }
  async handleControl(message, socket) {
    if (message?.type === 'custody-disposed') {
      const subscriber = this.retirementSubscribers.get(socket);
      if (!subscriber?.custody || message.nonce !== subscriber.nonce || !sameControl(message, subscriber.offer))
        return socket.destroy();
      subscriber.invalidated = true;
      send(socket, { ...subscriber.offer, type: 'custody-disposed', nonce: subscriber.nonce });
      return;
    }
    if (message?.type === 'custody-invalidated') {
      const subscriber = this.retirementSubscribers.get(socket), operation = message.operation;
      const pending = subscriber?.custody && subscriber.pending.get(operation?.id);
      if (this.stopped || !pending || message.nonce !== subscriber.nonce ||
        !sameControl(message, subscriber.offer) ||
        !['id', 'ownerNonce', 'sessionId', 'generation', 'participant', 'lifetimeId', 'role', 'type', 'preparedRevision']
          .every(key => operation[key] === pending.operation[key]) ||
        JSON.stringify(operation.target) !== JSON.stringify(pending.operation.target))
        return socket.destroy();
      pending.accept();
      return;
    }
    if (message?.type === 'observe-retirement' && !this.stopped) {
      if (message.custody === true) {
        await this.managedIdle(); await this.roleTail; await this.provider?.idle?.();
        if (this.stopped || this.actor?.blocked || this.actor?.draining ||
          this.actor?.snapshot.operations.some(operation => operation.status !== 'SETTLED'))
          return socket.destroy();
      }
      const record = await readJson(this.recordFile), state = await readRoleState(this.stateFile);
      if (!this.managed || !this.actor?.leaseHeld || !sameControl(message, state?.controlOffer) ||
        typeof message.nonce !== 'string' || !message.nonce || !await this.verify(record, record.processId))
        return socket.destroy();
      this.retirementSubscribers.set(socket, { offer: state.controlOffer, nonce: message.nonce,
        recordSHA256: createHash('sha256').update(await readFile(this.recordFile)).digest('hex'),
        ...(message.custody === true ? { custody: true, pending: new Map() } : {}) });
      socket.once('close', () => {
        const subscriber = this.retirementSubscribers.get(socket);
        this.retirementSubscribers.delete(socket);
        if (subscriber?.custody && !subscriber.invalidated && !this.stopped && this.actor?.leaseHeld) {
          this.custodyLost = true; this.recoveryBlocked = true; this.status = 'BLOCKED';
          this.bridge?.readinessChanged();
        }
      });
      if (message.custody === true) this.custodyLost = false;
      send(socket, { ...state.controlOffer, type: 'observing', nonce: message.nonce,
        ...(message.custody === true ? { slot: this.actor.snapshot } : {}) });
      return;
    }
    if (!['register', 'close'].includes(message?.type) ||
      this.stopped && !(this.managed && message.type === 'register' && this.pendingLaunches.has(message.launchToken))) return socket.destroy();
    // A viewer that registers or closes has just started or is about to stop: read fresh rows.
    bumpProcessQueryEpoch();
    const registration = this.roleWrite(async () => {
      const record = await readJson(this.recordFile);
      const state = await readRoleState(this.stateFile);
      if (!sameControl(message, state?.controlOffer) || message.scopeEpoch !== this.scopeEpoch ||
        !this.recordMatches(record) || record.launchToken !== message.launchToken ||
        !Number.isSafeInteger(message.pid) || !(await freshReads(() => this.verify(record, message.pid)))) return socket.destroy();
      if (message.scopeEpoch !== this.scopeEpoch) return socket.destroy();
      if (this.managed && message.type === 'register' && this.pendingLaunches.has(message.launchToken))
        this.launchRegistrations.set(message.launchToken, registration);
      if (this.stopped) {
        send(socket, { ...state.controlOffer, type: 'registered' });
        this.events.emit(`created:${message.launchToken}`, record.paneId);
        return;
      }
      if (message.type === 'register') {
        const ready = { ...record, processId: message.pid, ready: true };
        if (this.managed) {
          const rows = await (this.inspectShell ?? nativeShellProcesses)(record.creationProcessId);
          ready.viewerBirth = nativeIdentity(rows.find(row => row.ProcessId === message.pid)).birth;
        }
        await writeJson(this.recordFile, ready);
        this.ownedRoles.set('todo', ready);
        send(socket, { ...state.controlOffer, type: 'registered' });
        this.events.emit(`created:${message.launchToken}`, record.paneId);
        if (this.managed && this.actor.blocked) await this.observeRegisteredRun('todo', ready);
        if (this.managed) await this.registerManagedRole('todo', () => this.nativePane(record.paneId, ready));
        if (this.managed) await this.holdRoleViewer('todo', ready);
        this.bridge?.readinessChanged();
      } else {
        this.closureVersions.set('todo', (this.closureVersions.get('todo') ?? 0) + 1);
        if (this.managed) await this.managedWrite(actor => actor.closeRole(actor.capability('todo', 'todo'),
          { paneId: record.paneId, reason: 'q' }));
        await writeJson(this.recordFile, { ...record, manualClose: true });
        this.ownedRoles.delete('todo');
        this.bridge?.readinessChanged();
        const target = this.managed ? await this.nativePane(record.paneId, record) : null;
        const reply = () => new Promise(resolve => socket.end(`${JSON.stringify({ ...state.controlOffer, type: 'closed' })}\n`, resolve));
        if (this.managed) await this.mutate('todo', 'close', target, ['close', record.paneId], null, reply);
        else { await reply(); await this.herdr('close', record.paneId); }
      }
    });
    return registration;
  }
  roleState(record, connected = true) {
    const slot = this.actor?.snapshot;
    return { schema: 'omo-herdr-dashboard/role/1', ...this.binding, scopeEpoch: this.scopeEpoch,
      ...(this.managed && this.actor ? { managedScope: { ownerNonce: this.actor.snapshot.ownerNonce,
        sessionId: this.binding.sessionId, generation: this.actor.snapshot.generation,
        participant: 'todo', lifetimeId: this.lifetimeId } } : {}),
      role: 'todo', tabId: record.tabId, launchToken: record.launchToken, paneId: record.paneId,
      ...(connected && this.managed && !this.actor?.blocked && !this.actor?.draining &&
        slot?.operations.every(operation => operation.status === 'SETTLED') &&
        slot.roles.todo?.lifetimeId === this.lifetimeId && slot.roles.todo.viewer?.pid === record.processId
        ? { custodyBinding: { stateDir: this.stateDir, slotStateDir: this.stateDir,
          slotPath: this.actor.paths.slotPath, recordPath: this.recordFile, statePath: this.stateFile,
          parent: slot.parent, ownerNonce: slot.ownerNonce, generation: slot.generation, revision: slot.revision } } : {}),
      connected, language: this.language, updatedAt: new Date().toISOString(), availability: 'waiting',
      cachePath: this.storage.path, prefsPath: `${this.stateFile}.view.json`,
      presenceOffer: connected ? this.presence.offer('todo') : null,
      controlOffer: connected ? { pipePath: this.controlPath, v: 1, token: this.controlToken,
        ownerKey: this.binding.ownerKey, sessionId: this.binding.sessionId, scopeEpoch: this.scopeEpoch,
        role: 'todo', launchToken: record.launchToken, paneId: record.paneId, statePath: this.stateFile } : null };
  }
  bind(pi) {
    const accept = event => {
      if (this.stopped || event?.v !== 1 || event.kind !== 'dag-provider' ||
        !sameDashboardOwner(event, this.binding) || !event.provider) return;
      if (this.provider?.lifetimeId === event.provider.lifetimeId) return;
      this.offSource?.();
      this.provider = event.provider;
      const provider = this.provider;
      this.source = provider.getSource();
      this.offSource = provider.subscribeSource(source => {
        if (this.stopped || this.provider !== provider) return;
        const coverageChanged = this.source?.connected !== source.connected ||
          JSON.stringify(this.source?.runs) !== JSON.stringify(source.runs);
        this.source = source;
        // Worker progress is already saved in the same viewers' role files.
        // It does not replace their identities or change DAG coverage.
        if (this.managed && coverageChanged) { this.sourceDirty = true; this.sourceRefreshing = true; }
        this.bridge?.readinessChanged();
        void this.schedule().catch(() => {});
      });
      provider.setCoordinator(this.coordinator());
      void this.schedule().catch(() => {});
    };
    this.unsubscribers.push(pi.events.on(RECOVERY_V1.layoutQuery, event => {
      if (this.stopped || queryIssues(event).length) return;
      const scope = this.recoveryScope();
      if (!sameScope(event.scope, scope)) return;
      if (this.managed && (!this.actor?.leaseHeld || this.actor.blocked || this.actor.draining)) return;
      const replay = this.recovery.replay(scope);
      if (replay) pi.events.emit(RECOVERY_V1.layout, replay);
      else pi.events.emit(RECOVERY_V1.result, { v: 1, kind: 'result', scope, commandId: event.commandId,
        status: 'waiting', fourPaneComplete: false, core: { ready: false }, collab: { current: false } });
    }), pi.events.on(dashboardChannels.provider, accept),
      pi.events.on(dashboardChannels.reply, accept),
      pi.events.on(dashboardChannels.query, event => {
        if (!this.stopped && event?.v === 1 && event.kind === 'todo-owner' && sameDashboardOwner(event, this.binding))
          pi.events.emit(dashboardChannels.reply, { v: 1, requestId: event.requestId, kind: 'todo-owner',
            ...this.binding, provider: this.coordinator() });
      }),
      pi.events.on(dashboardChannels.retired, event => {
        if (sameDashboardOwner(event, this.binding) && event.lifetimeId === this.provider?.lifetimeId) {
          this.offSource?.(); this.provider = null; this.source = null; this.status = 'layout-waiting';
          this.ownedRoles.delete('dag'); this.ownedRoles.delete('workers');
          this.bridge?.readinessChanged();
        }
      }));
    this.pi = pi;
    pi.events.emit(dashboardChannels.provider, { v: 1, kind: 'todo-owner', ...this.binding, provider: this.coordinator() });
    pi.events.emit(dashboardChannels.query, { v: 1, requestId: randomUUID(), kind: 'dag-provider', ...this.binding });
  }
  coordinator() {
    const owner = this;
    return { lifetimeId: this.lifetimeId, binding: this.binding,
      get actor() { return owner.actor; },
      get todoRecord() { return { path: owner.recordFile, matches: record => owner.recordMatches(record) }; },
      gate: (role, type, target) => owner.layoutGate(role, type, target, passes.getStore()),
      passScope: (explicit, job) => passes.run({ explicit: Boolean(explicit || passes.getStore()?.explicit) }, job),
      join: lifetime => this.managedWrite(actor => actor.join('dag', lifetime)),
      perform: (role, type, target, dispatch) => this.perform(role, type, target, dispatch),
      mutate: (role, type, target, args, effect, beforeSubmit, check) => this.mutate(role, type, target, args, effect, beforeSubmit, check),
      target: paneId => this.currentTarget(paneId),
      reconcileLegacy: (root, reader, viewer) => this.reconcileLegacy(root, reader, viewer),
      obsoleteLocator: record => this.managedWrite(actor =>
        actor.obsoleteLocator(actor.capability(record.role === 'todo' ? 'todo' : 'dag', record.role), record)),
      retireRetainedRole: (retained, prepare) => this.retireRetainedRole(retained, prepare),
      registerRole: (role, readTarget) => this.registerManagedRole(role, readTarget),
      holdRoleViewer: (role, record) => this.holdRoleViewer(role, record),
      viewerDeath: role => this.viewerDeaths.get(role),
      repairedViewer: role => this.viewerDeaths.delete(role),
      observeRegisteredRun: (role, record) => this.observeRegisteredRun(role, record),
      closeRole: (role, paneId) => this.managedWrite(async actor => {
        await actor.closeRole(actor.capability(role === 'todo' ? 'todo' : 'dag', role), { paneId, reason: 'q' });
        this.ownedRoles.delete(role);
        this.bridge?.readinessChanged();
      }),
      noteRoleClosure: role => this.closureVersions.set(role, (this.closureVersions.get(role) ?? 0) + 1),
      terminalAuthority: (participant, lifetimeId, records) => this.terminalAuthority(participant, lifetimeId, records),
      ownsPublisherClaim: () => this.ownsPublisherClaim(),
      assertCurrent: () => this.assertManaged(),
      claim: { ...this.binding, lifetimeId: this.lifetimeId },
      closeTodoForLayout: () => this.roleWrite(async () => {
        if (this.managed) return false;
        const record = await readJson(this.recordFile);
        if (this.stopped || !this.recordMatches(record) || record.manualClose ||
          record.scopeEpoch !== this.scopeEpoch || !await this.verify(record)) return false;
        await writeJson(this.recordFile, { ...record, ready: false, manualClose: true });
        this.ownedRoles.delete('todo'); this.bridge?.readinessChanged();
        await this.herdr('close', record.paneId);
        return true;
      }),
      inspectTodo: async () => {
        if (this.stopped) return null;
        const record = await readJson(this.recordFile);
        if (!this.recordMatches(record) || record.manualClose || record.scopeEpoch !== this.scopeEpoch) return null;
        if (owner.actor?.snapshot.closures?.some(entry => entry.role === 'todo' &&
          entry.paneId === record.paneId && entry.sessionId === record.sessionId &&
          entry.reason === 'pane_not_found')) return null;
        return await this.verify(record) ? record : null;
      },
      schedule: options => this.schedule(options) };
  }
  async reconcileLegacy(root, reader = legacyPaneIdentity, viewer = this.viewer) {
    const entries = await discoverLegacyRecords(root, this.binding), observations = [];
    for (const { path, record } of entries) {
      if (!roles.has(record.role) || !record.paneId || record.manualClose) continue;
      const registered = this.actor?.snapshot.roles[record.role];
      try { await this.herdr('get', record.paneId); }
      catch (error) {
        if (!authoritativePaneNotFound(error)) throw blocked(error);
        if (!this.actor) throw blocked('legacy tombstone requires held owner');
        await this.managedWrite(actor => actor.obsoleteLocator(actor.capability(record.role === 'todo' ? 'todo' : 'dag', record.role), record));
        observations.push({ path, paneId: record.paneId, state: 'obsolete', code: error.code });
        continue;
      }
      if (registered?.generation === this.actor.snapshot.generation && registered.paneId === record.paneId ||
        this.ownedRoles.get(record.role)?.launchToken === record.launchToken && record.scopeEpoch === this.scopeEpoch) continue;
      if (typeof this.node === 'function') this.node = await this.node();
      const oldSource = pathKey(record.viewerPath).match(/^(.*\/)generation-(\d{6})(\/.*)$/);
      const currentSource = pathKey(viewer).match(/^(.*\/)generation-(\d{6})(\/.*)$/);
      const upgrade = this.claimRetired && sameDashboardOwner(record, this.binding) &&
        oldSource && currentSource && oldSource[1] === currentSource[1] && oldSource[3] === currentSource[3] &&
        Number(oldSource[2]) < Number(currentSource[2]) && registered?.generation < this.actor.snapshot.generation &&
        registered.paneId === record.paneId && registered.tabId === record.tabId &&
        registered.sessionId === record.sessionId && registered.statePath === record.statePath &&
        registered.launchToken === record.launchToken && registered.viewer?.pid === record.processId &&
        registered.viewer.birth === record.viewerBirth &&
        pathKey(registered.viewer.executable) === pathKey(record.nodePath);
      if (record.schema !== 'omo-herdr-dashboard/pane/1' ||
        !upgrade && (pathKey(record.viewerPath) !== pathKey(viewer) || pathKey(record.nodePath) !== pathKey(this.node))) {
        await this.actor?.diagnose('legacy-source', { code: 'SOURCE_MISMATCH' }, { role: record.role });
        throw blocked('legacy executable or viewer source mismatch');
      }
      const member = await this.membership(record.paneId);
      if (member.tabId !== record.tabId || record.paneId === this.binding.parentPaneId)
        throw blocked('legacy membership changed');
      const info = await this.herdr('process-info', '--pane', record.paneId);
      let identity = await reader(info, record, this.inspectShell), empty = false;
      // A successor has no old in-memory death observer. A held claim with
      // settled predecessor effects may reuse only that registered empty shell;
      // it cannot adopt an unregistered record or retire a guessed viewer PID.
      if (!identity && this.claimRetired && this.ownsPublisherClaim() &&
        !this.actor.blocked && !this.actor.draining && registered?.generation < this.actor.snapshot.generation &&
        registered.paneId === record.paneId && registered.sessionId === record.sessionId &&
        registered.statePath === record.statePath && registered.launchToken === record.launchToken &&
        registered.viewer?.pid === record.processId && registered.viewer.birth === record.viewerBirth &&
        pathKey(registered.viewer.executable) === pathKey(record.nodePath) &&
        this.actor.snapshot.operations.every(operation => operation.status === 'SETTLED')) {
        const observed = await observePaneShell(info, record, this.inspectShell);
        const foreground = info.process_info.foreground_processes ?? [];
        if (observed && !observed.children.length && foreground.length === 1 &&
          foreground[0].pid === record.creationProcessId && observed.shell.Birth === registered.shell.birth &&
          observed.shell.ProcessId === registered.shell.pid) {
          identity = { shell: nativeIdentity(observed.shell) }; empty = true;
        }
      }
      if (!identity) throw blocked('legacy shell or viewer identity unverified');
      if (upgrade && (registered.shell.pid !== identity.shell.pid || registered.shell.birth !== identity.shell.birth ||
        !empty && JSON.stringify(registered.viewer) !== JSON.stringify(identity.viewer)))
        throw blocked('registered older generation native role binding changed');
      const state = await readJson(record.statePath);
      if (!state || !sameDashboardOwner(state, record) || state.role !== record.role ||
        state.paneId !== record.paneId || state.launchToken !== record.launchToken)
        throw blocked('legacy role state changed');
      if (upgrade && (!this.managed || !this.actor.leaseHeld || this.actor.blocked || this.actor.draining ||
        state.managedScope?.generation !== registered.generation ||
        state.managedScope.sessionId !== registered.sessionId ||
        state.managedScope.participant !== registered.participant ||
        state.managedScope.lifetimeId !== registered.lifetimeId ||
        registered.prefsPath !== state.prefsPath ||
        registered.cachePath !== (record.role === 'todo' ? state.cachePath : record.statePath)))
        throw blocked('retained generation source is outside predecessor claim');
      if (empty && (state.managedScope?.generation !== registered.generation ||
        state.managedScope.sessionId !== registered.sessionId ||
        state.managedScope.participant !== registered.participant ||
        state.managedScope.lifetimeId !== registered.lifetimeId ||
        registered.prefsPath !== state.prefsPath ||
        registered.cachePath !== (record.role === 'todo' ? state.cachePath : record.statePath)))
        throw blocked('empty retained role source is outside predecessor claim');
      if (record.sessionId !== this.binding.sessionId && (!this.claimRetired ||
        registered?.paneId !== record.paneId || registered.sessionId !== record.sessionId ||
        registered.statePath !== record.statePath || registered.launchToken !== record.launchToken ||
        registered.shell.pid !== identity.shell.pid || registered.shell.birth !== identity.shell.birth ||
        !empty && JSON.stringify(registered.viewer) !== JSON.stringify(identity.viewer)))
        throw blocked('prior legacy conversation retirement or native role binding unverified');
      if (record.prepared && !identity.viewer || this.claimRetired && registered?.paneId === record.paneId) {
        const retained = { path, record, paneId: record.paneId, state: 'retained', identity, empty, upgrade: Boolean(upgrade) };
        if (record.sessionId !== this.binding.sessionId || empty || upgrade) {
          retained.target = Object.fromEntries(['paneId', 'tabId', 'shell', ...(empty ? [] : ['viewer']), 'statePath',
            'sourcePath', 'cachePath', 'prefsPath', 'launchToken', 'controlToken'].map(key => [key, registered[key]]));
          this.retainedTargets.set(record.paneId, retained);
        }
        observations.push(retained);
        continue;
      }
      const request = { stateDir: root, recordPath: path, parent: this.actor.snapshot.parent,
        herdr: this.herdr, slotPath: this.actor.paths.slotPath, slotStateDir: this.stateDir };
      let capture = await this.legacyObservation(path, request);
      let retirement = await observeLegacyRetirement(capture, request);
      if (!retirement && this.recoveryRetry && this.legacyObservationState(capture) === 'invalid') {
        capture = await this.legacyObservation(path, request);
        retirement = await observeLegacyRetirement(capture, request);
      }
      if (!retirement) throw blocked('legacy producer retirement and settled effects unverified');
      observations.push({ path, paneId: record.paneId, identity, retirement });
    }
    return observations;
  }
  async observeRetainedTarget(retained) {
    const { record, identity, target } = retained;
    const member = await this.membership(record.paneId);
    const state = await readRoleState(record.statePath);
    if (member.tabId !== record.tabId || !state || !sameDashboardOwner(state, record) ||
      state.role !== record.role || state.paneId !== record.paneId ||
      state.launchToken !== record.launchToken || state.scopeEpoch !== record.scopeEpoch)
      throw blocked('retained role source changed');
    const info = await this.herdr('process-info', '--pane', record.paneId);
    let actual;
    if (retained.empty) {
      const observation = await observePaneShell(info, record, this.inspectShell);
      const foreground = info.process_info.foreground_processes ?? [];
      if (observation && !observation.children.length && foreground.length === 1 &&
        foreground[0].pid === record.creationProcessId) actual = { shell: nativeIdentity(observation.shell) };
    } else actual = await legacyPaneIdentity(info, record, this.inspectShell);
    if (JSON.stringify(actual) !== JSON.stringify(identity)) throw blocked('retained role native identity changed');
    return structuredClone(target);
  }
  async holdRoleViewer(role, record) {
    const target = role === 'todo' ? await this.nativePane(record.paneId, record) : await this.provider.observeTarget(record);
    const old = this.viewerHolders.get(role);
    if (old && JSON.stringify(old.identity) === JSON.stringify(target.viewer)) return;
    const observer = await observeNativeExit(target.viewer, true);
    if (old) await old.observer.dispose();
    this.viewerHolders.set(role, { identity: target.viewer, target, record, observer });
  }
  async observeViewerDeath(role) {
    const held = this.viewerHolders.get(role);
    if (!held || this.viewerDeaths.has(role) || this.stopped) return;
    const exit = await held.observer.death();
    if (exit.disposed || !Number.isInteger(exit.exitCode) || this.stopped ||
      this.viewerHolders.get(role) !== held) return;
    this.viewerDeaths.set(role, { ...held, exit });
    if (role === 'todo') this.custodyLost = false;
    this.recoveryChanged = true;
    void this.schedule().catch(() => {});
  }
  async retireRetainedRole(retained, prepare) {
    const { record, identity, target } = retained;
    const before = (await this.herdr('layout', '--pane', record.paneId))?.layout;
    const layoutBinding = layout => JSON.stringify(layout && {
      tabId: layout.tab_id, workspaceId: layout.workspace_id, area: layout.area,
      zoomed: layout.zoomed, focusedPaneId: layout.focused_pane_id,
      panes: layout.panes?.map(pane => ({ id: pane.pane_id, rect: pane.rect })),
      splits: layout.splits,
    });
    if (before?.tab_id !== record.tabId || !before.panes?.some(pane => pane.pane_id === record.paneId))
      throw blocked('retained retirement geometry unavailable');
    await this.managedWrite(async () => {
      await this.observeRetainedTarget(retained);
    });
    if (retained.empty) {
      const member = await this.membership(record.paneId);
      const observation = await observePaneShell(await this.herdr('process-info', '--pane', record.paneId), record, this.inspectShell);
      if (member.tabId !== record.tabId || !observation || observation.children.length ||
        JSON.stringify(nativeIdentity(observation.shell)) !== JSON.stringify(identity.shell) ||
        layoutBinding((await this.herdr('layout', '--pane', record.paneId))?.layout) !== layoutBinding(before))
        throw blocked('empty retained shell or geometry changed before relaunch');
      this.retainedTargets.delete(record.paneId);
      await this.managedWrite(prepare);
      return;
    }
    await this.perform(record.role, 'retire', target, async () => {
      const receipt = await this.retireViewer(record, identity);
      if (receipt.manual) throw blocked('manual closure superseded retirement');
      const member = await this.membership(record.paneId);
      const info = await this.herdr('process-info', '--pane', record.paneId);
      const observation = await observePaneShell(info, record, this.inspectShell);
      if (member.tabId !== record.tabId || !observation || observation.children.length ||
        JSON.stringify(nativeIdentity(observation.shell)) !== JSON.stringify(identity.shell) ||
        layoutBinding((await this.herdr('layout', '--pane', record.paneId))?.layout) !== layoutBinding(before))
        throw blocked('original viewer exit did not retain the exact shell and geometry');
      this.retainedTargets.delete(record.paneId);
      return { result: receipt, effects: Promise.resolve() };
    });
    await this.managedWrite(prepare);
  }
  async replaceRetainedTodo(retained) {
    const old = retained.record;
    if (typeof this.node === 'function') this.node = await this.node();
    const record = { ...old, ...this.binding, scopeEpoch: this.scopeEpoch,
      statePath: this.stateFile, viewerPath: this.viewer, nodePath: this.node, launchToken: randomBytes(24).toString('hex'),
      prepared: true, ready: false, manualClose: false, layoutClaim: this.claim ?? old.layoutClaim };
    delete record.processId; delete record.viewerBirth;
    await this.retireRetainedRole(retained, () => this.roleWrite(async () => {
      await writeJson(this.recordFile, record);
      await writeJson(this.stateFile, this.roleState(record));
    }));
    return this.launchTodo(record);
  }
  legacyObservationState(observation) {
    const captured = retirementCaptures.get(observation);
    return captured?.alive() && !captured.invalid ? 'pending' : 'invalid';
  }
  captureLegacyProducer(request) { return captureLegacyProducer(request); }
  async legacyObservation(path, request) {
    let capture = this.legacyObservations.get(path);
    if (capture && this.recoveryRetry && this.legacyObservationState(capture) === 'invalid') {
      await capture.dispose();
      this.legacyObservations.delete(path);
      capture = null;
    }
    if (!capture) {
      capture = await this.captureLegacyProducer(request);
      this.legacyObservations.set(path, capture);
      void capture.release().then(() => {
        if (!this.stopped) { this.recoveryChanged = true; void this.schedule().catch(() => {}); }
      }, () => {});
    }
    return capture;
  }
  async observeRegisteredRun(role, record) {
    const operation = this.actor.snapshot.operations.find(op => op.status === 'UNKNOWN' && op.type === 'run' &&
      op.role === role && op.target.paneId === record.paneId && op.target.launchToken === record.launchToken &&
      op.ownerNonce === this.actor.snapshot.ownerNonce && op.generation === this.actor.snapshot.generation &&
      op.lifetimeId === this.actor.snapshot.participants[op.participant]?.lifetimeId);
    const retained = operation && this.effectObservations.get(operation.id);
    if (!retained || this.stopped || this.actor.draining || !this.actor.leaseHeld) return;
    const actual = await this.observeManaged({ target: operation.target });
    if (JSON.stringify(actual.target) !== JSON.stringify(operation.target)) return;
    retained.settled = true;
    this.recoveryChanged = true;
    void this.schedule().catch(() => {});
  }
  watchRecoveryMembership() {
    if (this.recoveryWatch || this.stopped || !this.actor) return this.recoveryWatch?.ready;
    this.recoveryWatch = { ready: (async () => {
      const before = (await this.herdr('layout', '--pane', this.binding.parentPaneId))?.layout;
      if (before?.tab_id !== this.actor.snapshot.parent.tabId) throw blocked('recovery membership unavailable');
      let members = before.panes.map(pane => pane.pane_id).sort().join('\n');
      const shape = layout => JSON.stringify([layout.area, layout.panes.map(pane => [pane.pane_id, pane.rect])]);
      let geometry = shape(before);
      const socket = net.createConnection(process.platform === 'win32'
        ? `\\\\.\\pipe\\${this.binding.socketPath}` : this.binding.socketPath);
      this.recoveryWatch.socket = socket;
      let accept, reject;
      const ready = new Promise((resolve, fail) => { accept = resolve; reject = fail; });
      const signal = AbortSignal.any([this.stopSignal.signal, AbortSignal.timeout(10000)]), id = randomUUID();
      const abort = () => { reject(signal.reason); socket.destroy(); };
      signal.addEventListener('abort', abort, { once: true });
      socket.once('error', reject);
      socket.once('close', () => reject(blocked('recovery subscription closed')));
      socket.once('connect', () => send(socket, { id, method: 'events.subscribe',
        params: { subscriptions: [{ type: 'layout.updated' }] } }));
      receive(socket, message => {
        if (message.id === id && message.error) reject(blocked(message.error.code));
        if (message.id === id && message.result?.type === 'subscription_started') accept();
        const layout = message.event === 'layout_updated' && message.data?.layout;
        if (layout?.tab_id !== before.tab_id || !layout.panes?.some(pane => pane.pane_id === this.binding.parentPaneId)) return;
        const current = layout.panes.map(pane => pane.pane_id).sort().join('\n');
        // A band waiting for size also retries on a geometry-only change.
        const resized = shape(layout) !== geometry;
        geometry = shape(layout);
        if (current === members && !(resized && this.sizeWaiting)) return;
        members = current;
        if (!this.stopped && (this.recoveryBlocked || this.sizeWaiting)) {
          if (this.recoveryBlocked) this.recoveryChanged = true;
          void this.schedule().catch(() => {});
        }
      });
      try { await ready; } finally { signal.removeEventListener('abort', abort); }
    })() };
    this.recoveryWatch.ready.catch(() => {});
    return this.recoveryWatch.ready;
  }
  schedule({ forceRoles = [], commandId } = {}) {
    const closureVersions = new Map(this.closureVersions);
    const pass = async () => {
      if (this.restartReopening && !forceRoles.length) return;
      const forceCurrent = role => forceRoles.includes(role) &&
        (closureVersions.get(role) ?? 0) === (this.closureVersions.get(role) ?? 0);
      if (this.managed) {
        if (forceRoles.length) this.warned.clear(); // An explicit /dashboard always gets its answer.
        if (this.actor?.blocked && this.placementWaiting && this.actor.snapshot.operations.some(operation =>
          operation.id === this.placementWaiting.operationId && operation.status === 'UNKNOWN')) {
          throw Object.assign(new Error(this.placementWaiting.message), { code: 'LAYOUT_WAIT' });
        }
        if (this.recoveryBlocked && !forceRoles.length && !this.recoveryChanged) {
          const actor = this.actor, slot = actor?.snapshot;
          // A latch only waits on a really unavailable actor. On a healthy READY
          // actor it is stale: clear it and take the retry path once instead.
          if (!actor?.leaseHeld || actor.blocked || actor.draining || this.custodyLost || this.managedBlocked ||
            slot.phase !== 'READY' || slot.operations.some(op => op.status !== 'SETTLED'))
            throw blocked('recovery awaits relevant membership/control change or explicit dashboard', 'RECOVERY_BLOCKED');
          this.recoveryBlocked = false;
          this.recoveryChanged = true;
        }
        const retry = forceRoles.length || this.recoveryChanged;
        this.recoveryRetry = Boolean(retry);
        this.recoveryChanged = false;
        if (this.actor?.blocked && retry) {
          for (const operation of this.actor.snapshot.operations.filter(op => ['UNKNOWN', 'DISPATCHED'].includes(op.status)))
            await this.actor.reconcile(operation.id);
        }
        this.assertManaged();
        try {
          const retainedTodo = await this.reconcileLegacy(this.stateDir);
          for (const retained of retainedTodo) if ((retained.record?.sessionId !== this.binding.sessionId || retained.empty || retained.upgrade) &&
            retained.state === 'retained') await this.replaceRetainedTodo(retained);
          const provider = this.provider;
          if (provider) {
            await provider.sourceReady;
            if (this.stopped || this.provider !== provider) return;
            await this.managedWrite(actor => actor.join('dag', provider.lifetimeId));
            const retainedPair = await provider.preflightRecovery?.();
            await provider.replaceRetained?.(retainedPair);
          }
          this.recoveryBlocked = false;
        } catch (error) {
          if (error?.code === 'LAYOUT_WAIT' && this.actor?.leaseHeld && !this.actor.blocked && !this.actor.draining)
            throw error;
          this.recoveryBlocked = true;
          await this.actor?.diagnose('schedule', error?.code ? error : { code: 'RECOVERY_BLOCKED' });
          await this.watchRecoveryMembership().catch(() => {});
          throw error;
        }
      }
      // Existing Todo must receive a new scope/offer even if a sibling role
      // was manually closed. Layout anchors only govern creation, not rebinding.
      const existing = await this.ensureTodo(null, false);
      const provider = this.provider;
      if (!provider) { this.status = 'layout-waiting'; return; }
      await provider.sourceReady;
      if (this.stopped || this.provider !== provider) return;
      if (this.managed) {
        await this.managedWrite(actor => actor.join('dag', provider.lifetimeId));
        if (this.sourceDirty || forceRoles.length) {
          this.sourceRefreshing = true;
          await this.managedWrite(async actor => {
            for (const role of ['todo', 'dag', 'workers']) if (actor.snapshot.roles[role]) {
              const retained = actor.snapshot.roles[role];
              if (actor.snapshot.closures.some(entry => entry.role === role && entry.paneId === retained.paneId &&
                entry.sessionId === retained.sessionId)) {
                const observation = await this.observeManaged({ locator: retained });
                if (observation.missing?.code === 'pane_not_found' && observation.missing.paneId === retained.paneId) continue;
              }
              const target = role === 'todo' ? await this.nativePane(actor.snapshot.roles.todo.paneId, await readJson(this.recordFile))
                : await provider.observeTarget(actor.snapshot.roles[role]);
              await actor.registerRole(actor.capability(role === 'todo' ? 'todo' : 'dag', role), target);
            }
          });
          const offers = Object.fromEntries(['todo', 'dag', 'workers'].map(role => [role, this.presence.rotateRole(role)]));
          await provider.setOffers(offers);
          const record = await readJson(this.recordFile);
          if (this.recordMatches(record) && record.paneId) await this.roleWrite(() => writeJson(this.stateFile, this.roleState(record)));
          this.bridge.invalidate();
          await this.presence.idle();
          this.sourceDirty = false; this.sourceRefreshing = false;
        }
      }
      const { tabId } = await this.membership(this.binding.parentPaneId);
      const record = await readJson(this.recordFile);
      // A conversation change keeps the parent's live role panes, so a missing
      // Todo record alone is not a new dashboard: the region decision is null
      // (fresh), a layout (inherited) or a wait, which mutates nothing. Every
      // managed pass decides it, a live Todo's evidence included (r4 B2); the gate
      // decides again before each mutation (r4 B3).
      const region = this.managed && provider.ownedRegion
        ? await provider.ownedRegion({ explicit: forceCurrent('todo'), lifetimeId: this.lifetimeId }) : null;
      const inherited = !record && Boolean(region);
      if (region && !region.layout) {
        // Panes that died with the previous host (not closed by the owner) are
        // reopened once per lifetime, as an explicit /dashboard would.
        if (region.reopen && !forceRoles.length && !this.restartReopened) {
          this.restartReopened = true;
          // Queued automatic passes step aside so the reopen runs next and no
          // stale 'panes were closed' notice is shown meanwhile.
          this.restartReopening = true;
          void this.schedule({ forceRoles: ['dag', 'workers', 'todo'] })
            .finally(() => { this.restartReopening = false; }).catch(() => {});
          return;
        }
        this.bandWaiting(region.waiting, forceRoles.length > 0);
        return;
      }
      // A new managed dashboard gets the band. An existing one keeps the layout
      // its Todo record, or else its inherited role records, carry: a record
      // without 'band' stays columns.
      const band = this.managed && Boolean(provider.bandLayout) &&
        (record ? record.layoutClaim?.layout === 'band' : !region || region.layout === 'band');
      const claim = { ...this.binding, tabId, lifetimeId: this.lifetimeId, layout: band ? 'band' : 'columns' };
      this.claim = claim;
      // The band opens its area by splitting the chat once; /dashboard does the
      // same when every role pane of a band is gone. An inherited region's chat
      // is never split again (G2).
      const reopen = band && !existing && record && forceCurrent('todo') && !region;
      if (this.managed && !existing && (!record && !region || reopen)) {
        if (band) {
          const waiting = await provider.bandPreflight();
          this.bandWaiting(waiting, forceRoles.length > 0);
          if (waiting) return;
        }
        await this.ensureTodo({ ...this.binding, paneId: this.binding.parentPaneId, tabId, initial: true,
          ratio: band ? provider.bandLayout.chatShare : 0.6 }, Boolean(reopen));
      }
      const offers = { dag: this.presence.offer('dag'), workers: this.presence.offer('workers') };
      const pair = await provider.openPair({ force: forceCurrent('dag') || forceCurrent('workers'),
        forceRoles: ['dag', 'workers'].filter(forceCurrent), forceCurrent, offers, claim });
      if (this.stopped || this.provider !== provider) return;
      for (const role of ['dag', 'workers']) {
        const record = pair?.[role];
        if (record?.role === role && sameDashboardOwner(record, this.binding) &&
          record.scopeEpoch === this.scopeEpoch && record.tabId === tabId && record.ready === true &&
          record.manualClose !== true && Number.isSafeInteger(record.processId)) this.ownedRoles.set(role, record);
        else this.ownedRoles.delete(role);
      }
      this.bridge?.readinessChanged();
      if (this.managed) this.bandWaiting(pair?.waiting, forceRoles.length > 0);
      if (pair?.waiting) return; // r4 B4: a wait ends the pass before any later mutation.
      if (!pair?.workers || pair.workers.role !== 'workers' || !sameDashboardOwner(pair.workers, this.binding) ||
        pair.workers.tabId !== tabId) {
        this.status = 'layout-waiting'; return;
      }
      await this.membership(pair.workers.paneId);
      // An inherited region without a Todo keeps it closed until /dashboard.
      if (!existing && !inherited || forceCurrent('todo')) await this.ensureTodo(pair.workers, forceCurrent('todo'));
      // Once the arrangement moves a pane (perform), READY and the bridge's coverage
      // wait for all of it, not for each step: its geometry and placement journal
      // are complete only when it returns (todo 9). A benign interruption keeps
      // that barrier through viewer renewals and later attempts until success.
      const arrangement = this.arrangement ??= { moving: false };
      try {
        const arranged = await provider.arrangeDashboard?.();
        // An explicit /dashboard migrated the dashboard to the band (todo 7): the
        // Todo record takes the band claim last, after the DAG and workers records.
        if (this.managed && arranged?.layout === 'band' && claim.layout !== 'band') {
          this.claim = { ...claim, layout: 'band' };
          await this.roleWrite(async () => {
            const current = await readJson(this.recordFile);
            if (this.recordMatches(current) && !current.manualClose && current.paneId)
              await writeJson(this.recordFile, { ...current, layoutClaim: this.claim });
          });
        }
        this.arrangement = null;
      } finally {
        if (arrangement.moving) this.bridge?.readinessChanged();
      }
      // Complete: the paints accepted while it ran decide READY now.
      if (arrangement.moving) await this.managedReady(actor => actor.snapshot.roles.todo?.ack?.sequence);
      // One settled layout per pass. A protocol/read failure here must not turn
      // a healthy recovery pass into a recovery block.
      try {
        await this.settleRecoveryLayout({ pair, tabId, explicit: forceRoles.length > 0, commandId });
      } catch (error) { this.warnOnce(error); }
    };
    // Every mutation of this pass runs in its scope, so the gate knows whether it
    // is automatic. A gate refusal is a wait: zero mutations, one notice, no latch.
    // The same refusal on consecutive passes is noticed once.
    return this.enqueue(() => passes.run({ explicit: forceRoles.length > 0 }, () => pass().then(() => {
      this.gateWaiting = undefined;
    }, error => {
      if (error?.code !== 'LAYOUT_WAIT') throw error;
      if (error.cause?.code === 'EFFECT_UNKNOWN') {
        const operation = this.actor.snapshot.operations.findLast(operation => operation.status === 'UNKNOWN');
        if (operation) this.placementWaiting = { operationId: operation.id, message: error.message };
      }
      if (this.gateWaiting === error.message) this.sizeWaiting = error.message;
      this.gateWaiting = error.message;
      this.bandWaiting(error.message, forceRoles.length > 0);
    })));
  }
  // Below the band minimum nothing is created. The owner hears it once per
  // reason (an explicit /dashboard always answers) and a size change retries.
  bandWaiting(message, force) {
    const previous = this.sizeWaiting;
    this.sizeWaiting = message;
    if (!message) return;
    this.status = 'layout-waiting';
    if (force || message !== previous) this.notify(message);
    void this.watchRecoveryMembership().catch(() => {});
  }
  async ensureTodo(anchor, force) {
    if (force) await this.roleTail;
    let record = await readJson(this.recordFile);
    // A tombstone retires the locator, not its evidence file. Only an obsolete
    // generation may bypass the current viewer guard, after fresh actor validation.
    if (this.managed && record && !record.manualClose &&
      this.recordMatches({ ...record, viewerPath: this.viewer }) && typeof record.nodePath === 'string' &&
      (pathKey(record.viewerPath) !== pathKey(this.viewer) || typeof this.node === 'function' ||
        pathKey(record.nodePath) !== pathKey(this.node)) &&
      this.actor?.snapshot.closures.some(entry => entry.role === 'todo' && entry.paneId === record.paneId &&
        entry.sessionId === record.sessionId && entry.reason === 'pane_not_found')) {
      if (typeof this.node === 'function') this.node = await this.node();
      if (pathKey(record.viewerPath) !== pathKey(this.viewer) || pathKey(record.nodePath) !== pathKey(this.node)) {
        await this.managedWrite(actor => actor.obsoleteLocator(actor.capability('todo', 'todo'), record));
        record = null;
      }
    }
    if (record && !this.recordMatches(record)) {
      if (this.managed) throw blocked('retained Todo record identity changed');
      this.status = 'layout-waiting'; return;
    }
    if (record?.manualClose && !force) return;
    if (record?.paneId && record.prepared && !record.processId) {
      const member = await this.membership(record.paneId);
      const identity = member.tabId === record.tabId &&
        await legacyPaneIdentity(await this.herdr('process-info', '--pane', record.paneId), record, this.inspectShell);
      if (!identity || identity.viewer) throw blocked('PREPARED shell changed or occupied');
      const state = await readRoleState(this.stateFile);
      if (!state || !sameDashboardOwner(state, record) || state.paneId !== record.paneId ||
        state.launchToken !== record.launchToken) throw blocked('PREPARED source changed');
      record = { ...record, scopeEpoch: this.scopeEpoch, layoutClaim: this.claim ?? record.layoutClaim };
      await this.roleWrite(async () => {
        await writeJson(this.recordFile, record);
        await writeJson(this.stateFile, this.roleState(record));
      });
      return this.launchTodo(record);
    }
    if (record?.paneId) {
      let process;
      try { process = await this.verify(record); } catch (error) {
        if (!/missing or ambiguous/.test(error.message)) throw error;
      }
      if (process) {
        record = await this.roleWrite(async () => {
          const current = await readJson(this.recordFile);
          if (this.stopped || current.launchToken !== record.launchToken || current.manualClose && !force) return null;
          const next = { ...current, scopeEpoch: this.scopeEpoch, layoutClaim: this.claim ?? current.layoutClaim,
            processId: process.pid, ready: true, manualClose: false };
          await writeJson(this.recordFile, next);
          await writeJson(this.stateFile, this.roleState(next));
          return next;
        });
        if (!record) return;
        const registered = !this.managed || await this.registerManagedRole('todo', async () => {
          try { return await this.nativePane(record.paneId, record); }
          catch (error) {
            const death = this.viewerDeaths.get('todo');
            if (error.cause !== 'role viewer changed' || !this.ownsPublisherClaim() ||
              this.actor.snapshot.operations.some(operation => operation.status !== 'SETTLED') ||
              !death || death.exit.disposed !== false || !Number.isInteger(death.exit.exitCode) ||
              !sameDashboardOwner(death.record, record) || death.record.scopeEpoch !== record.scopeEpoch ||
              death.record.launchToken !== record.launchToken || death.identity.pid !== record.processId ||
              death.identity.birth !== record.viewerBirth ||
              JSON.stringify(death.identity) !== JSON.stringify(this.actor.snapshot.roles.todo?.viewer)) throw error;
            return null;
          }
        }) !== false;
        if (registered) {
          this.ownedRoles.set('todo', record);
          this.bridge?.readinessChanged();
          this.status = 'current'; return record.paneId;
        }
      }
      const panes = (await this.herdr('list'))?.panes ?? [];
      if (panes.some(pane => pane.pane_id === record.paneId)) {
        const death = this.managed && this.viewerDeaths.get('todo');
        if (death && death.record.launchToken === record.launchToken &&
          death.identity.pid === record.processId && death.identity.birth === record.viewerBirth) {
          const info = await this.herdr('process-info', '--pane', record.paneId);
          const observed = await observePaneShell(info, record, this.inspectShell);
          if (!observed || observed.children.length || observed.shell.Birth !== death.target.shell.birth)
            throw blocked('dead viewer did not leave the original empty shell');
          const next = { ...record, launchToken: randomBytes(24).toString('hex'), prepared: true, ready: false };
          delete next.processId; delete next.viewerBirth;
          await this.roleWrite(async () => {
            const current = await readJson(this.recordFile);
            if (current.manualClose) throw blocked('manual closure superseded viewer repair');
            await writeJson(this.recordFile, next); await writeJson(this.stateFile, this.roleState(next));
          });
          this.viewerDeaths.delete('todo');
          this.bridge?.invalidate(); await this.presence.idle();
          return this.launchTodo(next);
        }
        if (this.managed) throw blocked('retained Todo native identity unverified');
        this.status = 'layout-waiting'; return;
      }
      if (this.managed) {
        await this.managedWrite(actor => actor.obsoleteLocator(actor.capability('todo', 'todo'), record));
        if (!force) return;
      }
      if (!force) { await writeJson(this.recordFile, { ...record, manualClose: true }); return; }
    } else if (record?.attempted && !force) return;
    if (!anchor) return;
    const { tabId } = await this.membership(anchor.paneId);
    if (tabId !== anchor.tabId) { this.status = 'layout-waiting'; return; }
    if (typeof this.node === 'function') this.node = await this.node();
    const launchToken = randomBytes(24).toString('hex');
    record = { schema: 'omo-herdr-dashboard/pane/1', ...this.binding, scopeEpoch: this.scopeEpoch, role: 'todo',
      statePath: this.stateFile, viewerPath: this.viewer, tabId, launchToken,
      nodePath: this.node, layoutClaim: this.claim, attempted: true, ready: false, manualClose: false };
    const previous = await readFile(this.recordFile, 'utf8').catch(error => error?.code === 'ENOENT' ? null : undefined);
    await writeJson(this.recordFile, record);
    if (this.stopped) return;
    let split;
    try {
      split = await this.mutate('todo', 'split', this.managed ? await this.currentTarget(anchor.paneId) : null,
        ['split', '--pane', anchor.paneId, '--direction', anchor.initial ? 'right' : 'down',
          '--ratio', anchor.initial ? String(anchor.ratio) : '0.5', '--cwd', this.cwd, '--no-focus']);
    } catch (error) {
      // A no-effect refusal created nothing, so the attempt never happened (r5).
      if (error?.noEffect === true && previous !== undefined)
        await (previous === null ? rm(this.recordFile, { force: true }) : writeFile(this.recordFile, previous));
      throw error;
    }
    if (!split?.pane?.pane_id) throw new Error('Owned Todo split returned no pane ID');
    record.paneId = split.pane.pane_id;
    await this.membership(record.paneId);
    const createdInfo = await this.herdr('process-info', '--pane', record.paneId);
    record.creationProcessId = createdInfo?.process_info?.shell_pid;
    const shell = await observePaneShell(createdInfo, record, this.inspectShell);
    if (shell) { record.creationDate = shell.shell.CreationDate; if (this.managed) record.creationBirth = shell.shell.Birth; }
    record.prepared = true;
    await writeJson(this.recordFile, record);
    await writeJson(this.stateFile, this.roleState(record));
    return this.launchTodo(record);
  }
  async launchTodo(record) {
    const launchToken = record.launchToken;
    const abort = new AbortController();
    const creation = once(this.events, `created:${launchToken}`,
      { signal: AbortSignal.any([abort.signal, ...(this.managed ? [] : [this.stopSignal.signal]), AbortSignal.timeout(30000)]) }); // was 10 s: a late but live viewer settled EFFECT_UNKNOWN
    creation.catch(() => {});
    this.pendingLaunches.add(launchToken);
    try {
      await this.mutate('todo', 'run', this.managed ? await this.nativePane(record.paneId, record) : null,
        ['run', record.paneId, shellCommand([this.node, this.viewer, '--state', this.stateFile,
          '--close-pane', record.paneId, '--launch-token', launchToken])], creation);
      await creation;
      if (this.managed) {
        const registration = this.launchRegistrations.get(launchToken);
        if (!registration) throw blocked('authenticated launch registration missing');
        await registration;
      }
      if (!this.stopped) this.status = 'current';
      return record.paneId;
    } catch (error) {
      if (this.managed) throw error;
      const current = await readJson(this.recordFile);
      if (!this.stopped && current?.launchToken === launchToken && Number.isSafeInteger(record.creationProcessId)) {
        await this.membership(record.paneId);
        const info = await this.herdr('process-info', '--pane', record.paneId);
        if (await canCleanupPane(info, record, this.inspectShell)) {
          await this.herdr('close', record.paneId);
          await writeJson(this.recordFile, { ...current, paneId: null, ready: false, failed: true });
        }
      }
      throw error;
    } finally { this.pendingLaunches.delete(launchToken); this.launchRegistrations.delete(launchToken); abort.abort(); }
  }
  rebase(ctx) {
    return this.enqueue(async () => {
      await this.roleWrite(() => {
        if (this.stopped) return;
        this.bridge?.observe({ type: 'session_tree' }, ctx);
        this.ctx = ctx;
        this.scopeEpoch++;
        this.ownedRoles.clear();
        this.presence.rebase({ ...this.binding, scopeEpoch: this.scopeEpoch });
        this.bridge?.observe(null, ctx);
      });
      await this.presence.idle();
    }).then(() => this.schedule());
  }
  ownsPublisherClaim() {
    const slot = this.actor?.snapshot, scope = this.publisherScope;
    return Boolean(this.actor?.leaseHeld && scope &&
      scope.lifetimeId === this.lifetimeId && scope.sessionId === this.binding.sessionId &&
      slot.ownerNonce === scope.ownerNonce && slot.generation === scope.generation &&
      slot.sessionId === scope.sessionId && slot.participants.todo?.lifetimeId === scope.lifetimeId &&
      slot.producer.pid === process.pid && JSON.stringify(slot.producer) === JSON.stringify(scope.producer) &&
      JSON.stringify(slot.parent) === JSON.stringify(scope.parent));
  }
  async terminalAuthority(participant, lifetimeId, records) {
    if (!this.ownsPublisherClaim()) return false;
    const slot = this.actor.snapshot;
    if (slot.participants[participant]?.lifetimeId !== lifetimeId) return false;
    const observed = await this.observeManaged({});
    if (JSON.stringify(observed.parent) !== JSON.stringify(slot.parent) ||
      JSON.stringify(observed.producer) !== JSON.stringify(slot.producer)) return false;
    for (const record of records) {
      const role = slot.roles[record.role];
      const state = await readRoleState(record.statePath);
      if (!role || role.participant !== participant || role.lifetimeId !== lifetimeId ||
        role.generation !== slot.generation || role.sessionId !== slot.sessionId ||
        record.scopeEpoch !== state?.scopeEpoch || role.paneId !== record.paneId ||
        role.statePath !== record.statePath || role.launchToken !== record.launchToken ||
        state?.managedScope?.ownerNonce !== slot.ownerNonce ||
        state.managedScope.generation !== slot.generation || state.managedScope.sessionId !== slot.sessionId ||
        state.managedScope.participant !== participant || state.managedScope.lifetimeId !== lifetimeId)
        return false;
      const target = Object.fromEntries(['paneId', 'tabId', 'shell', 'viewer', 'statePath', 'sourcePath',
        'cachePath', 'prefsPath', 'launchToken', 'controlToken'].map(key => [key, role[key]]));
      const native = await this.observeManaged({ target });
      if (JSON.stringify(native.parent) !== JSON.stringify(slot.parent) ||
        JSON.stringify(native.producer) !== JSON.stringify(slot.producer) ||
        JSON.stringify(native.target) !== JSON.stringify(target)) return false;
    }
    return this.ownsPublisherClaim() && this.actor.snapshot.participants[participant]?.lifetimeId === lifetimeId;
  }
  async stop() {
    if (this.stopping) return this.stopping;
    if (this.stopped) return;
    this.stopped = true;
    const provider = this.provider;
    const ownedClaim = this.ownsPublisherClaim();
    const releasing = ownedClaim ? this.actor.release() : null;
    releasing?.catch(() => {});
    const peerDrain = this.managed && ownedClaim && provider &&
      provider?.lifetimeId === this.actor.snapshot.participants.dag?.lifetimeId ? provider.drain() : null;
    peerDrain?.catch(() => {});
    this.stopSignal.abort();
    this.recoveryWatch?.socket?.destroy();
    this.offSource?.(); this.offReadiness?.();
    this.ownedRoles.clear();
    if (this.onPresenceEvent) this.presence?.events.off('event', this.onPresenceEvent);
    for (const off of this.unsubscribers) off();
    this.pi?.events.emit(dashboardChannels.retired, { v: 1, ...this.binding, lifetimeId: this.lifetimeId });
    this.bridge?.remove();
    this.stopping = (async () => {
    await this.tail;
    await Promise.allSettled([...this.controlJobs]);
    await this.roleTail;
    let terminalFailure;
    try {
      const record = await readJson(this.recordFile);
      if (this.storage && this.recordMatches(record) && record.paneId &&
        (!this.managed || record.scopeEpoch === this.scopeEpoch && !record.manualClose &&
          await this.terminalAuthority('todo', this.lifetimeId, [record])))
        await writeJson(this.stateFile, this.roleState(record, false));
    } catch (error) { terminalFailure = error; }
    try {
      for (const socket of this.controlSockets) if (!this.retirementSubscribers.has(socket)) socket.destroy();
      await this.presence?.close();
    } catch (error) { terminalFailure ??= error; }
    try {
      await this.managedTail;
      try { await peerDrain; } catch (error) { terminalFailure ??= error; }
      try {
        if (ownedClaim && this.ownsPublisherClaim()) await this.actor.drain('todo', this.lifetimeId);
      } catch (error) { terminalFailure ??= error; }
      try { await releasing; } catch (error) { terminalFailure ??= error; }
      if (this.actor?.snapshot.phase === 'RELEASED') for (const [socket, subscriber] of this.retirementSubscribers) {
        send(socket, { ...subscriber.offer, type: 'released', nonce: subscriber.nonce, slot: this.actor.snapshot,
          recordSHA256: subscriber.recordSHA256,
          stateSHA256: createHash('sha256').update(await readFile(this.stateFile)).digest('hex') });
      }
    } finally {
      // A failed drain is still a terminal lifetime boundary. release() closes
      // SQLite in its finally branch, retaining BLOCKED evidence if necessary.
      await Promise.allSettled([releasing, peerDrain]);
      for (const socket of this.controlSockets) socket.destroy();
      if (this.control?.listening) await new Promise(resolve => this.control.close(resolve));
      const disposed = await Promise.allSettled([
        ...[...this.legacyObservations.values()].map(value => Promise.resolve().then(() => value.dispose())),
        ...[...this.viewerHolders.values()].map(value => Promise.resolve().then(() => value.observer.dispose())),
      ]);
      for (const result of disposed) if (result.status === 'rejected') terminalFailure ??= result.reason;
    }
    if (terminalFailure) throw terminalFailure;
    })();
    return this.stopping;
  }
}
