// Native process and IPC primitives for macOS and Linux. Windows keeps its
// named pipes and WMI rows; on POSIX the same row shape comes from `ps`
// (and /proc on Linux) so the identity checks stay unchanged.
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { lstatSync, mkdirSync, readFileSync, readdirSync, readlinkSync } from 'node:fs';
import { lstat, unlink } from 'node:fs/promises';
import net from 'node:net';
import { join } from 'node:path';
import { promisify } from 'node:util';

const WINDOWS_PIPE = '\\\\.\\pipe\\';
const execute = promisify(execFile);

// No POSIX API reads the exit status of a process that is not our child.
// Observers that prove such a death report this sentinel instead.
export const UNOBSERVABLE_EXIT_CODE = -1;

// A fixed per-user directory keeps every participant (OmO, pane viewers,
// helpers) on the same path regardless of TMPDIR, and stays far below the
// 104-byte sun_path limit on macOS.
export const ipcRoot = () => `/tmp/omo-herdr-${process.getuid()}`;

function ensureIpcRoot() {
  const root = ipcRoot();
  try { mkdirSync(root, { mode: 0o700 }); } catch (error) { if (error.code !== 'EEXIST') throw error; }
  const stat = lstatSync(root);
  if (!stat.isDirectory() || stat.uid !== process.getuid() || (stat.mode & 0o077) !== 0)
    throw new Error(`Unsafe IPC directory ${root}: it must be a private directory owned by this user`);
  return root;
}

export function ipcPath(name, platform = process.platform) {
  if (platform === 'win32') return `${WINDOWS_PIPE}${name}`;
  return join(ensureIpcRoot(), `${createHash('sha256').update(name).digest('hex').slice(0, 32)}.sock`);
}

export function isIpcPath(value, platform = process.platform) {
  if (typeof value !== 'string') return false;
  if (platform === 'win32') return value.startsWith(WINDOWS_PIPE);
  const prefix = `${ipcRoot()}/`;
  return value.startsWith(prefix) && /^[0-9a-f]{32}\.sock$/.test(value.slice(prefix.length));
}

// A crashed process leaves its socket file behind; a refused connection
// proves nobody listens there anymore, so the path can be reused.
export async function clearStaleIpc(path, platform = process.platform) {
  if (platform === 'win32') return;
  try { if (!(await lstat(path)).isSocket()) return; } catch (error) { if (error.code === 'ENOENT') return; throw error; }
  const refused = await new Promise(resolve => {
    const socket = net.createConnection(path);
    socket.once('connect', () => { socket.destroy(); resolve(false); });
    socket.once('error', error => resolve(error.code === 'ECONNREFUSED'));
  });
  if (refused) await unlink(path).catch(error => { if (error.code !== 'ENOENT') throw error; });
}

export async function listenIpc(server, path) {
  await clearStaleIpc(path);
  await new Promise((resolve, reject) => {
    server.once('error', reject); server.once('listening', resolve); server.listen(path);
  });
}

const psOptions = { env: { ...process.env, LC_ALL: 'C', TZ: 'UTC' }, timeout: 30000,
  maxBuffer: 16 * 1024 * 1024, detached: true };
const months = { Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5, Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11 };
const lstart = /^\w{3}\s+(\w{3})\s+(\d{1,2})\s+(\d{2}):(\d{2}):(\d{2})\s+(\d{4})$/;

function parseStart(text) {
  const match = lstart.exec(text.trim());
  if (!match || months[match[1]] === undefined) return null;
  const [, month, day, hour, minute, second, year] = match;
  return Date.UTC(Number(year), months[month], Number(day), Number(hour), Number(minute), Number(second));
}

function parseTable(stdout) {
  const rows = new Map();
  for (const line of stdout.split('\n')) {
    const match = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(-?\d+)\s+(.+?)\s*$/.exec(line);
    if (!match) continue;
    const born = parseStart(match[5]);
    if (born === null) continue;
    rows.set(Number(match[1]), { pid: Number(match[1]), ppid: Number(match[2]), pgid: Number(match[3]),
      tpgid: Number(match[4]), born });
  }
  return rows;
}

function parseColumn(stdout) {
  const values = new Map();
  for (const line of stdout.split('\n')) {
    const match = /^\s*(\d+) (.*)$/.exec(line);
    if (match && match[2].trim()) values.set(Number(match[1]), match[2].replace(/\s+$/, ''));
  }
  return values;
}

async function argvRows(pids, run, platform) {
  const argv = new Map();
  if (platform === 'linux') {
    for (const pid of pids) {
      try {
        const values = readFileSync(`/proc/${pid}/cmdline`, 'utf8').split('\0');
        if (values.at(-1) === '') values.pop();
        if (values.length && values[0]) argv.set(pid, { executable: values[0], command: values.join(' ') });
      } catch { /* The process exited or is not ours. */ }
    }
    return argv;
  }
  // Darwin `comm` is the full argv[0] (spaces kept); `args` is argv joined by spaces.
  const list = pids.join(',');
  const [comm, args] = await Promise.all([
    run('ps', ['-ww', '-o', 'pid=,comm=', '-p', list], psOptions).then(result => parseColumn(result.stdout)),
    run('ps', ['-ww', '-o', 'pid=,args=', '-p', list], psOptions).then(result => parseColumn(result.stdout)),
  ]);
  for (const pid of pids) {
    if (comm.has(pid) && args.has(pid)) argv.set(pid, { executable: comm.get(pid), command: args.get(pid) });
  }
  return argv;
}

// Rows for `pid` and the children that form the foreground job of its
// terminal. A POSIX shell also parents prompt helpers and zpty workers; only a
// child leading its own process group that owns the terminal is "running in
// the pane", matching what Windows reports for a PowerShell child.
export async function queryPosixProcesses(pid, { run = execute, platform = process.platform } = {}) {
  if (!Number.isSafeInteger(pid) || pid < 1)
    throw Object.assign(new Error('process query skipped: invalid pid'), { code: 'SHELL_QUERY_INVALID_PID' });
  const { stdout } = await run('ps', ['-A', '-ww', '-o', 'pid=,ppid=,pgid=,tpgid=,lstart='], psOptions);
  const table = parseTable(stdout);
  if (!table.size) throw Object.assign(new Error('process query returned no output'), { code: 'SHELL_QUERY_EMPTY' });
  const self = table.get(pid);
  if (!self) return [];
  const children = [...table.values()].filter(row => row.ppid === pid && row.pid !== pid &&
    self.tpgid > 0 && row.pgid === self.tpgid && row.pgid !== self.pgid);
  const selected = [self, ...children];
  const argv = await argvRows(selected.map(row => row.pid), run, platform);
  return selected.filter(row => argv.has(row.pid)).map(row => ({
    ProcessId: row.pid, ParentProcessId: row.ppid,
    ExecutablePath: argv.get(row.pid).executable, CommandLine: argv.get(row.pid).command,
    CreationDate: new Date(row.born).toISOString(), Birth: String(row.born / 1000),
  }));
}

function processPresent(pid) {
  try { process.kill(pid, 0); return true; } catch (error) { return error.code === 'EPERM'; }
}

async function sameProcessAlive(identity, run) {
  if (!processPresent(identity.pid)) return false;
  let stdout = '';
  try { ({ stdout } = await run('ps', ['-o', 'stat=,lstart=', '-p', String(identity.pid)], psOptions)); }
  catch (error) { if (error.code === 1) return false; throw error; }
  const match = /^\s*(\S+)\s+(.+?)\s*$/.exec(stdout);
  if (!match) return false;
  if (match[1].startsWith('Z')) return false;
  const born = parseStart(match[2]);
  return born !== null && String(born / 1000) === identity.birth;
}

export async function processAlive(identity, { run = execute } = {}) {
  return sameProcessAlive(identity, run);
}

// Resolves when the exact process (pid and birth) is gone; rejects at the deadline.
export async function waitForProcessExit(identity, timeoutMs, { run = execute, signal } = {}) {
  const started = Date.now(), deadline = started + timeoutMs;
  let checks = 0;
  for (;;) {
    signal?.throwIfAborted();
    if (!processPresent(identity.pid)) return;
    // kill(0) cannot see a zombie or a reused pid; ps can, at a lower rate for long waits.
    if (checks++ % (Date.now() - started > 15000 ? 100 : 10) === 0 && !await sameProcessAlive(identity, run)) return;
    if (Date.now() >= deadline) throw new Error('native exit deadline');
    await new Promise(resolve => setTimeout(resolve, 100));
  }
}

// The processes holding the server side of a Unix socket path. This stands in
// for GetNamedPipeServerProcessId: a client never shows the bound path.
export async function ipcServerPids(path, { run = execute, platform = process.platform } = {}) {
  if (platform === 'linux') {
    const inodes = new Set();
    for (const line of readFileSync('/proc/net/unix', 'utf8').split('\n').slice(1)) {
      const fields = line.trim().split(/\s+/);
      if (fields.length >= 8 && fields.slice(7).join(' ') === path) inodes.add(`socket:[${fields[6]}]`);
    }
    if (!inodes.size) return [];
    const pids = [];
    for (const entry of readdirSync('/proc')) {
      if (!/^\d+$/.test(entry)) continue;
      let fds;
      try { fds = readdirSync(`/proc/${entry}/fd`); } catch { continue; }
      if (fds.some(fd => { try { return inodes.has(readlinkSync(`/proc/${entry}/fd/${fd}`)); } catch { return false; } }))
        pids.push(Number(entry));
    }
    return pids;
  }
  try {
    const { stdout } = await run('lsof', ['-nP', '-t', '--', path], psOptions);
    return [...new Set(stdout.split('\n').filter(Boolean).map(Number))];
  } catch (error) {
    if (error.code === 1 && !error.stderr?.trim()) return [];
    throw error;
  }
}
