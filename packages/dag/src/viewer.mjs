import { watch } from 'node:fs';
import net from 'node:net';
import { createHash } from 'node:crypto';
import { basename, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { terminalInput, enablePointer, disablePointer, watchHerdrFocus } from './terminal-input.mjs';
import { createHerdr } from './herdr.mjs';
import { t } from './i18n.mjs';
import { clearStaleIpc, ipcPath, isIpcPath } from '../../coordination/src/platform.mjs';
import { completedTasks, defaultTaskId, renderFrame, runningTarget, workersSections } from './render.mjs';
import { readJson } from './storage.mjs';
import { TASK_SCOPE, emptyViewState, isExpanded, loadViewState, saveViewState, setExpanded } from './view-state.mjs';

export function renderFixedFrame(state, options, view) {
  // The fixed role keeps renderFrame's own header row (name [done/total] summary), body and coloured footer,
  // an empty DAG pane included; no footer invites a mode switch.
  return renderFrame(state, { ...options, view, fixed: true });
}

const controlKeys = ['token', 'ownerKey', 'sessionId', 'scopeEpoch', 'role', 'launchToken', 'paneId', 'statePath'];
const sameControl = (a, b) => a?.v === 1 && b?.v === 1 && controlKeys.every(key => a[key] === b[key]);
const sameScope = (a, b) => a && b && ['ownerKey', 'sessionId', 'scopeEpoch'].every(key => a[key] === b[key]);
const samePath = (a, b) => {
  const left = resolve(a), right = resolve(b);
  return process.platform === 'win32' ? left.toLowerCase() === right.toLowerCase() : left === right;
};
function receive(socket, callback) {
  socket.setEncoding('utf8');
  let buffer = '';
  socket.on('data', chunk => {
    buffer += chunk;
    if (buffer.length > 4 * 1024 * 1024) return socket.destroy();
    let cut;
    while ((cut = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, cut); buffer = buffer.slice(cut + 1);
      try { callback(JSON.parse(line)); } catch { socket.destroy(); }
    }
  });
}
function roleControl(type, state, paneId, launchToken) {
  const offer = state?.controlOffer;
  if (!isIpcPath(offer?.pipePath) || paneId !== state.paneId ||
    launchToken !== state.launchToken || !sameScope(offer, state)) return Promise.resolve(false);
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(offer.pipePath);
    const timer = setTimeout(() => { socket.destroy(); reject(new Error('Role control response timed out')); }, 10000);
    socket.once('connect', () => socket.write(`${JSON.stringify({ ...offer, type, pid: process.pid })}\n`));
    receive(socket, message => {
      clearTimeout(timer); socket.end();
      resolve(sameControl(message, offer) && message.type === (type === 'register' ? 'registered' : 'closed'));
    });
    socket.once('error', error => { clearTimeout(timer); reject(error); });
    socket.once('close', () => { clearTimeout(timer); resolve(false); });
  });
}

const displayClock = {
  now: () => Date.now(), setInterval, clearInterval, setTimeout, clearTimeout,
};
export function createAnimation(draw, clock = displayClock) {
  let timer, frame = 0;
  return {
    get frame() { return frame; },
    update(state) {
      const running = state?.connected && (state.tasks?.some(task => task.status === 'running') ||
        state.runs?.some(run => run.nodes.some(node => node.state === 'running')));
      if (running && timer === undefined) timer = clock.setInterval(() => { frame++; draw(); }, 125);
      if (!running && timer !== undefined) { clock.clearInterval(timer); timer = undefined; }
    },
    stop() { if (timer !== undefined) clock.clearInterval(timer); timer = undefined; },
  };
}
// Read failures are transport gaps, not authoritative task completion. A fresh
// snapshot cancels the grace; identity and presence errors bypass it.
export function createReadGrace(report, clock = displayClock) {
  let timer, latest;
  return {
    fail(error) {
      latest = error;
      if (timer === undefined) timer = clock.setTimeout(() => {
        timer = undefined; report(latest);
      }, 2000);
    },
    clear() { if (timer !== undefined) clock.clearTimeout(timer); timer = undefined; latest = undefined; },
  };
}

export async function runViewer({ clock = displayClock } = {}) {
const file = process.argv[process.argv.indexOf('--state') + 1];
if (!process.argv.includes('--state') || !file) throw new Error('Usage: node viewer.mjs --state PATH');
const fixedView = process.argv.includes('--view') ? process.argv[process.argv.indexOf('--view') + 1] : undefined;
if (fixedView && !['dag', 'tasks'].includes(fixedView)) throw new Error('Managed --view must be dag or tasks');
const role = fixedView === 'dag' ? 'dag' : 'workers';
const paneId = process.argv.includes('--close-pane') ? process.argv[process.argv.indexOf('--close-pane') + 1] : undefined;
const launchToken = process.argv.includes('--launch-token') ? process.argv[process.argv.indexOf('--launch-token') + 1] : undefined;
let state = await readJson(file);
function validManaged(next) {
  return next?.schema === 'omo-herdr-dashboard/role/1' && next.role === role &&
    next.paneId === paneId && next.launchToken === launchToken &&
    Number.isSafeInteger(next.scopeEpoch) && next.scopeEpoch > 0 &&
    ['socketPath', 'parentPaneId', 'tabId', 'sessionId'].every(key => typeof next[key] === 'string' && next[key]) &&
    next.ownerKey === createHash('sha256').update(JSON.stringify([next.socketPath, next.parentPaneId, next.sessionId]))
      .digest('hex').slice(0, 24);
}
if (fixedView && launchToken && !validManaged(state)) throw new Error('Managed viewer launch identity mismatch');
let selectedId = state?.runs?.[0]?.id;
let view = fixedView ?? (state?.runs?.length ? 'dag' : 'tasks'), selectedTaskId;
let scroll = 0, error = '', timer, drawing = false, again = false;
let viewState = emptyViewState(state?.sessionId), viewError = '', saving = Promise.resolve(), closing = false;
const selectedNodes = new Map();
let revealSelection = false, verbose = false, detailSelection, completedExpanded = false;
let follow = true, target, lastFrame, dragY, disposeFocus, scrollAnchor;
let presenceClient, presenceMessage, presenceKey, controlKey, presenceNotice = '';
let retirementServer;
const retirementSockets = new Set();
const animation = createAnimation(draw, clock);
const readGrace = createReadGrace(message => { error = message; draw(); }, clock);
function bindPresence() {
  if (!fixedView || !interactive || !launchToken || !validManaged(state)) return;
  const control = JSON.stringify(state.controlOffer);
  if (state.controlOffer && control !== controlKey) {
    controlKey = control;
    void roleControl('register', state, paneId, launchToken).catch(cause => { error = cause.message; draw(); });
  }
  const offer = state.presenceOffer;
  const key = JSON.stringify(offer);
  if (key === presenceKey) return;
  presenceKey = key;
  presenceClient?.destroy(); presenceClient = undefined; presenceMessage = undefined;
  if (!offer) { presenceNotice = 'Waiting for presence'; return; }
  if (offer.v !== 1 || offer.role !== role || !sameScope(offer, state) ||
    !isIpcPath(offer.pipePath)) { presenceNotice = 'Invalid presence offer'; return; }
  const client = net.createConnection(offer.pipePath);
  presenceClient = client;
  const binding = message => message?.v === 1 && message.token === offer.token &&
    message.role === role && sameScope(message, offer);
  client.once('connect', () => client.write(`${JSON.stringify({ ...offer, type: 'hello' })}\n`));
  receive(client, message => {
    if (presenceClient !== client || !binding(message)) return client.destroy();
    if (message.type === 'snapshot') {
      if (!sameScope(message.snapshot, offer) || message.sequence !== message.snapshot?.sequence ||
        !Number.isSafeInteger(message.sequence) || message.sequence < 1 ||
        presenceMessage && message.sequence <= presenceMessage.sequence) return client.destroy();
      presenceMessage = message;
      presenceNotice = message.snapshot.availability === 'waiting' ? 'Waiting for source' : '';
      draw();
    } else if (message.type === 'status') {
      presenceMessage = undefined; presenceNotice = message.label ?? message.availability; draw();
    }
  });
  client.on('error', cause => { if (presenceClient === client) { presenceNotice = cause.message; draw(); } });
  client.once('close', () => {
    if (presenceClient !== client) return;
    presenceMessage = undefined; presenceNotice = 'Disconnected; last-known; not live'; draw();
  });
}
// With no run list on screen, ←→ walks every run; the footer's Run i/n counts the same list.
function selectableRuns() { return state?.runs ?? []; }
async function restorePreferences() {
  const scope = fixedView ? state : undefined;
  try {
    // A managed state names its own preference sidecar. Accept that exact path
    // only, so a foreign or rekeyed conversation cannot lend its detail state.
    if (fixedView && state?.prefsPath !== undefined &&
      (typeof state.prefsPath !== 'string' || !samePath(state.prefsPath, `${file}.view.json`)))
      throw new Error('Foreign DAG view preferences.');
    viewState = await loadViewState(file, state?.sessionId, scope); viewError = '';
  }
  catch (cause) { viewState = emptyViewState(state?.sessionId, scope); viewError = cause.message; }
  const navigation = viewState.navigation;
  follow = true;
  scroll = navigation?.scroll ?? 0;
  const anchorOffset = Number.isSafeInteger(navigation?.scrollOffset) ? navigation.scrollOffset : 0;
  scrollAnchor = typeof navigation?.scrollTaskId === 'string' ?
    { id: navigation.scrollTaskId, offset: anchorOffset, candidates: [navigation.scrollTaskId] } : undefined;
  view = fixedView ?? navigation?.view ?? view;
  selectedId = navigation?.selectedId ?? selectedId;
  selectedTaskId = navigation?.selectedTaskId;
  selectedNodes.clear();
  for (const pair of navigation?.selectedNodes ?? []) selectedNodes.set(...pair);
  completedExpanded = navigation?.completedExpanded ?? false;
}
await restorePreferences();
const interactive = Boolean(process.stdout.isTTY && process.stdin.isTTY);
function draw() {
  if (closing) return;
  const sections = workersSections(state);
  if (interactive) animation.update(state);
  if (!error) target = runningTarget(!fixedView ? state : fixedView === 'dag' ?
    { ...state, tasks: [] } : { ...state, runs: [], tasks: sections.roots }, target);
  const running = !error && follow && state?.connected ? target : undefined;
  if (running) {
    if (view !== running.view || running.view === 'dag' && selectedId !== running.runId) scroll = 0;
    view = fixedView ?? running.view;
    if (view === 'dag') selectedId = running.runId;
  }
  let runIndex = state?.runs?.findIndex(run => run.id === selectedId) ?? 0;
  if (runIndex < 0) { runIndex = 0; selectedId = state?.runs?.[0]?.id; }
  const run = state?.runs?.[runIndex];
  if (!fixedView && !state?.runs?.length) view = 'tasks';
  const roots = sections.roots;
  if (!roots.some(task => task.id === selectedTaskId)) selectedTaskId = defaultTaskId(state, roots);
  if (scrollAnchor &&
    ![scrollAnchor.id, ...(scrollAnchor.candidates ?? [])].some(id => roots.some(task => task.id === id)))
    scrollAnchor = undefined;
  if (!run?.nodes.some(node => node.id === selectedNodes.get(selectedId))) selectedNodes.set(selectedId, run?.nodes[0]?.id);
  const selection = JSON.stringify([state?.sessionId, view, view === 'tasks' ? null : selectedId,
    view === 'tasks' ? selectedTaskId : selectedNodes.get(selectedId)]);
  if (selection !== detailSelection) { verbose = false; detailSelection = selection; }
  // With no execution target, Follow ON pins the newest history card. Passing
  // its range through the existing runningTaskId seam keeps the viewport follow
  // behavior in render.mjs untouched.
  const followHistoryId = !running && !error && follow && state?.connected && view === 'tasks' ?
    sections.history.at(-1)?.id : undefined;
  const options = { columns: process.stdout.columns ?? 54, rows: process.stdout.rows ?? 48, runIndex, scroll, color: interactive,
    offline: Boolean(presenceKey && !presenceMessage),
    error, notice: viewError ? t(state?.language, 'viewError', { error: viewError }) : presenceNotice, completedExpanded,
    selectedNodeId: selectedNodes.get(selectedId), selectedTaskId, view, viewState, verbose, revealSelection, follow,
    runningNodeId: running?.nodeId, runningTaskId: running?.taskId ?? followHistoryId, frame: animation.frame, now: clock.now() };
  const render = () => fixedView ? renderFixedFrame(state, options, fixedView) : renderFrame(state, options);
  let result = render();
  // The task ranges are produced by the same ordered body. Resolve the manual
  // anchor to its task start plus the within-card offset; relayout only when
  // that row moved (arrivals/removals above it). A metrics-only update resolves
  // to the exact same row and keeps the visible window unchanged.
  if (!follow && !revealSelection && view === 'tasks' && scrollAnchor) {
    const anchored = scrollForAnchor(scrollAnchor, result.taskRanges);
    if (anchored !== undefined && result.scroll !== anchored) {
      options.scroll = anchored;
      result = render();
    }
  }
  lastFrame = result;
  scroll = result.scroll;
  const frame = result.text;
  const client = presenceClient, message = presenceMessage, paintedScope = state;
  process.stdout.write(interactive ? `\x1b[H${frame.replaceAll('\n', '\x1b[K\r\n')}\x1b[K` : `${frame}\n`, cause => {
    if (cause || closing || !client || presenceClient !== client || presenceMessage !== message ||
      !message || !sameScope(state, paintedScope) || !sameScope(message, state) || !state.connected ||
      !['current', 'empty', 'waiting'].includes(message.snapshot.availability) || client.destroyed) return;
    const offer = state.presenceOffer;
    client.write(`${JSON.stringify({ type: 'ack', v: offer.v, token: offer.token, ownerKey: offer.ownerKey,
      sessionId: offer.sessionId, scopeEpoch: offer.scopeEpoch, role, sequence: message.sequence })}\n`);
  });
}
// A manual scroll is anchored to the task under the viewport top plus the row
// offset inside that card, not to an absolute row that arrivals above it would
// silently replace. Candidates preserve the nearest surviving anchor if the
// primary card disappears.
function scrollForAnchor(anchor, ranges) {
  const primary = ranges?.[anchor.id];
  if (primary) {
    const offset = Math.min(anchor.offset, Math.max(0, primary.end - primary.start - 1));
    return Math.max(0, primary.start + offset);
  }
  for (const id of anchor.candidates ?? []) {
    const range = ranges?.[id];
    if (range) return range.start;
  }
  return undefined;
}
function captureScrollAnchor() {
  const ranges = view === 'tasks' ? lastFrame?.taskRanges : undefined;
  if (!ranges) return undefined;
  const entries = Object.entries(ranges);
  if (!entries.length) return undefined;
  const within = entries.findIndex(([, range]) => scroll >= range.start && scroll < range.end);
  const index = within >= 0 ? within : entries.findIndex(([, range]) => range.end > scroll);
  if (index < 0) return undefined;
  const [id, range] = entries[index];
  const ids = entries.map(([taskId]) => taskId);
  const candidates = [...new Set([...ids.slice(index, index + 4),
    ...ids.slice(Math.max(0, index - 2), index).reverse()])];
  return { id, offset: scroll - range.start, candidates };
}
async function refresh() {
  if (drawing) { again = true; return; }
  drawing = true;
  try {
    const next = await readJson(file);
    if (next) {
      if (fixedView && launchToken && !validManaged(next)) {
        throw Object.assign(new Error('Managed viewer launch identity changed'), { code: 'VIEWER_IDENTITY' });
      }
      const sessionChanged = next.sessionId !== state?.sessionId || fixedView && !sameScope(next, state);
      readGrace.clear(); state = next; error = '';
      if (sessionChanged) {
        await saving; selectedNodes.clear(); selectedTaskId = undefined; scroll = 0;
        selectedId = state?.runs?.[0]?.id; completedExpanded = false; scrollAnchor = undefined;
        follow = true; target = undefined; revealSelection = false;
        view = fixedView ?? (state?.runs?.length ? 'dag' : 'tasks');
        await restorePreferences();
      }
      bindPresence();
    }
    else readGrace.fail(t(state?.language, 'stateMissing'));
  } catch (cause) {
    if (cause.code === 'VIEWER_IDENTITY') { readGrace.clear(); error = cause.message; }
    else readGrace.fail(cause.message);
  }
  finally { drawing = false; }
  draw();
  if (again) { again = false; void refresh(); }
}
if (process.argv.includes('--once') || !interactive) { draw(); process.exit(0); }
process.stdout.write(`\x1b[?1049h\x1b[?25l${enablePointer}\x1b]0;${fixedView === 'tasks' ? `OmO ${t(state?.language, 'tasks')}` : 'OmO DAG'}\x07`);
process.stdin.setRawMode(true);
process.stdin.resume();
process.stdin.setEncoding('utf8');
const watcher = watch(dirname(file), (_event, name) => {
  if (name && String(name) !== basename(file)) return;
  clock.clearTimeout(timer); timer = clock.setTimeout(() => { void refresh(); }, 40);
});
watcher.on('error', cause => { error = cause.message; draw(); });
const resize = () => draw();
process.stdout.on('resize', resize);
// Keep presence paint acknowledgements alive even when there is no running work.
// Animation is a separate running-only clock; neither timer reads snapshots.
const presenceClock = fixedView ? clock.setInterval(() => {
  if (fixedView && presenceMessage) draw();
}, 1000) : undefined;
async function close(closePane = false) {
  if (closing) return;
  closing = true;
  clock.clearTimeout(timer); clock.clearInterval(presenceClock); animation.stop(); readGrace.clear();
  watcher.close(); process.stdout.off('resize', resize);
  presenceClient?.destroy();
  disposeInput();
  disposeFocus?.();
  for (const socket of retirementSockets) socket.destroy();
  if (retirementServer?.listening) await new Promise(resolve => retirementServer.close(resolve));
  await saving;
  process.stdin.setRawMode(false);
  process.stdout.write(`${disablePointer}\x1b[?25h\x1b[?1049l`);
  if (viewError) process.stderr.write(`${t(state?.language, 'viewError', { error: viewError })}\n`);
  if (closePane && process.argv.includes('--close-pane')) {
    const pane = process.argv[process.argv.indexOf('--close-pane') + 1];
    try {
      if (fixedView) await roleControl('close', state, pane, launchToken);
      else await createHerdr()('close', pane);
    }
    catch (error) { process.stderr.write(`${t(state?.language, 'closeFailed', { error: error.message })}\n`); }
  }
  process.exit(0);
}
const disposeInput = terminalInput(process.stdin, { key: (text, pressed) => {
  if (closing) return;
  const key = pressed.sequence || pressed.name || text;
  if (key === 'q' || key === '\x03' || key === '\x04') return void close(true);
  let manual = false, movedScroll = false, movedSelection = false;
  if (key === 'f') {
    follow = !follow;
    revealSelection = false;
    scrollAnchor = follow ? undefined : captureScrollAnchor();
  }
  if (['\x1b[B', 'j', '\x1b[A', 'k', '\x1b[6~', '\x1b[5~'].includes(key)) {
    follow = false; revealSelection = false;
    manual = movedScroll = true;
    scrollAnchor = undefined;
  }
  if (key === '\x1b[B' || key === 'j') scroll++;
  if (key === '\x1b[A' || key === 'k') scroll = Math.max(0, scroll - 1);
  if (key === '\x1b[6~') scroll += Math.max(1, (process.stdout.rows ?? 48) - 8);
  if (key === '\x1b[5~') scroll = Math.max(0, scroll - Math.max(1, (process.stdout.rows ?? 48) - 8));
  if (!fixedView && key === 't' && state?.runs?.length) {
    follow = false;
    manual = movedSelection = true;
    view = view === 'dag' ? 'tasks' : 'dag';
    scroll = 0; revealSelection = true; scrollAnchor = undefined;
  }
  if ((key === 'c' || pressed.name === 'c') && (view === 'tasks' ? completedTasks(state).length :
      (state?.runs?.length ?? 0) > 1)) {
    follow = false;
    manual = movedSelection = true;
    completedExpanded = !completedExpanded;
    scroll = 0; revealSelection = true; scrollAnchor = undefined;
  }
  if ((!fixedView || view === 'dag') && (key === '\x1b[C' || key === '\x1b[D')) {
    follow = false;
    manual = movedSelection = true;
    const runs = selectableRuns();
    const current = Math.max(0, runs.findIndex(run => run.id === selectedId));
    selectedId = runs[(current + (key === '\x1b[C' ? 1 : -1) + runs.length) % runs.length]?.id;
    if (runs.length) view = fixedView ?? 'dag';
    scroll = 0; revealSelection = false; scrollAnchor = undefined;
  }
  const run = state?.runs?.find(run => run.id === selectedId);
  const items = view === 'tasks' ? workersSections(state).roots : run?.nodes ?? [];
  const scope = view === 'tasks' ? TASK_SCOPE : run?.id;
  const itemId = view === 'tasks' ? selectedTaskId : selectedNodes.get(selectedId);
  if (items.length && (pressed.name === 'tab' || key === 'n' || key === 'p')) {
    follow = false;
    manual = movedSelection = true;
    scrollAnchor = undefined;
    const current = Math.max(0, items.findIndex(item => item.id === itemId));
    const direction = pressed.shift || key === 'p' ? -1 : 1;
    const nextId = items[(current + direction + items.length) % items.length].id;
    if (view === 'tasks') selectedTaskId = nextId;
    else selectedNodes.set(selectedId, nextId);
    revealSelection = true;
  }
  if (items.length && key === 'd') {
    follow = false;
    manual = true;
    scrollAnchor = undefined;
    // A temporary detail peek never writes or replaces the saved fold state.
    verbose = !verbose;
    revealSelection = true;
  }
  if (items.length && (key === ' ' || key === '\r' || key === '\n')) {
    follow = false;
    manual = true;
    scrollAnchor = undefined;
    verbose = false;
    const item = items.find(item => item.id === itemId);
    const status = view === 'tasks' ? item.status : item.state;
    setExpanded(viewState, scope, itemId, !isExpanded(viewState, scope, itemId, status));
    revealSelection = true;
  }
  draw();
  if (follow) scrollAnchor = undefined;
  else if (view === 'tasks') scrollAnchor = captureScrollAnchor();
  if (manual || key === 'f') {
    viewState.navigation ??= { follow };
    viewState.navigation.follow = follow;
    if (movedScroll || movedSelection) {
      viewState.navigation.scroll = scroll;
      if (view === 'tasks' && scrollAnchor) {
        viewState.navigation.scrollTaskId = scrollAnchor.id;
        viewState.navigation.scrollOffset = scrollAnchor.offset;
      } else if (view === 'tasks') {
        delete viewState.navigation.scrollTaskId;
        delete viewState.navigation.scrollOffset;
      }
    }
    if (movedSelection) {
      viewState.navigation.view = view;
      viewState.navigation.selectedId = selectedId;
      viewState.navigation.selectedTaskId = selectedTaskId;
      viewState.navigation.selectedNodes = [...selectedNodes].filter(pair => pair.every(value => typeof value === 'string'));
      viewState.navigation.completedExpanded = completedExpanded;
    }
    // A transient follow reveal/detail peek never becomes saved manual scroll.
    if (!viewError) {
      const snapshot = structuredClone(viewState);
      saving = saving.then(() => saveViewState(file, snapshot)).catch(cause => {
        viewError = cause.message; draw();
      });
    }
  }
}, focus: focused => {
  if (focused || closing) return;
  dragY = undefined; follow = true; revealSelection = false; verbose = false; scrollAnchor = undefined;
  draw();
}, pointer: mouse => {
  if (closing) return;
  if (mouse.release) { dragY = undefined; return; }
  if (mouse.button & 64) {
    follow = false; revealSelection = false; scrollAnchor = undefined;
    scroll = Math.max(0, scroll + (mouse.button & 1 ? 3 : -3));
  } else if ((mouse.button & 3) === 0) {
    follow = false; revealSelection = false; scrollAnchor = undefined;
    if (mouse.button & 32) {
      if (dragY !== undefined) scroll = Math.max(0, scroll + dragY - mouse.y);
    } else {
      const ranges = view === 'tasks' ? lastFrame?.taskRanges : lastFrame?.nodeRanges;
      const row = mouse.y - 1 - (lastFrame?.bodyStart ?? 0) + scroll;
      const inBody = mouse.y - 1 >= lastFrame?.bodyStart &&
        mouse.y - 1 < lastFrame.bodyStart + lastFrame.bodyRows;
      const hit = inBody ? Object.entries(ranges ?? {}).find(([, range]) => row >= range.start && row < range.end) ??
        (view === 'dag' ? Object.entries(lastFrame?.graphRanges ?? {}).find(([, range]) =>
          row >= range.start && row < range.end) : undefined) : undefined;
      if (hit) {
        const id = hit[0];
        if (view === 'tasks') selectedTaskId = id;
        else selectedNodes.set(selectedId, id);
        const item = view === 'tasks' ? workersSections(state).roots.find(item => item.id === id) :
          state.runs?.find(run => run.id === selectedId)?.nodes.find(item => item.id === id);
        if (item) setExpanded(viewState, view === 'tasks' ? TASK_SCOPE : selectedId, id,
          !isExpanded(viewState, view === 'tasks' ? TASK_SCOPE : selectedId, id, item.status ?? item.state));
      }
    }
    dragY = mouse.y;
  } else return;
  draw();
  if (!follow && view === 'tasks') scrollAnchor = captureScrollAnchor();
} });
disposeFocus = await watchHerdrFocus(paneId ?? process.env.HERDR_PANE_ID, () => {
  if (closing) return;
  dragY = undefined; follow = true; revealSelection = false; verbose = false; scrollAnchor = undefined; draw();
}, cause => { presenceNotice = `Focus: ${cause.message}`; draw(); })
  .catch(cause => { presenceNotice = `Focus: ${cause.message}`; return () => {}; });
process.on('SIGTERM', () => { void close(); }); process.on('SIGINT', () => { void close(); }); process.on('SIGHUP', () => { void close(); });
if (fixedView && launchToken) {
  retirementServer = net.createServer(socket => {
    retirementSockets.add(socket);
    socket.on('error', () => {});
    socket.once('close', () => retirementSockets.delete(socket));
    receive(socket, message => {
      if (closing || message?.type !== 'retire' || typeof message.nonce !== 'string' || !message.nonce ||
        message.statePath !== file || message.launchToken !== launchToken || message.paneId !== paneId ||
        !['role', 'sessionId', 'ownerKey', 'scopeEpoch', 'paneId', 'launchToken']
          .every(key => message[key] === state[key])) return socket.destroy();
      socket.end(`${JSON.stringify({ type: 'retiring', nonce: message.nonce,
        launchToken, manual: false })}\n`, () => { void close(false); });
    });
  });
  const retirementPath = ipcPath(`omo-herdr-viewer-retirement-${createHash('sha256').update(launchToken).digest('hex')}`);
  await clearStaleIpc(retirementPath);
  await new Promise((resolve, reject) => {
    retirementServer.once('error', reject); retirementServer.once('listening', resolve);
    retirementServer.listen(retirementPath);
  });
}
bindPresence();
draw();
// Recover a replacement that raced with watcher registration.
void refresh();
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await runViewer();
