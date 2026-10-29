import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import net from 'node:net';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { EventEmitter, once } from 'node:events';
import { readFile, readdir, realpath, rm, stat, writeFile } from 'node:fs/promises';
import { basename, join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { readJson, writeJson } from './storage.mjs';
import { pruneExpiredSnapshots, retentionDaysFromEnv } from './retention.mjs';
import { shellCommand } from './herdr.mjs';
import { normalizeRun, sessionRuns } from './model.mjs';
import { t, languageOf } from './i18n.mjs';
import { TaskData } from './task-data.mjs';
import { RECOVERY_V1, sameScope, validScope, yieldIssues } from '../../coordination/src/recovery.mjs';
import { ipcPath, queryPosixProcesses } from '../../coordination/src/platform.mjs';

export function viewKey(socket, pane, session) {
  return createHash('sha256').update(JSON.stringify([socket, pane, session])).digest('hex').slice(0, 24);
}

export function isColumnDashboard(layout, ids) {
  if (!layout?.area || layout.panes?.length !== 4 || new Set(Object.values(ids)).size !== 4) return false;
  const rects = Object.fromEntries(Object.entries(ids).map(([role, id]) => {
    const matches = layout.panes.filter(pane => pane.pane_id === id);
    return [role, matches.length === 1 ? matches[0].rect : null];
  }));
  const { parent, todo, workers, dag } = rects, area = layout.area;
  if (![parent, todo, workers, dag, area].every(rect => rect &&
    ['x', 'y', 'width', 'height'].every(key => Number.isFinite(rect[key])) && rect.width > 0 && rect.height > 0)) return false;
  return parent.x === area.x && parent.y === area.y && parent.height === area.height &&
    todo.x === parent.x + parent.width && todo.y === area.y &&
    workers.x === todo.x && workers.width === todo.width && workers.y === todo.y + todo.height &&
    todo.height + workers.height === area.height && dag.x === todo.x + todo.width &&
    dag.y === area.y && dag.height === area.height && dag.x + dag.width === area.x + area.width;
}

// Single place for the band layout ratios and floors (measured in layout-c task 1).
// chatShare applies only to fresh managed band creation, never to an existing dashboard.
// readableWorkersColumns is reporting-only and never a refusal reason.
export const BAND_LAYOUT = Object.freeze({
  chatShare: 0.5,
  todoHeight: 0.35,
  workersWidth: 0.4,
  minWorkersColumns: 24,
  readableWorkersColumns: 40,
  minTodoRows: 8,
  minLowerRows: 12,
  tolerance: 0,
});

// Band shape: parent on the left; Todo spans the rest of the width on top; workers (left) and DAG
// (right) share the row below it. Shape only, ratios are ignored.
export function isBandDashboard(layout, ids) {
  // Decision #185: a recorded collaboration pane directly left of workers joins the lower row.
  if (ids?.coordination !== undefined) {
    const { coordination, ...roles } = ids, panes = layout?.panes ?? [];
    const collab = panes.filter(pane => pane?.pane_id === coordination), workers = panes.find(pane => pane?.pane_id === roles.workers);
    if (collab.length !== 1 || !workers || Object.values(roles).includes(coordination)) return false;
    const [c, w] = [collab[0].rect, workers.rect];
    if (c?.y !== w?.y || c?.height !== w?.height || c?.x + c?.width !== w?.x) return false;
    return isBandDashboard({ ...layout, panes: panes.filter(pane => pane !== collab[0] && pane !== workers)
      .concat({ ...workers, rect: { ...w, x: c.x, width: c.width + w.width } }) }, roles);
  }
  if (!layout?.area || !Array.isArray(layout.panes) || layout.panes.length !== 4 || !ids || new Set(Object.values(ids)).size !== 4) return false;
  const rects = Object.fromEntries(Object.entries(ids).map(([role, id]) => {
    const matches = layout.panes.filter(pane => pane?.pane_id === id);
    return [role, matches.length === 1 ? matches[0].rect : null];
  }));
  const { parent, todo, workers, dag } = rects, area = layout.area;
  if (![parent, todo, workers, dag, area].every(rect => rect &&
    ['x', 'y', 'width', 'height'].every(key => Number.isFinite(rect[key])) && rect.width > 0 && rect.height > 0)) return false;
  return parent.x === area.x && parent.y === area.y && parent.height === area.height &&
    todo.x === parent.x + parent.width && todo.y === area.y && todo.x + todo.width === area.x + area.width &&
    todo.width === workers.width + dag.width &&
    workers.x === todo.x && workers.y === todo.y + todo.height &&
    dag.x === workers.x + workers.width && dag.y === workers.y && dag.height === workers.height &&
    todo.height + workers.height === area.height;
}

// Cell sizes of the band inside a dashboard region, rounded like a Herdr split
// (first child = round(fround(length * fround(ratio))), task-1 S2). `fits` is the
// hard floor; `readable` is reporting only and never a refusal reason.
export function bandSizes({ width, height }) {
  const first = (length, ratio) => Math.round(Math.fround(length * Math.fround(ratio)));
  const todoRows = first(height, BAND_LAYOUT.todoHeight), workersColumns = first(width, BAND_LAYOUT.workersWidth);
  const lowerRows = height - todoRows;
  return { todoRows, lowerRows, workersColumns, dagColumns: width - workersColumns,
    fits: workersColumns >= BAND_LAYOUT.minWorkersColumns && todoRows >= BAND_LAYOUT.minTodoRows &&
      lowerRows >= BAND_LAYOUT.minLowerRows,
    readable: workersColumns >= BAND_LAYOUT.readableWorkersColumns };
}

// The dashboard region a fresh band opens by splitting the chat once at chatShare.
export function freshBandRegion(parent) {
  const chat = Math.round(Math.fround(parent.width * Math.fround(BAND_LAYOUT.chatShare)));
  return { x: parent.x + chat, y: parent.y, width: parent.width - chat, height: parent.height };
}

const bandWaiting = 'layout-waiting: the dashboard needs a larger terminal (workers >= ' +
  `${BAND_LAYOUT.minWorkersColumns} columns, Todo >= ${BAND_LAYOUT.minTodoRows} rows, lower row >= ` +
  `${BAND_LAYOUT.minLowerRows} rows); enlarge this terminal`;
const unverifiedRegion = 'layout-waiting: an open dashboard pane cannot be verified in this tab; close it, then run /dashboard';
const claimWaiting = 'layout-waiting: the open dashboard panes have no single recorded layout; close them, then run /dashboard';
const closedWaiting = 'layout-waiting: the dashboard panes were closed; run /dashboard to reopen them';
const unplaceable = 'layout-waiting: the dashboard panes cannot be re-placed from this arrangement; close them, then run /dashboard';
const foreignWaiting = 'layout-waiting: this tab holds a pane the dashboard does not own; close it, then run /dashboard';
const placementUnverified = 'layout-waiting: a pane left by an interrupted dashboard placement cannot be verified; ' +
  'close it, then run /dashboard';
const placementMismatch = 'layout-waiting: an interrupted dashboard placement does not match the panes in this tab; ' +
  'close its extra pane if one is open, then run /dashboard';
const placementSchema = 'omo-herdr-dashboard/temporary-placement/1';
// Whether a journal phase proves its move's swap done (true) or not issued (false).
// SWAP_PREPARED leaves it to the panes (undefined).
const swapRecorded = { PREPARED: false, SPLIT_APPLIED: false, SPLIT_SETTLED: false, SWAP_PREPARED: undefined,
  SWAP_SETTLED: true, CLOSE_PREPARED: true };
const temporaryOf = journal => {
  const id = journal?.temporary?.paneId ?? journal?.createdPaneId;
  return typeof id === 'string' && id ? id : undefined;
};

const sameRect = (a, b) => ['x', 'y', 'width', 'height'].every(key => a?.[key] === b?.[key]);
// The same native tree, focus aside: tab, area, every pane rect and every split.
function sameLayout(a, b) {
  const shape = value => JSON.stringify([value.tab_id, value.area,
    (value.panes ?? []).map(pane => JSON.stringify([pane?.pane_id, pane?.rect])).sort(),
    (value.splits ?? []).map(split => JSON.stringify([split?.direction, split?.ratio, split?.rect])).sort()]);
  return Boolean(a && b) && shape(a) === shape(b);
}
// A split's two child rectangles, rounded like Herdr (task-1 S2).
function childRects(rect, direction, ratio) {
  const axis = direction === 'right' ? 'width' : 'height', coordinate = direction === 'right' ? 'x' : 'y';
  const size = Math.round(Math.fround(rect[axis] * Math.fround(ratio)));
  return [{ ...rect, [axis]: size }, { ...rect, [coordinate]: rect[coordinate] + size, [axis]: rect[axis] - size }];
}
const leavesOf = node => node.pane ? [node.pane] : [...leavesOf(node.first), ...leavesOf(node.second)];
const placeTree = (node, rect, out = new Map()) => {
  if (node.pane) return out.set(node.pane, rect);
  const [first, second] = childRects(rect, node.direction, node.ratio);
  placeTree(node.first, first, out);
  return placeTree(node.second, second, out);
};
const mapLeaves = (node, leaf) => node.pane ? leaf(node)
  : { ...node, first: mapLeaves(node.first, leaf), second: mapLeaves(node.second, leaf) };
const dropLeaf = (node, id) => node.pane ? node : node.first.pane === id ? node.second : node.second.pane === id
  ? node.first : { ...node, first: dropLeaf(node.first, id), second: dropLeaf(node.second, id) };
const swapLeaves = (node, a, b) => mapLeaves(node, leaf => leaf.pane === a ? { pane: b } : leaf.pane === b ? { pane: a } : leaf);
// The journaled temporary pane's leaf in a simulated tree.
const temporaryLeaf = '\u0000temporary';
// The same split tree: every direction, stored ratio (float32) and leaf, in order.
// Rects aside: a terminal resize keeps every ratio and moves no pane between leaves.
const sameTree = (a, b) => Boolean(a && b) && (a.pane || b.pane ? a.pane === b.pane : a.direction === b.direction &&
  Math.fround(a.ratio) === Math.fround(b.ratio) && sameTree(a.first, b.first) && sameTree(a.second, b.second));
// The same panes in the same places: every direction and leaf, ratios aside.
const sameShape = (a, b) => Boolean(a && b) && (a.pane || b.pane ? a.pane === b.pane : a.direction === b.direction &&
  sameShape(a.first, b.first) && sameShape(a.second, b.second));
// The trees of one journaled move: before it, after its temporary split, after its swap
// and after its temporary close.
function moveTrees(node, id, target, direction) {
  const split = mapLeaves(node, leaf => leaf.pane === target
    ? { direction, ratio: 0.5, first: leaf, second: { pane: temporaryLeaf } } : leaf);
  const swapped = swapLeaves(split, id, temporaryLeaf);
  return [node, split, swapped, dropLeaf(swapped, temporaryLeaf)];
}
// A simulated phase keeps the chat's rect, collapses no pane below one cell and, after a
// swap, lets focus return to the chat leftwards in at most three steps through role panes
// only (like restorePlacementFocus).
function phaseSafe(node, area, parent, chat, swapped) {
  const rects = placeTree(node, area);
  if (!sameRect(rects.get(parent), chat) || [...rects.values()].some(rect => rect.width < 1 || rect.height < 1))
    return false;
  const reaches = (id, steps) => {
    if (id === parent) return true;
    const from = rects.get(id);
    const left = [...rects].filter(([other, rect]) => other !== id && rect.x + rect.width === from.x &&
      Math.min(rect.y + rect.height, from.y + from.height) > Math.max(rect.y, from.y)).map(([other]) => other);
    return steps > 0 && left.length > 0 && left.every(other => other !== temporaryLeaf && reaches(other, steps - 1));
  };
  return swapped === undefined || reaches(swapped, 3);
}

// The native split tree behind a layout reply: { pane } leaves and { direction,
// ratio, first, second } splits, every split and pane used exactly once; else null.
function splitTree(layout) {
  const { panes, splits, area } = layout ?? {};
  const valid = rect => rect && ['x', 'y', 'width', 'height'].every(key => Number.isFinite(rect[key])) &&
    rect.width > 0 && rect.height > 0;
  if (!Array.isArray(panes) || !Array.isArray(splits) || splits.length !== panes.length - 1 || !valid(area) ||
    !panes.every(pane => typeof pane?.pane_id === 'string' && valid(pane.rect))) return null;
  const used = new Set();
  const build = rect => {
    const leaves = panes.filter(pane => sameRect(pane.rect, rect));
    const nodes = splits.filter(split => !used.has(split) && sameRect(split?.rect, rect));
    if (leaves.length + nodes.length !== 1) return null;
    if (leaves.length) return { pane: leaves[0].pane_id };
    const [split] = nodes;
    used.add(split);
    if (!['right', 'down'].includes(split.direction) || !(split.ratio > 0 && split.ratio < 1)) return null;
    const [a, b] = childRects(split.rect, split.direction, split.ratio);
    const first = build(a), second = first && build(b);
    return second ? { direction: split.direction, ratio: split.ratio, first, second } : null;
  };
  const root = build(area);
  return root && used.size === splits.length && leavesOf(root).length === panes.length ? root : null;
}

// The task-1 migration b2 (move the DAG below Todo, move workers right of the
// DAG, swap them), or its shortest suffix that already ends in the band, so an
// interrupted migration resumes from its geometry. The chat must be the root's
// left child and the region beside it exactly the three role panes. Every phase
// is simulated on the split tree before the first journal write: the chat keeps
// its rect, no pane collapses below one cell, and each focus restoration (left,
// at most three steps, like restorePlacementFocus) passes role panes only.
function planBandMigration(layout, ids) {
  const tree = splitTree(layout), roles = [ids.todo, ids.workers, ids.dag].sort().join('\n');
  if (!tree || tree.direction !== 'right' || tree.first.pane !== ids.parent || tree.second.pane ||
    leavesOf(tree.second).sort().join('\n') !== roles) return { waiting: unplaceable };
  const area = layout.area, chat = placeTree(tree, area).get(ids.parent);
  if (!bandSizes({ width: area.width - chat.width, height: area.height }).fits) return { waiting: bandWaiting };
  const safe = (node, swapped) => phaseSafe(node, area, ids.parent, chat, swapped);
  const steps = [['move', ids.dag, ids.todo, 'down'], ['move', ids.workers, ids.dag, 'right'], ['swap', ids.workers, ids.dag]];
  for (let start = steps.length; start >= 0; start--) {
    const plan = steps.slice(start), trees = [];
    let node = tree, ok = true;
    for (const [kind, id, target, direction] of plan) {
      // The tree each guard of this step must find (A3): the step's arguments
      // were decided on these, before, between and after its mutations.
      if (kind === 'swap') {
        trees.push([node, swapLeaves(node, id, target)]);
        node = trees.at(-1)[1]; ok = safe(node, id);
      } else {
        trees.push(moveTrees(node, id, target, direction));
        const [, split, swapped, closed] = trees.at(-1);
        ok = safe(split) && safe(swapped, id) && safe(closed);
        node = closed;
      }
      if (!ok) break;
    }
    const panes = [...placeTree(node, area)].map(([pane_id, rect]) => ({ pane_id, rect }));
    if (ok && isBandDashboard({ area, panes }, ids)) return { plan, trees, share: tree.ratio };
  }
  return { waiting: unplaceable };
}

// Whether a parent's dashboard region is new, inherited or unverifiable: a
// whitelist over each role's evidence, so a combination it does not name waits.
// Per role: its pane record ({ state: 'absent' | 'unreadable' | 'malformed' |
// 'valid', paneId, tabId, manualClose, layout }), the pane its parent-slot
// registration names (null without a pane id) and a Map from every pane either
// names to the tab ids of its list rows and whether it is authoritatively gone.
// A role is
// - silent: no record naming a pane, and no registration or one whose pane is gone;
// - closed: a valid record whose pane, like any registered one, is gone - or a
//   malformed one (e.g. written by an older generation's viewer) that names a pane
//   and every pane it or the registration names is authoritatively gone: nothing
//   is left open to verify;
// - consistent: a valid open record in the parent's tab, registered with the same
//   pane (or, while its launch is still pending, with no registration at all or one
//   whose pane is authoritatively gone, so an interrupted creation or reopening
//   resumes), which has exactly one list row, in the parent's tab;
// - otherwise unverified.
// FRESH (null) needs a readable list, the parent's tab and only silent roles, or
// closed ones on an explicit /dashboard: an automatic pass never reopens a
// dashboard this conversation's owner closed; panes that died with an earlier
// host lifetime (a restart) come back as { waiting, reopen: true }. INHERITED ({ layout }) needs
// consistent roles sharing one 'band' or 'columns' claim, the rest silent or
// closed. Everything else is WAIT ({ waiting }), never a throw: a throw latches
// recovery (IS-10, G4).
export function decideRegion({ tabId, readable, roles, explicit = false, lifetimeId }) {
  if (!readable || !tabId) return { waiting: unverifiedRegion };
  const layouts = new Set();
  let closed = false, died = true;
  for (const { record, registered, panes } of Object.values(roles)) {
    const named = [record.paneId, registered].filter(id => id !== undefined);
    if ((['absent', 'valid'].includes(record.state) || record.state === 'malformed' && record.paneId !== undefined) &&
      named.every(id => panes.get(id).gone)) {
      if (record.paneId !== undefined) {
        closed = true;
        // Died, not closed: claimed by an earlier host lifetime and never closed by
        // the owner (q or an observed absence marks manualClose).
        died &&= record.manualClose !== true && typeof lifetimeId === 'string' &&
          typeof record.lifetimeId === 'string' && record.lifetimeId !== lifetimeId;
      }
      continue;
    }
    const rows = panes.get(record.paneId)?.rows;
    if (record.state !== 'valid' || record.manualClose || record.tabId !== tabId ||
      registered !== record.paneId && !(record.pending && (registered === undefined || panes.get(registered)?.gone)) ||
      rows?.length !== 1 || rows[0] !== tabId) return { waiting: unverifiedRegion };
    layouts.add(record.layout);
  }
  if (!layouts.size) return closed && !explicit ? { waiting: closedWaiting, ...(died ? { reopen: true } : {}) } : null;
  const [layout] = layouts;
  return layouts.size === 1 && ['band', 'columns'].includes(layout) ? { layout } : { waiting: claimWaiting };
}

export function dagTitle(sessionId) {
  return `DAG · ${sessionId.slice(0, 8)}`;
}

export function isDagViewerPane(pane) {
  return [pane?.label, pane?.terminal_title, pane?.terminal_title_stripped]
    .some(name => typeof name === 'string' && (name.startsWith('DAG · ') || name === 'OmO DAG'));
}

function missingPane(error) {
  return /pane_not_found|unknown pane|pane .*not found/i.test(`${error.message} ${error.stderr ?? ''}`);
}

export class DagPane {
  constructor({ sessionId, parentPane, socket, stateDir, cwd, node, viewer, herdr, notify = () => {}, language = 'en', taskStateDir, retentionDays, streamDelay = 250, read = readJson, write = writeJson, now = () => new Date().toISOString(), cacheReads = false }) {
    Object.assign(this, { sessionId, parentPane, stateDir, cwd, node, viewer, herdr: cacheReads ? herdrRead(herdr) : herdr, notify, read, write, now });
    this.language = languageOf(language);
    this.key = viewKey(socket, parentPane, sessionId);
    this.stateFile = join(stateDir, `${this.key}.json`);
    this.recordFile = join(stateDir, `${this.key}.pane.json`);
    this.checkpointDir = join(taskStateDir ?? join(cwd, '.omo', 'senpi-task'), 'dag', 'runs');
    // Explicit option wins so tests need not mutate the shared process environment.
    this.retentionDays = retentionDays ?? retentionDaysFromEnv();
    this.streamDelay = streamDelay;
    this.queue = Promise.resolve();
    this.runs = [];
    this.stopped = false;
    this.tasks = [];
    this.streams = new Map();
    this.stateSignature = undefined;
    this.lastWrittenState = undefined;
    this.written = new Map();
    this.taskData = new TaskData({ cwd, sessionId, stateDir: taskStateDir, notify, read,
      onChange: names => { if (!this.stopped) this.enqueue(() => this.save(true, 'changed')); } });
  }

  // An identical warning is shown once until a job succeeds or the text changes;
  // an explicit force call (/dashboard) always reports its result.
  enqueue(job, { force = false } = {}) {
    const result = this.queue.then(job);
    this.queue = result.then(() => { this.lastWarning = undefined; }, error => {
      if (error?.code === 'LAYOUT_WAIT') return; // The Todo pass gives a gate wait its one notice.
      const message = `DAG pane: ${error.message}`;
      const repeat = message === this.lastWarning;
      this.lastWarning = message;
      if (force || !repeat) this.notify(message);
    });
    return result;
  }

  receive(payload) {
    const runs = sessionRuns(payload, this.sessionId, this.language);
    if (runs === null || this.stopped) return Promise.resolve();
    return this.enqueue(async () => {
      // RPC replaces transient runs; durable runs omitted by a snapshot remain recoverable.
      await this.restoreRuns(runs, true);
      await this.save(true, 'none');
    });
  }

  start() {
    return this.enqueue(async () => {
      // Housekeeping precedes restore so a fresh pane never lists pruned snapshots.
      await pruneExpiredSnapshots(this.stateDir, {
        keepFiles: [basename(this.stateFile), basename(this.recordFile), `${basename(this.stateFile)}.view.json`],
        days: this.retentionDays, notify: message => this.notify(t(this.language, 'pruneFailed', { error: message })) });
      const state = await readJson(this.stateFile);
      if (state?.sessionId === this.sessionId) {
        this.runs = state.runs ?? [];
        this.taskData.restore(state.tasks);
      }
      this.taskData.start();
      await this.restoreRuns();
      await this.save(true, 'full');
    });
  }

  receiveTasks(payload) {
    if (this.stopped) return Promise.resolve();
    return this.enqueue(async () => {
      if (this.taskData.receive(payload)) await this.save(true, 'none');
    });
  }

  // Streamed tokens arrive far more often than task records change; coalesce
  // them and rewrite the snapshot without rescanning the task store. The
  // returned promise settles after the coalesced save, so tests need no sleeps.
  // `since` marks when the current phase began and `lastAt` the latest event,
  // so the viewer clock can show elapsed waits and gaps between tokens.
  receiveStream(update) {
    if (this.stopped) return Promise.resolve();
    if (update.active) {
      const previous = this.streams.get(update.taskId);
      const samePhase = previous?.phase === update.phase && previous.tool === update.tool;
      this.streams.set(update.taskId, { phase: update.phase,
        ...(update.tool ? { tool: update.tool } : {}), ...(update.text ? { text: update.text } : {}),
        ...(update.attempt ? { attempt: update.attempt } : {}), ...(update.maxAttempts ? { maxAttempts: update.maxAttempts } : {}),
        since: samePhase ? previous.since : update.now, lastAt: update.now });
    } else if (!this.streams.delete(update.taskId)) return Promise.resolve();
    if (this.streamTimer) return this.streamFlush;
    this.streamFlush = new Promise(resolve => {
      this.streamTimer = setTimeout(() => {
        this.streamTimer = undefined;
        this.enqueue(() => this.save(true, 'none')).then(resolve, resolve);
      }, this.streamDelay);
      this.streamTimer.unref?.();
    });
    return this.streamFlush;
  }

  withStreams(tasks, connected) {
    if (!connected || !this.streams.size) return tasks;
    return tasks.map(task => {
      // A finished task never regains a stale activity from a late event.
      if (task.status !== 'running') { this.streams.delete(task.id); return task; }
      const activity = this.streams.get(task.id);
      return activity ? { ...task, activity } : task;
    });
  }

  async restoreRuns(runs = this.runs, preferLive = false) {
    let files;
    try { files = await readdir(this.checkpointDir); }
    catch (error) { if (error.code !== 'ENOENT') throw error; files = []; }
    const restored = new Map(runs.map(run => [run.id, run]));
    for (const file of files.filter(file => file.endsWith('.json'))) {
      try {
        const raw = await readJson(join(this.checkpointDir, file));
        if (raw === null) continue; // A checkpoint may disappear after readdir.
        if (typeof raw.parentSessionId !== 'string' || raw.schemaVersion !== 1) throw new Error('Invalid checkpoint header');
        if (raw.parentSessionId !== this.sessionId) continue;
        const run = normalizeRun(raw);
        if (!run) throw new Error('Invalid checkpoint run');
        // Startup/open distrust cached state; an incoming RPC may be ahead of disk.
        if (!preferLive || !restored.has(run.id)) restored.set(run.id, run);
      } catch (error) { this.notify(`DAG pane: Cannot read checkpoint ${file}: ${error.message}`); }
    }
    this.runs = [...restored.values()].sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }

  async writeChanged(path, value) {
    const serialized = JSON.stringify(value);
    if (this.written.get(path) === serialized) {
      try { await stat(path); return; }
      catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
    await this.write(path, value);
    this.written.set(path, serialized);
  }

  async save(connected, mode = 'full') {
    if (mode === 'full') this.tasks = await this.taskData.refresh(this.runs);
    else if (mode === 'changed') this.tasks = await this.taskData.refreshChanged();
    else { await this.taskData.idle(); this.tasks = this.taskData.snapshot(); }
    await this.taskData.idle();
    const tasks = this.withStreams(this.tasks, connected);
    const signature = JSON.stringify([this.runs, tasks, connected, this.language]);
    if (signature !== this.stateSignature) { this.stateSignature = signature; this.updatedAt = this.now(); }
    const payload = { version: 1, sessionId: this.sessionId, connected, language: this.language,
      updatedAt: this.updatedAt, runs: this.runs, tasks };
    await this.writeChanged(this.stateFile, payload);
    // All callers serialize saves through the queue, including task-only disk changes.
    if (connected && !this.stopped && (this.runs.length || this.tasks.length)) await this.ensure(false);
  }

  open() {
    return this.enqueue(async () => {
      if (!this.runs.length) {
        const state = await readJson(this.stateFile);
        if (state?.sessionId === this.sessionId) {
          this.runs = state.runs ?? [];
          this.taskData.restore(state.tasks);
        }
      }
      await this.restoreRuns();
      await this.save(true, 'full');
      return this.ensure(true);
    });
  }

  async listDagPanes() {
    try {
      const panes = (await this.herdr('list'))?.panes ?? [];
      const tab = panes.find(pane => pane.pane_id === this.parentPane)?.tab_id;
      return panes.filter(isDagViewerPane)
        .filter(pane => pane.pane_id && pane.pane_id !== this.parentPane && (!tab || pane.tab_id === tab))
        .map(pane => pane.pane_id);
    } catch (error) {
      if (error instanceof Error) return [];
      throw error;
    }
  }

  async closePane(paneId) {
    try { await this.herdr('close', paneId); }
    catch (error) {
      if (!missingPane(error)) this.notify(t(this.language, 'closeFailed', { error: error.message }));
    }
  }

  async closeDagPanes(keep) {
    for (const paneId of await this.listDagPanes()) {
      if (paneId !== keep) await this.closePane(paneId);
    }
  }

  async ensure(force) {
    const record = await readJson(this.recordFile);
    // Preserve a manually closed pane across events/reloads. /dag-pane explicitly reopens it.
    if (record && !force) return record.paneId;
    if (record?.paneId) {
      try {
        await this.herdr('get', record.paneId);
        if (record.ready) {
          await this.closeDagPanes(record.paneId);
          return record.paneId;
        }
        // Occupied leftover from a failed launch: close it, then replace.
        await this.closePane(record.paneId);
      } catch (error) {
        if (!missingPane(error)) throw error;
      }
    }
    if (typeof this.node === 'function') this.node = await this.node();
    // Record an attempt before mutation: a timeout must not create repeated orphan panes.
    await writeJson(this.recordFile, { attempted: true });
    await this.closeDagPanes();
    const result = await this.herdr('split', '--pane', this.parentPane, '--direction', 'right',
      '--ratio', '0.65', '--cwd', this.cwd, '--no-focus');
    const paneId = result?.pane?.pane_id;
    if (!paneId) throw new Error(t(this.language, 'missingPaneId'));
    await writeJson(this.recordFile, { paneId, ready: false });
    await this.herdr('rename', paneId, dagTitle(this.sessionId));
    await this.herdr('run', paneId, shellCommand([this.node, this.viewer, '--state', this.stateFile, '--close-pane', paneId]));
    await writeJson(this.recordFile, { paneId, ready: true });
    return paneId;
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.streamTimer);
    this.streams.clear();
    this.taskData.stop();
    return this.enqueue(() => this.save(false, 'none'));
  }
}

// The extension uses this fixed-role controller. The old standalone controller
// remains available to existing unmanaged callers, but is never a layout peer.
const sameOwner = (a, b) => a && b && ['socketPath', 'parentPaneId', 'sessionId', 'ownerKey']
  .every(key => a[key] === b[key]);
const pathKey = path => process.platform === 'win32' ? String(path).replaceAll('\\', '/').toLowerCase() : String(path);
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
const canonical = path => process.platform === 'win32' ? path.replaceAll('\\', '/').toLowerCase() : path;
const blocked = cause => Object.assign(new Error(`BLOCKED: ${cause?.message ?? cause}`), { inlineFallback: true, cause });
export async function probeManagedSlot({ stateDir, socketPath, parentPaneId }) {
  let root;
  try { root = canonical(await realpath(resolve(stateDir))); }
  catch (error) { return error.code === 'ENOENT' ? { blocked: false } : { blocked: true, code: 'SLOT_UNKNOWN' }; }
  let socket;
  try {
    const pipe = socketPath.replaceAll('/', '\\');
    socket = /^\\\\[.]\\pipe\\/i.test(pipe) ? canonical(pipe) : canonical(await realpath(resolve(socketPath)));
  } catch { return { blocked: true, code: 'SLOT_UNKNOWN' }; }
  const key = createHash('sha256').update(JSON.stringify([socket, parentPaneId])).digest('hex');
  const directory = join(root, 'parent-slots', key);
  try { await stat(join(directory, 'slot.json')); return { blocked: true, code: 'MANAGED_SLOT' }; }
  catch (error) { if (error.code !== 'ENOENT') return { blocked: true, code: 'SLOT_UNKNOWN' }; }
  try { await stat(join(directory, 'lease.sqlite')); }
  catch (error) { return error.code === 'ENOENT' ? { blocked: false } : { blocked: true, code: 'SLOT_UNKNOWN' }; }
  let db;
  try {
    db = new DatabaseSync(join(directory, 'lease.sqlite'), { readOnly: true, timeout: 0 });
    db.prepare('PRAGMA schema_version').get();
    return { blocked: true, code: 'MANAGED_SLOT' };
  } catch (error) { return { blocked: true, code: error.errcode === 5 ? 'SQLITE_BUSY' : 'SLOT_UNKNOWN' }; }
  finally { db?.close(); }
}
const nativeIdentity = row => {
  if (!Number.isSafeInteger(row?.ProcessId) || !row.Birth || !row.ExecutablePath || !row.CommandLine) throw blocked('native identity unavailable');
  return { pid: row.ProcessId, birth: row.Birth, executable: row.ExecutablePath,
    argvDigest: createHash('sha256').update(row.CommandLine).digest('hex') };
};

function receiveControl(socket, callback) {
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
function processMatch(info, record, expectedPid) {
  const matches = (info?.process_info?.foreground_processes ?? []).filter(process => {
    const argv = process.argv ?? [];
    return argv.length === 10 && pathKey(argv[0]) === pathKey(record.nodePath) && pathKey(argv[1]) === pathKey(record.viewerPath) &&
      argv[2] === '--view' && argv[3] === (record.role === 'dag' ? 'dag' : 'tasks') &&
      argv[4] === '--state' && pathKey(argv[5]) === pathKey(record.statePath) &&
      argv[6] === '--close-pane' && argv[7] === record.paneId &&
      argv[8] === '--launch-token' && argv[9] === record.launchToken &&
      (expectedPid === undefined || expectedPid === process.pid) &&
      (record.processId === undefined || record.processId === process.pid);
  });
  return matches.length === 1 ? matches[0] : null;
}
const executeProcessQuery = promisify(execFile);
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
export async function queryShellProcesses(pid, run = executeProcessQuery) {
  // Fail as a query failure without starting PowerShell: an empty row list would be read by
  // observePreviousProducer as proof that the producer died.
  if (!Number.isSafeInteger(pid) || pid < 1) throw Object.assign(new Error('process query skipped: invalid pid'), { code: 'SHELL_QUERY_INVALID_PID' });
  for (let attempt = 0; attempt < 2; attempt++) {
  const { stdout } = await run('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command',
    `[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); $ErrorActionPreference = 'Stop'; $rows = @(Get-CimInstance Win32_Process -Filter "ProcessId = ${pid} OR ParentProcessId = ${pid}" | Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine,@{Name='CreationDate';Expression={$_.CreationDate.ToUniversalTime().ToString('o')}},@{Name='Birth';Expression={$h=[Diagnostics.Process]::GetProcessById($_.ProcessId);try{$null=$h.Handle;$h.StartTime.ToUniversalTime().ToFileTimeUtc().ToString()}finally{$h.Dispose()}}}); ConvertTo-Json -InputObject $rows -Compress`],
  { timeout: 30000, maxBuffer: 65536 }); // was 5 s, same as the Todo process query
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
// Herdr list/process-info reads are repeated by layout passes a few times per
// second, and every uncached call synchronously spawns herdr on Windows. The
// 1000 ms cache is cleared by the shared process epoch (any pane mutation, a
// viewer register/close, or a Todo-side bump). Placement guards and final
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
const nativeShellProcesses = cachedProcessQuery(process.platform === 'win32' ? queryShellProcesses : queryPosixProcesses);
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
  if (!child.Birth || record.viewerBirth && child.Birth !== record.viewerBirth ||
    pathKey(child.ExecutablePath) !== pathKey(record.nodePath) || !argv ||
    !processMatch({ process_info: { foreground_processes: [{ pid: child.ProcessId, argv }] } }, record)) return null;
  actual.viewer = nativeIdentity(child);
  return actual;
}
// Read-only legacy Collab proof (todo 6). A v1 coordination record stores no
// launch token, node path or shell identity, so a record id alone can never
// prove or close the pane. The exact Herdr shell PID is queried through the
// exported cached process machinery; the one Node child must carry the full v1
// viewer command for this parent, session, record and managed locale, and its
// native Birth identities must bracket record.createdAt. Any missing or foreign
// evidence is legacy-waiting, never a fallback to the record's pane id.
export function legacyCollabCommand(record, paths, language) {
  if (!record || typeof record.viewer !== 'string' || !record.viewer ||
    typeof record.sessionId !== 'string' || !record.sessionId ||
    typeof record.paneId !== 'string' || !record.paneId ||
    !paths?.record || !paths?.status || !paths?.activity || !language) return null;
  return [record.viewer, '--status', paths.status, '--activity', paths.activity,
    '--session', record.sessionId, '--record', paths.record, '--lang', language,
    '--close-pane', record.paneId];
}
export async function observeLegacyCollab(info, record, options = {}, inspect = nativeShellProcesses) {
  if (!record || record.v !== 1 || record.manualClose === true) return null;
  if (typeof record.paneId !== 'string' || !record.paneId) return null;
  if (options.parentPane === undefined || record.parentPaneId !== options.parentPane) return null;
  if (typeof options.sessionId !== 'string' || record.sessionId !== options.sessionId) return null;
  if (typeof options.tabId !== 'string' || record.tabId !== options.tabId) return null;
  const reportedPane = info?.process_info?.pane_id;
  if (reportedPane !== undefined && reportedPane !== record.paneId) return null;
  const shellPid = info?.process_info?.shell_pid;
  if (!Number.isSafeInteger(shellPid) || shellPid < 1) return null;
  // Off Windows only an injected equivalent Herdr argv inspector is evidence;
  // the default PowerShell/WMI query is never started as a substitute.
  const platform = options.platform ?? process.platform;
  if (platform !== 'win32' && inspect === nativeShellProcesses) return null;
  const stateDir = options.stateDir ?? process.env.OMO_HERDR_COORDINATION_STATE_DIR;
  if (!stateDir) return null;
  const key = String(options.parentPane).replace(/[^A-Za-z0-9_-]/g, '_');
  const paths = { record: options.recordPath ?? join(stateDir, `${key}.pane.json`),
    status: options.statusPath ?? join(stateDir, `${key}.status.json`), activity: options.activityPath ?? null };
  if (!paths.activity) return null;
  const language = ['en', 'ko', 'zh-cn'].includes(options.language) ? options.language : null;
  if (!language) return null;
  const expected = legacyCollabCommand(record, paths, language);
  if (!expected) return null;
  // The memo is keyed by one core lifetime, pane id and native shell pid, and it
  // never bypasses the process observer. Every call re-reads the exact native
  // tuple — shell pid/birth/creation, the single Node child pid/birth/creation
  // and the argv after argv[0] — before a cached proof may be reused. A changed
  // tuple, a missing process, or a changed record/path/locale deletes the entry
  // and forces a fresh validation; the production inspector is the cached
  // process query, so an unchanged identity does not start another native scan.
  const cacheKey = `${options.lifetimeId ?? ''}\u0000${record.paneId}\u0000${shellPid}`;
  const invalidate = () => { if (options.cache instanceof Map) options.cache.delete(cacheKey); };
  const sameNative = (left, right) => Boolean(left && right) &&
    left.shellPid === right.shellPid && left.shellBirth === right.shellBirth &&
    left.shellCreationDate === right.shellCreationDate && left.viewerPid === right.viewerPid &&
    left.viewerBirth === right.viewerBirth && left.viewerCreationDate === right.viewerCreationDate &&
    left.argvDigest === right.argvDigest && Array.isArray(left.argv) && Array.isArray(right.argv) &&
    left.argv.length === right.argv.length && left.argv.every((value, index) => value === right.argv[index]);
  let rows;
  try { rows = await inspect(shellPid); } catch { invalidate(); return null; }
  if (!Array.isArray(rows) || !rows.length || new Set(rows.map(row => row.ProcessId)).size !== rows.length) {
    invalidate(); return null;
  }
  const shells = rows.filter(row => row.ProcessId === shellPid);
  if (shells.length !== 1) { invalidate(); return null; }
  const shell = shells[0], born = Date.parse(shell.CreationDate);
  if (!Number.isFinite(born) || !shell.ExecutablePath || !shell.CommandLine ||
    !/^[1-9]\d*$/.test(String(shell.Birth ?? ''))) { invalidate(); return null; }
  const children = rows.filter(row => row !== shell && row.ParentProcessId === shellPid);
  // Exactly one child, and no unrelated row in the queried shell's process set.
  if (children.length !== 1 || children.length !== rows.length - 1) { invalidate(); return null; }
  const child = children[0], childBorn = Date.parse(child.CreationDate);
  if (!Number.isSafeInteger(child.ProcessId) || child.ProcessId < 1 || child.ProcessId === shellPid ||
    !Number.isFinite(childBorn) || childBorn < born || !/^[1-9]\d*$/.test(String(child.Birth ?? '')) ||
    BigInt(child.Birth) < BigInt(shell.Birth) ||
    !['node', 'node.exe', 'nodejs', 'nodejs.exe'].includes(basename(child.ExecutablePath ?? '').toLowerCase())) {
    invalidate(); return null;
  }
  const createdAt = Date.parse(record.createdAt);
  if (!Number.isFinite(createdAt) || born > createdAt || createdAt > childBorn) { invalidate(); return null; }
  const foreground = info?.process_info?.foreground_processes;
  if (foreground !== undefined && (!Array.isArray(foreground) || foreground.length !== 1 ||
    ![shellPid, child.ProcessId].includes(foreground[0]?.pid))) { invalidate(); return null; }
  const argv = parseLaunchCommand(child.CommandLine);
  if (!argv || argv.length !== expected.length + 1 || pathKey(argv[0]) !== pathKey(child.ExecutablePath)) {
    invalidate(); return null;
  }
  // argv[0] is the native Node child. Everything after it must match the v1
  // viewer command exactly: viewer, status, activity, session, record, lang, close-pane.
  for (let index = 0; index < expected.length; index += 1) {
    const actual = argv[index + 1], wanted = expected[index];
    const same = [0, 2, 4, 8].includes(index)
      ? typeof actual === 'string' && pathKey(actual) === pathKey(wanted)
      : actual === wanted;
    if (!same) { invalidate(); return null; }
  }
  const native = { shellPid, shellBirth: String(shell.Birth), shellCreationDate: shell.CreationDate,
    viewerPid: child.ProcessId, viewerBirth: String(child.Birth), viewerCreationDate: child.CreationDate,
    argv: argv.slice(1), argvDigest: createHash('sha256').update(child.CommandLine).digest('hex') };
  const cached = options.cache instanceof Map ? options.cache.get(cacheKey) : null;
  if (!options.fresh && cached && sameNative(cached.native, native) &&
    cached.lifetimeId === (options.lifetimeId ?? null) && cached.paneId === record.paneId &&
    cached.recordAt === record.createdAt && cached.viewer === record.viewer &&
    cached.parentPane === options.parentPane && cached.sessionId === options.sessionId &&
    cached.tabId === options.tabId && cached.language === language &&
    cached.paths.record === paths.record && cached.paths.status === paths.status &&
    cached.paths.activity === paths.activity) return structuredClone(cached.proof);
  const proof = { v: 1, paneId: record.paneId, parentPaneId: options.parentPane,
    sessionId: options.sessionId, tabId: options.tabId, createdAt: record.createdAt,
    shellPid, shellBirth: native.shellBirth, shellCreationDate: native.shellCreationDate,
    viewerPid: native.viewerPid, viewerBirth: native.viewerBirth, viewerCreationDate: native.viewerCreationDate,
    viewerPath: record.viewer, language, expected, argv: native.argv, argvDigest: native.argvDigest };
  if (options.cache instanceof Map) {
    // One generation per lifetime/pane/shell key; a changed shell pid is a new
    // native identity and the older generations for this pane are dropped.
    for (const [other, entry] of options.cache) {
      if (other !== cacheKey && entry?.lifetimeId === (options.lifetimeId ?? null) && entry?.paneId === record.paneId)
        options.cache.delete(other);
    }
    options.cache.set(cacheKey, { lifetimeId: options.lifetimeId ?? null, paneId: record.paneId,
      recordAt: record.createdAt, viewer: record.viewer, parentPane: options.parentPane,
      sessionId: options.sessionId, tabId: options.tabId, language, paths: { ...paths }, native, proof });
  }
  return structuredClone(proof);
}
export async function paneViewerProcess(info, record, expectedPid, inspect) {
  const foreground = processMatch(info, record, expectedPid);
  if (foreground) return foreground;
  const shell = info?.process_info?.shell_pid, pid = expectedPid ?? record.processId;
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
  return processMatch({ process_info: { foreground_processes: [{ pid, argv }] } }, record, pid);
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
    Boolean(processMatch({ process_info: { foreground_processes: [{ pid: child.ProcessId, argv }] } }, record));
}

// A viewer must report 'created' within this window. 10 s was too short on a loaded host:
// the viewer started but registered late, the run settled EFFECT_UNKNOWN and latched the actor.
const launchConfirmationMs = 30000;

export class FixedDagPane extends DagPane {
  constructor(options) {
    super(options);
    this.inspectShell = options.inspectShell;
    this.managedAvailable = options.managedAvailable ?? false;
    this.managedStateDir = options.managedStateDir ?? process.env.OMO_HERDR_TODO_STATE_DIR ??
      join(homedir(), '.omo', 'agent', 'herdr-todo');
    this.binding = { socketPath: options.socket, parentPaneId: options.parentPane,
      sessionId: options.sessionId, ownerKey: this.key };
    this.legacyRecordFile = this.recordFile;
    this.directory = join(options.stateDir, this.key);
    this.stateFile = join(this.directory, 'source.json');
    this.scopeEpoch = options.scopeEpoch ?? 1;
    this.lifetimeId = randomUUID();
    this.sourceEvents = new EventEmitter();
    this.roleEvents = new EventEmitter();
    this.roleTail = Promise.resolve();
    this.stopSignal = new AbortController();
    this.offers = {};
    this.records = new Map();
    this.status = 'waiting';
    this.controlToken = randomBytes(32).toString('hex');
    this.controlPath = ipcPath(`omo-herdr-dag-control-${randomUUID()}`);
    this.controlSockets = new Set();
    this.controlJobs = new Set();
    this.pendingLaunches = new Set();
    this.launchRegistrations = new Map();
    this.closureVersions = new Map();
    this.events = options.events;
    this.timers = options.timers ?? { setTimeout, clearTimeout };
    this.yieldTimeoutMs = Number.isSafeInteger(options.yieldTimeoutMs) && options.yieldTimeoutMs > 0
      ? options.yieldTimeoutMs : 5000;
    this.coordinationStateDir = options.coordinationStateDir ?? process.env.OMO_HERDR_COORDINATION_STATE_DIR ?? null;
    const agentDir = options.agentDir ?? process.env.SENPI_CODING_AGENT_DIR ?? process.env.OMO_CODING_AGENT_DIR ??
      join(homedir(), '.omo', 'agent');
    this.coordinationActivityPath = options.coordinationActivityPath ?? join(agentDir, 'coordination-activity.jsonl');
    // The managed legacy component's installed locale, exactly as the coordination
    // extension chooses it; an env override wins, then the installed file, then en.
    const installedCoordinationLanguage = (() => {
      try { return JSON.parse(readFileSync(new URL('../../coordination/locale.json', import.meta.url), 'utf8')).language; }
      catch { return 'en'; }
    })();
    const coordinationLanguage = options.coordinationLanguage ??
      process.env.OMO_HERDR_COORDINATION_LANG ?? installedCoordinationLanguage;
    this.coordinationLanguage = ['en', 'ko', 'zh-cn'].includes(coordinationLanguage) ? coordinationLanguage : 'en';
    this.readCoordinationRecord = options.readCoordinationRecord;
    this.inspectLegacyCollab = options.inspectLegacyCollab;
    this.legacyCollabCache = new Map();
    this.sourceReady = new Promise((resolve, reject) => { this.sourceSaved = resolve; this.sourceRejected = reject; });
    this.sourceReady.catch(() => {});
  }
  roleFile(role) { return join(this.directory, `${role}.json`); }
  paneFile(role) { return join(this.directory, `${role}.pane.json`); }
  coordinationRecordPath() {
    if (!this.coordinationStateDir) return null;
    return join(this.coordinationStateDir, `${String(this.parentPane).replace(/[^A-Za-z0-9_-]/g, '_')}.pane.json`);
  }
  async coordinationRecord(tabId) {
    let record;
    try {
      if (typeof this.readCoordinationRecord === 'function') record = await this.readCoordinationRecord();
      else { const path = this.coordinationRecordPath(); record = path ? await readJson(path) : null; }
    } catch { record = null; }
    if (!record?.paneId || record.manualClose === true) return null;
    if (record.parentPaneId !== this.parentPane || record.sessionId !== this.sessionId) return null;
    if (record.tabId && record.tabId !== tabId) return null;
    return record;
  }
  roleWrite(job) {
    const result = this.roleTail.then(job);
    this.roleTail = result.catch(() => {});
    return result;
  }
  source() {
    return { runs: structuredClone(this.runs), tasks: structuredClone(this.tasks),
      connected: !this.stopped, updatedAt: this.updatedAt };
  }
  provider() {
    return { lifetimeId: this.lifetimeId, binding: this.binding, sourceReady: this.sourceReady,
      getSource: () => this.source(), setCoordinator: coordinator => {
        if (!this.stopped && sameOwner(coordinator?.binding, this.binding)) {
          this.coordinator = coordinator;
          if (coordinator.actor) { this.managedAvailable = true; this.managedActor = coordinator.actor; }
        }
      },
      idle: async () => { await this.queue; await this.roleTail; },
      openPair: options => this.openPair(options),
      bandLayout: BAND_LAYOUT,
      bandPreflight: () => this.bandPreflight(),
      authorityEvidence: () => this.authorityEvidence(),
      decideAuthority: (evidence, intent) => this.decideAuthority(evidence, intent),
      ownedRegion: options => this.ownedRegion(options),
      preflightRecovery: () => this.preflightRecovery(),
      replaceRetained: entries => this.replaceRetained(entries),
      arrangeDashboard: () => this.arrangeDashboard(),
      setOffers: offers => this.setOffers(offers), inspectPair: () => this.inspectPair(),
      observeTarget: target => this.nativeTarget(target),
      drain: () => this.stop(),
      subscribeSource: callback => {
        this.sourceEvents.on('source', callback);
        return () => this.sourceEvents.off('source', callback);
      } };
  }
  start() {
    return this.enqueue(async () => {
      try {
        await this.startControl();
        const state = await readJson(this.stateFile);
        if (state?.sessionId === this.sessionId) {
          this.runs = state.runs ?? []; this.taskData.restore(state.tasks);
        }
        this.taskData.start();
        await this.restoreRuns();
        await this.save(true, 'full');
        if (!this.stopped) { this.sourceSaved(); this.status = 'layout-waiting'; }
      } catch (error) { this.sourceRejected(error); throw error; }
    });
  }
  async startControl() {
    this.control = net.createServer(socket => {
      this.controlSockets.add(socket);
      socket.once('close', () => this.controlSockets.delete(socket));
      socket.on('error', () => {});
      receiveControl(socket, message => {
        const job = this.handleControl(message, socket);
        this.controlJobs.add(job);
        void job.catch(() => socket.destroy()).finally(() => this.controlJobs.delete(job));
      });
    });
    await new Promise((resolve, reject) => {
      this.control.once('error', reject); this.control.once('listening', resolve); this.control.listen(this.controlPath);
    });
  }
  async save(connected, mode = 'full') {
    if (mode === 'full') this.tasks = await this.taskData.refresh(this.runs);
    else if (mode === 'changed') this.tasks = await this.taskData.refreshChanged();
    else { await this.taskData.idle(); this.tasks = this.taskData.snapshot(); }
    await this.taskData.idle();
    if (this.stopped && connected) return;
    const signature = JSON.stringify([this.runs, this.tasks, connected]);
    const changed = signature !== this.sourceSignature;
    if (changed) this.updatedAt = this.now();
    await this.writeChanged(this.stateFile, { version: 1, ...this.binding, scopeEpoch: this.scopeEpoch,
      connected, language: this.language, updatedAt: this.updatedAt, runs: this.runs, tasks: this.tasks });
    await this.roleWrite(async () => {
      for (const [role, record] of this.records) {
        if (record.paneId) {
          const persisted = await readJson(this.paneFile(role));
          const current = { ...record, scopeEpoch: this.scopeEpoch,
            manualClose: persisted?.launchToken === record.launchToken ? persisted.manualClose : record.manualClose };
          this.records.set(role, current);
          await this.writeChanged(this.paneFile(role), current);
          await this.writeChanged(this.roleFile(role), this.roleState(current, connected));
        }
      }
    });
    this.sourceSignature = signature;
    if (!this.stopped && changed) this.sourceEvents.emit('source', this.source());
    // A source job must not await a coordinator job which needs this queue.
  }
  roleState(record, connected = true) {
    const role = record.role, statePath = this.roleFile(role);
    return { schema: 'omo-herdr-dashboard/role/1', version: 1, ...this.binding, scopeEpoch: this.scopeEpoch,
      ...(this.managedActor ? { managedScope: { ownerNonce: this.managedActor.snapshot.ownerNonce,
        sessionId: this.sessionId, generation: this.managedActor.snapshot.generation,
        participant: 'dag', lifetimeId: this.lifetimeId } } : {}),
      role, tabId: record.tabId, launchToken: record.launchToken, paneId: record.paneId,
      connected, language: this.language, updatedAt: this.updatedAt,
      availability: connected ? this.runs.length || this.tasks.length ? 'current' : 'empty' : 'last-known',
      runs: this.runs, tasks: this.tasks, prefsPath: `${statePath}.view.json`,
      presenceOffer: connected ? this.offers[role] ?? null : null,
      controlOffer: connected ? { pipePath: this.controlPath, v: 1, token: this.controlToken,
        ownerKey: this.key, sessionId: this.sessionId, scopeEpoch: this.scopeEpoch, role,
        launchToken: record.launchToken, paneId: record.paneId, statePath } : null };
  }
  async membership(paneId) {
    const panes = (await this.herdr('list'))?.panes ?? [];
    const parents = panes.filter(pane => pane.pane_id === this.parentPane);
    const matches = panes.filter(pane => pane.pane_id === paneId);
    if (parents.length !== 1 || matches.length !== 1 || !parents[0].tab_id ||
      matches[0].tab_id !== parents[0].tab_id) throw new Error('layout-waiting: missing or ambiguous owned anchor');
    return { tabId: parents[0].tab_id, pane: matches[0] };
  }
  recordMatches(record, role) {
    return record?.schema === 'omo-herdr-dashboard/pane/1' && record.role === role &&
      sameOwner(record, this.binding) && pathKey(record.statePath) === pathKey(this.roleFile(role)) &&
      pathKey(record.viewerPath) === pathKey(this.viewer) && typeof record.launchToken === 'string';
  }
  async verify(record, pid) {
    if (!this.recordMatches(record, record?.role) || !record.paneId) return null;
    const member = await this.membership(record.paneId);
    if (record.tabId !== member.tabId) return null;
    const state = await readJson(this.roleFile(record.role));
    if (!state || !sameOwner(state, record) || state.launchToken !== record.launchToken ||
      state.paneId !== record.paneId || state.role !== record.role) return null;
    const info = await this.herdr('process-info', '--pane', record.paneId);
    if (this.managedAvailable) {
      const identity = await legacyPaneIdentity(info, { ...record, processId: pid ?? record.processId }, this.inspectShell);
      return identity?.viewer ? { pid: identity.viewer.pid } : null;
    }
    return paneViewerProcess(info, record, pid, this.inspectShell);
  }
  async nativeTarget(target) {
    const member = await this.membership(target.paneId);
    const info = await this.herdr('process-info', '--pane', target.paneId);
    const pid = info?.process_info?.shell_pid;
    const rows = await (this.inspectShell ?? nativeShellProcesses)(pid);
    const shell = rows.filter(row => row.ProcessId === pid);
    if (shell.length !== 1) throw blocked('role shell unavailable');
    const actual = { paneId: member.pane.pane_id, tabId: member.tabId,
      shell: { pid, birth: nativeIdentity(shell[0]).birth } };
    if (target.statePath) {
      const role = ['dag', 'workers'].find(role => pathKey(this.roleFile(role)) === pathKey(target.statePath));
      const record = role && await readJson(this.paneFile(role));
      const state = role && await readJson(this.roleFile(role));
      if (!record || !state || !this.recordMatches(record, role) || state.scopeEpoch !== this.scopeEpoch ||
        record.creationProcessId !== pid || record.creationDate !== shell[0].CreationDate ||
        record.creationBirth && record.creationBirth !== shell[0].Birth ||
        !sameOwner(state, this.binding) || state.launchToken !== record.launchToken) throw blocked('role source or shell changed');
      Object.assign(actual, { statePath: this.roleFile(role), sourcePath: this.stateFile, cachePath: this.roleFile(role),
        prefsPath: state.prefsPath, launchToken: state.launchToken, controlToken: state.controlOffer?.token ?? this.controlToken });
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
  // A pane mutation changes which processes run where: cached identity rows are dropped before and after.
  async mutate(role, type, paneId, args, effect, check) {
    bumpProcessQueryEpoch();
    this.legacyCollabCache?.clear();
    try {
    if (!this.managedAvailable) return await this.herdr(...args);
    if (!this.coordinator?.actor) { this.status = 'BLOCKED'; throw blocked('managed W coordinator unavailable; restart this OmO window'); }
    const record = await readJson(this.paneFile(role));
    const target = record?.paneId === paneId ? await this.nativeTarget(record) : await this.coordinator.target(paneId);
    return await this.coordinator.mutate(role, type, target, args, effect, undefined, check);
    } finally { bumpProcessQueryEpoch(); }
  }
  async handleControl(message, socket) {
    if (this.stopped && !(this.managedAvailable && message?.type === 'register' && this.pendingLaunches.has(message.launchToken)) ||
      !['dag', 'workers'].includes(message?.role) ||
      !['register', 'close'].includes(message.type)) return socket.destroy();
    // A viewer that registers or closes has just started or is about to stop: read fresh rows.
    bumpProcessQueryEpoch();
    const role = message.role;
    const record = await readJson(this.paneFile(role));
    const state = await readJson(this.roleFile(role));
    if (!sameControl(message, state?.controlOffer) || message.scopeEpoch !== this.scopeEpoch ||
      !this.recordMatches(record, role) || record.launchToken !== message.launchToken ||
      !Number.isSafeInteger(message.pid) || !(await freshReads(() => this.verify(record, message.pid)))) return socket.destroy();
    const registration = this.roleWrite(async () => {
      const current = await readJson(this.paneFile(role));
      if (current.launchToken !== record.launchToken || current.scopeEpoch !== message.scopeEpoch ||
        message.scopeEpoch !== this.scopeEpoch) return socket.destroy();
      if (this.managedAvailable && message.type === 'register' && this.pendingLaunches.has(message.launchToken))
        this.launchRegistrations.set(message.launchToken, registration);
      if (this.stopped) {
        socket.write(`${JSON.stringify({ ...state.controlOffer, type: 'registered' })}\n`);
        this.roleEvents.emit(`created:${message.launchToken}`, record.paneId);
        return;
      }
      if (message.type === 'register') {
        const ready = { ...current, processId: message.pid, ready: true };
        if (this.managedAvailable) {
          const rows = await (this.inspectShell ?? nativeShellProcesses)(record.creationProcessId);
          ready.viewerBirth = nativeIdentity(rows.find(row => row.ProcessId === message.pid)).birth;
        }
        this.records.set(role, ready); await writeJson(this.paneFile(role), ready);
        socket.write(`${JSON.stringify({ ...state.controlOffer, type: 'registered' })}\n`);
        this.roleEvents.emit(`created:${message.launchToken}`, record.paneId);
        if (this.managedAvailable && this.coordinator.actor.blocked)
          await this.coordinator.observeRegisteredRun(role, ready);
        if (this.managedAvailable) await this.coordinator.registerRole(role, () => this.nativeTarget(ready));
        if (this.managedAvailable) await this.coordinator.holdRoleViewer(role, ready);
      } else {
        this.closureVersions.set(role, (this.closureVersions.get(role) ?? 0) + 1);
        if (this.managedAvailable) {
          this.coordinator.noteRoleClosure?.(role);
          this.coordinator.assertCurrent();
          await this.coordinator.closeRole(role, record.paneId);
        }
        const closed = { ...current, manualClose: true };
        this.records.set(role, closed); await writeJson(this.paneFile(role), closed);
        const reply = () => new Promise(resolve => socket.end(`${JSON.stringify({ ...state.controlOffer, type: 'closed' })}\n`, resolve));
        if (this.managedAvailable) {
          const target = await this.nativeTarget(current);
          await this.coordinator.mutate(role, 'close', target, ['close', record.paneId], null, reply);
        } else { await reply(); await this.herdr('close', record.paneId); }
      }
    });
    await registration;
  }
  setOffers(offers = {}) {
    return this.enqueue(async () => {
      for (const role of ['dag', 'workers']) {
        const offer = offers[role];
        if (offer && (offer.v !== 1 || offer.role !== role || !sameOwner({ ...offer,
          socketPath: this.binding.socketPath, parentPaneId: this.parentPane }, this.binding) ||
          !Number.isSafeInteger(offer.scopeEpoch) || offer.scopeEpoch < this.scopeEpoch))
          throw new Error('Stale or foreign presence offer');
      }
      const epoch = offers.dag?.scopeEpoch ?? offers.workers?.scopeEpoch;
      await this.roleWrite(() => {
        if (epoch) this.scopeEpoch = epoch;
        this.offers = { ...offers };
      });
      await this.save(true, 'none');
    });
  }
  async inspectPair() {
    const pair = {};
    for (const role of ['dag', 'workers']) {
      const record = await readJson(this.paneFile(role));
      if (!record || record.manualClose) continue;
      const obsolete = this.coordinator?.actor?.snapshot.closures?.some(entry => entry.role === role &&
        entry.paneId === record.paneId && entry.sessionId === record.sessionId && entry.reason === 'pane_not_found');
      if (!obsolete && await this.verify(record)) pair[role] = record;
    }
    return pair;
  }
  async preflightRecovery() {
    if (this.managedAvailable) for (const role of ['workers', 'dag']) {
      const record = await readJson(this.paneFile(role));
      // A split whose shell was never observed cannot acquire a birth from a later shell.
      if (this.recordMatches(record, role) && record.paneId && record.prepared && !record.processId &&
        (!record.creationDate || !record.creationBirth) && !await this.paneGone(record.paneId)) {
        this.status = 'layout-waiting';
        throw Object.assign(new Error('layout-waiting: the new dashboard shell has no verified checkpoint; ' +
          'close that pane, then run /dashboard'), { code: 'LAYOUT_WAIT' });
      }
    }
    if (this.coordinator?.actor && this.coordinator.reconcileLegacy)
      return this.coordinator.reconcileLegacy(this.stateDir, legacyPaneIdentity, this.viewer);
    const entries = await discoverLegacyRecords(this.stateDir, this.binding);
    for (const { path, record } of entries) {
      if (path === this.paneFile(record.role) || !record.paneId || record.manualClose) continue;
      // A standalone controller has no managed retirement/lease authority.
      this.status = 'BLOCKED';
      throw blocked('unverified exact-parent legacy prevents another standalone pair');
    }
  }
  async replaceRetained(entries = []) {
    for (const retained of entries) {
      if (retained.state !== 'retained' || retained.record?.sessionId === this.sessionId && !retained.empty && !retained.upgrade) continue;
      const role = retained.record.role;
      if (!['dag', 'workers'].includes(role) || !this.coordinator?.actor)
        throw blocked('retained fixed replacement requires the held coordinator');
      if (typeof this.node === 'function') this.node = await this.node();
      const record = { ...retained.record, ...this.binding, scopeEpoch: this.scopeEpoch,
        statePath: this.roleFile(role), viewerPath: this.viewer, nodePath: this.node, launchToken: randomBytes(24).toString('hex'),
        prepared: true, ready: false, manualClose: false, layoutClaim: this.claim ?? retained.record.layoutClaim };
      delete record.processId; delete record.viewerBirth;
      await this.coordinator.retireRetainedRole(retained, () => this.roleWrite(async () => {
        this.records.set(role, record);
        await writeJson(this.paneFile(role), record);
        await writeJson(this.roleFile(role), this.roleState(record));
      }));
      await this.launchRole(record);
    }
  }
  // A forced pair is explicit; otherwise it inherits the Todo pass it runs in.
  openPair(options = {}) {
    const scope = this.coordinator?.passScope;
    return scope ? scope(options.force, () => this.openPairScoped(options)) : this.openPairScoped(options);
  }
  openPairScoped({ force = false, forceRoles, forceCurrent = () => true, offers, claim } = {}) {
    const closureVersions = new Map(this.closureVersions);
    return this.enqueue(() => this.reportRefusal(async () => {
      if (this.stopped) return {};
      if (this.managedAvailable) {
        if (!this.coordinator?.actor) { this.status = 'BLOCKED'; throw blocked('managed W coordinator unavailable; restart this OmO window'); }
        try { this.coordinator.assertCurrent(); }
        catch (error) { this.status = 'BLOCKED'; throw blocked(error); }
        await this.coordinator.join(this.lifetimeId);
      } else if (!this.coordinator) {
        const probe = await probeManagedSlot({ stateDir: this.managedStateDir, socketPath: this.binding.socketPath,
          parentPaneId: this.parentPane });
        if (probe.blocked) { this.status = 'BLOCKED'; throw blocked(probe.code); }
      }
      if (claim && (!sameOwner(claim, this.binding) || this.coordinator &&
        claim.lifetimeId !== this.coordinator.lifetimeId)) return {};
      await this.preflightRecovery();
      if (offers) {
        for (const role of ['dag', 'workers']) {
          if (offers[role]?.ownerKey !== this.key || offers[role]?.sessionId !== this.sessionId ||
            offers[role]?.role !== role || offers[role]?.scopeEpoch < this.scopeEpoch)
            throw new Error('Stale or foreign presence offer');
        }
        await this.roleWrite(() => {
          this.scopeEpoch = offers.dag.scopeEpoch;
          this.offers = offers;
        });
      }
      await this.save(true, 'none');
      const parent = await this.membership(this.parentPane);
      if (claim && claim.tabId !== parent.tabId) return {};
      this.claim = claim ?? { ...this.binding, tabId: parent.tabId, lifetimeId: this.lifetimeId };
      await writeJson(join(this.directory, 'layout.json'), this.claim);
      // Legacy records without exact state/process/owner identity cannot be adopted
      // or title-cleaned. They also cannot authorize a second hidden layout.
      if (await readJson(this.legacyRecordFile) && !await readJson(this.paneFile('dag'))) {
        this.status = 'layout-waiting'; return {};
      }
      let todo = await this.coordinator?.inspectTodo?.();
      if (todo && (!sameOwner(todo, this.binding) || todo.role !== 'todo' || todo.tabId !== parent.tabId ||
        todo.scopeEpoch !== this.scopeEpoch || todo.ready !== true || todo.manualClose || todo.paneId === this.parentPane))
        throw new Error('layout-waiting: stale or foreign Todo anchor');
      if (todo) await this.membership(todo.paneId);
      let existing = await this.inspectPair();
      const forceRole = role => force && (forceRoles === undefined || forceRoles.includes(role)) && forceCurrent(role) &&
        (closureVersions.get(role) ?? 0) === (this.closureVersions.get(role) ?? 0);
      if (this.claim.layout === 'band') return this.openBand(todo, existing, parent.tabId, forceRole);
      if (this.claim.layout === 'columns') {
        const layout = (await this.herdr('layout', '--pane', this.parentPane))?.layout;
        const ids = todo && existing.dag && existing.workers &&
          { parent: this.parentPane, todo: todo.paneId, workers: existing.workers.paneId, dag: existing.dag.paneId };
        const reversible = ids && isColumnDashboard(layout, { ...ids, todo: ids.workers, workers: ids.todo });
        if (!this.managedAvailable && force && (todo || existing.dag || existing.workers) && (!ids || !isColumnDashboard(layout, ids) && !reversible)) {
          // Herdr 0.9.3 reports same-tab move as a no-op. Explicit reopen
          // rebuilds only verified role panes and retains their preference files.
          const owned = new Set([this.parentPane, todo?.paneId, existing.dag?.paneId, existing.workers?.paneId].filter(Boolean));
          if (layout?.tab_id !== parent.tabId || layout.panes?.some(pane => !owned.has(pane.pane_id)))
            throw new Error('layout-waiting: dashboard region contains unowned panes');
          await this.placementFocus(parent.tabId, this.parentPane);
          if (todo && !await this.coordinator.closeTodoForLayout?.()) throw new Error('layout-waiting: Todo reset could not be verified');
          for (const role of ['dag', 'workers']) {
            const record = existing[role];
            if (!record) continue;
            if (!await this.verify(record)) throw new Error('layout-waiting: role changed before reset');
            await this.roleWrite(() => writeJson(this.paneFile(role), { ...record, ready: false, manualClose: true }));
            await this.herdr('close', record.paneId);
            this.records.delete(role);
          }
          existing = {}; todo = null;
        }
        const dag = await this.ensureRole('dag', existing.workers?.paneId ?? todo?.paneId ?? this.parentPane,
          'right', existing.workers || todo ? '0.5' : '0.4', parent.tabId, () => forceRole('dag'));
        if (!dag) return {};
        const workers = await this.ensureRole('workers', todo?.paneId ?? dag.paneId,
          todo ? 'down' : 'right', '0.5', parent.tabId, () => forceRole('workers'), todo ? null : dag.paneId);
        this.status = workers ? 'current' : 'layout-waiting';
        return { dag, workers };
      }
      // Only a claim without a layout (a DAG pane with no Todo owner) may use the
      // legacy stacked placement below; a band or unknown claim never does.
      if (this.claim.layout !== undefined) throw new Error('layout-waiting: unknown dashboard layout claim');
      if (!existing.dag && existing.workers && todo) {
        // Closing the top DAG collapses the right-hand tree to workers/Todo
        // at 50/50. Restore their owned boundary before inserting the DAG.
        const geometry = async () => {
          const layout = (await this.herdr('layout', '--pane', existing.workers.paneId))?.layout;
          const rect = id => layout?.panes?.filter(pane => pane.pane_id === id);
          const rows = [this.parentPane, existing.workers.paneId, todo.paneId].map(rect);
          if (rows.some(values => values?.length !== 1)) throw new Error('layout-waiting: ambiguous owned geometry');
          const [parentRect, workerRect, todoRect] = rows.map(values => values[0].rect);
          if ([parentRect, workerRect, todoRect].some(value => !value ||
            !['x', 'y', 'width', 'height'].every(key => Number.isFinite(value[key])) || value.width <= 0 || value.height <= 0) ||
            workerRect.x !== parentRect.x + parentRect.width || workerRect.x !== todoRect.x ||
            workerRect.width !== todoRect.width || workerRect.y !== parentRect.y ||
            workerRect.y + workerRect.height !== todoRect.y || workerRect.height + todoRect.height !== parentRect.height)
            throw new Error('layout-waiting: non-contiguous owned region');
          return { parentRect, workerRect, todoRect };
        };
        const before = await geometry(), total = before.parentRect.height, desired = Math.round(total * .7);
        if (before.workerRect.height !== desired) {
          await this.membership(existing.workers.paneId); await this.membership(todo.paneId);
          await this.mutate('workers', 'arrange', existing.workers.paneId,
            ['resize', '--pane', existing.workers.paneId, '--direction', before.workerRect.height < desired ? 'down' : 'up',
              '--amount', String(Math.abs(desired - before.workerRect.height) / total)],
            { paneId: existing.workers.paneId, rect: { ...before.workerRect, height: desired } });
          const after = await geometry();
          if (after.workerRect.height !== desired || JSON.stringify(after.parentRect) !== JSON.stringify(before.parentRect) ||
            after.workerRect.x !== before.workerRect.x || after.workerRect.width !== before.workerRect.width)
            throw new Error('layout-waiting: owned boundary resize mismatch');
        }
      }
      const dagAnchor = existing.workers?.paneId ?? todo?.paneId ?? this.parentPane;
      const sibling = Boolean(existing.workers || todo);
      const dag = await this.ensureRole('dag', dagAnchor, sibling ? 'down' : 'right',
        existing.workers && todo ? String(4 / 7) : sibling ? '0.4' : '0.65', parent.tabId, () => forceRole('dag'),
        sibling ? dagAnchor : null);
      if (!dag) return {};
      const workers = await this.ensureRole('workers', todo?.paneId ?? dag.paneId, 'down',
        todo ? '0.5' : '0.4', parent.tabId, () => forceRole('workers'), todo?.paneId);
      this.status = workers ? 'current' : 'layout-waiting';
      return { dag, workers };
    }), { force });
  }
  // A role creation ensureRole refused reaches the Todo controller as a wait.
  async reportRefusal(job) {
    this.refusal = undefined;
    const pair = await job();
    return this.refusal ? { ...pair, waiting: pair?.waiting ?? this.refusal } : pair;
  }
  // Fresh band: Todo opens by splitting the chat at chatShare. Refuse before that
  // first mutation when the band it would open is below the floor.
  async bandPreflight() {
    const layout = (await this.herdr('layout', '--pane', this.parentPane))?.layout;
    const parents = layout?.panes?.filter(pane => pane.pane_id === this.parentPane);
    if (parents?.length !== 1) throw new Error('layout-waiting: missing or ambiguous owned anchor');
    return bandSizes(freshBandRegion(parents[0].rect)).fits ? null : bandWaiting;
  }
  // The final authorization's evidence (r6 A1), phase (a): every await. The
  // current geometry is read first and the region evidence last, so a change
  // made while the geometry is read is seen. decideAuthority is phase (b).
  async authorityEvidence() {
    return freshReads(async () => {
      const panes = await this.herdr('layout', '--pane', this.parentPane).then(result => result?.layout?.panes, () => undefined); const region = await this.regionEvidence({ explicit: false });
      return { panes, region };
    });
  }
  // Phase (b): synchronous, over that one snapshot; refusal text or null. FRESH
  // allows only Todo's chat split, within the 118x22 fresh minimum; INHERITED
  // (the pass claim) a role split, within the band floors of the owned region.
  decideAuthority({ panes, region }, { role, chat, claim }) {
    const changed = 'layout-waiting: the dashboard panes changed during this pass; run /dashboard';
    const decision = region.input ? decideRegion(region.input) : { waiting: region.waiting };
    const rows = Array.isArray(panes) ? panes : [];
    if (chat) {
      if (role !== 'todo' || decision !== null) return decision?.waiting ?? changed;
      if (claim !== 'band') return null;
      const parent = rows.filter(pane => pane?.pane_id === this.parentPane);
      if (parent.length !== 1) return unverifiedRegion;
      return bandSizes(freshBandRegion(parent[0].rect)).fits ? null : bandWaiting;
    }
    if (!decision?.layout || decision.layout !== claim) return decision?.waiting ?? changed;
    if (decision.layout !== 'band') return null;
    const ids = Object.values(region.input.roles).map(entry => entry.record.paneId).filter(id => typeof id === 'string' && id);
    const rects = rows.filter(pane => ids.includes(pane?.pane_id)).map(pane => pane.rect);
    if (!rects.length) return bandWaiting;
    const x = Math.min(...rects.map(rect => rect.x)), y = Math.min(...rects.map(rect => rect.y));
    const width = Math.max(...rects.map(rect => rect.x + rect.width)) - x;
    const height = Math.max(...rects.map(rect => rect.y + rect.height)) - y;
    return bandSizes({ width, height }).fits ? null : bandWaiting;
  }
  // The dashboard region this parent already has, decided by decideRegion from
  // every source per role: the Todo/workers/DAG pane records, the parent slot's
  // registrations and the native list. A record without a layout claims columns.
  // A role pane is never the chat pane. Presence, not value, is the signal
  // (attempt 5, verify-task-6-r4.md B1): a record file that exists or a
  // registration that exists is a signal whatever it holds; only ENOENT, or no
  // registration key, is absence. Unreadable registrations wait.
  async recordEvidence(path, matches) {
    let value;
    try { value = JSON.parse(await readFile(path, 'utf8')); }
    catch (error) { return { state: error?.code === 'ENOENT' ? 'absent' : 'unreadable' }; }
    if (value === null || typeof value !== 'object' || Array.isArray(value)) return { state: 'malformed' };
    const claim = value.layoutClaim;
    return matches(value) && (value.paneId === undefined ||
      typeof value.paneId === 'string' && value.paneId && value.paneId !== this.parentPane) &&
      (claim === undefined || claim !== null && typeof claim === 'object' && !Array.isArray(claim))
      ? { state: 'valid', paneId: value.paneId, tabId: value.tabId, manualClose: value.manualClose === true,
        layout: claim?.layout ?? 'columns', pending: value.ready !== true, lifetimeId: claim?.lifetimeId }
      : { state: 'malformed', paneId: typeof value.paneId === 'string' && value.paneId ? value.paneId : undefined,
        manualClose: value.manualClose === true,
        lifetimeId: claim !== null && typeof claim === 'object' ? claim.lifetimeId : undefined };
  }
  async ownedRegion(options) {
    const evidence = await this.regionEvidence(options);
    return evidence.input ? decideRegion(evidence.input) : evidence;
  }
  // The gathering half of ownedRegion: { input } for decideRegion, or { waiting }.
  async regionEvidence({ explicit = false, lifetimeId } = {}) {
    const panes = await this.herdr('list').then(result => result?.panes, () => undefined);
    const readable = Array.isArray(panes) && panes.every(pane => typeof pane?.pane_id === 'string' && pane.pane_id);
    const listed = id => Array.isArray(panes) ? panes.filter(pane => pane?.pane_id === id) : [];
    const parents = listed(this.parentPane);
    // An unmanaged dashboard has no parent slot, so it has no registration.
    let registrations = this.managedAvailable ? undefined : {};
    if (this.managedAvailable) try { registrations = this.coordinator.actor.snapshot.roles; } catch {}
    if (registrations === null || typeof registrations !== 'object') return { waiting: unverifiedRegion };
    const todoRecord = this.coordinator.todoRecord;
    const roles = {};
    for (const role of ['todo', 'workers', 'dag']) {
      // Todo's record comes from the Todo controller; a coordinator without one
      // (no Todo controller) contributes its registration only.
      const record = role !== 'todo' ? await this.recordEvidence(this.paneFile(role), value => this.recordMatches(value, role))
        : todoRecord ? await this.recordEvidence(todoRecord.path, todoRecord.matches) : { state: 'absent' };
      // A registration without a usable pane id names nothing that can be gone.
      const registered = !Object.hasOwn(registrations, role) ? undefined
        : typeof registrations[role]?.paneId === 'string' && registrations[role].paneId ? registrations[role].paneId : null;
      roles[role] = { record, registered, panes: new Map() };
      for (const id of new Set([record.paneId, registered].filter(id => id !== undefined))) roles[role].panes.set(id, {
        rows: listed(id).map(pane => pane.tab_id),
        gone: await this.paneGone(id, readable ? panes : null) });
    }
    return { input: { tabId: parents.length === 1 ? parents[0].tab_id : undefined, readable, roles, explicit, lifetimeId } };
  }
  // Authoritative absence: a fully readable list has no row for the pane and
  // Herdr answers pane_not_found for it.
  async paneGone(id, panes) {
    if (typeof id !== 'string' || !id || id === this.parentPane) return false;
    if (panes === undefined) panes = await this.herdr('list').then(result => result?.panes, () => undefined);
    if (!Array.isArray(panes) || panes.some(pane => typeof pane?.pane_id !== 'string' || !pane.pane_id ||
      pane.pane_id === id)) return false;
    try { await this.herdr('get', id); return false; }
    catch (error) { return authoritativePaneNotFound(error); }
  }
  // Band: Todo spans the top, workers (left) and DAG (right) share the row below.
  // Live roles are only re-verified. A missing role is split from a live
  // neighbour: workers below Todo, or left of a live DAG by split and swap; DAG
  // right of workers. Closure stays per role, so a closed DAG never stops workers.
  async openBand(todo, existing, tabId, forceRole) {
    const todoHeight = String(BAND_LAYOUT.todoHeight), workersWidth = String(BAND_LAYOUT.workersWidth);
    const live = {};
    for (const role of ['workers', 'dag']) if (existing[role]) {
      live[role] = await this.ensureRole(role, existing[role].paneId, 'right', workersWidth, tabId, () => forceRole(role));
      if (!live[role]) { this.status = 'layout-waiting'; return live; }
    }
    const records = { workers: await readJson(this.paneFile('workers')), dag: await readJson(this.paneFile('dag')) };
    const needed = role => !live[role] && (forceRole(role) || !records[role]?.manualClose);
    if (needed('workers') || needed('dag')) {
      // Measure the owned region before the first mutation of this pass.
      const owned = [todo?.paneId, records.workers?.paneId, records.dag?.paneId].filter(Boolean);
      const rects = ((await this.herdr('layout', '--pane', this.parentPane))?.layout?.panes ?? [])
        .filter(pane => owned.includes(pane.pane_id)).map(pane => pane.rect);
      if (rects.length) {
        const x = Math.min(...rects.map(rect => rect.x)), y = Math.min(...rects.map(rect => rect.y));
        const width = Math.max(...rects.map(rect => rect.x + rect.width)) - x;
        const height = Math.max(...rects.map(rect => rect.y + rect.height)) - y;
        if (!bandSizes({ width, height }).fits) { this.status = 'layout-waiting'; return { ...live, waiting: bandWaiting }; }
      }
    }
    let { workers, dag } = live;
    if (!workers && dag) workers = await this.ensureRole('workers', dag.paneId, 'right', workersWidth, tabId,
      () => forceRole('workers'), dag.paneId);
    else if (!workers && todo) workers = await this.ensureRole('workers', todo.paneId, 'down', todoHeight, tabId,
      () => forceRole('workers'));
    if (!dag && workers) dag = await this.ensureRole('dag', workers.paneId, 'right', workersWidth, tabId, () => forceRole('dag'));
    this.status = workers ? 'current' : 'layout-waiting';
    return { dag, workers };
  }
  async ensureRole(role, anchor, direction, ratio, tabId, force, beforePane) {
    if (force()) await this.roleTail;
    let record = await readJson(this.paneFile(role));
    // Actor tombstones retain their evidence files. Revalidate the held owner's
    // absence proof before replacing a locator from an obsolete generation.
    if (this.managedAvailable && record && !record.manualClose &&
      this.recordMatches({ ...record, viewerPath: this.viewer }, role) && typeof record.nodePath === 'string' &&
      this.coordinator?.actor?.snapshot.closures.some(entry => entry.role === role && entry.paneId === record.paneId &&
        entry.sessionId === record.sessionId && entry.reason === 'pane_not_found')) {
      if (typeof this.node === 'function') this.node = await this.node();
      if (pathKey(record.viewerPath) !== pathKey(this.viewer) || pathKey(record.nodePath) !== pathKey(this.node)) {
        await this.coordinator.obsoleteLocator(record);
        record = null;
      }
    }
    if (record && !this.recordMatches(record, role)) {
      if (this.managedAvailable) throw blocked('retained fixed record identity changed');
      return;
    }
    if (record?.manualClose && !force()) return;
    if (record?.paneId && record.prepared && !record.processId &&
      (!this.managedAvailable || !await this.paneGone(record.paneId))) {
      const member = await this.membership(record.paneId);
      const identity = member.tabId === record.tabId &&
        await legacyPaneIdentity(await this.herdr('process-info', '--pane', record.paneId), record, this.inspectShell);
      const state = await readJson(this.roleFile(role));
      if (!identity || identity.viewer || !state || !sameOwner(state, record) ||
        state.paneId !== record.paneId || state.launchToken !== record.launchToken)
        throw blocked('PREPARED fixed shell or source changed or occupied');
      record = { ...record, scopeEpoch: this.scopeEpoch, layoutClaim: this.claim };
      this.records.set(role, record);
      await this.roleWrite(async () => {
        await writeJson(this.paneFile(role), record);
        await writeJson(this.roleFile(role), this.roleState(record));
      });
      return this.launchRole(record);
    }
    if (record?.paneId) {
      let process;
      try { process = await this.verify(record); } catch (error) {
        if (!/missing or ambiguous/.test(error.message)) throw error;
      }
      if (process) {
        record = await this.roleWrite(async () => {
          const current = await readJson(this.paneFile(role));
          if (this.stopped || current.launchToken !== record.launchToken || current.manualClose && !force()) return null;
          const next = { ...current, scopeEpoch: this.scopeEpoch, layoutClaim: this.claim,
            processId: process.pid, ready: true, manualClose: false };
          this.records.set(role, next);
          await writeJson(this.paneFile(role), next);
          await writeJson(this.roleFile(role), this.roleState(next));
          return next;
        });
        if (!record) return;
        if (this.managedAvailable) await this.coordinator.registerRole(role, () => this.nativeTarget(record));
        return record;
      }
      const panes = (await this.herdr('list'))?.panes ?? [];
      if (panes.some(pane => pane.pane_id === record.paneId)) {
        const death = this.managedAvailable && this.coordinator.viewerDeath(role);
        if (death && death.record.launchToken === record.launchToken &&
          death.identity.pid === record.processId && death.identity.birth === record.viewerBirth) {
          const observed = await observePaneShell(await this.herdr('process-info', '--pane', record.paneId),
            record, this.inspectShell);
          if (!observed || observed.children.length || observed.shell.Birth !== death.target.shell.birth)
            throw blocked('dead fixed viewer did not leave the original empty shell');
          const next = { ...record, launchToken: randomBytes(24).toString('hex'), prepared: true, ready: false };
          delete next.processId; delete next.viewerBirth;
          await this.roleWrite(async () => {
            const current = await readJson(this.paneFile(role));
            if (current.manualClose) throw blocked('manual closure superseded fixed viewer repair');
            this.records.set(role, next);
            await writeJson(this.paneFile(role), next); await writeJson(this.roleFile(role), this.roleState(next));
          });
          this.coordinator.repairedViewer(role);
          return this.launchRole(next);
        }
        if (this.managedAvailable) throw blocked('retained fixed native identity unverified');
        return;
      }
      if (this.managedAvailable) {
        await this.coordinator.reconcileLegacy(this.stateDir, legacyPaneIdentity);
        const closure = this.coordinator.actor.snapshot.closures.find(entry => entry.role === role &&
          entry.paneId === record.paneId && entry.sessionId === record.sessionId && entry.reason === 'pane_not_found');
        if (!closure) throw blocked('fixed pane absence unverified');
        if (!force()) return;
      }
      if (!force()) {
        record = { ...record, manualClose: true }; this.records.set(role, record);
        await this.roleWrite(() => writeJson(this.paneFile(role), record)); return;
      }
    } else if (record?.attempted && !force()) return;
    // Defense in depth (verify-task-6-r3.md B1): a missing or closed record is no
    // permission to create a role while the parent slot still registers a pane for
    // it that is not authoritatively gone, and an automatic pass never swaps,
    // because a swap selects the new pane (G2, IS-8). Both refusals wait, unmutated.
    if (this.managedAvailable) {
      const registration = this.coordinator?.actor?.snapshot.roles?.[role];
      const refusal = registration && !await this.paneGone(registration.paneId) ? unverifiedRegion
        : beforePane && !force() ? `layout-waiting: the ${role} pane is closed; run /dashboard to reopen it` : null;
      if (refusal) { this.refusal = refusal; this.status = 'layout-waiting'; return; }
    }
    await this.membership(anchor);
    const focusBefore = beforePane ? await this.placementFocus(tabId, beforePane) : null;
    if (typeof this.node === 'function') this.node = await this.node();
    const launchToken = randomBytes(24).toString('hex');
    record = { schema: 'omo-herdr-dashboard/pane/1', ...this.binding, scopeEpoch: this.scopeEpoch,
      role, tabId, statePath: this.roleFile(role), viewerPath: this.viewer, launchToken,
      nodePath: this.node, layoutClaim: this.claim, attempted: true, ready: false, manualClose: false };
    let previous;
    const prepared = await this.roleWrite(async () => {
      const current = await readJson(this.paneFile(role));
      if (current?.manualClose && !force()) return false;
      previous = await readFile(this.paneFile(role), 'utf8').catch(error => error?.code === 'ENOENT' ? null : undefined);
      await writeJson(this.paneFile(role), record);
      return true;
    });
    if (!prepared) return;
    if (this.stopped) return;
    let result;
    try {
      result = await this.mutate(role, 'split', anchor,
        ['split', '--pane', anchor, '--direction', direction, '--ratio', ratio, '--cwd', this.cwd, '--no-focus']);
    } catch (error) {
      // A no-effect refusal created nothing, so the attempt never happened (r5).
      if (error?.noEffect === true && previous !== undefined) await this.roleWrite(() =>
        previous === null ? rm(this.paneFile(role), { force: true }) : writeFile(this.paneFile(role), previous));
      throw error;
    }
    if (!result?.pane?.pane_id) throw new Error('Owned fixed-role split returned no pane ID');
    record.paneId = result.pane.pane_id;
    await this.membership(record.paneId);
    const createdInfo = await this.herdr('process-info', '--pane', record.paneId);
    record.creationProcessId = createdInfo?.process_info?.shell_pid;
    const shell = await observePaneShell(createdInfo, record, this.inspectShell);
    if (shell) { record.creationDate = shell.shell.CreationDate; if (this.managedAvailable) record.creationBirth = shell.shell.Birth; }
    record.prepared = true;
    const persist = () => this.roleWrite(async () => {
      await writeJson(this.paneFile(role), record);
      await writeJson(this.roleFile(role), this.roleState(record));
    });
    if (beforePane && this.managedAvailable) {
      await persist();
      if (!record.creationDate || !record.creationBirth || shell.children.length) {
        this.refusal = 'layout-waiting: the new dashboard shell has no verified checkpoint; close that pane, then run /dashboard';
        this.status = 'layout-waiting';
        return;
      }
    }
    if (beforePane) {
      try {
        const current = await this.placementFocus(tabId, beforePane);
        if (current !== focusBefore) throw new Error('layout-waiting: input selection changed before placement');
        await this.mutate(role, 'arrange', record.paneId, ['swap', '--source-pane', record.paneId, '--target-pane', beforePane]);
        await this.restorePlacementFocus(record.paneId, focusBefore, tabId, beforePane);
      } catch (error) {
        if (this.managedAvailable) throw error;
        const selected = (await this.herdr('list'))?.panes?.filter(pane => pane.focused);
        if (selected?.length === 1 && selected[0].pane_id !== record.paneId &&
          await canCleanupPane(await this.herdr('process-info', '--pane', record.paneId), record, this.inspectShell)) {
          // The cleanup close of an unlaunched pane is a layout mutation too: gate it.
        const refusal = await this.coordinator?.gate?.(role, 'close', { paneId: record.paneId });
        if (refusal) throw Object.assign(new Error(refusal), { code: 'LAYOUT_WAIT' });
        await this.membership(record.paneId); await this.herdr('close', record.paneId); record.paneId = null;
        }
        record.failed = true;
        await this.roleWrite(() => writeJson(this.paneFile(role), record));
        throw error;
      }
    }
    this.records.set(role, record);
    await persist();
    return this.launchRole(record);
  }
  async launchRole(record) {
    const role = record.role, launchToken = record.launchToken;
    const abort = new AbortController();
    const creation = once(this.roleEvents, `created:${launchToken}`,
      { signal: AbortSignal.any([abort.signal, ...(this.managedAvailable ? [] : [this.stopSignal.signal]), AbortSignal.timeout(launchConfirmationMs)]) });
    creation.catch(() => {});
    this.pendingLaunches.add(launchToken);
    try {
      await this.mutate(role, 'run', record.paneId, ['run', record.paneId, shellCommand([this.node, this.viewer, '--view',
        role === 'dag' ? 'dag' : 'tasks', '--state', this.roleFile(role), '--close-pane', record.paneId,
        '--launch-token', launchToken])], creation);
      await creation;
      if (this.managedAvailable) {
        const registration = this.launchRegistrations.get(launchToken);
        if (!registration) throw blocked('authenticated launch registration missing');
        await registration;
      }
      return this.records.get(role);
    } catch (error) {
      if (this.managedAvailable) throw error;
      const current = await readJson(this.paneFile(role));
      if (!this.stopped && current?.launchToken === launchToken && Number.isSafeInteger(record.creationProcessId)) {
        await this.membership(record.paneId);
        const info = await this.herdr('process-info', '--pane', record.paneId);
        if (await canCleanupPane(info, record, this.inspectShell)) {
          await this.herdr('close', record.paneId);
          const failed = { ...current, paneId: null, ready: false, failed: true };
          this.records.set(role, failed);
          await this.roleWrite(() => writeJson(this.paneFile(role), failed));
        }
      }
      throw error;
    } finally { this.pendingLaunches.delete(launchToken); this.launchRegistrations.delete(launchToken); abort.abort(); }
  }
  // Managed (todo 7): the band is the only target. An explicit /dashboard re-places
  // the SAME panes into it with the journaled split/swap/close moves of task 1;
  // an automatic pass never does (owner decision 2026-10-15). Unmanaged
  // dashboards keep their columns arrangement exactly as before.
  arrangeDashboard() {
    return this.enqueue(async () => {
      const claimed = this.claim?.layout;
      if (this.stopped || claimed !== 'columns' && !(claimed === 'band' && this.managedAvailable)) return;
      const pair = await this.inspectPair(), todo = await this.coordinator?.inspectTodo?.();
      if (!pair.dag || !pair.workers || !todo) return;
      const tabId = this.claim.tabId;
      if (!sameOwner(todo, this.binding) || todo.scopeEpoch !== this.scopeEpoch || todo.ready !== true ||
        todo.manualClose || todo.tabId !== tabId) throw new Error('layout-waiting: invalid Todo placement owner');
      const ids = { parent: this.parentPane, todo: todo.paneId, workers: pair.workers.paneId, dag: pair.dag.paneId };
      const geometry = async () => (await this.herdr('layout', '--pane', this.parentPane))?.layout;
      let layout = await geometry();
      // Decision #185: the collaboration pane recorded for this chat is owned while it is in this tab.
      const collab = typeof this.coordinationRecord === 'function' ? await this.coordinationRecord(tabId) : null;
      if (collab?.paneId && layout?.panes?.some(pane => pane?.pane_id === collab.paneId)) ids.coordination = collab.paneId;
      // A matching shape is left as it is (G9): the band under a band claim, judged
      // within the owned panes' own rectangle so a pane the owner opened elsewhere
      // in the tab does not make a correct band look wrong; and unmanaged columns
      // under a columns claim.
      const owned = layout?.panes?.filter(pane => Object.values(ids).includes(pane?.pane_id)) ?? [];
      const edges = owned.map(({ rect }) => [rect?.x, rect?.y, rect?.x + rect?.width, rect?.y + rect?.height]);
      const [left, top] = [0, 1].map(index => Math.min(...edges.map(edge => edge[index])));
      const [right, bottom] = [2, 3].map(index => Math.max(...edges.map(edge => edge[index])));
      const ownedArea = { x: left, y: top, width: right - left, height: bottom - top };
      const ownedBand = owned.length === Object.keys(ids).length && isBandDashboard({ area: ownedArea, panes: owned }, ids);
      // Consult the unresolved journal before the shape no-op: a visible five-area
      // band may still be the target of an explicitly stuck four-area migration.
      // Reading it mutates nothing; the placement journal is written only after
      // the scoped yield and the matching native disappearance.
      const placementPath = () => join(this.directory, 'placement.json');
      let stuck;
      if (this.managedAvailable) {
        stuck = await readJson(placementPath()).catch(() => ({ phase: 'UNREADABLE' }));
        if (stuck?.phase === 'COMPLETE') stuck = undefined;
      }
      // A resolved/absent journal keeps today's no-op path.
      if (!stuck && (claimed === 'band' ? ownedBand : !this.managedAvailable && isColumnDashboard(layout, ids))) return;
      // The single mutation gate, asked before the placement journal or any move:
      // an automatic pass never rearranges (r4) and keeps a live columns
      // dashboard, or owned panes already in the band shape (G9), exactly as
      // they are, without a notice.
      const refusal = await this.coordinator?.gate?.('dag', 'arrange', { paneId: this.parentPane });
      if (refusal) {
        if (claimed === 'columns' && (isColumnDashboard(layout, ids) || ownedBand)) return;
        throw Object.assign(new Error(refusal), { code: 'LAYOUT_WAIT' });
      }
      // Managed preflight before the first journal write. Its refusals mutate
      // nothing, so they are waits, not recovery blocks (IS-10, G4).
      // Every wait names the owner's action (todo 15).
      const wait = message => Object.assign(new Error(/\/dashboard|enlarge this terminal/.test(message) ? message
        : `${message}; run /dashboard to continue`), { code: 'LAYOUT_WAIT' });
      const preflight = async job => {
        try { return await job(); }
        catch (error) {
          throw this.managedAvailable && !error?.code && /^layout-waiting: /.test(error?.message ?? '') ? wait(error.message) : error;
        }
      };
      // A scoped explicit yield before the four-area migration journals its first
      // move. Collab alone closes its own proved pane; core only observes the
      // matching native disappearance and never closes or runs it. No reply and no
      // disappearance keep the wait, with no journal mutation.
      const yieldCollab = async record => {
        const events = this.events;
        if (!events || typeof events.emit !== 'function' || typeof events.on !== 'function')
          throw wait('layout-waiting: the collaboration pane must yield before this migration; no recovery bus is connected');
        const scope = { ownerKey: this.coordinator?.binding?.ownerKey ?? this.binding.ownerKey,
          sessionId: this.binding.sessionId, lifetimeId: this.coordinator?.lifetimeId ?? this.lifetimeId,
          scopeEpoch: this.scopeEpoch,
          generation: this.coordinator?.actor?.snapshot?.generation ?? this.managedActor?.snapshot?.generation ?? 1,
          tabId };
        if (!validScope(scope)) throw wait('layout-waiting: the collaboration yield has no current scope');
        let legacyProof = null;
        if (record.v === 1) {
          let info = null;
          try { info = await this.herdr('process-info', '--pane', record.paneId); } catch { info = null; }
          legacyProof = await observeLegacyCollab(info, record, {
            parentPane: this.parentPane, sessionId: this.sessionId, tabId,
            stateDir: this.coordinationStateDir, recordPath: this.coordinationRecordPath(),
            activityPath: this.coordinationActivityPath, language: this.coordinationLanguage,
            lifetimeId: scope.lifetimeId, cache: this.legacyCollabCache },
          this.inspectLegacyCollab ?? nativeShellProcesses);
          if (!legacyProof)
            throw wait('layout-waiting: the legacy collaboration pane cannot be proved in this tab; ' +
              'leave it in place and restart the collaboration pane before /dashboard');
        }
        const commandId = randomUUID();
        const message = { v: 1, kind: 'yield', scope, commandId, reason: 'layout-migration',
          paneId: record.paneId, ...(legacyProof ? { legacyProof } : {}) };
        if (yieldIssues(message).length) throw wait('layout-waiting: the collaboration yield request is invalid');
        let timer, waiting, signalled = false;
        const offs = [];
        const wake = () => {
          if (waiting) { const done = waiting; waiting = undefined; done(); } else signalled = true;
        };
        const matches = value => value?.v === 1 && typeof value.kind === 'string' && sameScope(value.scope, scope);
        try {
          offs.push(events.on(RECOVERY_V1.layout, value => { if (matches(value)) wake(); }));
          offs.push(events.on(RECOVERY_V1.result, value => {
            if (matches(value) && (value.commandId === undefined || value.commandId === commandId)) wake();
          }));
          const deadline = new Promise(resolve => { timer = this.timers.setTimeout(resolve, this.yieldTimeoutMs); });
          // Arm the listeners first; the reply alone is never evidence, so every
          // wake re-reads the native layout until the exact pane is gone.
          events.emit(RECOVERY_V1.yield, message);
          for (;;) {
            const current = await freshReads(() => geometry());
            if (current && !current.panes?.some(pane => pane?.pane_id === record.paneId)) {
              this.legacyCollabCache?.clear(); bumpProcessQueryEpoch();
              return current;
            }
            const signal = signalled ? (signalled = false, Promise.resolve())
              : new Promise(resolve => { waiting = resolve; });
            if (await Promise.race([signal.then(() => true), deadline.then(() => false)]) === false)
              throw wait('layout-waiting: the collaboration pane has not yielded; run /dashboard again');
          }
        } finally {
          if (timer !== undefined) this.timers.clearTimeout(timer);
          for (const off of offs) try { off?.(); } catch { /* detached */ }
        }
      };
      // The claim becomes 'band' only after the band shape is verified: the DAG
      // and workers records here, the Todo record last by its controller. File
      // writes only, never a native mutation.
      const promote = async () => {
        const claim = { ...this.claim, layout: 'band' };
        await this.roleWrite(async () => {
          for (const role of ['dag', 'workers']) {
            const current = await readJson(this.paneFile(role));
            if (!this.recordMatches(current, role) || current.paneId !== ids[role] || current.manualClose) continue;
            const next = { ...current, layoutClaim: claim };
            this.records.set(role, next);
            await writeJson(this.paneFile(role), next);
          }
        });
        this.claim = claim;
        await writeJson(join(this.directory, 'layout.json'), claim);
        this.status = 'current';
        return { layout: 'band' };
      };
      // An unresolved placement journal is resolved through the actor below, or
      // waits. Its target is the four-area planner: only the core role panes are
      // named. A visible five-area band is not a reason to skip an explicit yield.
      const stuckOwned = Boolean(stuck) && stuck.schema === placementSchema &&
        sameOwner(stuck, this.binding) && ['right', 'down'].includes(stuck.direction) &&
        [stuck.source?.paneId, stuck.anchor?.paneId].every(id => [ids.todo, ids.workers, ids.dag].includes(id));
      if (this.managedAvailable) {
        // G9: the owned panes already have the band shape under a columns claim
        // (the owner arranged them, or a migration stopped after its moves).
        // Nothing moves and no ratio is re-applied; only the claim follows.
        if (!stuck && ownedBand) return promote();
        // An explicit migration of an incorrect/stuck band: the scoped yield runs
        // before the placement journal. The four-area planner below stays unchanged.
        if (ids.coordination && (!stuck || stuckOwned)) {
          layout = await yieldCollab(collab);
          delete ids.coordination;
        }
      }
      // Moving a pane collapses its old branch. A foreign pane must never be
      // resized as an incidental result of that collapse. The unresolved
      // journal's own temporary pane is the one other pane allowed here.
      const extra = layout?.panes?.some(pane => pane.pane_id === temporaryOf(stuck)) ? [temporaryOf(stuck)] : [];
      if (layout?.tab_id !== tabId || layout.panes?.length !== 4 + extra.length ||
        layout.panes.some(pane => ![...Object.values(ids), ...extra].includes(pane.pane_id)))
        throw this.managedAvailable ? wait(foreignWaiting) : new Error('layout-waiting: dashboard region contains unowned panes');
      await preflight(async () => { for (const id of Object.values(ids)) await this.membership(id); });
      const selected = await preflight(() => this.placementFocus(tabId, todo.paneId));
      let temporaryPaneId, share;
      // G2: the chat stays the root's left child with the same stored ratio. A
      // terminal resize rescales its rect; only a dashboard mutation could change it.
      const chatKept = current => {
        const roots = current?.splits?.filter(split => sameRect(split?.rect, current.area)) ?? [];
        const chat = current?.panes?.filter(pane => pane.pane_id === this.parentPane);
        return roots.length === 1 && roots[0].direction === 'right' && chat?.length === 1 &&
          Math.fround(roots[0].ratio) === Math.fround(share) && sameRect(childRects(current.area, 'right', roots[0].ratio)[0], chat[0].rect);
      };
      const guard = expected => freshReads(async () => {
        if (this.stopped || await this.placementFocus(tabId, todo.paneId) !== selected)
          throw new Error('layout-waiting: input selection changed during placement');
        if (!await this.verify(pair.dag) || !await this.verify(pair.workers) ||
          (await this.coordinator.inspectTodo())?.paneId !== todo.paneId)
          throw new Error('layout-waiting: dashboard owner changed during placement');
        const current = await geometry(), owned = [...Object.values(ids), ...(temporaryPaneId ? [temporaryPaneId] : [])];
        if (current?.tab_id !== tabId || current.panes?.length !== owned.length ||
          current.panes.some(pane => !owned.includes(pane.pane_id)))
          throw new Error('layout-waiting: dashboard region contains unowned panes');
        if (share !== undefined && !chatKept(current)) throw new Error('layout-waiting: the chat pane changed during placement');
        // A3: a planned step's arguments were decided on the planned tree. The tree
        // found here must be that one (the temporary pane in its planned leaf); the
        // step's final authorization then requires exactly this tree again.
        if (expected && !sameTree(splitTree(current),
          mapLeaves(expected, leaf => leaf.pane === temporaryLeaf ? { pane: temporaryPaneId } : leaf)))
          throw new Error('layout-waiting: the dashboard panes changed during placement');
        return current;
      });
      // A2: the same conditions again inside the actor's final authorization,
      // after every asynchronous preparation (custody, journal, target reads):
      // every await here, then a synchronous decision over that snapshot. The
      // native tree must still be the one the step was decided on.
      const stepCheck = expected => () => freshReads(async () => {
        const settled = job => job().then(value => ({ value }), error => ({ error }));
        const focus = await settled(() => this.placementFocus(tabId, todo.paneId));
        const dag = await settled(() => this.verify(pair.dag)), workers = await settled(() => this.verify(pair.workers));
        const owner = await settled(() => this.coordinator.inspectTodo());
        const current = await settled(geometry);
        return () => {
          const failed = [focus, dag, workers, owner, current].find(item => item.error)?.error;
          if (this.stopped || failed) return `layout-waiting: the placement could not be verified (${failed?.message ?? 'stopped'})`;
          if (focus.value !== selected) return 'layout-waiting: input selection changed during placement';
          if (!dag.value || !workers.value || owner.value?.paneId !== todo.paneId) return 'layout-waiting: dashboard owner changed during placement';
          if (share !== undefined && !chatKept(current.value)) return 'layout-waiting: the chat pane changed during placement';
          if (!sameLayout(current.value, expected)) return 'layout-waiting: the dashboard panes changed during placement';
        };
      });
      // One allowlisted blocked-diagnostics entry per placement wait (todo 8).
      const diagnosePlacement = code => this.coordinator.actor?.diagnose?.('placement', { code }, { role: 'dag' });
      // `phases` are the trees before the split, the swap and the close, and after the
      // close. `resume` (todo 8) continues an unresolved journal: { journal } starts its
      // move again from the split; { journal, temporary, swapped } goes on with its
      // verified temporary pane at the swap or, once swapped, at the close.
      const move = async (id, target, direction, phases, resume) => {
        const from = resume?.temporary ? resume.swapped ? 2 : 1 : 0;
        temporaryPaneId = resume?.temporary?.paneId;
        const accepted = await guard(phases[from]).catch(async error => {
          // Stopped before its first step, a resumed journal stays for the next /dashboard.
          if (resume && !error?.code && /^layout-waiting: /.test(error?.message ?? '')) {
            await diagnosePlacement('LAYOUT_WAIT');
            throw wait(error.message);
          }
          throw error;
        });
        if (!this.managedAvailable) {
          await this.mutate('dag', 'arrange', id, ['move', id, '--tab', tabId, '--target-pane', target, '--split', direction, '--no-focus']);
          await guard();
          return;
        }
        const journalPath = join(this.directory, 'placement.json');
        const previous = await readJson(journalPath);
        if (previous && previous.phase !== 'COMPLETE' && JSON.stringify(previous) !== JSON.stringify(resume?.journal)) {
          this.status = 'BLOCKED';
          throw blocked('unresolved owned temporary placement');
        }
        const source = await this.coordinator.target(id), anchor = await this.coordinator.target(target);
        const actor = this.coordinator.actor, lifetimeId = this.lifetimeId;
        const { ownerNonce, generation } = actor.snapshot;
        let temporary = resume?.temporary, stage = ['PREPARED', 'SPLIT_SETTLED', 'SWAP_SETTLED'][from];
        // A resumed journal keeps the owner, panes and layout it recorded (its
        // temporary split's lineage); only its old failure is dropped.
        const journal = from ? { ...resume.journal } : { schema: placementSchema, ...this.binding,
          lifetimeId, ownerNonce, generation, source, anchor, direction, selected, phase: stage,
          beforeLayout: await geometry(), temporary: null };
        for (const key of ['failedStage', 'error', 'failureLayout', 'layoutError', 'cleanup', 'cleanupError']) delete journal[key];
        const saveJournal = async phase => {
          journal.phase = phase;
          journal.temporary = temporary ?? null;
          journal.operations = actor.snapshot.operations;
          await this.roleWrite(() => writeJson(journalPath, journal));
        };
        const identities = async () => {
          if (this.coordinator.actor !== actor || this.lifetimeId !== lifetimeId ||
            actor.snapshot.ownerNonce !== ownerNonce || actor.snapshot.generation !== generation)
            throw blocked('placement owner or lifetime changed');
          this.coordinator.assertCurrent();
          if (JSON.stringify(await this.coordinator.target(id)) !== JSON.stringify(source) ||
            JSON.stringify(await this.coordinator.target(target)) !== JSON.stringify(anchor))
            throw blocked('placement native source or anchor changed');
        };
        const closeTemporary = async expected => {
          // target() accepts an unregistered temporary shell only when its exact
          // native membership is current and it has no foreground child.
          const current = await this.coordinator.target(temporary.paneId);
          if (JSON.stringify(current) !== JSON.stringify(temporary))
            throw blocked('temporary native shell changed');
          await saveJournal('CLOSE_PREPARED');
          await this.coordinator.mutate('dag', 'close', temporary, ['close', temporary.paneId], undefined, undefined, stepCheck(expected));
          temporary = undefined;
          temporaryPaneId = undefined;
        };
        if (!from) { await identities(); await saveJournal(stage); }
        try {
          if (!from) {
            const created = await this.mutate('dag', 'split', target,
              ['split', '--pane', target, '--direction', direction, '--ratio', '0.5', '--cwd', this.cwd, '--no-focus'], undefined, stepCheck(accepted));
            if (!created?.pane?.pane_id) throw blocked('temporary split has no native receipt');
            temporaryPaneId = created.pane.pane_id;
            journal.createdPaneId = temporaryPaneId;
            journal.splitReceipt = created;
            await saveJournal('SPLIT_APPLIED');
            temporary = await this.coordinator.target(created.pane.pane_id);
            stage = 'SPLIT_SETTLED'; await saveJournal(stage);
          }
          if (from < 2) {
            const swapping = await guard(phases[1]); await identities();
            await saveJournal('SWAP_PREPARED');
            await this.mutate('dag', 'arrange', id,
              ['swap', '--source-pane', id, '--target-pane', temporary.paneId], undefined, stepCheck(swapping));
            stage = 'SWAP_SETTLED'; await saveJournal(stage);
            await this.restorePlacementFocus(id, selected, tabId, todo.paneId);
          }
          const closing = await guard(phases[2]); await identities();
          await closeTemporary(closing);
          stage = 'COMPLETE'; await saveJournal(stage);
        } catch (error) {
          // Refused at the final authorization before the temporary split was
          // issued (A2): no native effect and nothing to resolve. The journal
          // returns to its previous state, so the next /dashboard resumes.
          if (error?.code === 'LAYOUT_WAIT' && error.noEffect === true && stage === 'PREPARED' && !temporaryPaneId) {
            await this.roleWrite(() => previous ? writeJson(journalPath, previous) : rm(journalPath, { force: true }));
            // A restarted move (todo 8) leaves its unresolved journal for the next /dashboard.
            if (resume) { await diagnosePlacement('LAYOUT_WAIT'); throw wait(error.message); }
            throw error;
          }
          journal.failedStage = stage;
          journal.error = { code: error.code, message: error.message };
          try { journal.failureLayout = await geometry(); }
          catch (layoutError) { journal.layoutError = { code: layoutError.code, message: layoutError.message }; }
          await saveJournal('BLOCKED');
          // Todo 8: a stop that left no unknown effect (the actor is usable and
          // recorded none) keeps the temporary pane as it is, with no further
          // mutation, and waits: the next explicit /dashboard resolves the journal.
          // Never replay or mutate after an UNKNOWN actor effect.
          if (temporaryPaneId && this.coordinator.actor === actor && !actor.blocked && actor.leaseHeld &&
            !actor.draining && error?.code !== 'EFFECT_UNKNOWN') {
            const waiting = error?.code === 'LAYOUT_WAIT' || /^layout-waiting: /.test(error?.message ?? '');
            await diagnosePlacement(waiting ? 'LAYOUT_WAIT' : 'IDENTITY_MISMATCH');
            this.status = 'layout-waiting';
            throw wait(waiting ? error.message
              : `layout-waiting: the dashboard placement stopped (${String(error?.message).replace(/^BLOCKED: /, '')})`);
          }
          this.status = 'BLOCKED';
          throw error?.code === 'LAYOUT_WAIT' ? new Error(error.message) : error;
        }
        await guard(phases[3]);
      };
      const swap = async (id, target, phases) => {
        const accepted = await guard(phases?.[0]);
        await this.mutate('dag', 'arrange', id, ['swap', '--source-pane', id, '--target-pane', target], undefined, stepCheck(accepted));
        await this.restorePlacementFocus(id, selected, tabId, todo.paneId);
        await guard(phases?.[1]);
      };
      // Todo 8: an unresolved journal is resolved through the actor in exactly two
      // cases: its temporary pane is authoritatively gone (no list row and
      // pane_not_found, the actor's target_gone rule), or it is still here with the
      // identity the journal recorded. The four owned panes are verified (the pair
      // and Todo above, every guard and step check), the recorded phase and the
      // panes choose the action, and anything else waits with one diagnostics entry.
      // Returns whether panes were moved.
      const resolvePlacement = async journal => {
        const unresolved = async (message, code) => {
          this.status = 'layout-waiting';
          await diagnosePlacement(code);
          throw wait(message);
        };
        const temporary = temporaryOf(journal), source = journal?.source?.paneId, anchor = journal?.anchor?.paneId;
        const recorded = journal?.phase === 'BLOCKED' ? journal.failedStage : journal?.phase;
        // A failure after SPLIT_SETTLED may have issued the swap: the panes decide.
        const swapped = !Object.hasOwn(swapRecorded, recorded ?? '') ? null
          : journal.phase === 'BLOCKED' && recorded === 'SPLIT_SETTLED' ? undefined : swapRecorded[recorded];
        const before = splitTree(journal?.beforeLayout), roles = Object.values(ids);
        if (journal?.schema !== placementSchema || !sameOwner(journal, this.binding) || !temporary || swapped === null ||
          !['right', 'down'].includes(journal.direction) || !before || typeof source !== 'string' ||
          typeof anchor !== 'string' || source === anchor || source === this.parentPane ||
          [source, anchor].includes(temporary) || journal.temporary && journal.temporary.paneId !== temporary)
          return unresolved(placementMismatch, 'OPERATION_MISMATCH');
        const now = await geometry(), tree = splitTree(now), area = now?.area;
        const sameRoles = leavesOf(before).sort().join('\n') === [...roles].sort().join('\n');
        // The chat share the guards keep, while the chat is the root's left child.
        const kept = tree?.direction === 'right' && tree.first.pane === this.parentPane ? tree.ratio : undefined;
        const complete = async () => {
          await this.roleWrite(() => writeJson(placementPath(), { ...journal, phase: 'COMPLETE' }));
          return false;
        };
        if (!now?.panes?.some(pane => pane.pane_id === temporary)) {
          if (!await this.paneGone(temporary)) return unresolved(placementUnverified, 'IDENTITY_MISMATCH');
          // (a) The temporary pane is gone. Nothing of the placement is left when its
          // moved or target pane is gone too, or when its swap was recorded or the
          // panes prove it.
          const gone = id => id !== this.parentPane && this.paneGone(id);
          if (await gone(source) || await gone(anchor) || swapped === true) return complete();
          const [pre, , , post] = sameRoles ? moveTrees(before, source, anchor, journal.direction) : [];
          if (sameShape(tree, post)) return complete();
          if (!sameShape(tree, pre)) return unresolved(placementMismatch, 'OPERATION_MISMATCH');
          // The move did not take place: it starts again from its first step through
          // the actor. One that would split or resize the chat (a generation-7 move
          // beside it, G2) is not repeated: nothing is left to place.
          const phases = moveTrees(tree, source, anchor, journal.direction);
          const chat = placeTree(tree, area).get(this.parentPane);
          const safe = (node, swappedId) => phaseSafe(node, area, this.parentPane, chat, swappedId);
          if (!safe(phases[1]) || !safe(phases[2], source) || !safe(phases[3])) return complete();
          share = kept;
          await move(source, anchor, journal.direction, phases, { journal });
          return true;
        }
        // (b) The temporary pane is here: its pane id, shell birth and tab are the
        // recorded ones, no process runs in it, and its split is this slot's own
        // (the journal's owner nonce and generation, settled).
        const identity = await this.coordinator.target(temporary).catch(() => undefined);
        const lineage = this.coordinator.actor.snapshot.operations.some(op => op.type === 'split' && op.status === 'SETTLED' &&
          op.ownerNonce === journal.ownerNonce && op.generation === journal.generation && op.target?.paneId === anchor);
        if (!journal.temporary || journal.temporary.tabId !== tabId || !lineage ||
          JSON.stringify(identity) !== JSON.stringify(journal.temporary))
          return unresolved(placementUnverified, 'IDENTITY_MISMATCH');
        const marked = tree && mapLeaves(tree, leaf => leaf.pane === temporary ? { pane: temporaryLeaf } : leaf);
        const [, split, after] = sameRoles && [source, anchor].every(id => roles.includes(id))
          ? moveTrees(before, source, anchor, journal.direction) : [];
        const chat = marked && placeTree(marked, area).get(this.parentPane);
        const safe = (node, swappedId) => phaseSafe(node, area, this.parentPane, chat, swappedId);
        let phases;
        if (swapped !== true && sameShape(marked, split)) {
          const next = swapLeaves(marked, source, temporaryLeaf);
          phases = [undefined, marked, next, dropLeaf(next, temporaryLeaf)];
          if (!safe(phases[2], source) || !safe(phases[3])) phases = undefined;
        } else if (swapped !== false && sameShape(marked, after)) {
          phases = [undefined, undefined, marked, dropLeaf(marked, temporaryLeaf)];
          if (!safe(phases[3])) phases = undefined;
        }
        if (!phases) return unresolved(placementMismatch, 'OPERATION_MISMATCH');
        share = kept;
        await move(source, anchor, journal.direction, phases, { journal, temporary: journal.temporary, swapped: !phases[1] });
        return true;
      };
      if (this.managedAvailable) {
        // The owner typed /dashboard in the chat: the migration starts and ends there (IS-8).
        if (selected !== this.parentPane) throw wait('layout-waiting: select the chat pane in this tab, then run /dashboard');
        let planned = stuck ? undefined : planBandMigration(layout, ids), moved = false;
        if (planned?.waiting) { this.status = 'layout-waiting'; throw wait(planned.waiting); }
        share = planned?.share;
        const rect = (value, id) => value?.panes?.find(pane => pane.pane_id === id)?.rect;
        const splitOf = (value, direction, area) => {
          const found = value?.splits?.filter(split => split?.direction === direction && sameRect(split.rect, area));
          return found?.length === 1 ? found[0] : null;
        };
        // Task-1 resize rules: the amount comes from the stored ratio of the split
        // being changed. Todo moves the band divider up or down; the DAG (left) or
        // workers (right) the row divider. Never workers or Todo left: that is the
        // chat's divider (S4).
        // A3: pane, direction and amount are read from `basis`, a band with the chat
        // kept, where they can only move a dashboard divider. The resize is authorized
        // against that exact tree, never a later one: on a changed row DAG left would
        // be the chat's divider.
        const resize = async (basis, paneId, direction, split, cells, length, expected) => {
          if (!split) throw new Error('layout-waiting: dashboard band split unavailable');
          await guard();
          try {
            await this.mutate('dag', 'arrange', paneId, ['resize', '--pane', paneId, '--direction', direction,
              '--amount', String(Math.abs(split.ratio - cells / length))], { paneId, rect: expected }, stepCheck(basis));
          } catch (error) {
            if (error?.code !== 'EFFECT_UNKNOWN') throw error;
            await diagnosePlacement('EFFECT_UNKNOWN');
            throw Object.assign(wait('layout-waiting: a dashboard resize has an unknown effect; ' +
              'restart this OmO window before running /dashboard'), { cause: error });
          }
        };
        try {
          if (stuck) {
            moved = await resolvePlacement(stuck);
            layout = await geometry();
            if (layout?.tab_id !== tabId || layout.panes?.length !== 4 ||
              layout.panes.some(pane => !Object.values(ids).includes(pane.pane_id))) throw wait(foreignWaiting);
            planned = planBandMigration(layout, ids);
            if (planned.waiting) throw wait(planned.waiting);
            share = planned.share;
          }
          for (const [index, [kind, id, target, direction]] of planned.plan.entries())
            await (kind === 'move' ? move(id, target, direction, planned.trees[index]) : swap(id, target, planned.trees[index]));
          const placed = await geometry();
          if (!isBandDashboard(placed, ids) || !chatKept(placed)) throw new Error('layout-waiting: dashboard band placement mismatch');
          // G9: ratios belong only to a pass that created or moved panes.
          if (planned.plan.length || moved) {
            const chat = rect(placed, ids.parent);
            const region = { x: chat.x + chat.width, y: placed.area.y, width: placed.area.width - chat.width, height: placed.area.height };
            const sizes = bandSizes(region);
            // The terminal may have shrunk since the preflight: the shape stays, the ratios wait.
            if (!sizes.fits) throw wait(bandWaiting);
            const todoRect = rect(placed, ids.todo);
            if (todoRect.height !== sizes.todoRows) await resize(placed, ids.todo, todoRect.height > sizes.todoRows ? 'up' : 'down',
              splitOf(placed, 'down', region), sizes.todoRows, region.height, { ...todoRect, height: sizes.todoRows });
            const banded = await geometry(), row = { ...region, y: region.y + sizes.todoRows, height: sizes.lowerRows };
            if (!isBandDashboard(banded, ids) || !chatKept(banded)) throw new Error('layout-waiting: the dashboard panes changed during placement');
            const workersRect = rect(banded, ids.workers), dagRect = rect(banded, ids.dag), rowSplit = splitOf(banded, 'right', row);
            if (workersRect.width > sizes.workersColumns) await resize(banded, ids.dag, 'left', rowSplit, sizes.workersColumns, row.width,
              { ...dagRect, x: row.x + sizes.workersColumns, width: row.width - sizes.workersColumns });
            else if (workersRect.width < sizes.workersColumns) await resize(banded, ids.workers, 'right', rowSplit, sizes.workersColumns,
              row.width, { ...workersRect, width: sizes.workersColumns });
            const final = await geometry();
            if (!isBandDashboard(final, ids) || !chatKept(final) || rect(final, ids.todo).height !== sizes.todoRows ||
              rect(final, ids.workers).width !== sizes.workersColumns) throw new Error('layout-waiting: dashboard band resize mismatch');
          }
          await guard();
        } catch (error) {
          // A stop that leaves no unresolved temporary placement made no unknown
          // change: the panes are where the last settled step put them, so it is a
          // wait and the next /dashboard resumes from that geometry.
          const phase = await readJson(placementPath()).then(journal => journal?.phase, () => 'UNREADABLE');
          // A2: a refusal at the final authorization is such a stop.
          if (error?.code === 'LAYOUT_WAIT' && error.noEffect === true && (!phase || phase === 'COMPLETE')) {
            this.status = 'layout-waiting';
            throw wait(error.message);
          }
          if (error?.code === 'LAYOUT_WAIT') this.status = 'layout-waiting';
          if (error?.code || !/^layout-waiting: /.test(error?.message ?? '') || phase && phase !== 'COMPLETE') throw error;
          this.status = 'layout-waiting';
          throw wait(error.message);
        }
        return promote();
      }
      if (!isColumnDashboard(layout, { ...ids, todo: ids.workers, workers: ids.todo }))
        throw new Error('layout-waiting: use /dashboard to rebuild the owned column layout');
      await swap(ids.todo, ids.workers);
      const placed = await geometry();
      if (!isColumnDashboard(placed, ids)) throw new Error('layout-waiting: dashboard column placement mismatch');
      const parent = placed.panes.find(pane => pane.pane_id === ids.parent).rect;
      const desired = Math.round(placed.area.width * .4);
      if (parent.width !== desired) {
        await guard();
        await this.mutate('dag', 'arrange', ids.parent, ['resize', '--pane', ids.parent, '--direction', parent.width > desired ? 'left' : 'right',
          '--amount', String(Math.abs(parent.width - desired) / placed.area.width)],
          { paneId: ids.parent, rect: { ...parent, width: desired } });
      }
      const final = await geometry();
      if (!isColumnDashboard(final, ids)) throw new Error('layout-waiting: dashboard resize mismatch');
    });
  }
  async placementFocus(tabId, anchor) {
    const panes = (await this.herdr('list'))?.panes ?? [];
    const selected = panes.filter(pane => pane.focused);
    const layout = (await this.herdr('layout', '--pane', anchor))?.layout;
    if (selected.length !== 1 || selected[0].tab_id !== tabId || layout?.tab_id !== tabId ||
      selected[0].pane_id !== layout.focused_pane_id)
      throw new Error('layout-waiting: placement requires the selected owned tab; select this tab and run /dashboard');
    const id = selected[0].pane_id;
    await this.membership(id);
    await this.verifyInputPane(id, anchor, tabId);
    return id;
  }
  async verifyInputPane(id, anchor, tabId) {
    if (id === this.parentPane || id === anchor) return;
    const record = [...this.records.values()].find(value => value.paneId === id && !value.manualClose);
    if (record && await this.verify(record)) return;
    const todo = await this.coordinator?.inspectTodo?.();
    if (todo && sameOwner(todo, this.binding) && todo.role === 'todo' && todo.paneId === id &&
      todo.tabId === tabId && todo.scopeEpoch === this.scopeEpoch && todo.ready === true && !todo.manualClose) return;
    throw new Error('layout-waiting: unverified input pane');
  }
  async restorePlacementFocus(created, original, tabId, anchor) {
    let source = created;
    for (let step = 0; step < 3 && source !== original; step++) {
      const panes = (await this.herdr('list'))?.panes ?? [];
      const selected = panes.filter(pane => pane.focused);
      if (selected.length !== 1 || selected[0].pane_id !== source || selected[0].tab_id !== tabId) return;
      await this.membership(source); await this.membership(original);
      await this.verifyInputPane(original, anchor, tabId);
      const layout = (await this.herdr('layout', '--pane', source))?.layout;
      const rect = id => layout?.panes?.filter(pane => pane.pane_id === id);
      const from = rect(source), to = rect(original);
      if (layout?.tab_id !== tabId || layout.focused_pane_id !== source || from?.length !== 1 || to?.length !== 1)
        throw new Error('layout-waiting: ambiguous focus restoration geometry');
      const a = from[0].rect, b = to[0].rect;
      if (![a, b].every(value => value && ['x', 'y', 'width', 'height'].every(key => Number.isFinite(value[key]))))
        throw new Error('layout-waiting: invalid focus restoration geometry');
      const direction = b.x < a.x ? 'left' : b.x > a.x ? 'right' : b.y < a.y ? 'up' : 'down';
      const neighbor = (await this.herdr('neighbor', '--pane', source, '--direction', direction))?.neighbor;
      const next = neighbor?.neighbor_pane_id;
      if (!next || neighbor.layout?.tab_id !== tabId || neighbor.layout.focused_pane_id !== source)
        throw new Error('layout-waiting: missing focus restoration neighbor');
      await this.membership(next);
      await this.verifyInputPane(next, anchor, tabId);
      const fresh = ((await this.herdr('list'))?.panes ?? []).filter(pane => pane.focused);
      if (fresh.length !== 1 || fresh[0].pane_id !== source || fresh[0].tab_id !== tabId) return;
      // A2: at the final authorization the step's pane still has the focus and
      // the tree is the one this direction was read from.
      const pane = source;
      const check = () => freshReads(async () => {
        const settled = job => job().then(value => ({ value }), error => ({ error }));
        const list = await settled(() => this.herdr('list')), current = await settled(() => this.herdr('layout', '--pane', pane));
        return () => {
          const failed = list.error ?? current.error;
          if (failed) return `layout-waiting: the placement could not be verified (${failed.message})`;
          const focused = (list.value?.panes ?? []).filter(value => value.focused);
          if (focused.length !== 1 || focused[0].pane_id !== pane || focused[0].tab_id !== tabId)
            return 'layout-waiting: input selection changed during placement';
          if (!sameLayout(current.value?.layout, layout)) return 'layout-waiting: the dashboard panes changed during placement';
        };
      });
      await this.mutate('dag', 'arrange', source, ['focus', '--pane', source, '--direction', direction], undefined, check);
      source = next;
    }
  }
  open() {
    if (this.managedAvailable && !this.coordinator?.actor) return this.enqueue(() => {
      this.status = 'BLOCKED'; throw blocked('managed W coordinator unavailable; restart this OmO window');
    }, { force: true });
    return this.coordinator ? this.coordinator.schedule({ forceRoles: ['dag', 'workers', 'todo'] }) : this.openPair({ force: true });
  }
  rebase() {
    return this.enqueue(async () => {
      await this.roleWrite(() => {
        this.scopeEpoch++;
        this.offers = {};
      });
      await this.save(true, 'none');
    });
  }
  async stop() {
    if (this.stopping) return this.stopping;
    if (this.stopped) return;
    this.stopped = true; this.taskData.stop();
    this.legacyCollabCache?.clear();
    const coordinator = this.coordinator, actor = this.managedActor;
    this.stopSignal.abort();
    this.roleEvents.emit('stopped');
    this.stopping = (async () => {
    await this.queue;
    await Promise.allSettled([...this.controlJobs]);
    await this.roleTail;
    let failure;
    const ownedClaim = this.managedAvailable && actor?.leaseHeld && coordinator?.actor === actor &&
      sameOwner(coordinator.binding, this.binding) &&
      actor.snapshot.participants.dag?.lifetimeId === this.lifetimeId && coordinator.ownsPublisherClaim?.() === true;
    try {
      if (!this.managedAvailable || ownedClaim &&
        await coordinator.terminalAuthority('dag', this.lifetimeId,
          [...this.records.values()].filter(record => record.paneId && !record.manualClose)))
        await this.save(false, 'none');
    }
    catch (error) { failure = error; }
    finally {
      for (const socket of this.controlSockets) socket.destroy();
      if (this.control?.listening) await new Promise(resolve => this.control.close(resolve));
      this.sourceEvents.removeAllListeners();
    }
    if (this.managedAvailable && ownedClaim && actor?.leaseHeld &&
      actor.snapshot.participants.dag?.lifetimeId === this.lifetimeId)
      await actor.drain('dag', this.lifetimeId);
    if (failure) throw failure;
    })();
    void this.stopping.finally(() => { this.stopping = undefined; }).catch(() => {});
    return this.stopping;
  }
}
