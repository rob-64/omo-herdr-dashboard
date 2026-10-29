import { watch } from 'node:fs';
import { readFile, writeFile, rename, rm, chmod } from 'node:fs/promises';
import { basename, dirname, isAbsolute, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { constants } from 'node:os';
import { terminalInput, enablePointer, disablePointer, watchHerdrFocus } from './terminal-input.mjs';
import { EventEmitter } from 'node:events';
import { createRoleClient } from './presence.mjs';
import { sameSnapshotScope, validSnapshot } from './storage.mjs';
import { captureLegacyProducer, createDashboardHerdr, createProducerCustodyServer,
  createViewerRetirementServer, readRoleState, registerRoleViewer, requestRoleClose } from './controller.mjs';
import { isClosedPhase, renderFrame, runningTarget, STRIKE_INTERVAL_MS, STRIKE_TOTAL_FRAMES } from './render.mjs';
import { t } from './i18n.mjs';
import { isIpcPath } from '../../coordination/src/platform.mjs';

const pathKey = path => process.platform === 'win32' ? resolve(path).toLowerCase() : resolve(path);
const samePath = (a, b) => typeof a === 'string' && typeof b === 'string' && pathKey(a) === pathKey(b);
export const VIEWER_FAILURE_GRACE_MS = 2000;
// One registration attempt at a time per current offer/epoch; capped after the last step.
export const REGISTRATION_RETRY_DELAYS = Object.freeze([100, 200, 400, 800, 1600, 2000]);
const registrationRetryDelay = attempt =>
  REGISTRATION_RETRY_DELAYS[Math.min(Math.max(0, attempt - 1), REGISTRATION_RETRY_DELAYS.length - 1)];
export function parseViewerArgs(args) {
  const result = { once: false };
  const fields = { '--state': 'statePath', '--close-pane': 'paneId', '--launch-token': 'launchToken' };
  for (let index = 0; index < args.length; index++) {
    if (args[index] === '--once') { result.once = true; continue; }
    const field = fields[args[index]];
    if (!field || result[field] !== undefined || !args[index + 1] || args[index + 1].startsWith('--')) {
      throw new TypeError('Usage: node viewer.mjs --state ABSOLUTE_ROLE_JSON --close-pane PANE_ID --launch-token TOKEN [--once]');
    }
    result[field] = args[++index];
  }
  if (!isAbsolute(result.statePath ?? '') || !result.paneId || !result.launchToken) {
    throw new TypeError('Absolute role state, pane ID and launch token are required');
  }
  return result;
}

export function validateRoleBinding(state, options) {
  if (!state || state.schema !== 'omo-herdr-dashboard/role/1' || state.role !== 'todo' ||
    state.paneId !== options.paneId || state.launchToken !== options.launchToken ||
    !samePath(state.prefsPath, `${options.statePath}.view.json`) ||
    !isAbsolute(state.cachePath ?? '')) throw new TypeError(t(state?.language, 'invalidBinding'));
  for (const offer of [state.presenceOffer, state.controlOffer]) {
    if (!offer) continue;
    if (offer.v !== 1 || !isIpcPath(offer.pipePath) ||
      typeof offer.token !== 'string' || !offer.token || offer.role !== 'todo' ||
      !sameSnapshotScope(offer, state)) throw new TypeError(t(state.language, 'invalidBinding'));
  }
  if (state.presenceOffer && state.presenceOffer.sequence !== 0) throw new TypeError(t(state.language, 'invalidBinding'));
  if (state.controlOffer && (state.controlOffer.paneId !== options.paneId ||
    state.controlOffer.launchToken !== options.launchToken ||
    !samePath(state.controlOffer.statePath, options.statePath))) throw new TypeError(t(state.language, 'invalidBinding'));
  return state;
}

export async function readViewerCache(state, diagnose = () => {}) {
  try {
    const cache = JSON.parse(await readFile(state.cachePath, 'utf8'));
    if (cache?.schema !== 'omo-herdr-todo/cache/1' || !validSnapshot(cache.snapshot)) {
      throw new TypeError(t(state.language, 'cacheError'));
    }
    if (sameSnapshotScope(cache.snapshot, state)) diagnose('cache-last-known');
    return sameSnapshotScope(cache.snapshot, state)
      ? { availability: 'last-known', snapshot: cache.snapshot, error: '' }
      : { availability: 'waiting', snapshot: null, error: '' };
  } catch (error) {
    diagnose('cache-read', error);
    return { availability: error.code === 'ENOENT' ? 'waiting' : 'error', snapshot: null,
      error: error.code === 'ENOENT' ? '' : error.message };
  }
}

export async function startViewer(options, { input = process.stdin, output = process.stdout,
  interactive = Boolean(input.isTTY && output.isTTY) && !options.once,
  clock = { now: Date.now, setTimeout, clearTimeout, setInterval, clearInterval },
  watchState = watch, readState = readRoleState, readCache = readViewerCache,
  register = registerRoleViewer, pid = process.pid } = {}) {
  // Direct API callers use the same CLI validation.
  if (!isAbsolute(options.statePath ?? '') || !options.paneId || !options.launchToken) {
    throw new TypeError('Absolute role state, pane ID and launch token are required');
  }
  interactive = interactive && !options.once;
  const events = new EventEmitter();
  let state = null, snapshot = null, availability = 'waiting', error = '';
  let client = null, message = null, generation = 0, observation = 0, validatedObservation = -1;
  let invalidState = false;
  // Every phase is listed by default; c folds closed phases away.
  let prefs = { scroll: 0, askScroll: 0, follow: true, completedExpanded: true }, scroll = 0, target = null;
  let refreshing = null, dirty = false, closing = false, closeJob;
  let registration = null;
  let painting = Promise.resolve(), saving = Promise.resolve(), lastFrame;
  let resolveClosed;
  const closed = new Promise(done => { resolveClosed = done; });
  const temporaryPrefs = `${options.statePath}.view.json.${pid}.pending`;
  let watcher, timer, disposeInput, disposeFocus, dragY, closeRetirement, closeCustody, custody, custodyKey, custodyClosure;
  let custodyWanted, custodyJobs = Promise.resolve();
  const failures = new Map(), completionFrames = new Map();
  let animationTimer, diagnostics = Promise.resolve();
  const diagnosticEntries = [];
  const diagnosticPath = resolve(dirname(options.statePath), 'todo.viewer-diagnostics.jsonl');
  const errorCode = cause => Object.hasOwn(constants.errno, cause?.code ?? '') ||
    ['INVALID_CACHE', 'STALE_SCOPE', 'SOURCE_ERROR', 'SyntaxError', 'TypeError', 'RangeError', 'Error'].includes(cause?.code) ? cause.code :
    ['SyntaxError', 'TypeError', 'RangeError', 'Error'].includes(cause?.name) ? cause.name : 'NONE';
  function diagnose(branch, cause) {
    if (!interactive) return;
    const entry = { time: new Date(clock.now?.() ?? Date.now()).toISOString(), branch, code: errorCode(cause) };
    diagnosticEntries.push(entry);
    while (diagnosticEntries.length > 200 ||
      Buffer.byteLength(diagnosticEntries.map(value => JSON.stringify(value)).join('\n')) > 64 * 1024)
      diagnosticEntries.shift();
    const text = diagnosticEntries.map(value => JSON.stringify(value)).join('\n') + '\n';
    diagnostics = diagnostics.then(async () => {
      await writeFile(diagnosticPath, text, { mode: 0o600 });
      await chmod(diagnosticPath, 0o600);
      events.emit('diagnostic', entry);
    }).catch(cause => events.emit('diagnostic-error', { code: errorCode(cause) }));
  }
  function recovered(channel) {
    const pending = failures.get(channel);
    if (pending) clock.clearTimeout(pending.timer);
    failures.delete(channel);
  }
  function cancelRegistration() {
    const entry = registration;
    registration = null;
    if (entry?.timer !== undefined) clock.clearTimeout(entry.timer);
  }
  // A registration reply belongs to the offer/epoch it was sent for. A reply that is no longer
  // current can neither connect, draw an error nor schedule another attempt.
  function settleRegistration(entry, epoch, attempt, accepted, cause) {
    if (registration === entry) entry.inFlight = false;
    if (closing || epoch !== generation || registration !== entry) {
      events.emit('registration-stale', { epoch, attempt, accepted,
        code: cause ? errorCode(cause) : accepted ? 'NONE' : 'DENIED' });
      return;
    }
    events.emit('registered', { accepted, scope: state, pid, attempt });
    if (accepted) {
      if (entry.timer !== undefined) clock.clearTimeout(entry.timer);
      registration = null;
      recovered('registration');
      connect(state, epoch);
      return;
    }
    failed('registration', cause ? 'registration-error' : 'registration-denied', cause, () => {
      availability = snapshot ? 'last-known' : 'error';
      error = cause ? cause.message : t(state?.language, 'registrationError');
    });
    if (cause) events.emit('control-error', cause);
    void draw();
    const delayMs = registrationRetryDelay(attempt);
    entry.timer = clock.setTimeout(() => {
      if (closing || epoch !== generation || registration !== entry) return;
      entry.timer = undefined;
      startRegistration(epoch);
    }, delayMs);
    entry.timer?.unref?.();
  }
  function startRegistration(epoch) {
    if (closing || epoch !== generation || !state?.controlOffer) return;
    if (!registration || registration.epoch !== epoch) {
      if (registration?.timer !== undefined) clock.clearTimeout(registration.timer);
      registration = { epoch, attempts: 0, inFlight: false, timer: undefined };
    }
    const entry = registration;
    if (entry.inFlight || entry.timer !== undefined) return;
    entry.inFlight = true;
    const attempt = ++entry.attempts;
    events.emit('registration-attempt', { epoch, attempt, scope: state });
    let response;
    try {
      response = register({ state, paneId: options.paneId, launchToken: options.launchToken, pid });
    } catch (cause) {
      settleRegistration(entry, epoch, attempt, false, cause);
      return;
    }
    Promise.resolve(response).then(
      accepted => settleRegistration(entry, epoch, attempt, accepted === true, null),
      cause => settleRegistration(entry, epoch, attempt, false, cause));
  }
  function failed(channel, branch, cause, fallback) {
    diagnose(branch, cause);
    const pending = failures.get(channel);
    if (pending) {
      pending.fallback = fallback;
      if (pending.expired) fallback();
      return;
    }
    if (!snapshot) { fallback(); return; }
    const entry = { fallback };
    entry.timer = clock.setTimeout(() => {
      if (closing || failures.get(channel) !== entry) return;
      // Keep the expired entry until recovery; later errors remain offline.
      entry.expired = true;
      entry.fallback(); void draw();
      events.emit('fallback', { channel, branch });
    }, VIEWER_FAILURE_GRACE_MS);
    failures.set(channel, entry);
    events.emit('fallback-pending', { channel, branch, code: errorCode(cause), delayMs: VIEWER_FAILURE_GRACE_MS });
  }
  function acceptSnapshot(value) {
    const previous = new Map((snapshot?.phases ?? []).flatMap((phase, phaseIndex) =>
      phase.tasks.map((task, taskIndex) => [snapshot.phaseSummaries[phaseIndex].taskKeys[taskIndex], task.status])));
    const keys = new Set();
    for (const [phaseIndex, phase] of value.phases.entries()) for (const [taskIndex, task] of phase.tasks.entries()) {
      const key = value.phaseSummaries[phaseIndex].taskKeys[taskIndex];
      keys.add(key);
      if (task.status === 'completed' && previous.has(key) && previous.get(key) !== 'completed')
        completionFrames.set(key, 0);
      else if (task.status !== 'completed') completionFrames.delete(key);
    }
    for (const key of completionFrames.keys()) if (!keys.has(key)) completionFrames.delete(key);
    snapshot = value;
    if (interactive && completionFrames.size && animationTimer === undefined) {
      animationTimer = clock.setInterval(() => {
        for (const [key, frame] of completionFrames) {
          if (frame + 1 >= STRIKE_TOTAL_FRAMES) completionFrames.delete(key);
          else completionFrames.set(key, frame + 1);
        }
        if (!completionFrames.size) { clock.clearInterval(animationTimer); animationTimer = undefined; }
        void draw();
      }, STRIKE_INTERVAL_MS);
      animationTimer?.unref?.();
    }
    if (!completionFrames.size && animationTimer !== undefined) {
      clock.clearInterval(animationTimer); animationTimer = undefined;
    }
  }
  async function disposeCustody() {
    const selected = custody;
    custody = null;
    if (selected) custodyClosure = await selected.dispose();
    return custodyClosure;
  }
  function queueCustody(next, epoch) {
    if (!interactive) return;
    const key = JSON.stringify([next.custodyBinding, next.managedScope, next.controlOffer]);
    if (key === custodyWanted) return;
    custodyWanted = key;
    custodyJobs = custodyJobs.then(() => bindCustody(next, epoch, key))
      .catch(cause => events.emit('custody-error', cause));
  }
  async function bindCustody(next, epoch, key) {
    if (closing || key !== custodyWanted || epoch !== generation) return;
    const binding = next.custodyBinding;
    if (key === custodyKey) return;
    if (!binding) {
      custodyKey = key;
      await disposeCustody();
      return;
    }
    if (!next.managedScope || binding.ownerNonce !== next.managedScope.ownerNonce ||
      binding.generation !== next.managedScope.generation || binding.statePath !== options.statePath ||
      typeof binding.stateDir !== 'string' || typeof binding.recordPath !== 'string')
      throw new TypeError('Invalid managed producer custody binding');
    const captured = await captureLegacyProducer({ ...binding, custody: true, herdr: createDashboardHerdr() });
    const slot = captured.bindings.initialSlot;
    if (closing || key !== custodyWanted || epoch !== generation || !slot ||
      slot.ownerNonce !== binding.ownerNonce || slot.generation !== binding.generation ||
      slot.operations.some(operation => operation.status !== 'SETTLED')) {
      await captured.dispose();
      return;
    }
    await disposeCustody();
    custody = captured; custodyKey = key;
    events.emit('custody-armed', { identity: captured.identity, parent: slot.parent,
      ownerNonce: slot.ownerNonce, generation: slot.generation });
  }
  const current = (epoch, source) => !closing && epoch === generation && source === client;
  const write = text => new Promise((done, fail) => output.write(text, cause => cause ? fail(cause) : done()));
  function draw() {
    if (closing) return painting;
    if (availability === 'current' || availability === 'empty') target = runningTarget(snapshot, target);
    const frame = renderFrame(snapshot, { columns: output.columns ?? 80, rows: output.rows ?? 24,
      language: state?.language, availability, error, color: interactive,
      completionFrames,
      ...prefs, scroll, target: availability === 'current' ? target : null,
      revealTarget: prefs.follow && availability === 'current' });
    scroll = frame.scroll;
    lastFrame = frame;
    const captured = { frame, client, message, generation, observation,
      scope: state && { ownerKey: state.ownerKey, sessionId: state.sessionId, scopeEpoch: state.scopeEpoch },
      availability };
    painting = painting.then(async () => {
      if (closing || captured.generation !== generation) return;
      // This viewer's PowerShell helpers re-title the shared console when they start, so every
      // frame sets the role title again (decision: unified pane titles).
      const raw = interactive ? `\x1b]0;${t(state?.language, 'paneTitle')}\x07` + frame.lines.map((line, index) =>
        `\x1b[${index + 1};1H\x1b[2K${line}`).join('') : `${frame.text}\n`;
      try {
        await write(raw);
        const matches = current(captured.generation, captured.client) && captured.message !== null &&
          captured.message === message && captured.observation === observation && validatedObservation === observation &&
          sameSnapshotScope(captured.scope, state) && ['current', 'empty', 'waiting'].includes(captured.availability) &&
          ['current', 'empty', 'waiting'].includes(availability);
        events.emit('frame', { ...frame, raw, scope: captured.scope,
          sequence: captured.message?.sequence, stale: !matches });
        if (matches) captured.client.painted(captured.message);
      } catch (cause) {
        events.emit('output-error', cause);
      }
    });
    return painting;
  }
  function retire(preserve = false) {
    generation++;
    cancelRegistration();
     // Any grace timer from the previous epoch would otherwise expire into the new one.
    recovered('registration');
    const old = client;
    client = null; message = null;
    if (!preserve) {
      snapshot = null; availability = 'waiting'; error = '';
      for (const channel of failures.keys()) recovered(channel);
      completionFrames.clear(); clock.clearInterval(animationTimer); animationTimer = undefined;
    }
    old?.close();
  }
  function connect(next, epoch) {
    if (closing || generation !== epoch || !next.presenceOffer) return;
    const source = createRoleClient({
      offer: next.presenceOffer,
      onSnapshot(value, exactMessage) {
        if (!current(epoch, source) || !validSnapshot(value) || !sameSnapshotScope(value, state)) return;
        recovered('presence'); recovered('registration'); recovered('cache');
        acceptSnapshot(value); message = exactMessage; availability = value.availability; error = '';
        events.emit('snapshot', { snapshot: value, message: exactMessage, client: source });
        void draw();
      },
      onStatus(status) {
        if (!current(epoch, source)) return;
        message = null;
        failed('presence', 'presence-status', status.error, () => {
          snapshot = validSnapshot(status.snapshot) && sameSnapshotScope(status.snapshot, state) ? status.snapshot : null;
          availability = snapshot ? 'last-known' : status.availability === 'waiting' ? 'waiting' : 'error';
          error = status.error?.message ?? '';
        });
        void draw();
      },
    });
    client = source;
    source.events.on('failure', cause => {
      if (!current(epoch, source)) return;
      message = null;
      failed('presence', 'presence-failure', cause, () => {
        availability = snapshot ? 'last-known' : 'error'; error = cause.message;
      });
      void draw();
    });
    source.events.on('disconnected', () => {
      if (!current(epoch, source)) return;
      message = null;
      failed('presence', 'presence-disconnected', null, () => {
        availability = snapshot ? 'disconnected' : error ? 'error' : 'waiting';
      });
      void draw();
      events.emit('disconnected');
    });
  }
  async function preferences(next, epoch) {
    prefs = { scroll: 0, askScroll: 0, follow: true, completedExpanded: true }; scroll = 0; target = null;
    try {
      const value = JSON.parse(await readFile(next.prefsPath, 'utf8'));
      if (epoch === generation && value.schema === 'omo-herdr-todo/view/1' && sameSnapshotScope(value, next) &&
        [value.scroll, value.askScroll].every(number => Number.isSafeInteger(number) && number >= 0) &&
        (value.follow === undefined || typeof value.follow === 'boolean') &&
        (value.completedExpanded === undefined || typeof value.completedExpanded === 'boolean') &&
        (value.closedFolded === undefined || typeof value.closedFolded === 'boolean')) {
        // Only an explicit fold (c) is restored; preferences saved under the old folded default are not.
        prefs = { scroll: value.scroll, askScroll: value.askScroll, follow: true,
          completedExpanded: typeof value.closedFolded === 'boolean' ? !value.closedFolded : true };
        scroll = prefs.scroll;
      }
    } catch (cause) {
      if (cause.code !== 'ENOENT' && epoch === generation) error = t(next.language, 'prefsError', { error: cause.message });
    }
  }
  async function apply(next) {
    const scopeChanged = !sameSnapshotScope(next, state);
    const offersChanged = JSON.stringify([next.presenceOffer, next.controlOffer]) !==
      JSON.stringify([state?.presenceOffer, state?.controlOffer]);
    const first = state === null;
    const reconnect = scopeChanged || offersChanged || first || invalidState;
    if (reconnect) retire(!scopeChanged && !first);
    invalidState = false;
    state = next;
    const epoch = generation;
    queueCustody(next, epoch);
    if (scopeChanged || first) {
      await saving;
      await preferences(next, epoch);
    }
    if (reconnect || !interactive) {
      const cached = await readCache(next, diagnose);
      if (epoch !== generation || closing) return;
      const installCache = () => { snapshot = cached.snapshot; availability = cached.availability; error = cached.error || error; };
      if (snapshot && interactive) failed('cache', 'cache-fallback', null, installCache);
      else installCache();
    }
    if (interactive && reconnect) {
      if (next.controlOffer) startRegistration(epoch);
      else { cancelRegistration(); connect(next, epoch); }
    }
  }
  function refresh() {
    if (closing) return Promise.resolve();
    dirty = true;
    if (refreshing) return refreshing;
    refreshing = (async () => {
      while (dirty && !closing) {
        dirty = false;
        try {
          const selectedObservation = observation;
          const next = await readState(options.statePath);
          if (selectedObservation !== observation) { dirty = true; continue; }
          if (next) {
            try { validateRoleBinding(next, options); }
            catch (cause) { retire(true); invalidState = true; throw cause; }
            await apply(next);
            recovered('state');
            if (selectedObservation === observation) validatedObservation = observation;
          }
          else {
            validatedObservation = -1;
            failed('state', 'state-missing', { code: 'ENOENT' }, () => {
              retire(true); invalidState = true; availability = snapshot ? 'last-known' : 'waiting';
              error = state ? t(state.language, 'stateMissing') : '';
            });
          }
        } catch (cause) {
          if (!state) { diagnose('state-initial', cause); throw cause; }
          validatedObservation = -1;
          failed('state', 'state-read', cause, () => {
            retire(true); invalidState = true; availability = snapshot ? 'last-known' : 'error'; error = cause.message;
          });
          events.emit('state-error', cause);
        }
        if (!closing) void draw();
      }
    })().finally(() => { refreshing = null; });
    return refreshing;
  }
  function savePreferences() {
    const value = { schema: 'omo-herdr-todo/view/1', ownerKey: state.ownerKey,
      sessionId: state.sessionId, scopeEpoch: state.scopeEpoch, ...prefs, closedFolded: !prefs.completedExpanded };
    const epoch = generation, path = state.prefsPath;
    saving = saving.then(async () => {
      if (closing || epoch !== generation) return;
      try {
        await writeFile(temporaryPrefs, JSON.stringify(value), { mode: 0o600 });
        if (epoch === generation) await rename(temporaryPrefs, path);
        else await rm(temporaryPrefs, { force: true });
        events.emit('preferences', value);
      } catch (cause) {
        await rm(temporaryPrefs, { force: true });
        if (epoch === generation) {
          events.emit('prefs-error', cause); error = t(state.language, 'prefsError', { error: cause.message });
          void draw();
        }
      }
    });
  }
  async function close(manual = false) {
    if (closeJob) return closeJob;
    closing = true;
    cancelRegistration();
    closeJob = (async () => {
      let accepted = false;
      await custodyJobs;
      await disposeCustody();
      if (manual && interactive && state?.controlOffer) {
        try { accepted = await requestRoleClose({ state, paneId: options.paneId, launchToken: options.launchToken, pid }); }
        catch (cause) { events.emit('control-error', cause); }
      }
      retire();
      await closeCustody?.();
      await closeRetirement?.();
      watcher?.close(); clock.clearInterval(timer);
      clock.clearInterval(animationTimer);
      for (const channel of failures.keys()) recovered(channel);
      output.off?.('resize', resize); disposeInput?.(); disposeFocus?.();
      if (interactive) { input.setRawMode?.(false); input.pause?.(); }
      await saving;
      await painting;
      await diagnostics;
      if (interactive) await write(`${disablePointer}\x1b[?25h\x1b[?1049l`).catch(cause => events.emit('output-error', cause));
      events.emit('closed', { manual, accepted });
      resolveClosed({ manual, accepted });
      return { manual, accepted };
    })();
    return closeJob;
  }
  function key(sequence) {
    if (closing) return;
    if (['q', '\x03', '\x04'].includes(sequence)) return close(true);
    const page = Math.max(1, lastFrame?.bodyRows ?? 1);
    const body = ['j', 'k', '\x1b[B', '\x1b[A', '\x1b[6~', '\x1b[5~'].includes(sequence);
    if (sequence === 'f') prefs.follow = !prefs.follow;
    else if (sequence === 'c' && snapshot?.phases.some(isClosedPhase)) {
      prefs.follow = false;
      prefs.completedExpanded = !prefs.completedExpanded;
    }
    else if (body || sequence === 'a' || sequence === 'z') {
      prefs.follow = false; // Pause this role before any manual movement.
      if (sequence === 'j' || sequence === '\x1b[B') scroll++;
      if (sequence === 'k' || sequence === '\x1b[A') scroll = Math.max(0, scroll - 1);
      if (sequence === '\x1b[6~') scroll += page;
      if (sequence === '\x1b[5~') scroll = Math.max(0, scroll - page);
      if (sequence === 'a') prefs.askScroll++;
      if (sequence === 'z') prefs.askScroll = Math.max(0, prefs.askScroll - 1);
    } else return;
    void draw();
    if (body) prefs.scroll = scroll;
    if (sequence === 'a' || sequence === 'z') prefs.askScroll = lastFrame.askScroll;
    if (state) savePreferences();
  }
  const keypress = (text, pressed) => { void key(pressed.sequence || text || pressed.name); };
  const pointer = mouse => {
    if (closing) return;
    if (mouse.release) { dragY = undefined; return; }
    if (mouse.button & 64) {
      prefs.follow = false;
      scroll = Math.max(0, scroll + (mouse.button & 1 ? 3 : -3));
    } else if ((mouse.button & 3) === 0) {
      prefs.follow = false;
      if (mouse.button & 32) {
        if (dragY !== undefined) scroll = Math.max(0, scroll + dragY - mouse.y);
      } else {
        const row = mouse.y - 1 - (lastFrame?.bodyStart ?? 0) + scroll;
        if (mouse.y - 1 >= lastFrame?.bodyStart && mouse.y - 1 < lastFrame.bodyStart + lastFrame.bodyRows &&
          [lastFrame?.closedRange, ...(lastFrame?.foldRanges ?? [])].some(range => range && row >= range.start && row < range.end))
          prefs.completedExpanded = !prefs.completedExpanded;
      }
      dragY = mouse.y;
    } else return;
    void draw();
  };
  const focus = focused => {
    if (focused || closing) return;
    dragY = undefined; prefs.follow = true;
    void draw();
  };
  const resize = () => { void draw(); };
  try {
    if (interactive) {
      // Subscribe first: atomic replacements during the initial read are queued.
      watcher = watchState(dirname(options.statePath), (_event, name) => {
        if (name && String(name) !== basename(options.statePath)) return;
        observation++;
        void refresh().catch(cause => events.emit('state-error', cause));
      });
      watcher.on('error', cause => {
        validatedObservation = -1;
        failed('state', 'state-watch', cause, () => {
          retire(true); invalidState = true;
          availability = snapshot ? 'last-known' : 'error'; error = cause.message;
        });
        void draw();
      });
      output.write(`\x1b[?1049h\x1b[?25l${enablePointer}\x1b]0;${t(state?.language, 'paneTitle')}\x07`);
      input.setRawMode?.(true); input.resume?.();
      disposeInput = terminalInput(input, { key: keypress, pointer, focus });
      output.on?.('resize', resize);
      // Repaint even idle/empty baselines. No frame completion means no renewal.
      timer = clock.setInterval(() => { void draw(); }, 1000);
      timer?.unref?.();
      closeRetirement = await createViewerRetirementServer(options, () => closing ? null : state,
        () => close(false), disposeCustody);
      closeCustody = await createProducerCustodyServer(options, () => custody);
    }
    await refresh();
    if (interactive) disposeFocus = await watchHerdrFocus(options.paneId, () => focus(false),
      cause => { error = `Focus: ${cause.message}`; void draw(); });
    if (!interactive) await painting;
  } catch (cause) {
    await close(false);
    throw cause;
  }
  return { events, closed, draw, refresh, key, close,
    get state() { return { role: state, snapshot, availability, error, ...prefs, target }; },
    get frame() { return lastFrame; } };
}

if (process.argv[1] && samePath(process.argv[1], fileURLToPath(import.meta.url))) {
  const viewer = await startViewer(parseViewerArgs(process.argv.slice(2)));
  if (process.stdin.isTTY && process.stdout.isTTY && !process.argv.includes('--once')) {
    const terminate = () => { void viewer.close(false); };
    const interrupt = () => { void viewer.close(true); };
    process.once('SIGTERM', terminate); process.once('SIGHUP', terminate); process.once('SIGINT', interrupt);
    await viewer.closed;
    process.off('SIGTERM', terminate); process.off('SIGHUP', terminate); process.off('SIGINT', interrupt);
  }
}
