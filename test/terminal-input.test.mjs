import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { readFile } from 'node:fs/promises';
import { terminalInput as dagInput, watchHerdrFocus as dagFocus } from '../packages/dag/src/terminal-input.mjs';
import { terminalInput as todoInput, watchHerdrFocus as todoFocus } from '../packages/todo/src/terminal-input.mjs';
import net from 'node:net';
import { once, EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

for (const [role, subscribe] of [['dag', dagFocus], ['todo', todoFocus]]) {
  test(`${role} resumes only on native focus loss and closes its subscription`, async t => {
    const socketPath = process.platform === 'win32' ? `dashboard-focus-${randomUUID()}` :
      // macOS caps Unix socket paths at 104 bytes; its per-user $TMPDIR is too long.
      join(process.platform === 'darwin' ? '/tmp' : tmpdir(), `dfocus-${randomUUID().slice(0, 8)}.sock`);
    const path = process.platform === 'win32' ? `\\\\.\\pipe\\${socketPath}` : socketPath;
    let peer;
    const server = net.createServer(socket => {
      peer = socket;
      socket.once('data', chunk => {
        const request = JSON.parse(chunk.toString());
        assert.equal(request.method, 'events.subscribe');
        assert.deepEqual(request.params.subscriptions.map(entry => entry.type),
          ['pane.focused', 'tab.focused', 'workspace.focused']);
        socket.write('{"id":"dashboard-focus","result":{"type":"subscription_started"}}\n');
      });
    });
    t.after(async () => { peer?.destroy(); await new Promise(done => server.close(done)); });
    const listening = once(server, 'listening');
    server.listen(path); await listening;
    const events = new EventEmitter(), failures = [];
    let blurs = 0;
    const close = await subscribe('own', () => { blurs++; events.emit('blur'); },
      cause => failures.push(cause), { HERDR_ENV: '1', HERDR_PANE_ID: 'own',
        HERDR_TAB_ID: 'tab', HERDR_WORKSPACE_ID: 'work', HERDR_SOCKET_PATH: socketPath });
    peer.write('{"event":"pane.focused","data":{"type":"pane_focused","pane_id":"own"}}\n');
    const blurred = once(events, 'blur', { signal: AbortSignal.timeout(5000) });
    peer.write('{"event":"pane.focused","data":{"type":"pane_');
    peer.write('focused","pane_id":"other"}}\n');
    await blurred;
    assert.equal(blurs, 1);
    for (const event of [{ type: 'tab_focused', tab_id: 'other-tab' },
      { type: 'workspace_focused', workspace_id: 'other-work' }]) {
      const changed = once(events, 'blur', { signal: AbortSignal.timeout(5000) });
      peer.write(`${JSON.stringify({ event: event.type, data: event })}\n`);
      await changed;
    }
    assert.equal(blurs, 3);
    const closed = once(peer, 'close', { signal: AbortSignal.timeout(5000) });
    close(); await closed;
    assert.deepEqual(failures, []);
  });
}

for (const [role, bind] of [['dag', dagInput], ['todo', todoInput]]) {
  test(`${role} consumes fragmented mouse/focus reports without leaking keys`, () => {
    const input = new PassThrough(), keys = [], pointers = [], focused = [];
    const dispose = bind(input, {
      key: (_text, pressed) => keys.push(pressed.sequence),
      pointer: value => pointers.push(value), focus: value => focused.push(value),
    });
    input.write('\x1b[<0;4;');
    assert.equal(pointers.length, 0);
    input.write('8M\x1b[<32;4;12M\x1b[<0;4;12m\x1b[');
    input.write('O\x1b[I\x1b[<65;4;12Mj\x1b[');
    input.write('A');
    assert.deepEqual(pointers, [
      { button: 0, x: 4, y: 8, release: false },
      { button: 32, x: 4, y: 12, release: false },
      { button: 0, x: 4, y: 12, release: true },
      { button: 65, x: 4, y: 12, release: false },
    ]);
    assert.deepEqual(focused, [false, true]);
    assert.deepEqual(keys, ['j', '\x1b[A']);
    dispose();
    assert.equal(input.listenerCount('data'), 0);
    input.write('q');
    assert.equal(keys.length, 2);
    input.destroy();
  });
}

test('independently shipped terminal decoders stay byte identical', async () => {
  const lf = async name => (await readFile(new URL(`../packages/${name}/src/terminal-input.mjs`, import.meta.url), 'utf8'))
    .replaceAll('\r\n', '\n');
  assert.equal(await lf('dag'), await lf('todo'));
});
