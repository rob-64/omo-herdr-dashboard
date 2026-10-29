import { cp, mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';
const source = dirname(fileURLToPath(import.meta.url));
const marker = '// managed by omo-herdr-dashboard';
async function text(file) {
  try { return await readFile(file, 'utf8'); }
  catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
}
async function atomic(file, content) {
  const temporary = `${file}.${randomUUID()}.tmp`;
  try { await writeFile(temporary, content, { mode: 0o600 }); await rename(temporary, file); }
  finally { await rm(temporary, { force: true }); }
}
export async function install(args) {
  const options = {};
  for (let i = 0; i < args.length; i++) {
    const option = args[i];
    if (!['--agent-dir', '--lang', '--dry-run'].includes(option) || option in options) throw Error('Invalid or duplicate option: ' + option);
    if (option === '--dry-run') options[option] = true;
    else {
      if (!args[i + 1] || args[i + 1].startsWith('--')) throw Error(option + ' requires a value');
      options[option] = args[++i];
    }
  }
  const language = options['--lang'] ?? 'en';
  if (!['en', 'ko', 'zh-cn'].includes(language)) throw Error('Unsupported language');
  const agent = resolve(options['--agent-dir'] ?? process.env.OMO_CODING_AGENT_DIR ?? process.env.SENPI_CODING_AGENT_DIR ?? join(homedir(), '.omo/agent'));
  const container = join(agent, 'herdr-dashboard/integration');
  const wrapper = join(agent, 'extensions/omo-herdr-dashboard.js');
  const before = await text(wrapper), pointerFile = join(container, 'current.json'), pointerBefore = await text(pointerFile);
  if (before !== undefined && !before.startsWith(marker + '\n')) throw Error('Foreign dashboard extension exists');
  let entries = [];
  try { entries = await readdir(container); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (entries.length && await text(join(container, '.installed-by')) !== marker) throw Error('Unmanaged dashboard installation');
  const previous = pointerBefore === undefined ? undefined : JSON.parse(pointerBefore).generation;
  if (previous !== undefined && (!/^generation-\d{6}$/.test(previous) || await text(join(container, previous, '.installed-by')) !== marker)) throw Error('Invalid previous generation');
  const runtime = ['extension.mjs', 'LICENSE', 'THIRD-PARTY-NOTICES.md', 'todo/extension.mjs', 'dag/extension.mjs', 'coordination/extension.mjs',
    ...['activity', 'controller', 'i18n', 'platform', 'proof', 'recovery', 'render', 'viewer'].map(file => `coordination/src/${file}.mjs`),
    ...['child-guard', 'controller', 'display-bridge', 'inline-recovery', 'native-observer', 'native-todo', 'parent-slot', 'presence', 'storage', 'viewer', 'terminal-input', 'render', 'i18n'].map(file => `todo/src/${file}.mjs`),
    ...['controller', 'herdr', 'i18n', 'model', 'render', 'retention', 'runtime', 'storage', 'stream-tap', 'task-data', 'terminal-input', 'view-state', 'viewer'].map(file => `dag/src/${file}.mjs`)];
  for (const file of runtime) await readFile(join(source, file));
  if (previous) {
    // An owned backup needs its entrypoints, not files introduced by this upgrade.
    for (const file of ['extension.mjs', 'todo/extension.mjs', 'dag/extension.mjs']) {
      await readFile(join(container, previous, file));
    }
  }
  const legacy = [];
  for (const component of ['todo', 'dag']) {
    const file = join(agent, `extensions/omo-herdr-${component}.js`), content = await text(file);
    if (content === undefined) continue;
    const pattern = new RegExp(`^// managed by omo-herdr-${component}\\r?\\nexport \\{ default \\} from '\\.\\./herdr-${component}/integration/(generation-\\d{6})/extension\\.mjs';\\r?\\n?$`);
    const match = pattern.exec(content);
    if (!match || await text(join(agent, `herdr-${component}/integration/${match[1]}/.installed-by`)) !== `// managed by omo-herdr-${component}`) throw Error('Foreign or unverifiable legacy extension: ' + file);
    legacy.push({ file, content });
  }
  const generation = `generation-${String(Math.max(0, ...entries.map(name => Number(/^generation-(\d+)$/.exec(name)?.[1] ?? 0))) + 1).padStart(6, '0')}`;
  const integration = join(container, generation);
  const plan = { extension: wrapper, integration, language, legacyExtensions: legacy.map(item => item.file), ...(previous ? { backup: join(container, previous) } : {}), command: '/dashboard' };
  if (options['--dry-run']) return plan;
  await mkdir(container, { recursive: true, mode: 0o700 });
  await mkdir(dirname(wrapper), { recursive: true, mode: 0o700 });
  const staged = join(container, `.stage-${randomUUID()}`);
  const removed = [];
  let promoted = false, switched = false;
  try {
    await mkdir(staged, { mode: 0o700 });
    for (const component of ['todo', 'dag', 'coordination']) {
      await cp(join(source, component), join(staged, component), { recursive: true });
      await writeFile(join(staged, component, 'locale.json'), JSON.stringify({ language }) + '\n');
    }
    for (const file of ['extension.mjs', 'LICENSE', 'THIRD-PARTY-NOTICES.md']) await cp(join(source, file), join(staged, file));
    await writeFile(join(staged, '.installed-by'), marker);
    await writeFile(join(staged, 'legacy-wrapper-backup.json'), JSON.stringify(legacy, null, 2), { mode: 0o600 });
    for (const file of runtime) await readFile(join(staged, file));
    // Presence alone is not a runtime check: the staged participants must be
    // able to load the exact shared protocol and first-paint proof reader.
    const protocol = await import(pathToFileURL(join(staged, 'coordination/src/recovery.mjs')).href +
      `?install=${randomUUID()}`);
    if (protocol.RECOVERY_V1?.layout !== 'omo.herdr.dashboard.layout.v1' ||
      typeof protocol.createRecoveryCoordinator !== 'function' || typeof protocol.layoutIssues !== 'function')
      throw Error('Shared four-pane recovery protocol did not load');
    const proof = await import(pathToFileURL(join(staged, 'coordination/src/proof.mjs')).href +
      `?install=${randomUUID()}`);
    if (typeof proof.inspectViewerProof !== 'function')
      throw Error('Shared viewer proof reader did not load');
    await rename(staged, integration); promoted = true;
    await atomic(wrapper, `${marker}\nexport { default } from '../herdr-dashboard/integration/${generation}/extension.mjs';\n`); switched = true;
    for (const item of legacy) {
      if (await text(item.file) !== item.content) throw Error('Legacy extension changed during migration');
      await rm(item.file); removed.push(item);
    }
    await atomic(pointerFile, JSON.stringify({ generation }) + '\n');
    await atomic(join(container, '.installed-by'), marker);
    return { installed: true, ...plan };
  } catch (error) {
    for (const item of removed) await atomic(item.file, item.content);
    if (switched) { if (before === undefined) await rm(wrapper, { force: true }); else await atomic(wrapper, before); }
    if (pointerBefore === undefined) await rm(pointerFile, { force: true }); else await atomic(pointerFile, pointerBefore);
    if (promoted) await rm(integration, { recursive: true, force: true });
    throw error;
  } finally { await rm(staged, { recursive: true, force: true }); }
}
