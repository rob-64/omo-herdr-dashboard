import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const fixture = await mkdtemp(join(tmpdir(), 'dashboard-package-'));
const env = { ...process.env, HOME: join(fixture, 'home'), USERPROFILE: join(fixture, 'home'), OMO_CODING_AGENT_DIR: join(fixture, 'default-agent'), SENPI_CODING_AGENT_DIR: join(fixture, 'default-agent'), PI_CODING_AGENT_DIR: join(fixture, 'default-agent'), npm_config_cache: join(fixture, 'cache') };
for (const key of Object.keys(env)) if (key.toLowerCase() === 'npm_config_allow_scripts') delete env[key];
const npm = process.env.npm_execpath ?? join(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js');
const run = (args, cwd = root) => execFileSync(process.execPath, [npm, ...args], { env, cwd, encoding: 'utf8' });
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
try {
  const output = JSON.parse(run(['pack', '--ignore-scripts', '--json', '--pack-destination', fixture]));
  const pack = Array.isArray(output) ? output[0] : output['omo-herdr-dashboard'];
  for (const file of pack.files) assert.ok(!/^(packages|test|scripts|\.omo|\.git|node_modules)\//.test(file.path), 'Private/development tree in package: ' + file.path);
  const consumer = join(fixture, 'consumer');
  await mkdir(consumer);
  run(['install', '--prefix', consumer, '--offline', '--ignore-scripts', '--no-audit', '--no-fund', '--package-lock=false', join(fixture, pack.filename)]);
  const installed = join(consumer, 'node_modules/omo-herdr-dashboard');
  let runtime = 0;
  for (const component of ['todo', 'dag']) {
    execFileSync(process.execPath, ['--input-type=module', '--eval',
      `await import(${JSON.stringify(pathToFileURL(join(installed, 'dist', component, 'src/viewer.mjs')).href)})`],
    { env, cwd: consumer, stdio: 'pipe' });
    const modules = component === 'todo' ?
      ['child-guard', 'controller', 'display-bridge', 'inline-recovery', 'native-observer', 'native-todo', 'parent-slot', 'presence', 'storage', 'viewer', 'terminal-input', 'render', 'i18n'].map(file => file + '.mjs') :
      (await readdir(join(root, 'packages', component, 'src'))).filter(file => file.endsWith('.mjs'));
    for (const file of ['extension.mjs', ...modules.map(file => 'src/' + file)]) {
      assert.equal(sha(await readFile(join(installed, 'dist', component, file))), sha(await readFile(join(root, 'dist', component, file))));
      assert.equal(sha(await readFile(join(installed, 'dist', component, file))), sha(await readFile(join(root, 'packages', component, file))));
      runtime++;
    }
  }
  const agent = join(fixture, 'agent');
  const result = JSON.parse(execFileSync(process.execPath, [join(installed, 'bin/omo-herdr-dashboard.mjs'), 'install', '--agent-dir', agent, '--lang', 'ko'], { env, encoding: 'utf8' }));
  assert.equal(result.installed, true);
  for (const component of ['todo', 'dag']) {
    for (const file of (await readdir(join(installed, 'dist', component, 'src'))).filter(file => file.endsWith('.mjs'))) {
      assert.equal(sha(await readFile(join(result.integration, component, 'src', file))),
        sha(await readFile(join(installed, 'dist', component, 'src', file))));
    }
  }
  const wrapper = await readFile(result.extension, 'utf8');
  assert.match(wrapper, /omo-herdr-dashboard/);
  console.log(JSON.stringify({ pass: true, archiveFiles: pack.files.length, runtimeModules: runtime, singleExtension: true, offlineInstall: true }));
} finally { await rm(fixture, { recursive: true, force: true }); }
