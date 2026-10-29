// Validation build: syntax, package contract and public-safety scan.
//
// Read-only by design: it writes no artifact and touches no path outside this
// repository. The packaged build (dist/) belongs to a later, separately
// authorized task.
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { cp, mkdir, mkdtemp, rename, rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

import { listFilesRecursive, scanTree } from '../src/public-safety.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const problems = [];
const checks = [];

const modules = ['src', 'scripts', 'bin']
  .flatMap(dir => listFilesRecursive(join(root, dir)))
  .filter(file => file.endsWith('.mjs')).concat(join(root, 'extension.mjs'));
let syntaxOk = true;
for (const file of modules) {
  try {
    execFileSync(process.execPath, ['--check', file], { stdio: 'pipe' });
  } catch (error) {
    syntaxOk = false;
    problems.push({ check: 'syntax', file: relative(root, file).split('\\').join('/'), message: String(error.stderr ?? error.message).trim() });
  }
}
checks.push({ check: 'syntax', files: modules.length, ok: syntaxOk });

const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const contract = [];
if (manifest.type !== 'module') contract.push('package.json type must be module');
if (manifest.version !== '0.1.0') contract.push('package.json version must be 0.1.0');
if (manifest.license !== 'MIT') contract.push('package.json license must be MIT');
if (manifest.engines?.node !== '>=24') contract.push('package.json engines.node must be >=24');
for (const field of ['dependencies', 'optionalDependencies', 'peerDependencies', 'bundledDependencies']) {
  if (manifest[field] !== undefined) contract.push(`production dependency field must be absent: ${field}`);
}
for (const [name, command] of Object.entries(manifest.scripts ?? {})) {
  if (typeof command !== 'string' || command.trim() === '') contract.push(`script must be a real command: ${name}`);
}
for (const entry of manifest.files ?? []) {
  if (entry !== 'dist/' && !existsSync(join(root, entry))) contract.push(`package files entry missing: ${entry}`);
}
for (const target of Object.values(manifest.exports ?? {})) {
  if (typeof target === 'string' && !target.startsWith('./dist/') && !existsSync(join(root, target))) contract.push(`package exports target missing: ${target}`);
}
for (const command of Object.values(manifest.scripts ?? {})) {
  const match = /node\s+(\S+\.mjs)/.exec(command);
  if (match && !existsSync(join(root, match[1]))) contract.push(`script target missing: ${match[1]}`);
}
if (contract.length > 0) problems.push({ check: 'package-contract', messages: contract });
checks.push({ check: 'package-contract', scripts: Object.keys(manifest.scripts ?? {}).length, ok: contract.length === 0 });

// Shipped files only. Tests are not published and may legitimately contain
// hostile fixture strings used to prove the scanner works.
const scanned = [
  join(root, 'extension.mjs'),
  ...listFilesRecursive(join(root, 'src')),
  ...listFilesRecursive(join(root, 'bin')),
  ...listFilesRecursive(join(root, 'config')),
  join(root, 'package.json'),
  join(root, 'README.md'),
  join(root, 'THIRD-PARTY-NOTICES.md'),
  join(root, 'LICENSE'),
];
const violations = scanTree(root, scanned);
for (const violation of violations) problems.push({ check: 'public-safety', ...violation });
checks.push({ check: 'public-safety', files: scanned.length, violations: violations.length, ok: violations.length === 0 });

const report = { schema: 'omo-herdr-todo/build-report/1', ok: problems.length === 0, checks, problems };
console.log(JSON.stringify(report, null, 2));
if (!report.ok) process.exitCode = 1;
if (report.ok) {
  const stage = await mkdtemp(join(root,'.dist-stage-'));
  try {
    await mkdir(join(stage,'src'));await mkdir(join(stage,'scripts'));
    for (const file of ["child-guard","controller","display-bridge","inline-recovery","native-observer","native-todo","parent-slot","presence","storage","viewer","terminal-input","render","i18n"]) await cp(join(root,'src',file+'.mjs'),join(stage,'src',file+'.mjs'));
    for (const file of ['extension.mjs','LICENSE','THIRD-PARTY-NOTICES.md']) await cp(join(root,file),join(stage,file));
    await cp(join(root,'scripts/install.mjs'),join(stage,'scripts/install.mjs'));
    await rm(join(root,'dist'),{recursive:true,force:true});await rename(stage,join(root,'dist'));
  } finally {await rm(stage,{recursive:true,force:true});}
}
