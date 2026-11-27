// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const assert = require("node:assert");
const childProcess = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const WEBVIEW_ASSETS = ["src/webview.js", "src/webview.css"];

function runfileFromManifest(rlocationPath) {
  const manifestFile = process.env.RUNFILES_MANIFEST_FILE;
  assert.ok(
    manifestFile,
    `${rlocationPath} is not in the runfiles tree and there is no runfiles manifest`,
  );
  const entry = fs
    .readFileSync(manifestFile, "utf8")
    .split(/\r?\n/)
    .find(line => line.startsWith(`${rlocationPath} `));
  assert.ok(entry, `${rlocationPath} is not in the runfiles manifest`);
  return entry.slice(rlocationPath.length + 1);
}

function runfile(rlocationPath) {
  const runfilesTreePath = path.join(process.env.TEST_SRCDIR, rlocationPath);
  return fs.existsSync(runfilesTreePath)
    ? runfilesTreePath
    : runfileFromManifest(rlocationPath);
}

function vscodeStub() {
  const stub = () =>
    new Proxy(function () {}, {
      get: (_, key) => (key === "then" ? undefined : stub()),
      apply: () => stub(),
      construct: () => stub(),
    });
  return stub();
}

function unpackVsix() {
  const unpackDir = fs.mkdtempSync(path.join(process.env.TEST_TMPDIR, "vsix-"));
  childProcess.execFileSync(
    runfile(process.env.ZIPPER),
    ["x", runfile(process.env.VSIX)],
    { cwd: unpackDir, stdio: "inherit" },
  );
  return unpackDir;
}

function provideVscodeModule(unpackDir) {
  const moduleDir = path.join(unpackDir, "node_modules", "vscode");
  fs.mkdirSync(moduleDir, { recursive: true });
  fs.writeFileSync(
    path.join(moduleDir, "index.js"),
    `module.exports = (${vscodeStub})();\n`,
  );
}

const unpackDir = unpackVsix();
provideVscodeModule(unpackDir);

const extensionDir = path.join(unpackDir, "extension");
const manifest = JSON.parse(
  fs.readFileSync(path.join(extensionDir, "package.json"), "utf8"),
);

for (const asset of WEBVIEW_ASSETS) {
  assert.ok(
    fs.existsSync(path.join(extensionDir, asset)),
    `${asset} is missing from the vsix`,
  );
}

for (const dependency of Object.keys(manifest.dependencies)) {
  assert.ok(
    fs.existsSync(
      path.join(extensionDir, "node_modules", dependency, "package.json"),
    ),
    `${dependency} is declared in package.json but missing from the vsix`,
  );
}

const extension = require(path.join(extensionDir, manifest.main));
assert.strictEqual(typeof extension.activate, "function");
assert.strictEqual(typeof extension.deactivate, "function");
