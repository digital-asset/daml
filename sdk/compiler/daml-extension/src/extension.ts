// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use strict";
// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from "vscode";
import * as path from "path";
import * as fs from "fs";
import { ViewColumn, ExtensionContext } from "vscode";
import * as util from "util";
import fetch from "node-fetch";
import { getOrd } from "fp-ts/lib/Array";
import { ordNumber } from "fp-ts/lib/Ord";
import { DamlLanguageClient } from "./language_client";
import { resetTelemetryConsent, getTelemetryConsent } from "./telemetry";
import { WebviewFiles, getVRFilePath } from "./virtual_resource_manager";
import * as child_process from "child_process";

const versionContextKey = "version";

var damlLanguageClients: { [projectPath: string]: DamlLanguageClient } = {};
var webviewFiles: WebviewFiles;
var outputChannel: vscode.OutputChannel = vscode.window.createOutputChannel(
  "Daml Extension Host",
);

// Extension activation
// Note: You can log debug information by using `console.log()`
// and then `Toggle Developer Tools` in VSCode. This will show
// output in the Console tab once the extension is activated.
export async function activate(context: vscode.ExtensionContext) {
  // Add entry for multi-ide readonly directory
  let filesConfig = vscode.workspace.getConfiguration("files");
  let multiIdeReadOnlyPattern = "**/.daml/unpacked-dars/**";
  // Explicit any type as typescript gets angry, its a map from pattern (string) to boolean
  let readOnlyInclude: any =
    filesConfig.inspect("readonlyInclude")?.workspaceValue || {};
  if (!readOnlyInclude[multiIdeReadOnlyPattern])
    filesConfig.update(
      "readonlyInclude",
      { ...readOnlyInclude, [multiIdeReadOnlyPattern]: true },
      vscode.ConfigurationTarget.Workspace,
    );

  // Display release notes on updates
  showReleaseNotesIfNewVersion(context);

  webviewFiles = {
    src: vscode.Uri.file(path.join(context.extensionPath, "src", "webview.js")),
    css: vscode.Uri.file(
      path.join(context.extensionPath, "src", "webview.css"),
    ),
  };

  await startLanguageServers(context);

  vscode.workspace.onDidChangeConfiguration(
    async (event: vscode.ConfigurationChangeEvent) => {
      if (event.affectsConfiguration("daml")) {
        await stopLanguageServers();
        await new Promise(resolve => setTimeout(resolve, 1000));
        await startLanguageServers(context);
      }
    },
  );

  let d1 = vscode.commands.registerCommand(
    "daml.showResource",
    (title, uri) => {
      let path = getVRFilePath(uri);
      if (!path) return;
      for (let projectPath in damlLanguageClients) {
        if (path.startsWith(projectPath))
          damlLanguageClients[projectPath].virtualResourceManager.createOrShow(
            title,
            uri,
          );
      }
    },
  );

  let d2 = vscode.commands.registerCommand("daml.openDamlDocs", openDamlDocs);

  let highlight = vscode.window.createTextEditorDecorationType({
    backgroundColor: "rgba(200,200,200,.35)",
  });

  let d3 = vscode.commands.registerCommand(
    "daml.revealLocation",
    (uri: string, startLine: number, endLine: number) => {
      var theEditor = null;
      for (let editor of vscode.window.visibleTextEditors) {
        if (editor.document.uri.toString() === uri) {
          theEditor = editor;
        }
      }
      function jumpToLine(editor: vscode.TextEditor) {
        let start: vscode.Position = new vscode.Position(startLine, 0);
        let end: vscode.Position = new vscode.Position(endLine + 1, 0);
        let range = new vscode.Range(start, end);
        editor.revealRange(range);
        editor.setDecorations(highlight, [range]);
        setTimeout(() => editor.setDecorations(highlight, []), 2000);
      }
      if (theEditor != null) {
        jumpToLine(theEditor);
      } else {
        vscode.workspace
          .openTextDocument(vscode.Uri.parse(uri))
          .then(doc => vscode.window.showTextDocument(doc, ViewColumn.One))
          .then(editor => jumpToLine(editor));
      }
    },
  );

  let d4 = vscode.commands.registerCommand(
    "daml.resetTelemetryConsent",
    resetTelemetryConsent(context),
  );

  context.subscriptions.push(d1, d2, d3, d4);
}

interface IdeManifest {
  [multiPackagePath: string]: { [envVarName: string]: string };
}

function parseIdeManifest(path: string): IdeManifest | null {
  try {
    const json = JSON.parse(fs.readFileSync(path, "utf8"));
    const manifest: IdeManifest = {};
    for (const entry of json) {
      manifest[entry["multi-package-directory"]] = entry.environment;
    }
    return manifest;
  } catch (e) {
    outputChannel.appendLine("Failed to parse ide-manifest file: " + e);
    return null;
  }
}

async function fileExists(path: string) {
  return new Promise(r => fs.access(path, fs.constants.F_OK, e => r(!e)));
}

async function startLanguageServers(context: ExtensionContext) {
  const config = vscode.workspace.getConfiguration("daml");
  const consent = await getTelemetryConsent(config, context);
  const rootPath = vscode.workspace.workspaceFolders?.[0].uri.fsPath;
  if (!rootPath) throw "Couldn't find workspace root";
  const multiIDESupport = config.get("multiPackageIdeSupport");

  var isGradleProject = false;
  const gradlewExists = await fileExists(rootPath + "/gradlew");
  // Of the 3 direnv extensions I found, mkhl.direnv is most popular and only one that populates environment variables for other extensions
  // As such, it is the one we recommend.
  const envrcExistsWithoutExt =
    (await fileExists(rootPath + "/.envrc")) &&
    vscode.extensions.getExtension("mkhl.direnv") == null;

  if (envrcExistsWithoutExt) {
    const warningMessage =
      "Found an .envrc file but the recommended direnv VSCode extension was not installed. Daml IDE may fail to start due to missing environment." +
      "\nWould you like to install this extension or attempt to continue without it?";
    const installAnswer = "Install recommended direnv extension";
    const doNotInstallAnswer = "Attempt to continue without";
    const doNotInstallkey = "no-install-direnv";

    // Don't ask if we previously selected doNotInstallAnswer
    if (!context.workspaceState.get(doNotInstallkey, false)) {
      const selection = await vscode.window.showWarningMessage(
        warningMessage,
        installAnswer,
        doNotInstallAnswer,
      );

      if (selection == doNotInstallAnswer)
        context.workspaceState.update(doNotInstallkey, true);
      else if (selection == installAnswer) {
        await vscode.commands.executeCommand("extension.open", "mkhl.direnv");
        vscode.window.showInformationMessage(
          "After installing, reload the VSCode window or restart the extension host through the command palette.",
        );
        return;
      }
    }
  }

  if (multiIDESupport && gradlewExists) {
    const generateIdeManifestCommand =
      rootPath + '/gradlew run --args="--project-dir ' + rootPath + '"';
    try {
      await util.promisify(child_process.exec)(generateIdeManifestCommand, {
        cwd: rootPath,
      });
      isGradleProject = true;
    } catch (e) {
      outputChannel.appendLine("Gradle setup failure: " + e);
      vscode.window
        .showInformationMessage(
          "Daml is starting without gradle environment.",
          "See Output",
        )
        .then((resp: string | undefined) => {
          if (resp) outputChannel.show();
        });
    }
  }

  if (isGradleProject) {
    const ideManifest: IdeManifest | null = parseIdeManifest(
      rootPath + "/build/ide-manifest.json",
    );
    if (!ideManifest)
      throw "Failed to find and parse ide manifest for gradle project.";
    let n = 0;
    for (const projectPath in ideManifest) {
      let envVars = ideManifest[projectPath];
      n++;
      damlLanguageClients[projectPath] = new DamlLanguageClient(
        projectPath,
        envVars,
        config,
        consent,
        n.toString(),
        context,
        webviewFiles,
      );
    }
  } else {
    damlLanguageClients[rootPath] = new DamlLanguageClient(
      rootPath,
      {},
      config,
      consent,
      "1",
      context,
      webviewFiles,
    );
  }
}

async function stopLanguageServers() {
  for (let projectPath in damlLanguageClients) {
    let damlLanguageClient = damlLanguageClients[projectPath];
    await damlLanguageClient.stop();
  }
  damlLanguageClients = {};
}

// this method is called when your extension is deactivated
export async function deactivate() {
  for (let projectPath in damlLanguageClients) {
    let damlLanguageClient = damlLanguageClients[projectPath];
    damlLanguageClient.forceStop();
  }
}

// Compare the extension version with the one stored in the global state.
// If they are different, we assume the user has updated the extension and
// we display the release notes for the new SDK release in a new tab.
// This should only occur the first time the user uses the extension after
// an update.
async function showReleaseNotesIfNewVersion(context: ExtensionContext) {
  const packageFile = path.join(context.extensionPath, "package.json");
  const packageData = await util.promisify(fs.readFile)(packageFile, "utf8");
  const extensionVersion = JSON.parse(packageData).version;
  const recordedVersion = context.globalState.get(versionContextKey);
  // Check if we have a new version of the extension and show the release
  // notes if so. Update the current version so we don't show them again until
  // the next update.
  if (
    typeof extensionVersion === "string" &&
    extensionVersion !== "" &&
    (!recordedVersion ||
      (typeof recordedVersion === "string" &&
        checkVersionUpgrade(recordedVersion, extensionVersion)))
  ) {
    await showReleaseNotes(extensionVersion);
    await context.globalState.update(versionContextKey, extensionVersion);
  }
}

// Check that `version2` is an upgrade from `version1`,
// i.e. that the components of the version number have increased
// (checked from major to minor version numbers).
function checkVersionUpgrade(version1: string, version2: string) {
  const comps1 = version1.split(".").map(Number);
  const comps2 = version2.split(".").map(Number);
  const o = getOrd(ordNumber);
  return o.compare(comps2, comps1) > 0;
}

// Show the release notes from the Daml Blog.
// We display the HTML in a new editor tab using a "webview":
// https://code.visualstudio.com/api/extension-guides/webview
async function showReleaseNotes(version: string) {
  try {
    const releaseNotesUrl = "https://blog.daml.com/release-notes/" + version;
    const res = await fetch(releaseNotesUrl);
    if (res.ok) {
      const panel = vscode.window.createWebviewPanel(
        "releaseNotes", // Identifies the type of the webview. Used internally
        `New Daml SDK ${version} Available`, // Title of the panel displayed to the user
        vscode.ViewColumn.One, // Editor column to show the new webview panel in
        {}, // No webview options for now
      );
      panel.webview.html = await res.text();
    }
  } catch (_error) {}
}

function openDamlDocs() {
  vscode.env.openExternal(vscode.Uri.parse("https://docs.daml.com"));
}
