// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import { DamlLanguageClient, EnvVars } from "./language_client";
import { resetTelemetryConsent, getTelemetryConsent } from "./telemetry";
import { WebviewFiles, getVRFilePath } from "./virtual_resource_manager";
import * as child_process from "child_process";
import * as semver from "semver";

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
      let vrPathMb = getVRFilePath(uri);
      if (!vrPathMb) return;

      // Need to normalize paths so that prefix comparison works on Windows,
      // where path separators can differ.
      let vrPath: string = path.normalize(vrPathMb);
      let isPrefixOfVrPath = (candidate: string) =>
        vrPath.startsWith(path.normalize(candidate) + path.sep);

      // Try to find a client for the virtual resource- if we can't, log to DevTools
      let foundAClient = false;
      for (let projectPath in damlLanguageClients) {
        if (isPrefixOfVrPath(projectPath)) {
          foundAClient = true;
          damlLanguageClients[projectPath].virtualResourceManager.createOrShow(
            title,
            uri,
          );
          break;
        }
      }

      if (!foundAClient) {
        console.log(
          `daml.showResource: Could not find a language client for ${vrPath}`,
        );
        vscode.window.showWarningMessage(
          `Could not show script results - could not find a language client for ${vrPath}`,
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

  let d5 = vscode.commands.registerCommand(
    "daml.installRecommendedDirenv",
    showRecommendedDirenvPage,
  );

  context.subscriptions.push(d1, d2, d3, d4, d5);
}

interface IdeManifest {
  [multiPackagePath: string]: EnvVars;
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

function fileExists(path: string): Promise<boolean> {
  return new Promise(r => fs.access(path, fs.constants.F_OK, e => r(!e)));
}

async function showRecommendedDirenvPage() {
  await vscode.commands.executeCommand("extension.open", "mkhl.direnv");
  vscode.window.showInformationMessage(
    "After installing, reload the VSCode window or restart the extension host through the command palette.",
  );
}

async function tryGenerateIdeManifest(
  rootPath: string,
  envrcExistsWithoutExt: boolean,
): Promise<boolean> {
  const generateIdeManifestCommand =
    rootPath + '/gradlew run --args="--project-dir ' + rootPath + '"';
  try {
    await util.promisify(child_process.exec)(generateIdeManifestCommand, {
      cwd: rootPath,
    });
    return true;
  } catch (e) {
    outputChannel.appendLine("Gradle setup failure: " + e);
    vscode.window
      .showInformationMessage(
        "Daml is starting without gradle environment." +
          (envrcExistsWithoutExt
            ? " You may be missing a direnv extension."
            : ""),
        "See Output",
        ...(envrcExistsWithoutExt ? ["Install direnv"] : []),
      )
      .then((resp: string | undefined) => {
        if (resp == "See Output") outputChannel.show();
        if (resp == "Install direnv") showRecommendedDirenvPage();
      });
  }
  return false;
}

async function startLanguageServers(context: ExtensionContext) {
  const config = vscode.workspace.getConfiguration("daml");
  const consent = await getTelemetryConsent(config, context);
  const rootPath = vscode.workspace.workspaceFolders?.[0].uri.fsPath;
  if (!rootPath) throw "Couldn't find workspace root";
  const multiIDESupport = config.get("multiPackageIdeSupport");
  const gradleSupport = config.get("multiPackageIdeGradleSupport");

  const gradlewExists = await fileExists(rootPath + "/gradlew");
  // Of the 3 direnv extensions I found, mkhl.direnv is most popular and only one that populates environment variables for other extensions
  // As such, it is the one we recommend.
  const envrcExistsWithoutExt: boolean =
    (await fileExists(rootPath + "/.envrc")) &&
    vscode.extensions.getExtension("mkhl.direnv") == null;

  if (envrcExistsWithoutExt) {
    const warningMessage =
      "Found an .envrc file but the recommended direnv VSCode extension is not installed. Daml IDE may fail to start due to missing environment variables." +
      "\nWould you like to install the recommended direnv extension or attempt to continue without it?";
    const installAnswer = "Open marketplace";
    const doNotInstallAnswer = "Continue without";
    const neverInstallAnswer = "Do not ask again";
    const doNotInstallkey = "no-install-direnv";

    // Don't ask if we previously selected doNotInstallAnswer
    if (!context.workspaceState.get(doNotInstallkey, false)) {
      const selection = await vscode.window.showWarningMessage(
        warningMessage,
        installAnswer,
        doNotInstallAnswer,
        neverInstallAnswer,
      );

      if (selection == neverInstallAnswer) {
        context.workspaceState.update(doNotInstallkey, true);
        vscode.window.showInformationMessage(
          "Warning disabled. If you need the extension in future, run Install Daml Recommended Direnv in the command pallette.",
        );
      } else if (selection == installAnswer) {
        await showRecommendedDirenvPage();
        return;
      }
    }
  }

  var isGradleProject = false;
  if (multiIDESupport && gradleSupport && gradlewExists) {
    isGradleProject = await tryGenerateIdeManifest(
      rootPath,
      envrcExistsWithoutExt,
    );
  }

  if (isGradleProject) {
    const ideManifest: IdeManifest | null = parseIdeManifest(
      rootPath + "/build/ide-manifest.json",
    );
    if (!ideManifest)
      throw "Failed to find and parse ide manifest for gradle project.";
    // If we only have one Multi-IDE, we don't need the `--ide-identifier`, which gives us slightly improved backwards compatibility
    const singleMultiIde = Object.keys(ideManifest).length == 1;
    let n = 0;
    for (const projectPath in ideManifest) {
      let envVars = ideManifest[projectPath];
      n++;
      const languageClient = await DamlLanguageClient.build(
        projectPath,
        envVars,
        config,
        consent,
        context,
        webviewFiles,
        singleMultiIde ? undefined : n.toString(),
      );
      if (languageClient) damlLanguageClients[projectPath] = languageClient;
    }
  } else {
    const languageClient = await DamlLanguageClient.build(
      rootPath,
      process.env,
      config,
      consent,
      context,
      webviewFiles,
    );
    if (languageClient) damlLanguageClients[rootPath] = languageClient;
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
        semver.lt(recordedVersion, extensionVersion)))
  ) {
    await showReleaseNotes(extensionVersion);
    await context.globalState.update(versionContextKey, extensionVersion);
  }
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
