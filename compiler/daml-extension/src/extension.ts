// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

'use strict';
// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';
import * as cp from 'child_process';
import * as tmp from 'tmp';
import { LanguageClient, LanguageClientOptions, RequestType, NotificationType, TextDocumentIdentifier, TextDocument, ExecuteCommandRequest } from 'vscode-languageclient';
import { Uri, Event, TextDocumentContentProvider, ViewColumn, EventEmitter, window, QuickPickOptions, ExtensionContext, env, WorkspaceConfiguration } from 'vscode';
import * as which from 'which';
import * as util from 'util';
import fetch from 'node-fetch';
import { getOrd } from 'fp-ts/lib/Array';
import { ordNumber } from 'fp-ts/lib/Ord';

let damlRoot: string = path.join(os.homedir(), '.daml');

const versionContextKey = 'version'

type WebviewFiles = {
    src: Uri;  // The JavaScript file.
    css: Uri;
}

var damlLanguageClient: LanguageClient;
// Extension activation
// Note: You can log debug information by using `console.log()`
// and then `Toggle Developer Tools` in VSCode. This will show
// output in the Console tab once the extension is activated.
export async function activate(context: vscode.ExtensionContext) {
    // Start the language clients
    let config = vscode.workspace.getConfiguration('daml')
    // Get telemetry consent
    const consent = getTelemetryConsent(config, context);

    // Display release notes on updates
    showReleaseNotesIfNewVersion(context);

    damlLanguageClient = createLanguageClient(config, await consent);
    damlLanguageClient.registerProposedFeatures();

    const webviewFiles: WebviewFiles = {
        src:
            vscode.Uri.file(path.join(context.extensionPath, 'src', 'webview.js')).
            with({scheme: 'vscode-resource'}),
        css:
            vscode.Uri.file(path.join(context.extensionPath, 'src', 'webview.css')).
            with({scheme: 'vscode-resource'}),
    };
    let virtualResourceManager = new VirtualResourceManager(damlLanguageClient, webviewFiles);
    context.subscriptions.push(virtualResourceManager);

    let _unused = damlLanguageClient.onReady().then(() => {
        startKeepAliveWatchdog();
        damlLanguageClient.onNotification(
            DamlVirtualResourceDidChangeNotification.type,
            (params) => virtualResourceManager.setContent(params.uri, params.contents)
        );
        damlLanguageClient.onNotification(
            DamlVirtualResourceNoteNotification.type,
            (params) => virtualResourceManager.setNote(params.uri, params.note)
        );
    });

    damlLanguageClient.start();

    let d1 = vscode.commands.registerCommand(
       'daml.showResource',
        (title, uri) => virtualResourceManager.createOrShow(title, uri)
    );

    let d2 = vscode.commands.registerCommand('daml.openDamlDocs', openDamlDocs);
    let d5 = vscode.commands.registerCommand('daml.visualize', visualize);

    let highlight = vscode.window.createTextEditorDecorationType({ backgroundColor: 'rgba(200,200,200,.35)' });

    let d3 = vscode.commands.registerCommand('daml.revealLocation', (uri: string, startLine: number, endLine: number) => {
      var theEditor = null;
      for (let editor of vscode.window.visibleTextEditors) {
          if (editor.document.uri.toString() === uri) {
              theEditor = editor;
          }
      }
      function jumpToLine(editor: vscode.TextEditor) {
        let start: vscode.Position = new vscode.Position(startLine, 0)
        let end: vscode.Position = new vscode.Position(endLine+1, 0)
        let range = new vscode.Range(start, end);
        editor.revealRange(range);
        editor.setDecorations(highlight, [range]);
        setTimeout(() => editor.setDecorations(highlight, []), 2000);
      }
      if (theEditor != null) {
          jumpToLine(theEditor)
      } else {
          vscode.workspace.openTextDocument(vscode.Uri.parse(uri))
            .then(doc => vscode.window.showTextDocument(doc, ViewColumn.One))
            .then(editor => jumpToLine(editor))
      }
    });

    let d4 = vscode.commands.registerCommand("daml.resetTelemetryConsent", resetTelemetryConsent(context));

    context.subscriptions.push(d1, d2, d3, d4, d5);
}

// Compare the extension version with the one stored in the global state.
// If they are different, we assume the user has updated the extension and
// we display the release notes for the new SDK release in a new tab.
// This should only occur the first time the user uses the extension after
// an update.
async function showReleaseNotesIfNewVersion(context: ExtensionContext) {
    const packageFile = path.join(context.extensionPath, 'package.json');
    const packageData = await util.promisify(fs.readFile)(packageFile, "utf8");
    const extensionVersion = JSON.parse(packageData).version;
    const recordedVersion = context.globalState.get(versionContextKey);
    // Check if we have a new version of the extension and show the release
    // notes if so. Update the current version so we don't show them again until
    // the next update.
    if (typeof extensionVersion === 'string' && extensionVersion !== '' &&
        (!recordedVersion || typeof recordedVersion === 'string' && checkVersionUpgrade(recordedVersion, extensionVersion))) {
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

// Show the release notes from the DAML Blog.
// We display the HTML in a new editor tab using a "webview":
// https://code.visualstudio.com/api/extension-guides/webview
async function showReleaseNotes(version: string) {
    try {
        const releaseNotesUrl = 'https://blog.daml.com/release-notes/' + version;
        const res = await fetch(releaseNotesUrl);
        if (res.ok) {
            const panel = vscode.window.createWebviewPanel(
                'releaseNotes', // Identifies the type of the webview. Used internally
                `New DAML SDK ${version} Available`, // Title of the panel displayed to the user
                vscode.ViewColumn.One, // Editor column to show the new webview panel in
                {} // No webview options for now
            );
            panel.webview.html = await res.text();
        }
    } catch (_error) {}
}

function getViewColumnForShowResource(): ViewColumn {
    const active = vscode.window.activeTextEditor;
    if (!active || !active.viewColumn) { return ViewColumn.One; }
    switch (active.viewColumn) {
        case ViewColumn.One: return ViewColumn.Two;
        case ViewColumn.Two: return ViewColumn.Three;
        default: return active.viewColumn;
    }
}

function visualize() {
    if (vscode.window.activeTextEditor) {
        let currentFile = vscode.window.activeTextEditor.document.fileName
        if (vscode.window.activeTextEditor.document.languageId != "daml") {
            vscode.window.showInformationMessage("Open the daml file to visualize")
        }
        else {
            damlLanguageClient.sendRequest(ExecuteCommandRequest.type,
                { command: "daml/damlVisualize", arguments: [currentFile] }).then(dotFileContents => {
                    vscode.workspace.openTextDocument({ content: dotFileContents, language: "dot" })
                        .then(doc => vscode.window.showTextDocument(doc, vscode.ViewColumn.One, true)
                            .then(_ => loadPreviewIfAvailable()))
                });
        }
    }
    else {
        vscode.window.showInformationMessage("Please open a DAML module to be visualized and then run the command")
    }
}

function loadPreviewIfAvailable() {
    if (vscode.extensions.getExtension("EFanZh.graphviz-preview")) {
        vscode.commands.executeCommand("graphviz.showPreviewToSide")
    }
    else{
        vscode.window.showInformationMessage("Install Graphviz Preview (https://marketplace.visualstudio.com/items?itemName=EFanZh.graphviz-preview) plugin to see graph for this dot file")
    }
}

function openDamlDocs() {
    vscode.env.openExternal(vscode.Uri.parse("https://docs.daml.com"));
}

function addIfInConfig(config:vscode.WorkspaceConfiguration, baseArgs: string[], toAdd: [string, string[]][]): string[]{
    let addedArgs : string[][] = toAdd
        .filter(x => config.get(x[0]))
        .map(x => x[1]);
    addedArgs.unshift(baseArgs);
    return [].concat.apply([], <any>addedArgs);
}

export function createLanguageClient(config: vscode.WorkspaceConfiguration, telemetryConsent: boolean|undefined): LanguageClient {
    // Options to control the language client
    let clientOptions: LanguageClientOptions = {
        // Register the server for DAML
        documentSelector: ["daml"],
    };

    let command: string;
    let args: string[] = ["ide", "--"];

    try {
        command = which.sync("daml");
    } catch (ex) {
        const damlCmdPath = path.join(damlRoot, "bin", "daml");
        if (fs.existsSync(damlCmdPath)) {
            command = damlCmdPath;
        } else {
            vscode.window.showErrorMessage("Failed to start the DAML language server. Make sure the assistant is installed.");
            throw new Error("Failed to locate assistant.");
        }
    }

    if (telemetryConsent === true){
        args.push('--telemetry');
    } else if (telemetryConsent === false){
        args.push('--optOutTelemetry')
    } else if (telemetryConsent == undefined) {
        // The user has not made an explicit choice.
        args.push('--telemetry-ignored')
    }
    const extraArgsString = config.get("extraArguments", "").trim();
    // split on an empty string returns an array with a single empty string
    const extraArgs = extraArgsString === "" ? [] : extraArgsString.split(" ");
    args = args.concat(extraArgs);
    const serverArgs : string[] = addIfInConfig(config, args,
        [ ['debug', ['--debug']]
        , ['experimental', ['--experimental']]
        , ['profile', ['+RTS', '-h', "-RTS"]]
        ]);

    if(config.get('experimental')){
        vscode.window.showWarningMessage('DAMLs Experimental feature flag is enabled, this may cause instability')
    }

    return new LanguageClient(
        'daml-language-server', 'DAML Language Server',
        { args: serverArgs, command: command, options: {cwd: vscode.workspace.rootPath }},
        clientOptions, true);
}

// this method is called when your extension is deactivated
export function deactivate() {
    // unLinkSyntax();
    // Stop keep-alive watchdog and terminate language server.
    stopKeepAliveWatchdog();
    (<any>damlLanguageClient)._childProcess.kill('SIGTERM');
}

// Keep alive timer for periodically checking that the server is responding
// to requests in a timely manner. If the server fails to respond it is
// terminated with SIGTERM.
var keepAliveTimer : NodeJS.Timer;
let keepAliveInterval = 60000; // Send KA every 60s.

// Wait for max 120s before restarting process.
// NOTE(JM): If you change this, make sure to also change the server-side timeouts to get
// detailed errors rather than cause a restart.
// Legacy DAML timeout for language server is defined in
// DA.Daml.LanguageServer.
let keepAliveTimeout = 120000;

function startKeepAliveWatchdog() {
    clearTimeout(keepAliveTimer);
    keepAliveTimer = setTimeout(keepAlive, keepAliveInterval);
}

function stopKeepAliveWatchdog() {
    clearTimeout(keepAliveTimer);
}

function keepAlive() {
    function killDamlc() {
        vscode.window.showErrorMessage(
           "Sorry, you’ve hit a bug requiring a DAML Language Server restart. We’d greatly appreciate a bug report — ideally with example files."
        );

        // Terminate the damlc process with SIGTERM. The language client will restart the process automatically.
        // NOTE(JM): Verify that this works on Windows.
        // https://nodejs.org/api/child_process.html#child_process_child_kill_signal
        (<any>damlLanguageClient)._childProcess.kill('SIGTERM');

        // Restart the watchdog after 10s
        setTimeout(startKeepAliveWatchdog, 10000);
    }

    let killTimer = setTimeout(killDamlc, keepAliveTimeout);
    damlLanguageClient.sendRequest(DamlKeepAliveRequest.type, null).then(r => {
        // Keep-alive request succeeded, clear the kill timer
        // and reschedule the keep-alive.
        clearTimeout(killTimer);
        startKeepAliveWatchdog();
    });
}

// Custom requests

namespace DamlKeepAliveRequest {
    export let type =
      new RequestType<void, void, void, void>('daml/keepAlive');
}

// Custom notifications

interface VirtualResourceChangedParams {
    /** The virtual resource uri */
    uri: string;

    /** The new contents of the virtual resource */
    contents: string;
}

namespace DamlVirtualResourceDidChangeNotification {
    export let type =
      new NotificationType<VirtualResourceChangedParams, void>(
        'daml/virtualResource/didChange'
      );
}

interface VirtualResourceNoteParams {
    /** The virtual resource uri */
    uri: string;

    /** The note to set on the virtual resource */
    note: string;
}

namespace DamlVirtualResourceNoteNotification {
    export let type =
      new NotificationType<VirtualResourceNoteParams, void>(
        'daml/virtualResource/note'
      );
}

type UriString = string;
type ScenarioResult = string;
type SelectedView = string;

class VirtualResourceManager {
    // Note (MK): While it is tempting to switch to Map<Uri, …> for these types
    // Map uses reference equality for objects so this goes horribly wrong.
    // Mapping from URIs to the web view panel
    private _panels: Map<UriString, vscode.WebviewPanel> = new Map<UriString, vscode.WebviewPanel>();
    // Mapping from URIs to the HTML content of the webview
    private _panelContents: Map<UriString, ScenarioResult> = new Map<UriString, ScenarioResult>();
    // Mapping from URIs to selected view
    private _panelStates: Map<UriString, SelectedView> = new Map<UriString, SelectedView>();
    private _client: LanguageClient;
    private _disposables: vscode.Disposable[] = [];
    private _webviewFiles : WebviewFiles;

    constructor(client: LanguageClient, webviewFiles: WebviewFiles) {
        this._client = client;
        this._webviewFiles = webviewFiles;
    }

    private open(uri: UriString) {
        this._client.sendNotification(
            'textDocument/didOpen', {
                textDocument: {
                    uri: uri,
                    languageId: '',
                    version: 0,
                    text: ''
                }
            }
        );
    }

    private close(uri: UriString) {
        this._client.sendNotification(
            'textDocument/didClose', {
                textDocument: {uri: uri}
            }
        );
    }

    public createOrShow(title: string, uri: UriString) {
		const column = getViewColumnForShowResource();

        let panel = this._panels.get(uri);
        if (panel) {
            panel.reveal(column);
            return;
        }
        this.open(uri);
        panel = vscode.window.createWebviewPanel(
            'daml',
            title,
            column,
            { enableScripts: true, enableFindWidget: true, enableCommandUris: true }
        );
        panel.onDidDispose(
            () => {this._panels.delete(uri); this.close(uri);},
            null,
            this._disposables
        );
        panel.webview.onDidReceiveMessage(
            message => {
                switch (message.command) {
                    case 'selected_view':
                        this._panelStates.set(uri, message.value);
                        break;
                }
            }
        );
        this._panels.set(uri, panel);
        panel.webview.html = this._panelContents.get(uri) || '';
    }

    public setContent(uri: UriString, contents: ScenarioResult) {
        contents = contents.replace('$webviewSrc', this._webviewFiles.src.toString());
        contents = contents.replace('$webviewCss', this._webviewFiles.css.toString());
        this._panelContents.set(uri, contents);
        const panel = this._panels.get(uri);
        if (panel) {
            // append timestamp to force page reload (prevent using cache) as otherwise notes are not getting cleared
            panel.webview.html = contents + "<!-- " + new Date() + " -->";
            const panelState = this._panelStates.get(uri);
            if (panelState) {
                panel.webview.postMessage({command: 'select_view', value: panelState});
            };
        }
    }

    public setNote(uri: UriString, note: string) {
        const panel = this._panels.get(uri);
        if (panel) {
            panel.webview.postMessage({command: 'add_note', value: note});
        }
    }

    public dispose() {
        for (const panel of this._panels.values()) {
            panel.dispose();
        }
        for (const disposable of this._disposables.values()) {
            disposable.dispose();
        }
    }
}

let telemetryOverride = {
    enable: "Enable",
    disable: "Disable",
    fromConsent: "From consent popup"
}
const options = {
    yes : "Yes, I would like to help improve DAML!",
    no : "No, I'd rather not.",
    read : "I'd like to read the privacy policy first."
}

function ifYouChangeYourMind(){
    window.showInformationMessage(
        'If you change your mind, data sharing preferences can be found in settings under "daml.telemetry"'
        )
}

const telemetryConsentKey = 'telemetry-consent'
const privacyPolicy = 'https://www.digitalasset.com/privacy-policy'


function setConsentState(ex : ExtensionContext, val : undefined|boolean){
    ex.globalState.update(telemetryConsentKey, val);
}

function resetTelemetryConsent(ex: ExtensionContext){
    return function(){
        setConsentState(ex, undefined);
    }
}

function handleResult(ex : ExtensionContext, res : string|undefined) : boolean|undefined{
    if(typeof res === 'undefined'){
        return undefined;
    }else switch(res){
        case options.yes: {
            setConsentState(ex, true);
            ifYouChangeYourMind();
            return true;
        }
        case options.no: {
            setConsentState(ex, false);
            ifYouChangeYourMind();
            return false;
        }
        case options.read: {
            vscode.env.openExternal(vscode.Uri.parse(privacyPolicy));
            return false;
        }
        default: throw "Unrecognised telemetry option";
    }
}

async function telemetryPopUp () : Promise <string | undefined> {
    let qpo: QuickPickOptions = {
        placeHolder: "Do you want to allow the collection of usage data"
    };
    return window.showQuickPick([
        options.yes,
        options.read,
        options.no
    ] , qpo)
}

async function getTelemetryConsent (config: WorkspaceConfiguration, ex: ExtensionContext) : Promise<boolean|undefined> {
    switch(config.get("telemetry") as string){
        case telemetryOverride.enable:
            return true;
        case telemetryOverride.disable:
            return false;
        case telemetryOverride.fromConsent:{
            const consent = ex.globalState.get(telemetryConsentKey)
            if(typeof consent === 'boolean'){
                return consent;
            }
            const res = await telemetryPopUp();
            // the user has closed the popup, ask again on next startup
            return handleResult(ex, res);
        }
        default: throw "Unexpected telemetry override option"
    }
}
