// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

'use strict';
// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';
import * as cp from 'child_process';
import { LanguageClient, LanguageClientOptions, RequestType, NotificationType, TextDocumentIdentifier, TextDocument } from 'vscode-languageclient';
import { Uri, Event, TextDocumentContentProvider, ViewColumn, EventEmitter, window, QuickPickOptions, ExtensionContext, env, WorkspaceConfiguration } from 'vscode'
import * as which from 'which';

let damlRoot: string = path.join(os.homedir(), '.daml');
let daSdkPath: string = path.join(os.homedir(), '.da');
let daCmdPath: string = path.join(daSdkPath, 'bin', 'da');

var damlLanguageClient: LanguageClient;
// Extension activation
export async function activate(context: vscode.ExtensionContext) {
    // Start the language clients
    let config = vscode.workspace.getConfiguration('daml')
    // Get telemetry consent
    const consent = getTelemetryConsent(config, context);

    damlLanguageClient = createLanguageClient(config, await consent);
    // lsClient.trace = 2;


    const webviewSrc: Uri =
        vscode.Uri.file(path.join(context.extensionPath, 'src', 'webview.js')).
        with({scheme: 'vscode-resource'});
    let virtualResourceManager = new VirtualResourceManager(damlLanguageClient, webviewSrc);
    context.subscriptions.push(virtualResourceManager);

    let _unused = damlLanguageClient.onReady().then(() => {
        startKeepAliveWatchdog();
        setupWorkspaceValidationStatusBarItem(damlLanguageClient);
        damlLanguageClient.onNotification(
            DamlVirtualResourceDidChangeNotification.type,
            (params) => virtualResourceManager.setContent(params.uri, params.contents)
        );
    });

    damlLanguageClient.start();

    let d1 = vscode.commands.registerCommand(
       'daml.showResource',
        (title, uri) => virtualResourceManager.createOrShow(title, uri)
    );

    let d2 = vscode.commands.registerCommand('daml.openDamlDocs', openDamlDocs);

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

    let d4 = vscode.commands.registerCommand("daml.upgrade", modifyBuffer)
    let d5 = vscode.commands.registerCommand("daml.resetTelemetryConsent", resetTelemetryConsent(context));

    context.subscriptions.push(d1, d2, d3, d4, d5);
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

function openDamlDocs() {
    vscode.env.openExternal(vscode.Uri.parse("https://docs.daml.com"));
}

function modifyBuffer(filePath: {uri: string}) {
    damlLanguageClient.sendRequest(DamlUpgradeRequest.type, {uri: filePath}).then(textEdits => {
        let edits = new vscode.WorkspaceEdit();
        let parsed = vscode.Uri.parse(filePath.uri);
        edits.set(parsed, textEdits);
        vscode.workspace.applyEdit(edits);
    });
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
    let args: string[];

    const daArgs = ["run", "damlc", "--", "lax", "ide"];

    try {
        command = which.sync("daml");
        args = ["ide"];
    } catch (ex) {
        try {
            command = which.sync("da");
            args = daArgs;
        } catch (ex) {
            const damlCmdPath = path.join(damlRoot, "bin", "daml");
            if (fs.existsSync(damlCmdPath)) {
                command = damlCmdPath;
                args = ["ide"];
            } else if (fs.existsSync(daCmdPath)) {
                command = daCmdPath;
                args = daArgs;
            } else {
                vscode.window.showErrorMessage("Failed to start the DAML language server. Make sure the assistant is installed.");
                throw new Error("Failed to locate assistant.");
            }
        }
    }

    if (telemetryConsent === true){
        args.push('--telemetry');
    } else if (telemetryConsent === false){
        args.push('--optOutTelemetry')
    }
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
// DA.Service.Daml.LanguageServer.
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

namespace DamlUpgradeRequest {
    export let type =
      new RequestType<{uri: TextDocumentIdentifier}, vscode.TextEdit[], string, void>('daml/upgrade');
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

interface WorkspaceValidationsParams {
    /** Tracks the number of validations we have already finished. */
    finishedValidations: number;
    /** Tracks the number of total validation steps we need to perform. */
    totalValidations: number;
}

namespace DamlWorkspaceValidationsNotification {
    export let type =
      new NotificationType<WorkspaceValidationsParams, void>(
        'daml/workspace/validations'
      );
}

class VirtualResourceManager {
    // Mapping from URIs to the web view panel
    private _panels: Map<string, vscode.WebviewPanel> = new Map<string, vscode.WebviewPanel>();
    // Mapping from URIs to the HTML content of the webview
    private _panelContents: Map<string, string> = new Map<string, string>();
    // Mapping from URIs to selected view
    private _panelStates: Map<string, string> = new Map<string, string>();
    private _client: LanguageClient;
    private _disposables: vscode.Disposable[] = [];
    private _webviewSrc : Uri;

    constructor(client: LanguageClient, webviewSrc: Uri) {
        this._client = client;
        this._webviewSrc = webviewSrc;
    }

    private open(uri: string) {
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

    private close(uri: string) {
        this._client.sendNotification(
            'textDocument/didClose', {
                textDocument: {uri: uri}
            }
        );
    }

    public createOrShow(title: string, uri: string) {
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
            () => {this.close(uri);},
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

    public setContent(uri: string, contents: string) {
        contents = contents.replace('$webviewSrc', this._webviewSrc.toString());
        this._panelContents.set(uri, contents);
        const panel = this._panels.get(uri);
        if (panel) {
            panel.webview.html = contents;
            const panelState = this._panelStates.get(uri);
            if (panelState) {
                panel.webview.postMessage({command: 'select_view', value: panelState});
            };
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

// StatusBarItem

/**
 * The text to display on the workspace validations StatusBarItem.
 */
function statusBarText(finishedValidations: number, totalValidations: number) {
    return `DAML files checked: ${finishedValidations} / ${totalValidations}`;
}

/**
 * Adds a StatusBarItem to the left of the StatusBar and attaches itself to the right event to track
 * updates.
 */
function setupWorkspaceValidationStatusBarItem(client: LanguageClient) {
    let statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 0);
    statusBarItem.text = statusBarText(0, 0);
    statusBarItem.show()
    var updateTimer: NodeJS.Timer;

    client.onNotification(DamlWorkspaceValidationsNotification.type, (params) => {
        let setStatusBarText =
          () => statusBarItem.text = statusBarText(params.finishedValidations, params.totalValidations);

        clearTimeout(updateTimer);
        if (params.finishedValidations == params.totalValidations) {
          setStatusBarText();
        } else {
          // To avoid annoying flicker due to periodic re-validations only update
          // every 300ms
          updateTimer = setTimeout(setStatusBarText, 300);
        }
    });
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
