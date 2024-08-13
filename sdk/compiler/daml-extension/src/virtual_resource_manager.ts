// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use strict";

import * as vscode from "vscode";
import { LanguageClient, NotificationType } from "vscode-languageclient/node";
import { Uri, ViewColumn, ExtensionContext } from "vscode";
import { URLSearchParams } from "url";

export type WebviewFiles = {
  src: Uri; // The JavaScript file.
  css: Uri;
};

interface VirtualResourceChangedParams {
  /** The virtual resource uri */
  uri: string;

  /** The new contents of the virtual resource */
  contents: string;
}

export namespace DamlVirtualResourceDidChangeNotification {
  export let type = new NotificationType<VirtualResourceChangedParams>(
    "daml/virtualResource/didChange",
  );
}

interface VirtualResourceNoteParams {
  /** The virtual resource uri */
  uri: string;

  /** The note to set on the virtual resource */
  note: string;
}

export namespace DamlVirtualResourceNoteNotification {
  export let type = new NotificationType<VirtualResourceNoteParams>(
    "daml/virtualResource/note",
  );
}

interface VirtualResourceProgressedParams {
  /** The virtual resource uri */
  uri: string;

  /** Number of milliseconds passed since the resource started */
  millisecondsPassed: number;

  /** Unix timestamp where the resource started running */
  startedAt: number;
}

export namespace DamlVirtualResourceDidProgressNotification {
  export let type = new NotificationType<VirtualResourceProgressedParams>(
    "daml/virtualResource/didProgress",
  );
}

type UriString = string;
type ScenarioResult = string;
type View = {
  selected: string;
  showArchived: boolean;
  showDetailedDisclosure: boolean;
};

function getViewColumnForShowResource(): ViewColumn {
  const active = vscode.window.activeTextEditor;
  if (!active || !active.viewColumn) {
    return ViewColumn.One;
  }
  switch (active.viewColumn) {
    case ViewColumn.One:
      return ViewColumn.Two;
    case ViewColumn.Two:
      return ViewColumn.Three;
    default:
      return active.viewColumn;
  }
}

export function getVRFilePath(uri: UriString): string | null {
  let params = new URLSearchParams(Uri.parse(uri).query);
  return params.get("file");
}

export class VirtualResourceManager {
  // Note (MK): While it is tempting to switch to Map<Uri, …> for these types
  // Map uses reference equality for objects so this goes horribly wrong.
  // Mapping from URIs to the web view panel
  private _panels: Map<UriString, vscode.WebviewPanel> = new Map<
    UriString,
    vscode.WebviewPanel
  >();
  // Mapping from URIs to the HTML content of the webview
  private _panelContents: Map<UriString, ScenarioResult> = new Map<
    UriString,
    ScenarioResult
  >();
  // Mapping from URIs to selected view
  private _panelViews: Map<UriString, View> = new Map<UriString, View>();
  private _lastStatusUpdate: Map<UriString, number> = new Map<
    UriString,
    number
  >();
  private _client: LanguageClient;
  private _disposables: vscode.Disposable[] = [];
  private _webviewFiles: WebviewFiles;
  private _context: ExtensionContext;

  constructor(
    client: LanguageClient,
    webviewFiles: WebviewFiles,
    context: ExtensionContext,
  ) {
    this._client = client;
    this._webviewFiles = webviewFiles;
    this._context = context;
  }

  private open(uri: UriString) {
    this._client.sendNotification("textDocument/didOpen", {
      textDocument: {
        uri: uri,
        languageId: "",
        version: 0,
        text: "",
      },
    });
  }

  private close(uri: UriString) {
    this._client.sendNotification("textDocument/didClose", {
      textDocument: { uri: uri },
    });
  }

  public createOrShow(title: string, uri: UriString) {
    const column = getViewColumnForShowResource();

    let panel = this._panels.get(uri);
    if (panel) {
      panel.reveal(column);
      return;
    }
    this.open(uri);
    panel = vscode.window.createWebviewPanel("daml", title, column, {
      enableScripts: true,
      enableFindWidget: true,
      enableCommandUris: true,
    });
    panel.onDidDispose(
      () => {
        this._panels.delete(uri);
        this.close(uri);
      },
      null,
      this._disposables,
    );
    let defaultView: View = this._context.workspaceState.get(uri) || {
      selected: "table",
      showArchived: false,
      showDetailedDisclosure: false,
    };
    let updateView = (v: View, key: string, value: Object) => {
      let updatedView = { ...v, [key]: value };
      this._panelViews.set(uri, updatedView);
      this._context.workspaceState.update(uri, updatedView);
    };
    panel.webview.onDidReceiveMessage(message => {
      const v = this._panelViews.get(uri) || defaultView;
      switch (message.command) {
        case "set_selected_view":
          updateView(v, "selected", message.value);
          break;
        case "set_show_archived":
          updateView(v, "showArchived", message.value);
          break;
        case "set_show_detailed_disclosure":
          updateView(v, "showDetailedDisclosure", message.value);
          break;
      }
    });
    this._panels.set(uri, panel);
    panel.webview.html =
      this._panelContents.get(uri) || "Loading virtual resource...";
  }

  public setProgress(
    uri: UriString,
    millisecondsPassed: number,
    startedAt: number,
  ) {
    const panel = this._panels.get(uri);
    if (panel == undefined) return;
    const updateTimestamp = this._lastStatusUpdate.get(uri);
    const isOutOfDate = updateTimestamp != null && updateTimestamp > startedAt;
    if (isOutOfDate) return;
    this._lastStatusUpdate.set(uri, startedAt);
    panel.webview.html =
      `Virtual resource has been running for ${millisecondsPassed} ms ` +
      "<!-- " +
      new Date() +
      " -->";
  }

  public setContent(uri: UriString, contents: ScenarioResult) {
    let defaultView: View = this._context.workspaceState.get(uri) || {
      selected: "table",
      showArchived: false,
      showDetailedDisclosure: false,
    };
    const panel = this._panels.get(uri);
    if (panel) {
      contents = contents.replace(
        "$webviewSrc",
        panel.webview.asWebviewUri(this._webviewFiles.src).toString(),
      );
      contents = contents.replace(
        "$webviewCss",
        panel.webview.asWebviewUri(this._webviewFiles.css).toString(),
      );
      this._panelContents.set(uri, contents);
      this._lastStatusUpdate.set(uri, Date.now());
      // append timestamp to force page reload (prevent using cache) as otherwise notes are not getting cleared
      panel.webview.html = contents + "<!-- " + new Date() + " -->";
      const panelView = this._panelViews.get(uri);
      const actualDefault = contents.includes('class="table"')
        ? defaultView
        : { ...defaultView, selected: "transaction" };
      panel.webview.postMessage({
        command: "set_view",
        value: panelView || actualDefault,
      });
    }
  }

  public setNote(uri: UriString, note: string) {
    const panel = this._panels.get(uri);
    if (panel) {
      panel.webview.postMessage({ command: "add_note", value: note });
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
