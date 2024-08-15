// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use strict";

import * as vscode from "vscode";
import * as path from "path";
import * as fs from "fs";
import * as os from "os";
import {
  LanguageClient,
  LanguageClientOptions,
  RequestType,
} from "vscode-languageclient/node";
import * as which from "which";
import {
  DamlSdkInstallProgress,
  SdkInstallState,
  handleDamlSdkInstallProgress,
} from "./sdk_install";
import {
  VirtualResourceManager,
  DamlVirtualResourceDidChangeNotification,
  DamlVirtualResourceNoteNotification,
  DamlVirtualResourceDidProgressNotification,
  WebviewFiles,
} from "./virtual_resource_manager";

namespace DamlKeepAliveRequest {
  export let type = new RequestType<void, void, void>("daml/keepAlive");
}

let damlRoot: string = path.join(os.homedir(), ".daml");

export class DamlLanguageClient {
  languageClient: LanguageClient;
  virtualResourceManager: VirtualResourceManager;
  context: vscode.ExtensionContext;
  webviewFiles: WebviewFiles;
  isMultiIde: boolean = false;

  // Keep alive timer for periodically checking that the server is responding
  // to requests in a timely manner. If the server fails to respond it is
  // terminated with SIGTERM.
  private keepAliveTimer: NodeJS.Timer | null = null;
  private keepAliveInterval = 60000; // Send KA every 60s.

  // Wait for max 120s before restarting process.
  // NOTE(JM): If you change this, make sure to also change the server-side timeouts to get
  // detailed errors rather than cause a restart.
  // Legacy Daml timeout for language server is defined in
  // DA.Daml.LanguageServer.
  private keepAliveTimeout = 120000;

  constructor(
    rootPath: string,
    envVars: { [envVarName: string]: string },
    config: vscode.WorkspaceConfiguration,
    telemetryConsent: boolean | undefined,
    identifier: string,
    _context: vscode.ExtensionContext,
    _webviewFiles: WebviewFiles,
  ) {
    this.context = _context;
    this.webviewFiles = _webviewFiles;

    this.languageClient = this.createLanguageClient(
      rootPath,
      envVars,
      config,
      telemetryConsent,
      identifier,
    );
    this.languageClient.registerProposedFeatures();

    this.virtualResourceManager = new VirtualResourceManager(
      this.languageClient,
      this.webviewFiles,
      this.context,
    );
    this.context.subscriptions.push(this.virtualResourceManager);

    let _unused = this.languageClient.onReady().then(() => {
      this.startKeepAliveWatchdog();
      this.languageClient.onNotification(
        DamlVirtualResourceDidChangeNotification.type,
        params =>
          this.virtualResourceManager.setContent(params.uri, params.contents),
      );
      this.languageClient.onNotification(
        DamlVirtualResourceNoteNotification.type,
        params => this.virtualResourceManager.setNote(params.uri, params.note),
      );
      this.languageClient.onNotification(
        DamlVirtualResourceDidProgressNotification.type,
        params =>
          this.virtualResourceManager.setProgress(
            params.uri,
            params.millisecondsPassed,
            params.startedAt,
          ),
      );
      let sdkInstallState: SdkInstallState = {};
      this.languageClient.onNotification(DamlSdkInstallProgress.type, params =>
        handleDamlSdkInstallProgress(
          sdkInstallState,
          params,
          this.languageClient,
        ),
      );
    });

    this.languageClient.start();
  }

  async stop() {
    // Stop the Language server
    this.stopKeepAliveWatchdog();
    await this.languageClient.stop();
    this.virtualResourceManager.dispose();
    const index = this.context.subscriptions.indexOf(
      this.virtualResourceManager,
      0,
    );
    if (index > -1) {
      this.context.subscriptions.splice(index, 1);
    }
  }

  async forceStop() {
    this.stopKeepAliveWatchdog();
    if (this.isMultiIde) await this.languageClient.stop();
    else (<any>this.languageClient)._serverProcess.kill("SIGTERM");
  }

  private addIfInConfig(
    config: vscode.WorkspaceConfiguration,
    baseArgs: string[],
    toAdd: [string, string[]][],
  ): string[] {
    let addedArgs: string[][] = toAdd
      .filter(x => config.get(x[0]))
      .map(x => x[1]);
    addedArgs.unshift(baseArgs);
    return [].concat.apply([], <any>addedArgs);
  }

  private getLanguageServerArgs(
    config: vscode.WorkspaceConfiguration,
    telemetryConsent: boolean | undefined,
    identifier: string,
  ): string[] {
    const multiIDESupport = config.get("multiPackageIdeSupport");
    this.isMultiIde = !!multiIDESupport;
    const logLevel = config.get("logLevel");
    const isDebug = logLevel == "Debug" || logLevel == "Telemetry";

    let args: string[] = [multiIDESupport ? "multi-ide" : "ide", "--"];

    if (telemetryConsent === true) {
      args.push("--telemetry");
    } else if (telemetryConsent === false) {
      args.push("--optOutTelemetry");
    } else if (telemetryConsent == undefined) {
      // The user has not made an explicit choice.
      args.push("--telemetry-ignored");
    }
    if (multiIDESupport === true) {
      args.push("--log-level=" + logLevel);
      args.push("--ide-identifier=" + identifier);
    } else {
      if (isDebug) args.push("--debug");
    }
    const extraArgsString = config.get("extraArguments", "").trim();
    // split on an empty string returns an array with a single empty string
    const extraArgs = extraArgsString === "" ? [] : extraArgsString.split(" ");
    args = args.concat(extraArgs);
    const serverArgs: string[] = this.addIfInConfig(config, args, [
      ["profile", ["+RTS", "-h", "-RTS"]],
      ["autorunAllTests", ["--studio-auto-run-all-scenarios=yes"]],
    ]);

    return serverArgs;
  }

  // Update the manual watchers in haskell side to be aware of the root directory, and only ask for that

  private createLanguageClient(
    rootPath: string,
    envVars: { [envVarName: string]: string },
    config: vscode.WorkspaceConfiguration,
    telemetryConsent: boolean | undefined,
    identifier: string,
  ): LanguageClient {
    // Options to control the language client
    let clientOptions: LanguageClientOptions = {
      // Register the server for Daml
      documentSelector: [
        { language: "daml", pattern: rootPath + "/**/*.daml" },
      ],
    };

    let command: string;

    try {
      command = which.sync("daml");
    } catch (ex) {
      const damlCmdPath = path.join(damlRoot, "bin", "daml");
      if (fs.existsSync(damlCmdPath)) {
        command = damlCmdPath;
      } else {
        vscode.window.showErrorMessage(
          "Failed to start the Daml language server. Make sure the assistant is installed.",
        );
        throw new Error("Failed to locate assistant.");
      }
    }

    const serverArgs = this.getLanguageServerArgs(
      config,
      telemetryConsent,
      identifier,
    );

    const languageClient = new LanguageClient(
      "daml-language-server-" + identifier,
      "Daml Language Server " + identifier,
      {
        args: serverArgs,
        command: command,
        options: { cwd: rootPath, env: envVars, shell: true },
      },
      clientOptions,
      true,
    );
    return languageClient;
  }

  private startKeepAliveWatchdog() {
    this.stopKeepAliveWatchdog();
    this.keepAliveTimer = setTimeout(
      () => this.keepAlive(this.languageClient),
      this.keepAliveInterval,
    );
  }

  private stopKeepAliveWatchdog() {
    if (this.keepAliveTimer) clearTimeout(this.keepAliveTimer);
  }

  private keepAlive(languageClient: LanguageClient) {
    let self = this;
    function killDamlc() {
      vscode.window.showErrorMessage(
        "Sorry, you’ve hit a bug requiring a Daml Language Server restart. We’d greatly appreciate a bug report — ideally with example files.",
      );

      // Terminate the damlc process with SIGTERM. The language client will restart the process automatically.
      // NOTE(JM): Verify that this works on Windows.
      // https://nodejs.org/api/child_process.html#child_process_child_kill_signal
      (<any>languageClient)._childProcess.kill("SIGTERM");

      // Restart the watchdog after 10s
      setTimeout(self.startKeepAliveWatchdog, 10000);
    }

    let killTimer = setTimeout(killDamlc, this.keepAliveTimeout);
    languageClient.sendRequest(DamlKeepAliveRequest.type, null).then(r => {
      // Keep-alive request succeeded, clear the kill timer
      // and reschedule the keep-alive.
      clearTimeout(killTimer);
      this.startKeepAliveWatchdog();
    });
  }
}
