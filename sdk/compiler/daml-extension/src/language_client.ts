// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import * as semver from "semver";
import * as util from "util";
import * as child_process from "child_process";

namespace DamlKeepAliveRequest {
  export let type = new RequestType<void, void, void>("daml/keepAlive");
}

let damlRoot: string = path.join(os.homedir(), ".daml");

export interface EnvVars {
  [envVarName: string]: string | undefined;
}

class UnsupportedFeature extends Error {
  featureName: string;
  sdkVersion: string;

  constructor(_featureName: string, _sdkVersion: string) {
    super();
    this.featureName = _featureName;
    this.sdkVersion = _sdkVersion;
  }

  render(): string {
    return `Daml IDE feature ${this.featureName} is not supported in SDK Version ${this.sdkVersion}\nPlease update your daml SDK`;
  }
}

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

  static async build(
    rootPath: string,
    envVars: EnvVars,
    config: vscode.WorkspaceConfiguration,
    telemetryConsent: boolean | undefined,
    _context: vscode.ExtensionContext,
    _webviewFiles: WebviewFiles,
    identifier: string | undefined = undefined,
  ): Promise<DamlLanguageClient | null> {
    try {
      const [languageClient, multiIdeSupport] =
        await DamlLanguageClient.createLanguageClient(
          rootPath,
          envVars,
          config,
          telemetryConsent,
          identifier,
        );
      return new DamlLanguageClient(
        languageClient,
        multiIdeSupport,
        _context,
        _webviewFiles,
      );
    } catch (err) {
      if (!(err instanceof UnsupportedFeature)) throw err;
      vscode.window.showErrorMessage(err.render());
      return null;
    }
  }

  constructor(
    languageClient: LanguageClient,
    multiIdeSupport: boolean,
    _context: vscode.ExtensionContext,
    _webviewFiles: WebviewFiles,
  ) {
    this.context = _context;
    this.webviewFiles = _webviewFiles;

    this.languageClient = languageClient;
    this.languageClient.registerProposedFeatures();
    this.isMultiIde = multiIdeSupport;

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
    // Regular IDE didn't correctly respond to `stop`, so we used SIGTERM
    // Multi-IDE needs to shutdown subprocesses, so needs to use `stop`
    if (this.isMultiIde) await this.languageClient.stop();
    else (<any>this.languageClient)._serverProcess.kill("SIGTERM");
  }

  private static addIfInConfig(
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

  private static async getSdkVersion(
    damlPath: string,
    projectPath: string | undefined,
  ): Promise<string | undefined> {
    // Ordered by priority
    const selectionStrings = [
      "selected by env var ",
      "project SDK version from daml.yaml",
      "default SDK version for new projects",
    ];
    const { stdout } = await util.promisify(child_process.exec)(
      damlPath + " version",
      { cwd: projectPath },
    );
    const lines = stdout.split("\n");
    for (const selection of selectionStrings) {
      const line = lines.find((line: string) => line.includes(selection));
      if (line) return line.trim().split(" ")[0];
    }
  }

  private static async getMultiIdeIdentifierSupport(
    damlPath: string,
    projectPath: string | undefined,
  ): Promise<boolean> {
    const { stdout } = await util.promisify(child_process.exec)(
      damlPath + " multi-ide --help",
      { cwd: projectPath },
    );
    return stdout.includes("--ide-identifier");
  }

  private static getMultiIdeSupport(
    config: vscode.WorkspaceConfiguration,
    sdkVersion: string | undefined,
  ): boolean {
    const multiIDESupport: boolean =
      config.get("multiPackageIdeSupport") || false;

    // We'll say multi-ide is introduced in 2.9.0. The command existed
    // in 2.8, but it was unfinished, so we shouldn't allow it to be used.
    if (
      multiIDESupport &&
      sdkVersion &&
      sdkVersion != "0.0.0" &&
      semver.lt(sdkVersion, "2.9.0")
    ) {
      vscode.window.showWarningMessage(
        `Selected Daml SDK version (${sdkVersion}) does not support Multi-IDE.\nMulti-IDE is disabled for this project.`,
      );
      return false;
    }
    return multiIDESupport;
  }

  private static getLanguageServerArgs(
    config: vscode.WorkspaceConfiguration,
    telemetryConsent: boolean | undefined,
    multiIdeSupport: boolean,
    identifier: string | undefined,
  ): string[] {
    const logLevel = config.get("logLevel");
    const isDebug = logLevel == "Debug" || logLevel == "Telemetry";

    let args: string[] = [multiIdeSupport ? "multi-ide" : "ide", "--"];

    if (telemetryConsent === true) {
      args.push("--telemetry");
    } else if (telemetryConsent === false) {
      args.push("--optOutTelemetry");
    } else if (telemetryConsent == undefined) {
      // The user has not made an explicit choice.
      args.push("--telemetry-ignored");
    }
    if (multiIdeSupport === true) {
      args.push("--log-level=" + logLevel);
      if (identifier) args.push("--ide-identifier=" + identifier);
    } else {
      if (isDebug) args.push("--debug");
    }
    const extraArgsString = config.get("extraArguments", "").trim();
    // split on an empty string returns an array with a single empty string
    const extraArgs = extraArgsString === "" ? [] : extraArgsString.split(" ");
    args = args.concat(extraArgs);
    const serverArgs: string[] = DamlLanguageClient.addIfInConfig(
      config,
      args,
      [
        ["profile", ["+RTS", "-h", "-RTS"]],
        // Use old flag `studio-auto-run-all-scenarios` until we're sure all supported SDKs have `studio-auto-run-all-scripts`
        ["autorunAllTests", ["--studio-auto-run-all-scenarios=yes"]],
      ],
    );

    return serverArgs;
  }

  static findDamlCommand(): string {
    try {
      return which.sync("daml");
    } catch (ex) {
      const damlCmdPath = path.join(damlRoot, "bin", "daml");
      if (fs.existsSync(damlCmdPath)) {
        return damlCmdPath;
      } else {
        vscode.window.showErrorMessage(
          "Failed to start the Daml language server. Make sure the assistant is installed.",
        );
        throw new Error("Failed to locate assistant.");
      }
    }
  }
  private static async createLanguageClient(
    rootPath: string,
    envVars: EnvVars,
    config: vscode.WorkspaceConfiguration,
    telemetryConsent: boolean | undefined,
    identifier: string | undefined,
  ): Promise<[LanguageClient, boolean]> {
    // Options to control the language client
    let clientOptions: LanguageClientOptions = {
      // Register the server for Daml
      documentSelector: [
        { language: "daml", pattern: rootPath + "/**/*.daml" },
      ],
    };

    const command = DamlLanguageClient.findDamlCommand();
    const sdkVersion = await DamlLanguageClient.getSdkVersion(
      command,
      rootPath,
    );
    const multiIdeSupport = DamlLanguageClient.getMultiIdeSupport(
      config,
      sdkVersion,
    );
    const identifierSupport =
      multiIdeSupport &&
      DamlLanguageClient.getMultiIdeIdentifierSupport(command, rootPath);
    if (!identifierSupport && identifier != undefined)
      throw new UnsupportedFeature("Gradle Support", sdkVersion || "unknown");
    const serverArgs = DamlLanguageClient.getLanguageServerArgs(
      config,
      telemetryConsent,
      multiIdeSupport,
      identifier,
    );

    const languageClient = new LanguageClient(
      "daml-language-server" + (identifier ? "-" + identifier : ""),
      "Daml Language Server" + (identifier ? " " + identifier : ""),
      {
        args: serverArgs,
        command: command,
        options: { cwd: rootPath, env: envVars, shell: true },
      },
      clientOptions,
      true,
    );
    return [languageClient, multiIdeSupport];
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
