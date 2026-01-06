// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use strict";

import * as vscode from "vscode";
import { LanguageClient, NotificationType } from "vscode-languageclient/node";

interface DamlSdkInstallProgressNotification {
  sdkVersionIdentifier: string; // Unique indentifier with overrides hash i.e. 3.4.0-with-overrides-495374952876
  sdkVersionRendered: string; // Human readable version string i.e. 3.4.0 (with 2 overrides)
  kind: "begin" | "report" | "end";
  progress: number;
}

export namespace DamlSdkInstallProgress {
  export let type = new NotificationType<DamlSdkInstallProgressNotification>(
    "daml/sdkInstallProgress",
  );
}

interface DamlSdkInstallCancelNotification {
  sdkVersionIdentifier: string;
}

namespace DamlSdkInstallCancel {
  export let type = new NotificationType<DamlSdkInstallCancelNotification>(
    "daml/sdkInstallCancel",
  );
}

type Progress = vscode.Progress<{ increment: number }>;
export type SdkInstallState = {
  [sdkVersion: string]: {
    progress: Progress;
    resolve: (_: void) => void;
    reported: number;
  };
};

// Handle the SdkInstall work done tokens separately, as we want them to popup as a notification, but VSCode LSPClient doesn't give us a way to do this
export function handleDamlSdkInstallProgress(
  sdkInstallState: SdkInstallState,
  message: DamlSdkInstallProgressNotification,
  damlLanguageClient: LanguageClient,
): void {
  switch (message.kind) {
    case "begin":
      vscode.window.withProgress<void>(
        {
          location: vscode.ProgressLocation.Notification,
          cancellable: true,
          title: "Installing Daml SDK " + message.sdkVersionRendered,
        },
        async (
          progress: Progress,
          cancellationToken: vscode.CancellationToken,
        ) => {
          cancellationToken.onCancellationRequested(() => {
            delete sdkInstallState[message.sdkVersionIdentifier];
            damlLanguageClient.sendNotification(DamlSdkInstallCancel.type, {
              sdkVersionIdentifier: message.sdkVersionIdentifier,
            });
          });
          return new Promise<void>((resolve, _) => {
            sdkInstallState[message.sdkVersionIdentifier] = {
              progress,
              resolve,
              reported: 0,
            };
          });
        },
      );
      break;
    case "report":
      let progressData = sdkInstallState[message.sdkVersionIdentifier];
      if (!progressData) return;
      let diff = Math.max(0, message.progress - progressData.reported);
      progressData.progress.report({ increment: diff });
      progressData.reported += diff;
      break;
    case "end":
      sdkInstallState[message.sdkVersionIdentifier]?.resolve();
      delete sdkInstallState[message.sdkVersionIdentifier];
      break;
  }
}
