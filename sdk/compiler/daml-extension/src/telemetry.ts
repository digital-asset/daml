// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use strict";

import * as vscode from "vscode";
import {
  window,
  QuickPickOptions,
  ExtensionContext,
  WorkspaceConfiguration,
} from "vscode";

let telemetryOverride = {
  enable: "Enable",
  disable: "Disable",
  fromConsent: "From consent popup",
};
const options = {
  yes: "Yes, I would like to help improve Daml!",
  no: "No, I'd rather not.",
  read: "I'd like to read the privacy policy first.",
};

function ifYouChangeYourMind() {
  window.showInformationMessage(
    'If you change your mind, data sharing preferences can be found in settings under "daml.telemetry"',
  );
}

const telemetryConsentKey = "telemetry-consent";
const privacyPolicy = "https://www.digitalasset.com/privacy-policy";

function setConsentState(ex: ExtensionContext, val: undefined | boolean) {
  ex.globalState.update(telemetryConsentKey, val);
}

export function resetTelemetryConsent(ex: ExtensionContext) {
  return function () {
    setConsentState(ex, undefined);
  };
}

function handleResult(
  ex: ExtensionContext,
  res: string | undefined,
): boolean | undefined {
  if (typeof res === "undefined") {
    return undefined;
  } else
    switch (res) {
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
      default:
        throw "Unrecognised telemetry option";
    }
}

async function telemetryPopUp(): Promise<string | undefined> {
  let qpo: QuickPickOptions = {
    placeHolder: "Do you want to allow the collection of usage data",
  };
  return window.showQuickPick([options.yes, options.read, options.no], qpo);
}

export async function getTelemetryConsent(
  config: WorkspaceConfiguration,
  ex: ExtensionContext,
): Promise<boolean | undefined> {
  switch (config.get("telemetry") as string) {
    case telemetryOverride.enable:
      return true;
    case telemetryOverride.disable:
      return false;
    case telemetryOverride.fromConsent: {
      const consent = ex.globalState.get(telemetryConsentKey);
      if (typeof consent === "boolean") {
        return consent;
      }
      const res = await telemetryPopUp();
      // the user has closed the popup, ask again on next startup
      return handleResult(ex, res);
    }
    default:
      throw "Unexpected telemetry override option";
  }
}
