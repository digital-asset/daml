// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ConfigFunctions } from "../index";
import { loadExportedFunction } from "../utils";
import { ConfigFile } from "./v1";

// ----------------------------------------------------------------------------
// Loading and validating
// ----------------------------------------------------------------------------

const migrationAdvice = `
You are using an old version of the frontend config.
To migrate to version 2.0, do the following changes:
- In template IDs, replace dots by colons
  (e.g., 'Iou.Iou' -> 'Iou:Iou')
- When reading contract or choice arguments, use 'DamlLfValue.toJSON' or 'DamlLfValue.evalPath'
  (e.g., 'argument.foo.bar' -> 'DamlLfValue.toJSON(argument).foo.bar')
  (e.g., 'argument.foo.bar' -> 'DamlLfValue.evalPath(argument, ["foo", "bar"])
- Bump the version of your config file to 2.0
  (e.g., export const version = {schema: 'navigator-config', major: 2, minor: 0 };)
`.trim();

export function load(
  exports: Partial<ConfigFile>,
  _major: number,
  _minor: number,
): ConfigFunctions {
  // TODO: Improve error handling.
  // Config errors and warnings should display as a dismissable error message in the UI.
  alert(
    "Your frontend-config.js is out of date. See the browser console for more info.",
  );
  console.warn(migrationAdvice);
  return {
    theme: loadExportedFunction(
      exports,
      "theme",
      ["userId", "party", "role"],
      (_userId: string, _party: string, _role: string) => ({}),
    ),
    customViews: loadExportedFunction(
      exports,
      "customViews",
      ["userId", "party", "role"],
      (_userId: string, _party: string, _role: string) => ({}),
    ),
  };
}
