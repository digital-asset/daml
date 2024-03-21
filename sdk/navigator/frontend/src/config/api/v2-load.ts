// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ConfigFunctions } from "../index";
import { loadExportedFunction } from "../utils";
import { ConfigFile } from "./v1";

// ----------------------------------------------------------------------------
// Loading and validating
// ----------------------------------------------------------------------------

export function load(
  exports: Partial<ConfigFile>,
  _major: number,
  _minor: number,
): ConfigFunctions {
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
