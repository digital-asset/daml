// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.appendonly

import com.daml.platform.sandbox.ScenarioLoadingITBase
import com.daml.platform.sandbox.config.SandboxConfig

// This file is identical to com.daml.platform.sandbox.ScenarioLoadingITNewIdentifier,
// except that it overrides config such that the append-only schema is used.
// TODO append-only: Remove this class once the mutating schema is removed
class ScenarioLoadingITNewIdentifier extends ScenarioLoadingITBase {
  override protected def config: SandboxConfig =
    super.config.copy(
      enableAppendOnlySchema = true
    )
}
