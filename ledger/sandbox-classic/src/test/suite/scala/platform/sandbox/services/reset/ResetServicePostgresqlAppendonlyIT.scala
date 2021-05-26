// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.reset

import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.SandboxFixture

// TODO append-only: Remove this class once the mutating schema is removed
final class ResetServicePostgresqlAppendonlyIT
    extends ResetServiceDatabaseIT
    with SandboxFixture
    with SandboxBackend.Postgresql {
  override protected def config: SandboxConfig =
    super.config.copy(
      enableAppendOnlySchema = true
    )
}
