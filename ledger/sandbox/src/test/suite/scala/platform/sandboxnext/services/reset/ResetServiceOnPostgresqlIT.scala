// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext.services.reset

import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.services.reset.ResetServiceITBase
import com.daml.platform.sandboxnext.SandboxNextFixture

final class ResetServiceOnPostgresqlIT
    extends ResetServiceITBase
    with SandboxNextFixture
    with SandboxBackend.Postgresql {
  override def spanScaleFactor: Double = super.spanScaleFactor * 8
}
