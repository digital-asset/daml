// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext.services.reset

import com.digitalasset.platform.sandbox.SandboxBackend
import com.digitalasset.platform.sandbox.services.reset.ResetServiceITBase
import com.digitalasset.platform.sandboxnext.SandboxNextFixture
import org.scalatest.Ignore

@Ignore
final class ResetServiceOnPostgresqlIT
    extends ResetServiceITBase
    with SandboxNextFixture
    with SandboxBackend.Postgresql {
  override def spanScaleFactor: Double = super.spanScaleFactor * 4
}
