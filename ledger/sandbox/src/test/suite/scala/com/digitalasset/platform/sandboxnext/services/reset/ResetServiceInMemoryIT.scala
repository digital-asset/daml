// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext.services.reset

import com.digitalasset.platform.sandbox.services.reset.ResetServiceITBase
import com.digitalasset.platform.sandboxnext.SandboxNextFixture
import org.scalatest.Ignore

@Ignore
final class ResetServiceInMemoryIT extends ResetServiceITBase with SandboxNextFixture {
  override def spanScaleFactor: Double = super.spanScaleFactor * 2
}
