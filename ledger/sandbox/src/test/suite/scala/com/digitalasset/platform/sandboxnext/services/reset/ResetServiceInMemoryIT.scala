// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext.services.reset

import com.digitalasset.platform.sandbox.services.reset.ResetServiceITBase
import com.digitalasset.platform.sandboxnext.SandboxNextFixture

final class ResetServiceInMemoryIT extends ResetServiceITBase with SandboxNextFixture {
  override def spanScaleFactor: Double = super.spanScaleFactor * 2
}
