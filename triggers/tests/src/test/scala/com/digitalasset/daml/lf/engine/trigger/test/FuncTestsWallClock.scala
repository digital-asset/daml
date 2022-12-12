// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.platform.services.time.TimeProviderType

final class FuncTestsWallClock extends AbstractFuncTests {

  override def config = super.config.copy(participants =
    singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = TimeProviderType.WallClock
      )
    )
  )
}
