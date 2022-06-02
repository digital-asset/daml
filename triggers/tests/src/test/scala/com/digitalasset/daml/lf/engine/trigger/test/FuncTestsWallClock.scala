// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import com.daml.ledger.sandbox.SandboxOnXForTest.ParticipantId
import com.daml.platform.services.time.TimeProviderType

final class FuncTestsWallClock extends AbstractFuncTests {

  override def config = super.config.copy(participants =
    Map(
      ParticipantId -> super.config
        .participants(ParticipantId)
        .copy(
          apiServer = super.config
            .participants(ParticipantId)
            .apiServer
            .copy(
              timeProviderType = TimeProviderType.WallClock
            )
        )
    )
  )
}
