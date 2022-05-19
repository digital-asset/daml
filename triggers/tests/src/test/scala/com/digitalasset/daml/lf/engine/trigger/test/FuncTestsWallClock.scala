// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import com.daml.ledger.runner.common.Config.SandboxParticipantId
import com.daml.platform.services.time.TimeProviderType

final class FuncTestsWallClock extends AbstractFuncTests {

  override def newConfig = super.newConfig.copy(
    genericConfig = super.newConfig.genericConfig.copy(participants =
      Map(
        SandboxParticipantId -> super.newConfig.genericConfig
          .participants(SandboxParticipantId)
          .copy(
            apiServer = super.newConfig.genericConfig
              .participants(SandboxParticipantId)
              .apiServer
              .copy(
                timeProviderType = TimeProviderType.WallClock
              )
          )
      )
    )
  )
}
