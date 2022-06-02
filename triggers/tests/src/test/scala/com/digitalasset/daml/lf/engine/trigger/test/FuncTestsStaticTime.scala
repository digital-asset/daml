// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import com.daml.ledger.sandbox.SandboxOnXForTest.SandboxParticipantId
import com.daml.platform.services.time.TimeProviderType

final class FuncTestsStaticTime extends AbstractFuncTests {

  override def config = super.config.copy(participants =
    Map(
      SandboxParticipantId -> super.config
        .participants(SandboxParticipantId)
        .copy(
          apiServer = super.config
            .participants(SandboxParticipantId)
            .apiServer
            .copy(
              timeProviderType = TimeProviderType.Static
            )
        )
    )
  )
}
