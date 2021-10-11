// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on

import java.time.Duration

import com.daml.lf.data.Ref
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import scalaz.syntax.tag._

package object sql {

  private[sql] val Name = LedgerName("Daml-on-SQL")

  private[sql] val DefaultConfig = SandboxConfig.defaultConfig.copy(
    participantId = Ref.ParticipantId.assertFromString(Name.unwrap.toLowerCase()),
    delayBeforeSubmittingLedgerConfiguration = Duration.ZERO,
    implicitPartyAllocation = false,
  )

}
