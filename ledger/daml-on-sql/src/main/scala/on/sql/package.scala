// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on

import com.daml.ledger.participant.state.v1
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import scalaz.syntax.tag._

package object sql {

  private[sql] val Name = LedgerName("DAML-on-SQL")

  private[sql] val DefaultConfig = SandboxConfig.defaultConfig.copy(
    participantId = v1.ParticipantId.assertFromString(Name.unwrap.toLowerCase()),
    ledgerConfig = LedgerConfiguration.defaultLedgerBackedIndex,
    devMode = false,
    implicitPartyAllocation = false,
  )

}
