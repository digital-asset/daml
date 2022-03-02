// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

sealed trait PartyAllocationConfiguration {
  def allocateParties: Boolean

  def waitForAllParticipants: Boolean
}

object PartyAllocationConfiguration {

  case object OpenWorld extends PartyAllocationConfiguration {
    override val allocateParties: Boolean = false

    override val waitForAllParticipants: Boolean = false
  }

  case object ClosedWorld extends PartyAllocationConfiguration {
    override val allocateParties: Boolean = true

    override val waitForAllParticipants: Boolean = false
  }

  case object ClosedWorldWaitingForAllParticipants extends PartyAllocationConfiguration {
    override val allocateParties: Boolean = true

    override val waitForAllParticipants: Boolean = true
  }

}
