// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

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
