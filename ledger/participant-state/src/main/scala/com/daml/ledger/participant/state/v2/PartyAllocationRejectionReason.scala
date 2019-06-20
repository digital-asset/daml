// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

sealed abstract class PartyAllocationRejectionReason extends Product with Serializable {
  def description: String
}

object PartyAllocationRejectionReason {

  /** The requested party name already exists */
  final case object AlreadyExists extends PartyAllocationRejectionReason {
    override def description: String = "Party already exists"
  }

  /** The requested party name is not valid */
  final case object InvalidName extends PartyAllocationRejectionReason {
    override def description: String = "Party name is invalid"
  }

  /** The participant was not authorized to submit the allocation request */
  final case object ParticipantNotAuthorized extends PartyAllocationRejectionReason {
    override def description: String = "Participant is not authorized to allocate a party"
  }
}
