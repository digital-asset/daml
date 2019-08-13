// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import com.digitalasset.ledger.api.domain.PartyDetails

sealed abstract class PartyAllocationResult extends Product with Serializable {
  def description: String
}

object PartyAllocationResult {

  /** The party was successfully allocated */
  final case class Ok(result: PartyDetails) extends PartyAllocationResult {
    override def description: String = "Party successfully allocated"
  }

  /** Synchronous party allocation is not supported */
  final case object NotSupported extends PartyAllocationResult {
    override def description: String = "Party allocation not supported"
  }

  /** The requested party name already exists */
  final case object AlreadyExists extends PartyAllocationResult {
    override def description: String = "Party already exists"
  }

  /** The requested party name is not valid */
  final case class InvalidName(details: String) extends PartyAllocationResult {
    override def description: String = "Party name is invalid: " + details
  }

  /** The participant was not authorized to submit the allocation request */
  final case object ParticipantNotAuthorized extends PartyAllocationResult {
    override def description: String = "Participant is not authorized to allocate a party"
  }
}
