// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

sealed abstract class PartyEntry() extends Product with Serializable

object PartyEntry {
  final case class AllocationAccepted(
      submissionId: Option[String],
      partyDetails: IndexerPartyDetails,
  ) extends PartyEntry

  final case class AllocationRejected(
      submissionId: String,
      reason: String,
  ) extends PartyEntry
}
