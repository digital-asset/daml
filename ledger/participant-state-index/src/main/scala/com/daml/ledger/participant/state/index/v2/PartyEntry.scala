// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

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
