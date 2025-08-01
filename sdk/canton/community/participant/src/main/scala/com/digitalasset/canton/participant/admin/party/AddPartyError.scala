// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PartyManagementServiceErrorGroup
import com.digitalasset.canton.error.{CantonBaseError, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.topology.SynchronizerId

sealed trait AddPartyError extends CantonBaseError {
  def requestId: AddPartyRequestId
}

object AddPartyError extends PartyManagementServiceErrorGroup {

  @Explanation(
    """|This error indicates that adding a party with active contracts to a participant and a synchronizer
       |has been interrupted by a participant having disconnected from a synchronizer."""
  )
  @Resolution("Reconnect the participant to the synchronizer to unblock adding the party.")
  object DisconnectedFromSynchronizer
      extends ErrorCode(
        id = "ADD_PARTY_DISCONNECTED_FROM_SYNCHRONIZER",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(
        requestId: AddPartyRequestId,
        synchronizerId: SynchronizerId,
        reason: String,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = reason)
        with AddPartyError
  }

  // TODO(#22136): Complete error categorization and remove "Other" error code.
  @Explanation(
    """|This error is currently used as a catch-all for a variety of failures that prevent adding a
       |party to a participant and synchronizer."""
  )
  @Resolution(
    "Inspect the message, potentially rebuild the target participant, and retry adding the party."
  )
  object Other
      extends ErrorCode(
        id = "ADD_PARTY_OTHER",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(requestId: AddPartyRequestId, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Party replication $requestId: $reason")
        with AddPartyError
  }
}
