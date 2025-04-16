// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PartyManagementServiceErrorGroup
import com.digitalasset.canton.error.{CantonBaseError, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.SynchronizerId

sealed trait PartyManagementServiceError extends Product with Serializable with CantonBaseError

object PartyManagementServiceError extends PartyManagementServiceErrorGroup {

  object InternalError
      extends ErrorCode(
        id = "INTERNAL_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  object InvalidArgument
      extends ErrorCode(
        id = "INVALID_ARGUMENT_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  object IOStream
      extends ErrorCode(
        id = "IO_STREAM_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  @Explanation(
    """The participant does not support serving an ACS snapshot at the requested timestamp. Likely causes:
      |1) Ledger state processing has not yet caught up with the topology state.
      |2) Requested timestamp is not the effective time of a topology transaction."""
  )
  @Resolution(
    """Ensure the specified timestamp is the effective time of a topology transaction that has been sequenced
      |on the specified synchronizer. If so, retry after a bit (possibly repeatedly)."""
  )
  object InvalidAcsSnapshotTimestamp
      extends ErrorCode(
        id = "INVALID_ACS_SNAPSHOT_TIMESTAMP_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateSeekAfterEnd,
      ) {
    final case class Error(
        requestedTimestamp: CantonTimestamp,
        synchronizerId: SynchronizerId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not yet support serving an ACS snapshot at the requested timestamp $requestedTimestamp on synchronizer $synchronizerId"
        )
        with PartyManagementServiceError
  }

}
