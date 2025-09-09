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

  object InvalidArgument
      extends ErrorCode(
        id = "INVALID_ARGUMENT_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  object InvalidState
      extends ErrorCode(
        id = "INVALID_STATE_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  object IOStream
      extends ErrorCode(
        id = "IO_STREAM_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  @Explanation(
    """The participant does not (yet) support serving a ledger offset at the requested timestamp. This may have happened
      |because the ledger state processing has not yet caught up."""
  )
  @Resolution(
    """Ensure the requested timestamp is valid. If so, retry after some time (possibly repeatedly)."""
  )
  object InvalidTimestamp
      extends ErrorCode(
        id = "INVALID_TIMESTAMP_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateSeekAfterEnd,
      ) {
    final case class Error(
        synchronizerId: SynchronizerId,
        requestedTimestamp: CantonTimestamp,
        force: Boolean,
        reason: String,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"No ledger offset found for the requested timestamp $requestedTimestamp on synchronizer $synchronizerId (using force flag = $force): $reason"
        )
        with PartyManagementServiceError
  }

}
