// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.LedgerSubmissionId
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.InjectionErrorGroup
import com.digitalasset.canton.error.*
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.DuplicateCommand
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidDeduplicationPeriodField
import com.digitalasset.canton.ledger.participant.state.ChangeId

object CommandDeduplicationError extends InjectionErrorGroup {

  @Explanation(
    """The specified deduplication offset is syntactically malformed."""
  )
  @Resolution(
    """Use a deduplication offset that was produced by this participant node."""
  )
  // TODO(#5990) Unify with upstream NonHexOffset and OffsetOutOfRange
  object MalformedDeduplicationOffset
      extends ErrorCode(
        id = "MALFORMED_DEDUPLICATION_OFFSET",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(error: String)
        extends TransactionErrorImpl(
          cause = error,
          // This error is generated only after in-flight submission checking and therefore reported asynchronously,
          // with appropriate submission rank checks
          definiteAnswer = true,
        )
  }

  final case class DuplicateCommandReject(
      changeId: ChangeId,
      // use the same field name as defined in com.daml.error.GrpcStatuses.CompletionOffsetKey
      // use a Hexstring instead of LedgerSyncOffset so that we don't get the pretty printer in the way
      completion_offset: String,
      accepted: Boolean,
      existingSubmissionId: Option[LedgerSubmissionId],
  ) extends TransactionErrorImpl(
        "Command submission already exists.",
        // This error is generated only after in-flight submission checking and therefore reported asynchronously,
        // with appropriate submission rank checks
        definiteAnswer = true,
      )(DuplicateCommand.code)

  // TODO(#7348) add error for submission rank conflicts

  final case class DeduplicationPeriodStartsTooEarlyErrorWithOffset(
      changeId: ChangeId,
      requestedPeriod: DeduplicationPeriod,
      // machine readable field for the earliest supported offset;
      // must be the same as com.digitalasset.canton.ledger.error.LedgerApiErrors.EarliestOffsetMetadataKey
      earliest_offset: String,
  ) extends TransactionErrorImpl(
        "Deduplication period starts too early. The error metadata field earliest_offset contains the earliest deduplication offset currently allowed.",
        // This error is generated only after in-flight submission checking and therefore reported asynchronously,
        // with appropriate submission rank checks
        definiteAnswer = true,
      )(InvalidDeduplicationPeriodField.code)

  final case class DeduplicationPeriodStartsTooEarlyErrorWithDuration(
      changeId: ChangeId,
      requestedPeriod: DeduplicationPeriod,
      longest_duration: String, // machine readable field for the longest supported deduplication duration
  ) extends TransactionErrorImpl(
        "Deduplication period starts too early. The error metadata field longest_duation contains the longest deduplication duration currently allowed.",
        // This error is generated only after in-flight submission checking and therefore reported asynchronously,
        // with appropriate submission rank checks
        definiteAnswer = true,
      )(InvalidDeduplicationPeriodField.code)

}
