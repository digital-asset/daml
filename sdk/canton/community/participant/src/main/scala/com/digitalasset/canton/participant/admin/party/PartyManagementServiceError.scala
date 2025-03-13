// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.base.error.{ErrorCategory, ErrorCode}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PartyManagementServiceErrorGroup
import com.digitalasset.canton.error.{CantonBaseError, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext

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

}
