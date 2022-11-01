// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.error.definitions.groups

import com.daml.error.{ContextualizedErrorLogger, DamlErrorWithDefiniteAnswer, ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.error.definitions.{groups}

@Explanation("Errors raised by Ledger API admin services.")
object AdminServices extends LedgerApiErrors.AdminServicesErrorGroup {

  val UserManagement: UserManagementServiceErrorGroup.type =
    UserManagementServiceErrorGroup
  val PartyManagement: PartyManagementServiceErrorGroup.type =
    PartyManagementServiceErrorGroup

  @Explanation("This rejection is given when a new configuration is rejected.")
  @Resolution("Fetch newest configuration and/or retry.")
  object ConfigurationEntryRejected
      extends ErrorCode(
        id = "CONFIGURATION_ENTRY_REJECTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    case class Reject(_message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = _message
        )

  }

  @Explanation("This rejection is given when a package upload is rejected.")
  @Resolution("Refer to the detailed message of the received error.")
  object PackageUploadRejected
      extends ErrorCode(
        id = "PACKAGE_UPLOAD_REJECTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    case class Reject(_message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = _message
        )

  }

  @Explanation(
    "A cryptographic key used by the configured system is not valid"
  )
  @Resolution("Contact support.")
  object InternallyInvalidKey
      extends ErrorCode(
        id = "INTERNALLY_INVALID_KEY",
        ErrorCategory.SystemInternalAssumptionViolated, // Should have been caught by the participant
      ) {
    case class Reject(_message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = _message
        )
  }

}
