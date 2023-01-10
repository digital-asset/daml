// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.{DamlErrorWithDefiniteAnswer, LedgerApiErrors}
import com.daml.error.definitions.groups
import com.daml.error.{ContextualizedErrorLogger, ErrorCategory, ErrorCode, Explanation, Resolution}

@Explanation("Errors raised by Ledger API admin services.")
object AdminServices extends LedgerApiErrors.AdminServicesErrorGroup {

  val UserManagement: groups.UserManagementServiceErrorGroup.type =
    groups.UserManagementServiceErrorGroup
  val IdentityProviderConfig: groups.IdentityProviderConfigServiceErrorGroup.type =
    groups.IdentityProviderConfigServiceErrorGroup
  val PartyManagement: groups.PartyManagementServiceErrorGroup.type =
    groups.PartyManagementServiceErrorGroup

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
