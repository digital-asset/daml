// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.{LedgerApiErrors}
import com.daml.error.definitions.groups
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  Explanation,
  Resolution,
}

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

}
