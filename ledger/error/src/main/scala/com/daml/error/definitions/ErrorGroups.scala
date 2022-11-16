// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error.{ErrorClass, ErrorGroup, Explanation}

object ErrorGroups {
  val rootErrorClass: ErrorClass = ErrorClass.root()

  object ParticipantErrorGroup extends ErrorGroup()(rootErrorClass) {

    @Explanation("Common errors raised in Daml services and components.")
    object CommonErrors extends ErrorGroup()

    @Explanation("Errors raised by the Participant Index persistence layer.")
    object IndexErrors extends ErrorGroup() {
      object DatabaseErrors extends ErrorGroup()
    }

    @Explanation("Errors raised by or forwarded by the Ledger API.")
    object LedgerApiErrors extends ErrorGroup() {

      @Explanation("Errors raised by Ledger API admin services.")
      object AdminServices extends ErrorGroup() {
        object UserManagementServiceErrorGroup extends ErrorGroup()
        object PartyManagementServiceErrorGroup extends ErrorGroup()
      }

      @Explanation("Authentication and authorization errors.")
      object AuthorizationChecks extends ErrorGroup()

      @Explanation(
        "Errors raised during the command execution phase of the command submission evaluation."
      )
      object CommandExecution extends ErrorGroup()

      @Explanation(
        "Potential consistency errors raised due to race conditions during command submission or returned as submission rejections by the backing ledger."
      )
      object ConsistencyErrors extends ErrorGroup()

      @Explanation("Errors raised by the Package Management Service on package uploads.")
      object PackageServiceError extends ErrorGroup()

      @Explanation("Validation errors raised when evaluating requests in the Ledger API.")
      object RequestValidation extends ErrorGroup()

      @Explanation(
        "Generic submission rejection errors returned by the backing ledger's write service."
      )
      object WriteServiceRejections extends ErrorGroup()
    }
  }
}
