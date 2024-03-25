// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error

import com.daml.error.{ErrorClass, ErrorGroup}

object ParticipantErrorGroup extends ErrorGroup()(ErrorClass.root()) {
  abstract class CommonErrorGroup extends ErrorGroup()

  abstract class IndexErrorGroup extends ErrorGroup() {
    abstract class DatabaseErrorGroup extends ErrorGroup()
  }

  abstract class LedgerApiErrorGroup extends ErrorGroup()
  object LedgerApiErrorGroup extends LedgerApiErrorGroup {
    abstract class AdminServicesErrorGroup extends ErrorGroup()
    object AdminServicesErrorGroup extends AdminServicesErrorGroup {
      abstract class UserManagementServiceErrorGroup extends ErrorGroup()

      abstract class PartyManagementServiceErrorGroup extends ErrorGroup()

      abstract class IdentityProviderConfigServiceErrorGroup extends ErrorGroup()
    }

    abstract class AuthorizationChecksErrorGroup extends ErrorGroup()

    abstract class CommandExecutionErrorGroup extends ErrorGroup()

    abstract class ConsistencyErrorGroup extends ErrorGroup()

    abstract class PackageServiceErrorGroup extends ErrorGroup()

    abstract class RequestValidationErrorGroup extends ErrorGroup()

    abstract class WriteServiceRejectionErrorGroup extends ErrorGroup()
  }
}
