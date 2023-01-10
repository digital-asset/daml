// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error.{ErrorClass, ErrorGroup}

object ErrorGroups {
  val rootErrorClass: ErrorClass = ErrorClass.root()

  object ParticipantErrorGroup extends ErrorGroup()(rootErrorClass) {
    abstract class CommonErrorGroup extends ErrorGroup()
    abstract class IndexErrorGroup extends ErrorGroup() {
      abstract class DatabaseErrorGroup extends ErrorGroup()
    }
    abstract class LedgerApiErrorGroup extends ErrorGroup() {
      abstract class AdminServicesErrorGroup extends ErrorGroup() {
        abstract class UserManagementServiceErrorGroup extends ErrorGroup()
        abstract class PartyManagementServiceErrorGroup extends ErrorGroup()
        abstract class IdentityProviderConfigServiceErrorGroup extends ErrorGroup()
      }
      abstract class AuthorizationChecks extends ErrorGroup()
      abstract class CommandExecutionErrorGroup extends ErrorGroup()
      abstract class ConsistencyErrors extends ErrorGroup()
      abstract class PackageServiceErrorGroup extends ErrorGroup()
      abstract class RequestValidation extends ErrorGroup()
      abstract class WriteServiceRejections extends ErrorGroup()
    }
  }
}
