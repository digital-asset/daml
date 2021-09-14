// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import com.daml.error.{ErrorClass, ErrorGroup}

object ErrorGroups {

  private implicit val errorClass: ErrorClass = ErrorClass.root()

  abstract class ConfigErrorGroup extends ErrorGroup()

  abstract class CommandErrorGroup extends ErrorGroup()

  abstract class ProtoDeserializationErrorGroup extends ErrorGroup

  object ParticipantErrorGroup extends ErrorGroup() {
    abstract class SyncServiceErrorGroup extends ErrorGroup()
    abstract class PackageServiceErrorGroup extends ErrorGroup()
    abstract class PruningServiceErrorGroup extends ErrorGroup()
    object TransactionErrorGroup extends ErrorGroup() {
      // Errors emitted by Ledger Api server
      abstract class LedgerApiErrorGroup extends ErrorGroup()
      // TransactionInjectErrors are initial injection errors resulting from the canton sync service
      abstract class InjectionErrorGroup extends ErrorGroup()
      // TransactionRoutingErrors are routing errors resulting from the domain router
      abstract class RoutingErrorGroup extends ErrorGroup()
      // TransactionSubmissionErrors are routing errors resulting from the transaction processor
      abstract class SubmissionErrorGroup extends ErrorGroup()
    }
  }

  abstract class ClockErrorGroup extends ErrorGroup() {}

}
