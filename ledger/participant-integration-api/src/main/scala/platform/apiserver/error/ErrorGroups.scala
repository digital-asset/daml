// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import com.daml.error.{ErrorClass, ErrorGroup}

object ErrorGroups {

  private implicit val errorClass: ErrorClass = ErrorClass.root()

  abstract class ProtoDeserializationErrorGroup extends ErrorGroup

  object ParticipantErrorGroup extends ErrorGroup() {
    abstract class PackageServiceErrorGroup extends ErrorGroup()
    abstract class PruningServiceErrorGroup extends ErrorGroup()
    object TransactionErrorGroup extends ErrorGroup() {
      // Errors emitted by Ledger Api server
      abstract class LedgerApiErrorGroup extends ErrorGroup()
      // TransactionSubmissionErrors are routing errors resulting from the transaction processor
      abstract class SubmissionErrorGroup extends ErrorGroup()
    }
  }
}
