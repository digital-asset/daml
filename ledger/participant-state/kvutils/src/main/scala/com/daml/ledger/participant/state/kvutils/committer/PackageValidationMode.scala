// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

/** Defines the different package validation modes. */
sealed abstract class PackageValidationMode extends Product with Serializable

object PackageValidationMode {

  /** Specifies that the committer should validate packages before
    * committing them to the ledger.
    * When using this mode, the packages committed to the ledger can
    * be fully trusted and do not have to be validated when loaded
    * into the engine.  */
  case object Strict extends PackageValidationMode

  /** Specifies that the committer should perform a fast validation of
    * the packages before committing them to the ledger.
    * This mode is useful for ledger integrations that cannot handle
    * long-running submissions (> 10s).
    * When using this mode, the packages committed to the ledger
    * cannot be trusted and must be validated every time they are
    * loaded into the engine.  */
  case object Lenient extends PackageValidationMode

  /** Specifies that the committer should not perform any validation of
    * packages before committing them to the ledger.
    * This should be used only by non distributed ledgers, like
    * DAML-on-SQL, where the validation done in the API server
    * can be trusted.  */
  case object No extends PackageValidationMode
}
