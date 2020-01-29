// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.Transaction._
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SExpr.SDefinitionRef
import com.digitalasset.daml.lf.transaction.Node.GlobalKey

/** The result from small-step evaluation.
  * If the result is not Done or Continue, then the machine
  * must be fed before it can be stepped further. */
sealed abstract class SResult extends Product with Serializable

object SResult {
  final case class SResultError(err: SError) extends SResult
  final case object SResultContinue extends SResult

  /** Update or scenario interpretation requires the current
    * ledger time. */
  final case class SResultNeedTime(callback: Time.Timestamp => Unit) extends SResult

  /** Update interpretation requires access to a contract on the ledger. */
  final case class SResultNeedContract(
      contractId: AbsoluteContractId,
      templateId: TypeConName,
      committers: Set[Party],
      // Callback to signal that the contract was not present
      // or visible. Returns true if this was recoverable.
      cbMissing: Unit => Boolean,
      cbPresent: ContractInst[Value[AbsoluteContractId]] => Unit,
  ) extends SResult

  /** Machine needs a definition that was not present when the machine was
    * initialized. The caller must retrieve the definition and fill it in
    * the packages cache it had provided to initialize the machine.
    */
  final case class SResultMissingDefinition(
      ref: SDefinitionRef,
      callback: CompiledPackages => Unit,
  ) extends SResult

  /** Commit the partial transaction to the (scenario) ledger.
    * Machine expects the value back with the contract ids rewritten
    * to be absolute. */
  final case class SResultScenarioCommit(
      value: SValue,
      tx: Transaction,
      committers: Set[Party],
      callback: SValue => Unit,
  ) extends SResult

  final case class SResultScenarioInsertMustFail(
      committers: Set[Party],
      optLocation: Option[Location],
  ) extends SResult

  /** A "must fail" update resulted in a partial transaction, try and
    * commit this transaction with the expectation that it fails.
    * The callback signals success and clears the partial transaction. */
  final case class SResultScenarioMustFail(
      ptx: Transaction,
      committers: Set[Party],
      callback: Unit => Unit,
  ) extends SResult

  /** Pass the ledger time and return back the new ledger time. */
  final case class SResultScenarioPassTime(
      relTime: Long,
      callback: Time.Timestamp => Unit,
  ) extends SResult

  /** A conversion of a string into a party is requested. */
  final case class SResultScenarioGetParty(
      partyText: String,
      callback: Party => Unit,
  ) extends SResult

  final case class SResultNeedKey(
      key: GlobalKey,
      committers: Set[Party],
      // Callback to signal that the key was not present.
      // returns true if this was recoverable.
      cbMissing: Unit => Boolean,
      cbPresent: AbsoluteContractId => Unit,
  ) extends SResult
}
