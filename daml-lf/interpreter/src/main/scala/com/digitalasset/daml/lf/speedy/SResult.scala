// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.Time
import com.daml.lf.transaction.GlobalKeyWithMaintainers
import com.daml.lf.speedy.SError._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

/** The result from small-step evaluation.
  * If the result is not Done or Continue, then the machine
  * must be fed before it can be stepped further.
  */
sealed abstract class SResult extends Product with Serializable

object SResult {
  final case class SResultError(err: SError) extends SResult

  /** The speedy machine has completed evaluation to reach a final value.
    * And, if the evaluation was on-ledger, a completed transaction.
    */
  final case class SResultFinal(v: SValue) extends SResult

  /** Update or scenario interpretation requires the current
    * ledger time.
    */
  final case class SResultNeedTime(callback: Time.Timestamp => Unit) extends SResult

  /** Update interpretation requires access to a contract on the ledger. */
  final case class SResultNeedContract(
      contractId: ContractId,
      committers: Set[Party],
      // Callback
      // returns the next expression to evaluate.
      // In case of failure the call back does not throw but returns a SErrorDamlException
      callback: Value.ContractInstance => Unit,
  ) extends SResult

  /** Machine needs a definition that was not present when the machine was
    * initialized. The caller must retrieve the definition and fill it in
    * the packages cache it had provided to initialize the machine.
    */
  final case class SResultNeedPackage(
      pkg: PackageId,
      context: language.Reference,
      callback: CompiledPackages => Unit,
  ) extends SResult

  final case class SResultScenarioSubmit(
      committers: Set[Party],
      commands: SValue,
      location: Option[Location],
      mustFail: Boolean,
      callback: SValue => Unit,
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
      key: GlobalKeyWithMaintainers,
      committers: Set[Party],
      // Callback.
      // In case of failure, the callback sets machine control to an SErrorDamlException and return false
      callback: Option[ContractId] => Boolean,
  ) extends SResult

  sealed abstract class SVisibleToStakeholders extends Product with Serializable
  object SVisibleToStakeholders {
    // actAs and readAs are only included for better error messages.
    final case class NotVisible(
        actAs: Set[Party],
        readAs: Set[Party],
    ) extends SVisibleToStakeholders
    final case object Visible extends SVisibleToStakeholders

    def fromSubmitters(
        actAs: Set[Party],
        readAs: Set[Party] = Set.empty,
    ): Set[Party] => SVisibleToStakeholders = {
      val readers = actAs union readAs
      stakeholders =>
        if (readers.intersect(stakeholders).nonEmpty) {
          SVisibleToStakeholders.Visible
        } else {
          SVisibleToStakeholders.NotVisible(actAs, readAs)
        }
    }
  }
}
