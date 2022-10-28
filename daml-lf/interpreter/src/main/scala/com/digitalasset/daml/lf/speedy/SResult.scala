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

sealed abstract class Question

object Question {

  final case class OnLedger(question: OnLedger.Question) extends Question

  object OnLedger {
    sealed abstract class Question

    /** Update interpretation requires access to a contract on the ledger. */
    final case class NeedContract(
        contractId: ContractId,
        committers: Set[Party],
        // Callback
        // returns the next expression to evaluate.
        // In case of failure the call back does not throw but returns a SErrorDamlException
        callback: Value.ContractInstance => Unit,
    ) extends Question

    final case class SResultNeedKey(
        key: GlobalKeyWithMaintainers,
        committers: Set[Party],
        // Callback.
        // In case of failure, the callback sets machine control to an SErrorDamlException and return false
        callback: Option[ContractId] => Boolean,
    ) extends Question

    /** Update or scenario interpretation requires the current
      * ledger time.
      */
    final case class NeedTime(callback: Time.Timestamp => Unit) extends Question

    /** Machine needs a definition that was not present when the machine was
      * initialized. The caller must retrieve the definition and fill it in
      * the packages cache it had provided to initialize the machine.
      */
    final case class NeedPackage(
        pkg: PackageId,
        context: language.Reference,
        callback: CompiledPackages => Unit,
    ) extends Question
  }

  final case class Scenario(question: Scenario.Question) extends Question

  object Scenario {
    sealed abstract class Question

    final case class Submit(
        committers: Set[Party],
        commands: SValue,
        location: Option[Location],
        mustFail: Boolean,
        callback: SValue => Unit,
    ) extends Question

    final case class GetTime(callback: Time.Timestamp => Unit) extends Question

    /** Pass the ledger time and return back the new ledger time. */
    final case class PassTime(
        relTime: Long,
        callback: Time.Timestamp => Unit,
    ) extends Question

    /** A conversion of a string into a party is requested. */
    final case class GetParty(
        partyText: String,
        callback: Party => Unit,
    ) extends Question

  }

}

/** The result from small-step evaluation.
  * If the result is not Done or Continue, then the machine
  * must be fed before it can be stepped further.
  */
sealed abstract class SResult[+Q] extends Product with Serializable

object SResult {

  final case class SResultQuestion[Q](question: Q) extends SResult[Q]

  /** The speedy machine has completed evaluation to reach a final value.
    * And, if the evaluation was on-ledger, a completed transaction.
    */
  final case class SResultFinal(v: SValue, tx: Option[PartialTransaction.Result])
      extends SResult[Nothing]

  final case class SResultError(err: SError) extends SResult[Nothing]

  sealed abstract class SVisibleToStakeholders
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
