// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.GlobalKeyWithMaintainers
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

object Question {
  sealed abstract class Update extends Product with Serializable
  object Update {

    /** Update interpretation requires the current ledger time.
      */
    final case class NeedTime(callback: Time.Timestamp => Unit) extends Update

    /** Update interpretation requires access to a contract on the ledger. */
    final case class NeedContract(
        contractId: ContractId,
        committers: Set[Party],
        // Callback
        // returns the next expression to evaluate.
        // In case of failure the call back does not throw but returns a SErrorDamlException
        callback: Value.ThinContractInstance => Unit,
    ) extends Update

    /** Contract info for upgraded contract needs verification by ledger */
    final case class NeedUpgradeVerification(
        coid: ContractId,
        signatories: Set[Party],
        observers: Set[Party],
        keyOpt: Option[GlobalKeyWithMaintainers],
        callback: Option[String] => Unit,
    ) extends Update

    /** Machine needs a definition that was not present when the machine was
      * initialized. The caller must retrieve the definition and fill it in
      * the packages cache it had provided to initialize the machine.
      */
    final case class NeedPackage(
        pkg: PackageId,
        context: language.Reference,
        callback: CompiledPackages => Unit,
    ) extends Update

    final case class NeedKey(
        key: GlobalKeyWithMaintainers,
        committers: Set[Party],
        // Callback.
        // In case of failure, the callback sets machine control to an SErrorDamlException and return false
        callback: Option[ContractId] => Boolean,
    ) extends Update
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
  final case class SResultFinal(v: SValue) extends SResult[Nothing]

  final case class SResultError(err: SError) extends SResult[Nothing]

  final case object SResultInterruption extends SResult[Nothing]

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
