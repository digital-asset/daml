// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.Time
import com.daml.lf.transaction.GlobalKeyWithMaintainers
import com.daml.lf.speedy.SError._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

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
        callback: Value.ContractInstance => Unit,
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

    final case class NeedAuthority(
        using: Set[Party],
        requesting: Set[Party],
        // Callback: the request is granted
        callback: Boolean => Unit,
    ) extends Update
  }

  sealed abstract class Scenario extends Product with Serializable
  object Scenario {

    final case class Submit(
        committers: Set[Party],
        commands: SValue,
        location: Option[Location],
        mustFail: Boolean,
        callback: SValue => Unit,
    ) extends Scenario

    /** Update interpretation requires the current time. */
    final case class GetTime(callback: Time.Timestamp => Unit) extends Scenario

    /** Pass the ledger time and return back the new time. */
    final case class PassTime(
        relTime: Long,
        callback: Time.Timestamp => Unit,
    ) extends Scenario

    /** A conversion of a string into a party is requested. */
    final case class GetParty(
        partyText: String,
        callback: Party => Unit,
    ) extends Scenario

  }

}

/** The result from small-step evaluation.
  * If the result is not Done or Continue, then the machine
  * must be fed before it can be stepped further.
  */
sealed abstract class SResult[+Q] extends Product with Serializable

object SResult {
  final case class SResultError(err: SError) extends SResult[Nothing]

  /** The speedy machine has completed evaluation to reach a final value.
    * And, if the evaluation was on-ledger, a completed transaction.
    */
  final case class SResultFinal(v: SValue) extends SResult[Nothing]

  final case class SResultQuestion[Q](question: Q) extends SResult[Q]

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
