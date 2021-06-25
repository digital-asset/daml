// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref._
import com.daml.lf.data.Time
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.{GlobalKey, Transaction => Tx}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.scenario.ScenarioLedger

object SError {

  /** Errors that can arise during interpretation */
  sealed abstract class SError extends RuntimeException with Product with Serializable

  /** A malformed expression was encountered. The assumption is that the
    * expressions are type-checked and the loaded packages have been validated,
    * hence we do not have separate errors for e.g. unknown values.
    */
  final case class SErrorCrash(reason: String) extends SError {
    override def toString = "CRASH: " + reason
  }

  /** Operation is only supported in on-ledger mode but was
    * called in off-ledger mode.
    */
  final case class SRequiresOnLedger(operation: String) extends SError {
    override def toString = s"Requires on-ledger mode: " + operation
  }

  def crash[A](reason: String): A =
    throw SErrorCrash(reason)

  /** Daml exceptions that should be reported to the user. */
  final case class SErrorDamlException(error: interpretation.Error) extends SError

  /** Errors from scenario interpretation. */
  sealed trait SErrorScenario extends SError

  final case class ScenarioErrorContractNotEffective(
      coid: ContractId,
      templateId: Identifier,
      effectiveAt: Time.Timestamp,
  ) extends SErrorScenario

  final case class ScenarioErrorContractNotActive(
      coid: ContractId,
      templateId: Identifier,
      consumedBy: EventId,
  ) extends SErrorScenario

  /** A fetch or exercise was being made against a contract that has not
    * been disclosed to 'committer'.
    */
  final case class ScenarioErrorContractNotVisible(
      coid: ContractId,
      templateId: Identifier,
      actAs: Set[Party],
      readAs: Set[Party],
      observers: Set[Party],
  ) extends SErrorScenario

  /** A fetchByKey or lookupByKey was being made against a key
    * for which the contract exists but has not
    * been disclosed to 'committer'.
    */
  final case class ScenarioErrorContractKeyNotVisible(
      coid: ContractId,
      key: GlobalKey,
      actAs: Set[Party],
      readAs: Set[Party],
      stakeholders: Set[Party],
  ) extends SErrorScenario

  /** The transaction failed due to a commit error */
  final case class ScenarioErrorCommitError(commitError: ScenarioLedger.CommitError)
      extends SErrorScenario

  /** The transaction produced by the update expression in a 'mustFailAt' succeeded. */
  final case class ScenarioErrorMustFailSucceeded(tx: Tx.Transaction) extends SErrorScenario

  /** Invalid party name supplied to 'getParty'. */
  final case class ScenarioErrorInvalidPartyName(name: String, msg: String) extends SErrorScenario

  /** Tried to allocate a party that already exists. */
  final case class ScenarioErrorPartyAlreadyExists(name: String) extends SErrorScenario

}
