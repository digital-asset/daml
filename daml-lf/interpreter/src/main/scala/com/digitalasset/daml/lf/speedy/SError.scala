// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.VersionRange
import com.daml.lf.data.Ref._
import com.daml.lf.data.Time
import com.daml.lf.ledger.EventId
import com.daml.lf.ledger.FailedAuthorization
import com.daml.lf.transaction.{GlobalKey, NodeId, Transaction => Tx}
import com.daml.lf.value.{Value, ValueVersion}
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

  /** DAML exceptions that can be caught. These include
    * arithmetic errors, call to error builtin or update
    * errors. */
  sealed abstract class SErrorDamlException extends SError

  /** Arithmetic error such as division by zero */
  final case class DamlEArithmeticError(message: String) extends SErrorDamlException {
    override def toString: String = message
  }

  /** User initiated error, via e.g. 'abort' or 'assert' */
  final case class DamlEUserError(message: String) extends SErrorDamlException

  /** An inexhaustive pattern match */
  final case class DamlEMatchError(reason: String) extends SErrorDamlException

  /** Template pre-condition (ensure) evaluated to false and the transaction
    * was aborted. */
  final case class DamlETemplatePreconditionViolated(
      templateId: TypeConName,
      optLocation: Option[Location],
      arg: Value[ContractId],
  ) extends SErrorDamlException

  /** A fetch or an exercise on a transaction-local contract that has already
    * been consumed. */
  final case class DamlELocalContractNotActive(
      coid: ContractId,
      templateId: TypeConName,
      consumedBy: NodeId,
  ) extends SErrorDamlException

  /** Error during an operation on the update transaction. */
  final case class DamlETransactionError(
      reason: String,
  ) extends SErrorDamlException

  /** A create a contract key without maintainers */
  final case class DamlECreateEmptyContractKeyMaintainers(
      templateId: TypeConName,
      arg: Value[ContractId],
      key: Value[Nothing],
  ) extends SErrorDamlException

  /** A fetch or lookup a contract key without maintainers */
  final case class DamlEFetchEmptyContractKeyMaintainers(
      templateId: TypeConName,
      key: Value[Nothing],
  ) extends SErrorDamlException

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

  /** We tried to fetch / exercise a contract of the wrong type --
    * see <https://github.com/digital-asset/daml/issues/1005>.
    */
  final case class DamlEWronglyTypedContract(
      coid: ContractId,
      expected: TypeConName,
      actual: TypeConName,
  ) extends SErrorDamlException

  /** We tried to fetch data with disallowed value version --
    *  see <https://github.com/digital-asset/daml/issues/5164>
    */
  final case class DamlEDisallowedInputValueVersion(
      allowed: VersionRange[ValueVersion],
      actual: ValueVersion,
  ) extends SErrorDamlException

  /** There was an authorization failure during execution. */
  final case class DamlEFailedAuthorization(
      nid: NodeId,
      fa: FailedAuthorization,
  ) extends SErrorDamlException

  /** A fetch or exercise was being made against a contract that has not
    * been disclosed to 'committer'. */
  final case class ScenarioErrorContractNotVisible(
      coid: ContractId,
      templateId: Identifier,
      actAs: Set[Party],
      readAs: Set[Party],
      observers: Set[Party],
  ) extends SErrorScenario

  /** A fetchByKey or lookupByKey was being made against a key
    * for which the contract exists but has not
    * been disclosed to 'committer'. */
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
