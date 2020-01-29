// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.transaction.Transaction.Transaction
import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}

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

  def crash[A](reason: String): A =
    throw SErrorCrash(reason)

  /** DAML exceptions that can be caught. These include
    * arithmetic errors, call to error builtin or update
    * errors. */
  sealed trait SErrorDamlException extends SError

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
      arg: Transaction.Value[ContractId],
  ) extends SErrorDamlException

  /** A fetch or an exercise on a transaction-local contract that has already
    * been consumed. */
  final case class DamlELocalContractNotActive(
      coid: ContractId,
      templateId: TypeConName,
      consumedBy: Transaction.NodeId,
  ) extends SErrorDamlException

  /** Error during an operation on the update transaction. */
  final case class DamlETransactionError(
      reason: String,
  ) extends SErrorDamlException

  /** The submitter was not in the key maintainers on lookup.
    * See <https://github.com/digital-asset/daml/issues/1866>.
    */
  final case class DamlESubmitterNotInMaintainers(
      templateId: TypeConName,
      submitter: Party,
      maintainers: Set[Party],
  ) extends SErrorDamlException

  /** Errors from scenario interpretation. */
  sealed trait SErrorScenario extends SError

  final case class ScenarioErrorContractNotEffective(
      coid: AbsoluteContractId,
      templateId: Identifier,
      effectiveAt: Time.Timestamp,
  ) extends SErrorScenario

  final case class ScenarioErrorContractNotActive(
      coid: AbsoluteContractId,
      templateId: Identifier,
      consumedBy: Ledger.ScenarioNodeId,
  ) extends SErrorScenario

  /** We tried to fetch / exercise a contract of the wrong type --
    * see <https://github.com/digital-asset/daml/issues/1005>.
    */
  final case class DamlEWronglyTypedContract(
      coid: ContractId,
      expected: TypeConName,
      actual: TypeConName,
  ) extends SErrorDamlException

  /** A fetch or exercise was being made against a contract that has not
    * been disclosed to 'committer'. */
  final case class ScenarioErrorContractNotVisible(
      coid: AbsoluteContractId,
      templateId: Identifier,
      committer: Party,
      observers: Set[Party],
  ) extends SErrorScenario

  /** The commit of the transaction failed due to authorization errors. */
  final case class ScenarioErrorCommitError(commitError: Ledger.CommitError) extends SErrorScenario

  /** The transaction produced by the update expression in a 'mustFailAt' succeeded. */
  final case class ScenarioErrorMustFailSucceeded(tx: Transaction) extends SErrorScenario

  /** Invalid party name supplied to 'getParty'. */
  final case class ScenarioErrorInvalidPartyName(name: String, msg: String) extends SErrorScenario
}
