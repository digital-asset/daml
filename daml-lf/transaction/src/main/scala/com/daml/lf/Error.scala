// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package interpretation

import com.daml.lf.data.Ref.{Location, Party, TypeConName, ChoiceName}
import com.daml.lf.transaction.{GlobalKey, NodeId}
import com.daml.lf.language.Ast
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

/** Daml exceptions that should be reported to the user
  */
sealed abstract class Error extends Serializable with Product {
  override def toString: String = s"$productPrefix(${productIterator.mkString(",")}"
}

object Error {

  /** Unhandled exceptions */
  final case class UnhandledException(exceptionType: Ast.Type, value: Value) extends Error

  /** User initiated error, via e.g. 'abort' or 'assert' */
  final case class UserError(message: String) extends Error

  final case class ContractNotFound(cid: Value.ContractId) extends Error

  /** Template pre-condition (ensure) evaluated to false and the transaction
    * was aborted. Note that the compiler will throw instead of returning False
    * for code in LF >= 1.14 so this will never be thrown for newer versions.
    */
  final case class TemplatePreconditionViolated(
      templateId: TypeConName,
      optLocation: Option[Location],
      arg: Value,
  ) extends Error

  /** A fetch or an exercise on a transaction-local contract that has already
    * been consumed.
    */
  final case class ContractNotActive(
      coid: ContractId,
      templateId: TypeConName,
      consumedBy: NodeId,
  ) extends Error

  final case class LocalContractKeyNotVisible(
      coid: ContractId,
      key: GlobalKey,
      actAs: Set[Party],
      readAs: Set[Party],
      stakeholders: Set[Party],
  ) extends Error

  /** Fetch-by-key failed
    */
  final case class ContractKeyNotFound(
      key: GlobalKey
  ) extends Error

  /** Two contracts with the same key were active at the same time.
    * See com.daml.lf.transaction.Transaction.DuplicateContractKey
    * for more details.
    */
  final case class DuplicateContractKey(
      key: GlobalKey
  ) extends Error

  /** A create with a contract key failed because the list of maintainers was empty */
  final case class CreateEmptyContractKeyMaintainers(
      templateId: TypeConName,
      arg: Value,
      key: Value,
  ) extends Error

  /** A fetch or lookup of a contract key without maintainers */
  final case class FetchEmptyContractKeyMaintainers(
      templateId: TypeConName,
      key: Value,
  ) extends Error

  /** We tried to fetch / exercise a contract of the wrong type --
    * see <https://github.com/digital-asset/daml/issues/1005>.
    */
  final case class WronglyTypedContract(
      coid: ContractId,
      expected: TypeConName,
      actual: TypeConName,
  ) extends Error

  /** There was an authorization failure during execution. */
  final case class FailedAuthorization(
      nid: NodeId,
      fa: ledger.FailedAuthorization,
  ) extends Error

  // We do not include the culprit value in the NonComparableValues Error
  // as are not serializable.
  final case object NonComparableValues extends Error

  // Attempt to compare the global contract ID `globalCid` and a local
  // contract ID with same discriminator. See the "Contract ID
  // Comparability" section in the contract ID specification
  // (//daml-lf/spec/contract-id.rst) for more details.
  final case class ContractIdComparability(globalCid: ContractId.V1) extends Error

  final case class ContractIdInContractKey(key: Value) extends Error

  final case object ValueExceedsMaxNesting extends Error

  /** A choice guard returned false, invalidating some expectation. */
  final case class ChoiceGuardFailed (
      coid: ContractId,
      templateId: TypeConName,
      choiceName: ChoiceName,
      byInterface: Option[TypeConName],
  ) extends Error

}
