// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package interpretation

import com.daml.lf.data.Ref.{ChoiceName, Location, Party, TypeConName}
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

  final case class RejectedAuthorityRequest(
      holding: Set[Party],
      requesting: Set[Party],
  ) extends Error

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

  /** When caching a disclosed contract key, hashing the contract key generated an error. */
  final case class DisclosedContractKeyHashingError(
      coid: ContractId,
      key: GlobalKey,
      declaredHash: crypto.Hash,
  ) extends Error

  final case class ContractKeyNotVisible(
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

  /** The ledger provided an inconsistent view of a contract key.
    * See com.daml.lf.transaction.Transaction.DuplicateContractKey
    * for more details.
    */
  final case class InconsistentContractKey(
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

  /** We tried to fetch / exercise a contract by interface, but
    * the contract does not implement this interface.
    */
  final case class ContractDoesNotImplementInterface(
      interfaceId: TypeConName,
      coid: ContractId,
      templateId: TypeConName,
  ) extends Error

  /** We tried to exercise a contract by required interface, but
    * the contract does not implement the requiring interface.
    */
  final case class ContractDoesNotImplementRequiringInterface(
      requiringInterfaceId: TypeConName,
      requiredInterfaceId: TypeConName,
      coid: ContractId,
      templateId: TypeConName,
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

  // Error that can be thrown by dev or PoC feature only
  final case class Dev(location: String, error: Dev.Error) extends Error

  object Dev {

    sealed abstract class Error extends Serializable with Product

    /** A choice guard returned false, invalidating some expectation. */
    final case class ChoiceGuardFailed(
        coid: ContractId,
        templateId: TypeConName,
        choiceName: ChoiceName,
        byInterface: Option[TypeConName],
    ) extends Error

    // TODO https://github.com/digital-asset/daml/issues/16151
    // Move outside Dev when the feature goes GA.
    /** We tried to soft fetch / soft exercise a contract of the wrong type --
      * see <https://github.com/digital-asset/daml/issues/16151>.
      */
    final case class WronglyTypedContractSoft(
        coid: ContractId,
        expected: TypeConName,
        accepted: List[TypeConName],
        actual: TypeConName,
    ) extends Error

    final case class Limit(error: Limit.Error) extends Error

    object Limit {

      sealed abstract class Error extends Serializable with Product

      final case class ValueNesting(limit: Int) extends Error

      final case class ContractSignatories(
          coid: Value.ContractId,
          templateId: TypeConName,
          arg: Value,
          signatories: Set[Party],
          limit: Int,
      ) extends Error

      final case class ContractObservers(
          coid: Value.ContractId,
          templateId: TypeConName,
          arg: Value,
          observers: Set[Party],
          limit: Int,
      ) extends Error

      final case class ChoiceControllers(
          cid: Value.ContractId,
          templateId: TypeConName,
          choiceName: ChoiceName,
          arg: Value,
          controllers: Set[Party],
          limit: Int,
      ) extends Error

      final case class ChoiceObservers(
          cid: Value.ContractId,
          templateId: TypeConName,
          choiceName: ChoiceName,
          arg: Value,
          observers: Set[Party],
          limit: Int,
      ) extends Error

      final case class ChoiceAuthorizers(
          cid: Value.ContractId,
          templateId: TypeConName,
          choiceName: ChoiceName,
          arg: Value,
          observers: Set[Party],
          limit: Int,
      ) extends Error

      final case class TransactionInputContracts(limit: Int) extends Error
    }

  }

}
