// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package interpretation

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Location, Party, TypeConId}
import com.digitalasset.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, Node, NodeId}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

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

  final case class UnresolvedPackageName(packageName: Ref.PackageName) extends Error

  /** Template pre-condition (ensure) evaluated to false and the transaction
    * was aborted. Note that the compiler will throw instead of returning False
    * for code in LF >= 1.14 so this will never be thrown for newer versions.
    */
  final case class TemplatePreconditionViolated(
      templateId: TypeConId,
      optLocation: Option[Location],
      arg: Value,
  ) extends Error

  /** A fetch or an exercise on a transaction-local contract that has already
    * been consumed.
    */
  final case class ContractNotActive(
      coid: ContractId,
      templateId: TypeConId,
      consumedBy: NodeId,
  ) extends Error

  /** When caching a disclosed contract key, hashing the contract key generated an error. */
  final case class DisclosedContractKeyHashingError(
      coid: ContractId,
      key: GlobalKey,
      declaredHash: crypto.Hash,
  ) extends Error

  /** Fetch-by-key failed
    */
  final case class ContractKeyNotFound(
      key: GlobalKey
  ) extends Error

  /** Two contracts with the same key were active at the same time.
    * See com.digitalasset.daml.lf.transaction.Transaction.DuplicateContractKey
    * for more details.
    */
  final case class DuplicateContractKey(
      key: GlobalKey
  ) extends Error

  /** The ledger provided an inconsistent view of a contract key.
    * See com.digitalasset.daml.lf.transaction.Transaction.DuplicateContractKey
    * for more details.
    */
  final case class InconsistentContractKey(
      key: GlobalKey
  ) extends Error

  /** A create with a contract key failed because the list of maintainers was empty */
  final case class CreateEmptyContractKeyMaintainers(
      templateId: TypeConId,
      arg: Value,
      key: Value,
  ) extends Error

  /** A fetch or lookup of a contract key without maintainers */
  final case class FetchEmptyContractKeyMaintainers(
      templateId: TypeConId,
      key: Value,
      packageName: Ref.PackageName,
  ) extends Error

  /** We tried to fetch / exercise a contract of the wrong type --
    * see <https://github.com/digital-asset/daml/issues/1005>.
    */
  final case class WronglyTypedContract(
      coid: ContractId,
      expected: TypeConId,
      actual: TypeConId,
  ) extends Error

  /** We tried to fetch / exercise a contract by interface, but
    * the contract does not implement this interface.
    */
  final case class ContractDoesNotImplementInterface(
      interfaceId: TypeConId,
      coid: ContractId,
      templateId: TypeConId,
  ) extends Error

  /** We tried to exercise a contract by required interface, but
    * the contract does not implement the requiring interface.
    */
  final case class ContractDoesNotImplementRequiringInterface(
      requiringInterfaceId: TypeConId,
      requiredInterfaceId: TypeConId,
      coid: ContractId,
      templateId: TypeConId,
  ) extends Error

  /** There was an authorization failure during execution. */
  final case class FailedAuthorization(
      nid: NodeId,
      fa: ledger.FailedAuthorization,
  ) extends Error

  // We do not include the culprit value in the NonComparableValues Error
  // as are not serializable.
  final case object NonComparableValues extends Error

  // Attempt to compare the global/relative contract ID `globalCid` and a local/relative
  // contract ID with same discriminator / local prefix. See the "Contract ID
  // Comparability" section in the contract ID specification
  // (//daml-lf/spec/contract-id.rst) for more details.
  final case class ContractIdComparability(globalCid: ContractId) extends Error

  final case class ContractIdInContractKey(key: Value) extends Error

  /** A value has been nested beyond a given depth limit
    *
    * @param limit nesting limit that was exceeded
    */
  final case class ValueNesting(limit: Int) extends Error

  /** User thrown failure with grpc status/metadata
    *
    * @param errorId Defines the errorId metadata value in the ErrorInfoDetail metadata of the resulting GrpcStatus
    * @param failureCategory Canton error category ID, defines the GrpcStatusCode and retry metadata of the resulting GrpcStatus
    * @param errorMessage Message placed in the top level GrpcStatus message field (with prefixes)
    * @param metadata Key value metadata placed in the ErrorInfoDetail of the resulting GrpcStatus
    */
  final case class FailureStatus(
      errorId: String,
      failureCategory: Int,
      errorMessage: String,
      metadata: Map[String, String],
  ) extends Error

  sealed case class Upgrade(error: Upgrade.Error) extends Error

  object Upgrade {
    sealed abstract class Error extends Serializable with Product

    final case class ValidationFailed(
        coid: ContractId,
        srcTemplateId: TypeConId,
        dstTemplateId: TypeConId,
        signatories: Set[Party],
        observers: Set[Party],
        keyOpt: Option[GlobalKeyWithMaintainers],
        msg: String,
    ) extends Error

    // TODO https://github.com/digital-asset/daml/issues/17647:
    //  - add coid, srcTmplId (alternatively pkgId of srcTmplId), and dstTempId
    final case class DowngradeDropDefinedField(
        expectedType: Ast.Type,
        fieldIndex: Long,
        actualValue: Value,
    ) extends Error

    final case class DowngradeFailed(expectedType: Ast.Type, actualValue: Value) extends Error

  }

  sealed case class Crypto(error: Crypto.Error) extends Error

  object Crypto {
    sealed abstract class Error extends Serializable with Product

    final case class MalformedByteEncoding(value: String, cause: String) extends Error

    final case class MalformedKey(key: String, cause: String) extends Error

    final case class MalformedSignature(signature: String, cause: String) extends Error
  }

  // Error that can be thrown by dev or PoC feature only
  final case class Dev(location: String, error: Dev.Error) extends Error

  object Dev {

    sealed abstract class Error extends Serializable with Product

    sealed case class Conformance(
        provided: Node.Create,
        recomputed: Node.Create,
        details: String,
    ) extends Error

    /** A choice guard returned false, invalidating some expectation. */
    final case class ChoiceGuardFailed(
        coid: ContractId,
        templateId: TypeConId,
        choiceName: ChoiceName,
        byInterface: Option[TypeConId],
    ) extends Error

    // TODO https://github.com/digital-asset/daml/issues/16151
    // Move outside Dev when the feature goes GA.
    /** We tried to soft fetch / soft exercise a contract of the wrong type --
      * see <https://github.com/digital-asset/daml/issues/16151>.
      */
    final case class WronglyTypedContractSoft(
        coid: ContractId,
        expected: TypeConId,
        accepted: List[TypeConId],
        actual: TypeConId,
    ) extends Error

    final case class Limit(error: Limit.Error) extends Error

    object Limit {

      sealed abstract class Error extends Serializable with Product

      final case class ContractSignatories(
          coid: Value.ContractId,
          templateId: TypeConId,
          arg: Value,
          signatories: Set[Party],
          limit: Int,
      ) extends Error

      final case class ContractObservers(
          coid: Value.ContractId,
          templateId: TypeConId,
          arg: Value,
          observers: Set[Party],
          limit: Int,
      ) extends Error

      final case class ChoiceControllers(
          cid: Value.ContractId,
          templateId: TypeConId,
          choiceName: ChoiceName,
          arg: Value,
          controllers: Set[Party],
          limit: Int,
      ) extends Error

      final case class ChoiceObservers(
          cid: Value.ContractId,
          templateId: TypeConId,
          choiceName: ChoiceName,
          arg: Value,
          observers: Set[Party],
          limit: Int,
      ) extends Error

      final case class ChoiceAuthorizers(
          cid: Value.ContractId,
          templateId: TypeConId,
          choiceName: ChoiceName,
          arg: Value,
          observers: Set[Party],
          limit: Int,
      ) extends Error

      final case class TransactionInputContracts(limit: Int) extends Error
    }

    final case class MalformedContractId(value: String) extends Error
  }

}
