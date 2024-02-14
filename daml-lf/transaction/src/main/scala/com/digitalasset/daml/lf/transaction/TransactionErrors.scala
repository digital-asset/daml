// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.ledger.FailedAuthorization
import com.daml.lf.value.Value.ContractId

/** Defines the errors raised by [[ContractStateMachine]] and its clients:
  *  - [[DuplicateContractId]]
  *  - [[DuplicateContractKey]]
  *  - [[InconsistentContractKey]]
  *  - [[AuthFailureDuringExecution]]
  * , and classifies them into three overlapping categories:
  *  - [[CreateError]]
  *  - [[ExerciseError]]
  *  - [[FetchError]]
  *  - [[LookupError]]
  *  - [[KeyInputError]]
  *  - [[TransactionError]]
  */
object TransactionErrors {

  /** Signals that the transaction tried to create two contracts with the same
    * contract ID or tried to create a contract whose contract ID has been
    * previously successfully fetched.
    */
  final case class DuplicateContractId(
      contractId: ContractId
  ) extends Serializable
      with Product

  /** Signals that within the transaction we got to a point where
    * two contracts with the same key were active.
    *
    * Note that speedy only detects duplicate key collisions
    * if both contracts are used in the transaction in by-key operations
    * meaning lookup, fetch or exercise-by-key or local creates.
    *
    * Two notable cases that will never produce duplicate key errors
    * is a standalone create or a create and a fetch (but not fetch-by-key)
    * with the same key.
    *
    * For ledger implementors this means that (for contract key uniqueness)
    * it is sufficient to only look at the inputs and the outputs of the
    * transaction while leaving all internal checks within the transaction
    * to the engine.
    */
  final case class DuplicateContractKey(
      key: GlobalKey
  ) extends Serializable
      with Product

  /** Signals that within the transaction we got to a point where
    * we tried to exercise or fetch a contract that is known
    * to have been archived.
    */
  final case class ContractNotActive(contractId: ContractId) extends Serializable with Product

  /** An exercise, fetch or lookupByKey failed because the mapping of key -> contract id
    * was inconsistent with earlier nodes (in execution order). This can happened in case
    * of a race condition between the contract and the contract keys queried to the ledger
    * during an interpretation.
    */
  final case class InconsistentContractKey(key: GlobalKey) extends Serializable with Product

  final case class AuthFailureDuringExecution(
      nid: NodeId,
      fa: FailedAuthorization,
  ) extends Serializable
      with Product

  /** Errors raised when building transactions with [[com.daml.lf.speedy.PartialTransaction]]:
    *   - [[DuplicateContractId]]
    *   - [[DuplicateContractKey]]
    *   - [[AuthFailureDuringExecution]]
    */
  sealed trait TransactionError extends Serializable with Product

  final case class DuplicateContractIdTxError(
      duplicateContractId: DuplicateContractId
  ) extends TransactionError

  final case class DuplicateContractKeyTxError(
      duplicateContractKey: DuplicateContractKey
  ) extends TransactionError

  final case class AuthFailureDuringExecutionTxError(
      authFailureDuringExecution: AuthFailureDuringExecution
  ) extends TransactionError

  object TransactionError {
    def inject(error: DuplicateContractId): TransactionError =
      DuplicateContractIdTxError(error)

    def inject(error: DuplicateContractKey): TransactionError =
      DuplicateContractKeyTxError(error)

    def inject(error: AuthFailureDuringExecution): TransactionError =
      AuthFailureDuringExecutionTxError(error)

    def from(error: CreateError): TransactionError = error match {
      case DuplicateContractIdCreateError(e) => inject(e)
      case DuplicateContractKeyCreateError(e) => inject(e)
    }
  }

  /** The errors returned by [[ContractStateMachine.State.handleNode]] and, as a consequence,
    * [[HasTxNodes.contractKeyInputs]] (hence the name):
    *   - [[DuplicateContractId]]
    *   - [[DuplicateContractKey]]
    *   - [[InconsistentContractKey]]
    *   - [[ContractNotActive]]
    */
  sealed trait KeyInputError extends Serializable with Product

  final case class DuplicateContractIdKIError(
      duplicateContractId: DuplicateContractId
  ) extends KeyInputError

  final case class DuplicateContractKeyKIError(
      duplicateContractKey: DuplicateContractKey
  ) extends KeyInputError

  final case class InconsistentContractKeyKIError(
      inconsistentContractKey: InconsistentContractKey
  ) extends KeyInputError

  final case class ContractNotActiveKeyKIError(
      contractNotActive: ContractNotActive
  ) extends KeyInputError

  object KeyInputError {
    def inject(error: DuplicateContractId): KeyInputError =
      DuplicateContractIdKIError(error)

    def inject(error: DuplicateContractKey): KeyInputError =
      DuplicateContractKeyKIError(error)

    def inject(error: InconsistentContractKey): KeyInputError =
      InconsistentContractKeyKIError(error)

    def inject(error: ContractNotActive): KeyInputError =
      ContractNotActiveKeyKIError(error)

    def from(error: CreateError): KeyInputError = error match {
      case DuplicateContractIdCreateError(e) => inject(e)
      case DuplicateContractKeyCreateError(e) => inject(e)
    }

    def from(error: ExerciseError): KeyInputError = error match {
      case InconsistentContractKeyExerciseError(e) => inject(e)
      case ContractNotActiveExerciseError(e) => inject(e)
    }

    def from(error: FetchError): KeyInputError = error match {
      case InconsistentContractKeyFetchError(e) => inject(e)
      case ContractNotActiveFetchError(e) => inject(e)
    }

    def from(error: LookupError): KeyInputError = error match {
      case InconsistentContractKeyLookupError(e) => inject(e)
    }
  }

  /** The errors returned by [[ContractStateMachine.State.visitCreate]]:
    *   - [[DuplicateContractId]]
    *   - [[DuplicateContractKey]]
    */
  sealed trait CreateError extends Serializable with Product

  final case class DuplicateContractIdCreateError(
      duplicateContractId: DuplicateContractId
  ) extends CreateError

  final case class DuplicateContractKeyCreateError(
      duplicateContractKey: DuplicateContractKey
  ) extends CreateError

  object CreateError {
    def inject(error: DuplicateContractId): CreateError =
      DuplicateContractIdCreateError(error)

    def inject(error: DuplicateContractKey): CreateError =
      DuplicateContractKeyCreateError(error)
  }

  /** The errors returned by [[ContractStateMachine.State.visitExercise()]]:
    *   - [[InconsistentContractKey]]
    *   - [[ContractNotActive]]
    */
  sealed trait ExerciseError extends Serializable with Product

  final case class InconsistentContractKeyExerciseError(
      inconsistentContractKey: InconsistentContractKey
  ) extends ExerciseError

  final case class ContractNotActiveExerciseError(
      contractNotActive: ContractNotActive
  ) extends ExerciseError

  object ExerciseError {
    def inject(error: InconsistentContractKey): ExerciseError =
      InconsistentContractKeyExerciseError(error)

    def inject(error: ContractNotActive): ExerciseError =
      ContractNotActiveExerciseError(error)
  }

  /** The errors returned by [[ContractStateMachine.State.visitFetch()]]:
    *   - [[InconsistentContractKey]]
    *   - [[ContractNotActive]]
    */
  sealed trait FetchError extends Serializable with Product

  final case class InconsistentContractKeyFetchError(
      inconsistentContractKey: InconsistentContractKey
  ) extends FetchError

  final case class ContractNotActiveFetchError(
      contractNotActive: ContractNotActive
  ) extends FetchError

  object FetchError {
    def inject(error: InconsistentContractKey): FetchError =
      InconsistentContractKeyFetchError(error)

    def inject(error: ContractNotActive): FetchError =
      ContractNotActiveFetchError(error)
  }

  /** The errors returned by [[ContractStateMachine.State.visitLookup()]]:
    *   - [[InconsistentContractKey]]
    */
  sealed trait LookupError extends Serializable with Product

  final case class InconsistentContractKeyLookupError(
      inconsistentContractKey: InconsistentContractKey
  ) extends LookupError

  object LookupError {
    def inject(error: InconsistentContractKey): LookupError =
      InconsistentContractKeyLookupError(error)
  }
}
