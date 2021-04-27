// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.participant.state.v1.{CommittedTransaction, RejectionReason}

/** Performs post-commit validation on transactions for Sandbox Classic.
  * This is intended exclusively as a temporary replacement for
  * [[com.daml.platform.store.ActiveLedgerState]] and [[com.daml.platform.store.ActiveLedgerStateManager]]
  * so that the old post-commit validation backed by the old participant schema can be
  * dropped and the DAML-on-X-backed implementation of the Sandbox can skip it entirely.
  *
  * Post-commit validation is relevant for three reasons:
  * - keys can be referenced by two concurrent interpretations, potentially leading to
  *   either create nodes with duplicate active keys or lookup-by-key nodes referring to
  *   inactive keys
  * - contracts may have been consumed by a concurrent interpretation, potentially leading
  *   to double spends
  * - the transaction's ledger effective time is determined after interpretation,
  *   meaning that causal monotonicity cannot be verified while interpreting a command
  */
private[appendonlydao] sealed trait PostCommitValidation {

  def validate(
      transaction: CommittedTransaction,
      transactionLedgerEffectiveTime: Instant,
      divulged: Set[ContractId],
  )(implicit connection: Connection): Option[RejectionReason]

}

private[appendonlydao] object PostCommitValidation {

  /** Accept unconditionally a transaction.
    *
    * Designed to be used by a ledger integration that
    * already performs post-commit validation.
    */
  object Skip extends PostCommitValidation {
    @inline override def validate(
        committedTransaction: CommittedTransaction,
        transactionLedgerEffectiveTime: Instant,
        divulged: Set[ContractId],
    )(implicit connection: Connection): Option[RejectionReason] =
      None
  }

  final class BackedBy(data: PostCommitValidationData, validatePartyAllocation: Boolean)
      extends PostCommitValidation {

    def validate(
        transaction: CommittedTransaction,
        transactionLedgerEffectiveTime: Instant,
        divulged: Set[ContractId],
    )(implicit connection: Connection): Option[RejectionReason] = {

      val causalMonotonicityViolation =
        validateCausalMonotonicity(transaction, transactionLedgerEffectiveTime, divulged)

      val invalidKeyUsage = validateKeyUsages(transaction)

      val unallocatedParties =
        if (validatePartyAllocation)
          validateParties(transaction)
        else
          None

      unallocatedParties.orElse(invalidKeyUsage.orElse(causalMonotonicityViolation))
    }

    /** Do all exercise, fetch and lookup-by-key nodes
      * 1. exist, and
      * 2. refer exclusively to contracts with a ledger effective time smaller than or equal to the transaction's?
      */
    private def validateCausalMonotonicity(
        transaction: CommittedTransaction,
        transactionLedgerEffectiveTime: Instant,
        divulged: Set[ContractId],
    )(implicit connection: Connection): Option[RejectionReason] = {
      val referredContracts = collectReferredContracts(transaction, divulged)
      if (referredContracts.isEmpty) {
        None
      } else {
        data
          .lookupMaximumLedgerTime(referredContracts)
          .map(validateCausalMonotonicity(_, transactionLedgerEffectiveTime))
          .getOrElse(Some(UnknownContract))
      }
    }

    private def validateCausalMonotonicity(
        maximumLedgerEffectiveTime: Option[Instant],
        transactionLedgerEffectiveTime: Instant,
    ): Option[RejectionReason] =
      maximumLedgerEffectiveTime
        .filter(_.isAfter(transactionLedgerEffectiveTime))
        .fold(Option.empty[RejectionReason])(contractLedgerEffectiveTime => {
          Some(
            CausalMonotonicityViolation(
              contractLedgerEffectiveTime = contractLedgerEffectiveTime,
              transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
            )
          )
        })

    private def validateParties(
        transaction: CommittedTransaction
    )(implicit connection: Connection): Option[RejectionReason] = {
      val informees = transaction.informees
      val allocatedInformees = data.lookupParties(informees.toSeq).map(_.party)
      if (allocatedInformees.toSet == informees)
        None
      else
        Some(RejectionReason.PartyNotKnownOnLedger("Some parties are unallocated"))
    }

    private def collectReferredContracts(
        transaction: CommittedTransaction,
        divulged: Set[ContractId],
    ): Set[ContractId] = {
      transaction.inputContracts.diff(divulged)
    }

    private def validateKeyUsages(
        transaction: CommittedTransaction
    )(implicit connection: Connection): Option[RejectionReason] =
      transaction
        .foldInExecutionOrder[Result](Right(State.empty(data)))(
          exerciseBegin = (acc, _, exe) => {
            val newAcc = acc.flatMap(validateKeyUsages(exe, _))
            (newAcc, true)
          },
          exerciseEnd = (acc, _, _) => acc,
          rollbackBegin = (acc, _, _) => (acc.map(_.beginRollback()), true),
          rollbackEnd = (acc, _, _) => acc.map(_.endRollback()),
          leaf = (acc, _, leaf) => acc.flatMap(validateKeyUsages(leaf, _)),
        )
        .fold(Some(_), _ => None)

    private def validateKeyUsages(
        node: Node,
        state: State,
    )(implicit connection: Connection): Either[RejectionReason, State] =
      node match {
        case c: Create =>
          state.validateCreate(c.versionedKey.map(convert(c.versionedCoinst.template, _)), c.coid)
        case l: LookupByKey =>
          state.validateLookupByKey(convert(l.templateId, l.versionedKey), l.result)
        case e: Exercise if e.consuming =>
          state.removeKeyIfDefined(e.versionedKey.map(convert(e.templateId, _)))
        case _ =>
          // fetch and non-consuming exercise nodes don't need to validate
          // anything with regards to contract keys and do not alter the
          // state in a way which is relevant for the validation of
          // subsequent nodes
          Right(state)
      }

  }

  private type Result = Either[RejectionReason, State]

  /** The active ledger key state during validation.
    * After a rollback node, we restore the state at the
    * beginning of the rollback.
    *
    * @param contracts Active contracts created in
    *  the current transaction that have a key indexed
    *  by a hash of their key.
    * @param removed Hashes of contract keys that are known to
    *  to be archived. Note that a later create with the same
    *  key will remove the entry again.
    */
  private final case class ActiveState(
      val contracts: Map[Hash, ContractId],
      val removed: Set[Hash],
  ) {
    def add(key: Key, id: ContractId): ActiveState =
      copy(
        contracts = contracts.updated(key.hash, id),
        removed = removed - key.hash,
      )

    def remove(key: Key): ActiveState =
      copy(
        contracts = contracts - key.hash,
        removed = removed + key.hash,
      )
  }

  /** Represents the state of an ongoing validation.
    * It must be carried over as the transaction is
    * validated one node at a time in pre-order
    * traversal for this to make sense.
    *
    * @param currentState The current active ledger state.
    * @param rollbackStack Stack of states at the beginning of rollback nodes so we can
    *  restore the state at the end of the rollback. The most recent rollback
    *  comes first.
    * @param data Data about committed contracts for post-commit validation purposes.
    *  This is never changed durng the traversal of the transaction.
    */
  private final case class State(
      private val currentState: ActiveState,
      private val rollbackStack: List[ActiveState],
      private val data: PostCommitValidationData,
  ) {

    def validateCreate(maybeKey: Option[Key], id: ContractId)(implicit
        connection: Connection
    ): Either[RejectionReason, State] =
      maybeKey.fold[Result](Right(this)) { key =>
        lookup(key).fold[Result](Right(add(key, id)))(_ => Left(DuplicateKey))
      }

    // `causalMonotonicity` already reports unknown contracts, no need to check it here
    def removeKeyIfDefined(maybeKey: Option[Key]): Right[RejectionReason, State] =
      Right(maybeKey.fold(this)(remove))

    def validateLookupByKey(key: Key, expectation: Option[ContractId])(implicit
        connection: Connection
    ): Either[RejectionReason, State] = {
      val result = lookup(key)
      if (result == expectation) Right(this)
      else Left(MismatchingLookup(expectation, result))
    }

    def beginRollback(): State =
      copy(
        rollbackStack = currentState :: rollbackStack
      )

    def endRollback(): State = rollbackStack match {
      case Nil =>
        throw new IllegalStateException(s"Internal error: rollback end but rollbackStack was empty")
      case head :: tail =>
        copy(
          currentState = head,
          rollbackStack = tail,
        )
    }

    private def add(key: Key, id: ContractId): State =
      copy(currentState = currentState.add(key, id))

    private def remove(key: Key): State =
      copy(
        currentState = currentState.remove(key)
      )

    private def lookup(key: Key)(implicit connection: Connection): Option[ContractId] =
      currentState.contracts.get(key.hash).orElse {
        if (currentState.removed(key.hash)) None
        else data.lookupContractKeyGlobally(key)
      }

  }

  private object State {
    def empty(data: PostCommitValidationData): State =
      State(ActiveState(Map.empty, Set.empty), Nil, data)
  }

  private[events] val DuplicateKey: RejectionReason =
    RejectionReason.Inconsistent("DuplicateKey: contract key is not unique")

  private[events] def MismatchingLookup(
      expectation: Option[ContractId],
      result: Option[ContractId],
  ): RejectionReason =
    RejectionReason.Inconsistent(
      s"Contract key lookup with different results: expected [$expectation], actual [$result]"
    )

  private[events] val UnknownContract: RejectionReason =
    RejectionReason.Inconsistent("Unknown contract")

  private[events] def CausalMonotonicityViolation(
      contractLedgerEffectiveTime: Instant,
      transactionLedgerEffectiveTime: Instant,
  ): RejectionReason =
    RejectionReason.InvalidLedgerTime(
      s"Encountered contract with LET [$contractLedgerEffectiveTime] greater than the LET of the transaction [$transactionLedgerEffectiveTime]"
    )
}
