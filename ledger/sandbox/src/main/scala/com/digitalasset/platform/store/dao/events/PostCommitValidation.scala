// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.participant.state.v1.{CommittedTransaction, RejectionReason}
import com.daml.logging.ThreadLogger

/**
  * Performs post-commit validation on transactions for Sandbox Classic.
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
private[dao] sealed trait PostCommitValidation {

  def validate(
      transaction: CommittedTransaction,
      transactionLedgerEffectiveTime: Instant,
      divulged: Set[ContractId],
  )(implicit connection: Connection): Option[RejectionReason]

}

private[dao] object PostCommitValidation {

  /**
    * Accept unconditionally a transaction.
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

  final class BackedBy(data: PostCommitValidationData) extends PostCommitValidation {

    def validate(
        transaction: CommittedTransaction,
        transactionLedgerEffectiveTime: Instant,
        divulged: Set[ContractId],
    )(implicit connection: Connection): Option[RejectionReason] = {
      ThreadLogger.traceThread("PostCommitValidation.validate")

      val causalMonotonicityViolation =
        validateCausalMonotonicity(transaction, transactionLedgerEffectiveTime, divulged)

      val invalidKeyUsage = validateKeyUsages(transaction)

      invalidKeyUsage.orElse(causalMonotonicityViolation)
    }

    /**
      * Do all exercise, fetch and lookup-by-key nodes
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
        .fold(Option.empty[RejectionReason])(
          contractLedgerEffectiveTime => {
            Some(
              CausalMonotonicityViolation(
                contractLedgerEffectiveTime = contractLedgerEffectiveTime,
                transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
              )
            )
          }
        )

    private def collectReferredContracts(
        transaction: CommittedTransaction,
        divulged: Set[ContractId],
    ): Set[ContractId] = {
      val (createdInTransaction, referred) =
        transaction.fold((Set.empty[ContractId], Set.empty[ContractId])) {
          case ((created, ids), (_, c: Create)) =>
            (created + c.coid, ids)
          case ((created, ids), (_, e: Exercise)) if !divulged(e.targetCoid) =>
            (created, ids + e.targetCoid)
          case ((created, ids), (_, f: Fetch)) if !divulged(f.coid) =>
            (created, ids + f.coid)
          case ((created, ids), (_, l: LookupByKey)) =>
            (created, l.result.filterNot(divulged).fold(ids)(ids + _))
          case ((created, ids), _) => (created, ids)
        }
      referred.diff(createdInTransaction)
    }

    private def validateKeyUsages(transaction: CommittedTransaction)(
        implicit connection: Connection): Option[RejectionReason] =
      transaction
        .fold[Result](Right(State.empty(data))) {
          case (Right(state), (_, node)) => validateKeyUsages(node, state)
          case (rejection, _) => rejection
        }
        .fold(Some(_), _ => None)

    private def validateKeyUsages(
        node: Node,
        state: State,
    )(implicit connection: Connection): Either[RejectionReason, State] =
      node match {
        case c: Create =>
          state.validateCreate(c.key.map(convert(c.coinst.template, _)), c.coid)
        case l: LookupByKey =>
          state.validateLookupByKey(convert(l.templateId, l.key), l.result)
        case e: Exercise if e.consuming =>
          state.removeKeyIfDefined(e.key.map(convert(e.templateId, _)))
        case _ =>
          // fetch and non-consuming exercise nodes don't need to validate
          // anything with regards to contract keys and do not alter the
          // state in a way which is relevant for the validation of
          // subsequent nodes
          Right(state)
      }

  }

  private type Result = Either[RejectionReason, State]

  /**
    * Represents the state of an ongoing validation.
    * It must be carried over as the transaction is
    * validated one node at a time in pre-order
    * traversal for this to make sense.
    *
    * @param contracts All contracts created as part of the current transaction
    * @param removed Ensures indexed contracts are not referred to by key if they are removed in the current transaction
    * @param data Data about committed contracts for post-commit validation purposes
    */
  private final case class State(
      private val contracts: Map[Hash, ContractId],
      private val removed: Set[Hash],
      private val data: PostCommitValidationData,
  ) {

    def validateCreate(maybeKey: Option[Key], id: ContractId)(
        implicit connection: Connection): Either[RejectionReason, State] =
      maybeKey.fold[Result](Right(this)) { key =>
        lookup(key).fold[Result](Right(add(key, id)))(_ => Left(DuplicateKey))
      }

    // `causalMonotonicity` already reports unknown contracts, no need to check it here
    def removeKeyIfDefined(maybeKey: Option[Key])(
        implicit connection: Connection): Right[RejectionReason, State] =
      Right(maybeKey.fold(this)(remove))

    def validateLookupByKey(key: Key, expectation: Option[ContractId])(
        implicit connection: Connection): Either[RejectionReason, State] = {
      val result = lookup(key)
      if (result == expectation) Right(this)
      else Left(MismatchingLookup(expectation, result))
    }

    private def add(key: Key, id: ContractId): State =
      copy(
        contracts = contracts.updated(key.hash, id),
        removed = removed - key.hash,
      )

    private def remove(key: Key): State =
      copy(
        contracts = contracts - key.hash,
        removed = removed + key.hash,
      )

    private def lookup(key: Key)(implicit connection: Connection): Option[ContractId] =
      contracts.get(key.hash).orElse {
        if (removed(key.hash)) None
        else data.lookupContractKeyGlobally(key)
      }

  }

  private object State {
    def empty(data: PostCommitValidationData): State =
      State(Map.empty, Set.empty, data)
  }

  private[events] val DuplicateKey: RejectionReason =
    RejectionReason.Disputed("DuplicateKey: contract key is not unique")

  private[events] def MismatchingLookup(
      expectation: Option[ContractId],
      result: Option[ContractId],
  ): RejectionReason =
    RejectionReason.Disputed(
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
