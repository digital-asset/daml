// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.api.domain.RejectionReason

/**
  * Performs post-commit validation on transactions for Sandbox Classic.
  * This is intended exclusively as a temporary replacement for
  * [[com.daml.platform.store.ActiveLedgerState]] and [[com.daml.platform.store.ActiveLedgerStateManager]]
  * so that the old post-commit validation backed by the old participant schema can be
  * dropped and the DAML-on-X-backed implementation of the Sandbox can skip it entirely.
  *
  * Post-commit validation is relevant for two reasons:
  * - keys can be referenced by two concurrent interpretations, potentially leading to
  *   either create nodes with duplicate active keys or lookup-by-key nodes referring to
  *   inactive keys
  * - the transaction's ledger effective time is determined after interpretation,
  *   meaning that causal monotonicity cannot be verified while interpreting a command
  */
sealed trait PostCommitValidation {

  def validate(
      transaction: Transaction,
      transactionLedgerEffectiveTime: Instant,
      divulged: Set[ContractId],
      submitter: Party,
  )(implicit connection: Connection): Set[RejectionReason]

}

object PostCommitValidation {

  /**
    * Accept unconditionally a transaction.
    *
    * Designed to be used by a ledger integration that
    * already performs post-commit validation.
    */
  object Skip extends PostCommitValidation {
    override def validate(
        transaction: Transaction,
        transactionLedgerEffectiveTime: Instant,
        divulged: Set[ContractId],
        submitter: Party,
    )(implicit connection: Connection): Set[RejectionReason] =
      Set.empty
  }

  final class BackedBy(data: PostCommitValidationData) extends PostCommitValidation {

    def validate(
        transaction: Transaction,
        transactionLedgerEffectiveTime: Instant,
        divulged: Set[ContractId],
        submitter: Party,
    )(implicit connection: Connection): Set[RejectionReason] = {

      val causalMonotonicityRejection =
        validateCausalMonotonicity(transaction, transactionLedgerEffectiveTime, divulged)

      val invalidKeyUsageRejection =
        validateKeyUsages(transaction, submitter)

      causalMonotonicityRejection.union(invalidKeyUsageRejection)
    }

    /**
      * Do all exercise, fetch and lookup-by-key nodes
      * 1. exist, and
      * 2. refer exclusively to contracts with a ledger effective time smaller than or equal to the transaction's?
      */
    private def validateCausalMonotonicity(
        transaction: Transaction,
        transactionLedgerEffectiveTime: Instant,
        divulged: Set[ContractId],
    )(implicit connection: Connection): Set[RejectionReason] = {

      // Collect all the identifiers of contracts that have not been divulged in the current transaction
      val maximumLedgerEffectiveTime =
        data.lookupMaximumLedgerTime(
          transaction.fold(Set.empty[ContractId]) {
            case (ids, (_, e: Exercise)) if !divulged(e.targetCoid) => ids + e.targetCoid
            case (ids, (_, f: Fetch)) if !divulged(f.coid) => ids + f.coid
            case (ids, (_, l: LookupByKey)) => l.result.filterNot(divulged).fold(ids)(ids + _)
            case (ids, _) => ids
          }
        )

      // Query the committed contracts store for the maximum ledger effective time of the
      // collected contracts and returns an error if that is strictly greater than the
      // ledger effective time of the current transaction.
      maximumLedgerEffectiveTime
        .map(
          _.filter(_.isAfter(transactionLedgerEffectiveTime)).fold(Set.empty[RejectionReason])(
            contractLedgerEffectiveTime => {
              Set(
                CausalMonotonicityViolation(
                  contractLedgerEffectiveTime = contractLedgerEffectiveTime,
                  transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                )
              )
            }
          )
        )
        .getOrElse(Set(UnknownContract))
    }

    private def validateKeyUsages(transaction: Transaction, submitter: Party)(
        implicit connection: Connection): Set[RejectionReason] = {
      transaction
        .fold(State.empty(data, submitter)) {
          case (state, (_, node)) => validateKeyUsages(node, state)
        }
        .errors
    }

    private def validateKeyUsages(
        node: Node,
        state: State,
    )(implicit connection: Connection): State = {

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
          state
      }
    }
  }

  /**
    * Represents the state of an ongoing validation.
    * It must be carried over as the transaction is
    * validated one node at a time in pre-order
    * traversal for this to make sense.
    *
    * @param errors Accumulates errors retrieved so far while validating
    * @param contracts All contracts created as part of the current transaction
    * @param removed Ensures indexed transactions are not referred to if they are removed in the current transaction
    * @param submitter The submitter of the current transaction
    */
  private final case class State(
      errors: Set[RejectionReason],
      private val contracts: Map[Hash, ContractId],
      private val removed: Set[Hash],
      private val submitter: Party,
      private val data: PostCommitValidationData,
  ) {

    def validateCreate(maybeKey: Option[Key], id: ContractId)(
        implicit connection: Connection): State =
      maybeKey.fold(this) { key =>
        lookup(key).fold(add(key, id))(_ => error(DuplicateKey))
      }

    // `causalMonotonicity` already reports unknown contracts, no need to check it here
    def removeKeyIfDefined(maybeKey: Option[Key])(implicit connection: Connection): State =
      maybeKey.fold(this)(remove)

    def validateLookupByKey(key: Key, expectation: Option[ContractId])(
        implicit connection: Connection): State = {
      val result = lookup(key)
      if (result == expectation) this
      else copy(errors = errors + MismatchingLookup(expectation, result))
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

    private def error(reason: RejectionReason): State =
      copy(errors = errors + reason)

    private def lookup(key: Key)(implicit connection: Connection): Option[ContractId] =
      contracts.get(key.hash).orElse {
        if (removed(key.hash)) None
        else data.lookupContractKey(submitter, key)
      }

  }

  private object State {
    def empty(data: PostCommitValidationData, submitter: Party): State =
      State(Set.empty, Map.empty, Set.empty, submitter, data)
  }

  private val DuplicateKey: RejectionReason =
    RejectionReason.Disputed("DuplicateKey: contract key is not unique")

  private def MismatchingLookup(
      expectation: Option[ContractId],
      result: Option[ContractId],
  ): RejectionReason =
    RejectionReason.Disputed(
      s"Contract key lookup with different results: expected [$expectation], actual [$result]"
    )

  private val UnknownContract: RejectionReason =
    RejectionReason.Inconsistent(s"Could not lookup contract")

  private def CausalMonotonicityViolation(
      contractLedgerEffectiveTime: Instant,
      transactionLedgerEffectiveTime: Instant,
  ): RejectionReason =
    RejectionReason.InvalidLedgerTime(
      s"Encountered contract with LET [$contractLedgerEffectiveTime] greater than the LET of the transaction [$transactionLedgerEffectiveTime]"
    )

}
