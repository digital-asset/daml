// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.keys

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractKey,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.committer.Committer.Step
import com.daml.ledger.participant.state.kvutils.committer.transaction.TransactionCommitter
import com.daml.ledger.participant.state.kvutils.committer.transaction.TransactionCommitter.DamlTransactionEntrySummary
import com.daml.ledger.participant.state.kvutils.committer.transaction.keys.KeyConsistencyValidation.checkNodeKeyConsistency
import com.daml.ledger.participant.state.kvutils.committer.transaction.keys.KeyMonotonicityValidation.checkContractKeysCausalMonotonicity
import com.daml.ledger.participant.state.kvutils.committer.transaction.keys.KeyUniquenessValidation.checkNodeKeyUniqueness
import com.daml.ledger.participant.state.kvutils.committer.{StepContinue, StepResult}
import com.daml.ledger.participant.state.v1.RejectionReason
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{Node, NodeId}
import com.daml.lf.value.Value.ContractId

private[transaction] object ContractKeysValidation {

  def validateKeys(
      transactionCommitter: TransactionCommitter
  ): Step[DamlTransactionEntrySummary] =
    (commitContext, transactionEntry) => {
      val damlState = commitContext
        .collectInputs[(DamlStateKey, DamlStateValue), Map[DamlStateKey, DamlStateValue]] {
          case (key, Some(value)) if key.hasContractKey => key -> value
        }
      val contractKeyDamlStateKeysToContractIds: Map[DamlStateKey, RawContractId] =
        damlState.collect {
          case (k, v) if k.hasContractKey && v.getContractKeyState.getContractId.nonEmpty =>
            k -> v.getContractKeyState.getContractId
        }
      // State before the transaction
      val contractKeyDamlStateKeys: Set[DamlStateKey] =
        contractKeyDamlStateKeysToContractIds.keySet
      val contractKeysToContractIds: Map[DamlContractKey, RawContractId] =
        contractKeyDamlStateKeysToContractIds.map(m => m._1.getContractKey -> m._2)

      for {
        stateAfterMonotonicityCheck <- checkContractKeysCausalMonotonicity(
          transactionCommitter,
          commitContext.recordTime,
          contractKeyDamlStateKeys,
          damlState,
          transactionEntry,
        )
        finalState <- performTraversalContractKeysChecks(
          transactionCommitter,
          commitContext.recordTime,
          contractKeyDamlStateKeys,
          contractKeysToContractIds,
          stateAfterMonotonicityCheck,
        )
      } yield finalState
    }

  private def performTraversalContractKeysChecks(
      transactionCommitter: TransactionCommitter,
      recordTime: Option[Timestamp],
      contractKeyDamlStateKeys: Set[DamlStateKey],
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      transactionEntry: DamlTransactionEntrySummary,
  ): StepResult[DamlTransactionEntrySummary] = {

    val keysValidationOutcome = transactionEntry.transaction
      .foldInExecutionOrder[KeyValidationStatus](
        Right(KeyValidationState(activeStateKeys = contractKeyDamlStateKeys))
      )(
        exerciseBegin = (status, _, exerciseBeginNode) =>
          onCurrentStatus(status)(
            checkNodeContractKey(exerciseBeginNode, contractKeysToContractIds, _)
          ),
        leaf = (status, _, leafNode) =>
          onCurrentStatus(status)(checkNodeContractKey(leafNode, contractKeysToContractIds, _)),
        exerciseEnd = (accum, _, _) => accum,
      )

    keysValidationOutcome match {
      case Right(_) =>
        StepContinue(transactionEntry)
      case Left(error) =>
        val message = error match {
          case Duplicate =>
            "DuplicateKeys: at least one contract key is not unique"
          case Inconsistent =>
            "InconsistentKeys: at least one contract key has changed since the submission"
        }
        transactionCommitter.reject(
          recordTime,
          transactionCommitter.buildRejectionLogEntry(
            transactionEntry,
            RejectionReason.Inconsistent(message),
          ),
        )
    }
  }

  private def checkNodeContractKey(
      node: Node.GenNode[NodeId, ContractId],
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      initialState: KeyValidationState,
  ): KeyValidationStatus =
    for {
      stateAfterUniquenessCheck <- initialState.onActiveStateKeys(
        checkNodeKeyUniqueness(
          node,
          _,
        )
      )
      finalState <- stateAfterUniquenessCheck.onSubmittedContractKeysToContractIds(
        checkNodeKeyConsistency(
          contractKeysToContractIds,
          node,
          _,
        )
      )
    } yield finalState

  private[keys] type RawContractId = String

  private[keys] sealed trait KeyValidationError
  private[keys] case object Duplicate extends KeyValidationError
  private[keys] case object Inconsistent extends KeyValidationError

  /** The state used during key validation.
    *
    * @param activeStateKeys The currently active contract keys for uniqueness
    *  checks, starting with the keys active before the transaction.
    * @param submittedContractKeysToContractIds Map of keys to their expected assignment at
    *  the beginning of the transaction, starting with an empty map.
    *  E.g., a create (with nothing before) means the key must have been inactive
    *  at the beginning.
    */
  private[keys] final class KeyValidationState private[ContractKeysValidation] (
      private[keys] val activeStateKeys: Set[DamlStateKey],
      private[keys] val submittedContractKeysToContractIds: KeyConsistencyValidation.State,
  ) {

    def onSubmittedContractKeysToContractIds(
        f: KeyConsistencyValidation.State => KeyConsistencyValidation.Status
    ): KeyValidationStatus =
      f(submittedContractKeysToContractIds).map(newSubmitted =>
        new KeyValidationState(
          activeStateKeys = this.activeStateKeys,
          submittedContractKeysToContractIds = newSubmitted,
        )
      )

    def onActiveStateKeys(
        f: Set[DamlStateKey] => Either[KeyValidationError, Set[DamlStateKey]]
    ): KeyValidationStatus =
      f(activeStateKeys).map(newActive =>
        new KeyValidationState(
          activeStateKeys = newActive,
          submittedContractKeysToContractIds = this.submittedContractKeysToContractIds,
        )
      )
  }

  private[keys] object KeyValidationState {
    def apply(activeStateKeys: Set[DamlStateKey]): KeyValidationState =
      new KeyValidationState(activeStateKeys, Map.empty)

    def apply(
        submittedContractKeysToContractIds: Map[DamlContractKey, Option[
          RawContractId
        ]]
    ): KeyValidationState =
      new KeyValidationState(Set.empty, submittedContractKeysToContractIds)
  }

  private[keys] type KeyValidationStatus =
    Either[KeyValidationError, KeyValidationState]

  def onCurrentStatus[E, A](
      status: Either[E, A]
  )(f: A => Either[E, A]): Either[E, A] = status.flatMap(f)
}
