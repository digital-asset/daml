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
        Right(
          KeyValidationState(activeStateKeys = contractKeyDamlStateKeys)
        )
      )(
        (keyValidationStatus, _, exerciseBeginNode) =>
          checkNodeContractKey(
            exerciseBeginNode,
            contractKeysToContractIds,
            keyValidationStatus,
          ),
        (keyValidationStatus, _, leafNode) =>
          checkNodeContractKey(leafNode, contractKeysToContractIds, keyValidationStatus),
        (accum, _, _) => accum,
      )

    keysValidationOutcome match {
      case Right(_) =>
        StepContinue(transactionEntry)
      case Left(error) =>
        val message = error match {
          case Duplicate =>
            "DuplicateKeys: at least one contract key is not unique"
          case Inconsistent(contractKey, actual, expected) =>
            s"InconsistentKeys: at least one contract key has changed since the submission $contractKey -> $actual vs $expected"
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
      keyValidationStatus: KeyValidationStatus,
  ): KeyValidationStatus =
    for {
      initialState <- keyValidationStatus
      stateAfterUniquenessCheck <- checkNodeKeyUniqueness(
        node,
        initialState,
      )
      finalState <- checkNodeKeyConsistency(
        contractKeysToContractIds,
        node,
        stateAfterUniquenessCheck,
      )
    } yield finalState

  private[keys] type RawContractId = String

  private[keys] sealed trait KeyValidationError
  private[keys] case object Duplicate extends KeyValidationError
  private[keys] final case class Inconsistent(
      contractKey: String,
      actual: Option[RawContractId],
      expected: Option[RawContractId],
  ) extends KeyValidationError

  private[keys] final class KeyValidationState private[ContractKeysValidation] (
      private[keys] val activeStateKeys: Set[DamlStateKey],
      private[keys] val submittedContractKeysToContractIds: Map[DamlContractKey, Option[
        RawContractId
      ]],
  ) {
    def +(state: KeyValidationState): KeyValidationState = {
      val newContractKeyMappings =
        state.submittedContractKeysToContractIds -- submittedContractKeysToContractIds.keySet
      new KeyValidationState(
        activeStateKeys = state.activeStateKeys,
        submittedContractKeysToContractIds =
          submittedContractKeysToContractIds ++ newContractKeyMappings,
      )
    }
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
}
