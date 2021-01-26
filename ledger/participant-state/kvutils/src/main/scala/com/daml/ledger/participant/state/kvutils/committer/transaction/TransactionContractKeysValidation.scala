// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractKey,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.committer.Committer.Step
import com.daml.ledger.participant.state.kvutils.committer.transaction.TransactionCommitter.{
  DamlTransactionEntrySummary,
  damlContractKey,
  globalKey,
}
import com.daml.ledger.participant.state.kvutils.committer.{
  StepContinue,
  StepResult
}
import com.daml.ledger.participant.state.v1.RejectionReason
import com.daml.lf.data.Ref.TypeConName
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{Node, NodeId}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

object TransactionContractKeysValidation {

  private[transaction] def validate(
      transactionCommitter: TransactionCommitter
  ): Step[DamlTransactionEntrySummary] =
    (commitContext, transactionEntry) => {
      val damlState = commitContext
        .collectInputs[(DamlStateKey, DamlStateValue),
                       Map[DamlStateKey, DamlStateValue]] {
          case (key, Some(value)) if key.hasContractKey => key -> value
        }
      val contractKeyDamlStateKeysToContractIds
        : Map[DamlStateKey, RawContractId] =
        damlState.collect {
          case (k, v)
              if k.hasContractKey && v.getContractKeyState.getContractId.nonEmpty =>
            k -> v.getContractKeyState.getContractId
        }
      val contractKeyDamlStateKeys: Set[DamlStateKey] =
        contractKeyDamlStateKeysToContractIds.keySet
      val contractKeysToContractIds: Map[DamlContractKey, RawContractId] =
        contractKeyDamlStateKeysToContractIds.map(m =>
          m._1.getContractKey -> m._2)

      for {
        stateAfterMonotonicityCheck <- checkTransactionContractKeysCausalMonotonicity(
          transactionCommitter,
          commitContext.recordTime,
          contractKeyDamlStateKeys,
          damlState,
          transactionEntry,
        )
        finalState <- performTransactionTraversalContractKeysChecks(
          transactionCommitter,
          commitContext.recordTime,
          contractKeyDamlStateKeys,
          contractKeysToContractIds,
          stateAfterMonotonicityCheck,
        )
      } yield finalState
    }

  /** LookupByKey nodes themselves don't actually fetch the contract.
    * Therefore we need to do an additional check on all contract keys
    * to ensure the referred contract satisfies the causal monotonicity invariant.
    * This could be reduced to only validate this for keys referred to by
    * NodeLookupByKey.
    */
  private def checkTransactionContractKeysCausalMonotonicity(
      transactionCommitter: TransactionCommitter,
      recordTime: Option[Timestamp],
      keys: Set[DamlStateKey],
      damlState: Map[DamlStateKey, DamlStateValue],
      transactionEntry: DamlTransactionEntrySummary,
  ): StepResult[DamlTransactionEntrySummary] = {
    val causalKeyMonotonicity = keys.forall { key =>
      val state = damlState(key)
      val keyActiveAt =
        Conversions
          .parseTimestamp(state.getContractKeyState.getActiveAt)
          .toInstant
      !keyActiveAt.isAfter(transactionEntry.ledgerEffectiveTime.toInstant)
    }
    if (causalKeyMonotonicity)
      StepContinue(transactionEntry)
    else
      transactionCommitter.reject(
        recordTime,
        transactionCommitter.buildRejectionLogEntry(
          transactionEntry,
          RejectionReason.Inconsistent("Causal monotonicity violated"),
        ),
      )
  }

  private def performTransactionTraversalContractKeysChecks(
      transactionCommitter: TransactionCommitter,
      recordTime: Option[Timestamp],
      contractKeyDamlStateKeys: Set[DamlStateKey],
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      transactionEntry: DamlTransactionEntrySummary,
  ): StepResult[DamlTransactionEntrySummary] = {
    val keysValidationOutcome = transactionEntry.transaction
      .foldInExecutionOrder[KeyValidationStatus](
        Right(
          KeyValidationState(activeDamlStateKeys = contractKeyDamlStateKeys))
      )(
        (keyValidationStatus, _, exerciseBeginNode) =>
          validateNodeContractKey(
            exerciseBeginNode,
            contractKeysToContractIds,
            keyValidationStatus,
        ),
        (keyValidationStatus, _, leafNode) =>
          validateNodeContractKey(leafNode,
                                  contractKeysToContractIds,
                                  keyValidationStatus),
        (accum, _, _) => accum,
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

  private def validateNodeContractKey(
      node: Node.GenNode[NodeId, ContractId],
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      keyValidationStatus: KeyValidationStatus,
  ): KeyValidationStatus =
    for {
      initialState <- keyValidationStatus
      stateAfterUniquenessCheck <- validateNodeKeyUniqueness(
        node,
        initialState,
      )
      finalState <- validateNodeKeyConsistency(
        contractKeysToContractIds,
        node,
        stateAfterUniquenessCheck,
      )
    } yield finalState

  private def validateNodeKeyUniqueness(
      node: Node.GenNode[NodeId, ContractId],
      keyValidationState: KeyValidationState,
  ): KeyValidationStatus =
    keyValidationState match {
      case KeyValidationState(activeDamlStateKeys, _) =>
        node match {
          case exercise: Node.NodeExercises[NodeId, ContractId]
              if exercise.key.isDefined && exercise.consuming =>
            val stateKey = Conversions.globalKeyToStateKey(
              globalKey(exercise.templateId, exercise.key.get.key)
            )
            Right(
              keyValidationState.copy(
                activeDamlStateKeys = activeDamlStateKeys - stateKey)
            )

          case create: Node.NodeCreate[ContractId] if create.key.isDefined =>
            val stateKey =
              Conversions.globalKeyToStateKey(
                globalKey(create.coinst.template, create.key.get.key)
              )

            if (activeDamlStateKeys.contains(stateKey))
              Left(Duplicate)
            else
              Right(
                keyValidationState.copy(
                  activeDamlStateKeys = activeDamlStateKeys + stateKey))

          case _ => Right(keyValidationState)
        }
    }

  private def validateNodeKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      node: Node.GenNode[NodeId, ContractId],
      keyValidationState: KeyValidationState,
  ): KeyValidationStatus =
    node match {
      case exercise: Node.NodeExercises[NodeId, ContractId] =>
        validateKeyConsistency(
          contractKeysToContractIds,
          exercise.key,
          Some(exercise.targetCoid),
          exercise.templateId,
          keyValidationState,
        )

      case create: Node.NodeCreate[ContractId] =>
        validateKeyConsistency(
          contractKeysToContractIds,
          create.key,
          None,
          create.templateId,
          keyValidationState,
        )

      case fetch: Node.NodeFetch[ContractId] =>
        validateKeyConsistency(
          contractKeysToContractIds,
          fetch.key,
          Some(fetch.coid),
          fetch.templateId,
          keyValidationState,
        )

      case lookupByKey: Node.NodeLookupByKey[ContractId] =>
        validateKeyConsistency(
          contractKeysToContractIds,
          Some(lookupByKey.key),
          lookupByKey.result,
          lookupByKey.templateId,
          keyValidationState,
        )
    }

  private def validateKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      key: Option[Node.KeyWithMaintainers[Value[ContractId]]],
      targetContractId: Option[ContractId],
      templateId: TypeConName,
      keyValidationState: KeyValidationState,
  ): KeyValidationStatus =
    key.fold(Right(keyValidationState): KeyValidationStatus) {
      submittedKeyWithMaintainers =>
        val submittedDamlContractKey =
          damlContractKey(templateId, submittedKeyWithMaintainers.key)
        val newKeyValidationState =
          keyValidationState.addSubmittedDamlContractKeyToContractIdIfEmpty(
            submittedDamlContractKey,
            targetContractId,
          )
        validateKeyConsistency(
          contractKeysToContractIds,
          submittedDamlContractKey,
          newKeyValidationState,
        )
    }

  private def validateKeyConsistency(
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      submittedDamlContractKey: DamlContractKey,
      keyValidationState: KeyValidationState,
  ): KeyValidationStatus =
    if (keyValidationState.submittedDamlContractKeysToContractIds
          .get(
            submittedDamlContractKey
          )
          .flatten != contractKeysToContractIds.get(submittedDamlContractKey))
      Left(Inconsistent)
    else
      Right(keyValidationState)

  private type RawContractId = String

  private sealed trait KeyValidationError
  private case object Duplicate extends KeyValidationError
  private case object Inconsistent extends KeyValidationError

  private case class KeyValidationState(
      activeDamlStateKeys: Set[DamlStateKey],
      submittedDamlContractKeysToContractIds: Map[DamlContractKey,
                                                  Option[RawContractId]] =
        Map.empty,
  ) {
    def addSubmittedDamlContractKeyToContractIdIfEmpty(
        submittedDamlContractKey: DamlContractKey,
        contractId: Option[ContractId],
    ): KeyValidationState =
      copy(
        submittedDamlContractKeysToContractIds =
          if (!submittedDamlContractKeysToContractIds.contains(
                submittedDamlContractKey)) {
            submittedDamlContractKeysToContractIds + (submittedDamlContractKey -> contractId
              .map(
                _.coid
              ))
          } else {
            submittedDamlContractKeysToContractIds
          }
      )
  }

  private type KeyValidationStatus =
    Either[KeyValidationError, KeyValidationState]
}
