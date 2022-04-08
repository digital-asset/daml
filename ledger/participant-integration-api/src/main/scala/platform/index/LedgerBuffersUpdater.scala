// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.daml.ledger.api.DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update.{CommandRejected, TransactionAccepted}
import com.daml.ledger.participant.state.v2.{CompletionInfo, Update}
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.store.appendonlydao.events._
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.{CompletionDetails, LedgerEndMarker}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

object LedgerBuffersUpdater {
  private val prepareUpdatesParallelism = 2
  private val ec: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(prepareUpdatesParallelism))

  def flow: Flow[(Iterable[(Offset, Update)], LedgerEndMarker), TransactionLogUpdate, NotUsed] =
    Flow[(Iterable[(Offset, Update)], LedgerEndMarker)]
      .mapAsync(prepareUpdatesParallelism)(batch => Future(transformBatch(batch))(ec))
      .async
      .flatMapConcat { updates => Source.fromIterator(() => updates) }

  private def transformBatch(
      batch: (Iterable[(Offset, Update)], LedgerEndMarker)
  ): Iterator[TransactionLogUpdate] = {
    val (updatesBatch, ledgerEndMarker) = batch

    val transactionLogUpdates =
      updatesBatch.view.collect {
        case (offset, u: TransactionAccepted) => updateToTransactionAccepted(offset, u)
        case (offset, u: CommandRejected) => updateToSubmissionRejected(offset, u)
      }.toVector

    if (transactionLogUpdates.isEmpty) {
      Iterator(ledgerEndMarker)
    } else {
      (transactionLogUpdates :+ ledgerEndMarker).iterator
    }
  }

  private def updateToSubmissionRejected(offset: Offset, u: CommandRejected) = {
    val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
      deduplicationInfo(u.completionInfo)

    TransactionLogUpdate.SubmissionRejected(
      offset = offset,
      completionDetails = CompletionDetails(
        CompletionFromTransaction.rejectedCompletion(
          recordTime = u.recordTime,
          offset = offset,
          commandId = u.completionInfo.commandId,
          status = u.reasonTemplate.status,
          applicationId = u.completionInfo.applicationId,
          optSubmissionId = u.completionInfo.submissionId,
          optDeduplicationOffset = deduplicationOffset,
          optDeduplicationDurationSeconds = deduplicationDurationSeconds,
          optDeduplicationDurationNanos = deduplicationDurationNanos,
        ),
        submitters = u.completionInfo.actAs.toSet,
      ),
    )
  }

  private def updateToTransactionAccepted(offset: Offset, u: TransactionAccepted) = {
    val rawEvents = u.transaction.transaction
      .foldInExecutionOrder(List.empty[(NodeId, Node)])(
        exerciseBegin = (acc, nid, node) => ((nid -> node) :: acc, ChildrenRecursion.DoRecurse),
        // Rollback nodes are not indexed
        rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        leaf = (acc, nid, node) => (nid -> node) :: acc,
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
      .reverseIterator

    val blinding = u.blindingInfo.getOrElse(Blinding.blind(u.transaction))

    val events = rawEvents.collect {
      case (nodeId, create: Create) =>
        TransactionLogUpdate.CreatedEvent(
          eventOffset = offset,
          transactionId = u.transactionId,
          nodeIndex = nodeId.index,
          eventSequentialId = 0L,
          eventId = EventId(u.transactionId, nodeId),
          contractId = create.coid,
          ledgerEffectiveTime = u.transactionMeta.ledgerEffectiveTime,
          templateId = create.templateId,
          commandId = u.optCompletionInfo.map(_.commandId).getOrElse(""),
          workflowId = u.transactionMeta.workflowId.map(_.toString).getOrElse(""),
          contractKey =
            create.key.map(k => com.daml.lf.transaction.Versioned(create.version, k.key)),
          treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty),
          flatEventWitnesses = create.stakeholders,
          submitters = u.optCompletionInfo
            .map(_.actAs.toSet)
            .getOrElse(Set.empty),
          createArgument = com.daml.lf.transaction.Versioned(create.version, create.arg),
          createSignatories = create.signatories,
          createObservers = create.stakeholders.diff(create.signatories),
          createAgreementText = Some(create.agreementText).filter(_.nonEmpty),
        )
      case (nodeId, exercise: Exercise) =>
        TransactionLogUpdate.ExercisedEvent(
          eventOffset = offset,
          transactionId = u.transactionId,
          nodeIndex = nodeId.index,
          eventSequentialId = 0L,
          eventId = EventId(u.transactionId, nodeId),
          contractId = exercise.targetCoid,
          ledgerEffectiveTime = u.transactionMeta.ledgerEffectiveTime,
          templateId = exercise.templateId,
          commandId = u.optCompletionInfo.map(_.commandId).getOrElse(""),
          workflowId = u.transactionMeta.workflowId.map(_.toString).getOrElse(""),
          contractKey =
            exercise.key.map(k => com.daml.lf.transaction.Versioned(exercise.version, k.key)),
          treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty),
          flatEventWitnesses = if (exercise.consuming) exercise.stakeholders else Set.empty,
          submitters = u.optCompletionInfo
            .map(_.actAs.toSet)
            .getOrElse(Set.empty),
          choice = exercise.choiceId,
          actingParties = exercise.actingParties,
          children = exercise.children.iterator
            .map(EventId(u.transactionId, _).toLedgerString.toString)
            .toSeq,
          exerciseArgument = exercise.versionedChosenValue,
          exerciseResult = exercise.versionedExerciseResult,
          consuming = exercise.consuming,
        )
    }

    TransactionLogUpdate.TransactionAccepted(
      transactionId = u.transactionId,
      workflowId = u.transactionMeta.workflowId.getOrElse(""), // TODO check
      effectiveAt = u.transactionMeta.ledgerEffectiveTime,
      offset = offset,
      events = events.toVector,
      completionDetails = u.optCompletionInfo.map(completionInfo => {
        val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
          deduplicationInfo(completionInfo)

        CompletionDetails(
          CompletionFromTransaction.acceptedCompletion(
            recordTime = u.recordTime,
            offset = offset,
            commandId = completionInfo.commandId,
            transactionId = u.transactionId,
            applicationId = completionInfo.applicationId,
            optSubmissionId = completionInfo.submissionId,
            optDeduplicationOffset = deduplicationOffset,
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
          ),
          submitters = completionInfo.actAs.toSet,
        )
      }),
    )
  }

  private def deduplicationInfo(completionInfo: CompletionInfo) =
    completionInfo.optDeduplicationPeriod
      .map {
        case DeduplicationOffset(offset) =>
          (Some(offset.toHexString), None, None)
        case DeduplicationDuration(duration) =>
          (None, Some(duration.getSeconds), Some(duration.getNano))
      }
      .getOrElse((None, None, None))
}
