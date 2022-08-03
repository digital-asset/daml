// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.ledger.api.DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{CompletionInfo, Update}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref.HexString
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Node.{Create, Exercise}
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{Node, NodeId}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.index.InMemoryStateUpdater.UpdaterFlow
import com.daml.platform.store.CompletionFromTransaction
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.CompletionDetails
import com.daml.platform.{Contract, InMemoryState, Key, Party}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

final class InMemoryStateUpdater(
    prepareUpdatesParallelism: Int,
    prepareUpdatesExecutionContext: ExecutionContext,
    updateCachesExecutionContext: ExecutionContext,
    metrics: Metrics,
)(
    updateCaches: Vector[TransactionLogUpdate] => Unit,
    convertTransactionAccepted: (
        Offset,
        Update.TransactionAccepted,
    ) => TransactionLogUpdate.TransactionAccepted,
    convertTransactionRejected: (
        Offset,
        Update.CommandRejected,
    ) => TransactionLogUpdate.TransactionRejected,
    updateLedgerEnd: (Offset, Long) => Unit,
) {

  // TODO LLP: Considering directly returning this flow instead of the wrapper
  def flow: UpdaterFlow =
    Flow[(Vector[(Offset, Update)], Long)]
      .filter(_._1.nonEmpty)
      .mapAsync(prepareUpdatesParallelism) { case (batch, lastEventSequentialId) =>
        Future {
          val updatesBatch =
            batch.collect {
              case (offset, u: Update.TransactionAccepted) => convertTransactionAccepted(offset, u)
              case (offset, u: Update.CommandRejected) => convertTransactionRejected(offset, u)
            }
          (updatesBatch, batch.last._1, lastEventSequentialId)
        }(prepareUpdatesExecutionContext)
      }
      .async
      .mapAsync(1) { case (updates, lastOffset, lastEventSequentialId) =>
        Future {
          updateCaches(updates)
          updateLedgerEnd(lastOffset, lastEventSequentialId)
          metrics.daml.index.ledgerEndSequentialId.updateValue(lastEventSequentialId)
        }(updateCachesExecutionContext)
      }
}

private[platform] object InMemoryStateUpdater {
  type UpdaterFlow = Flow[(Vector[(Offset, Update)], Long), Unit, NotUsed]

  private val logger = ContextualizedLogger.get(getClass)

  def owner(
      inMemoryState: InMemoryState,
      prepareUpdatesParallelism: Int,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): ResourceOwner[InMemoryStateUpdater] = for {
    prepareUpdatesExecutor <- ResourceOwner.forExecutorService(() =>
      new InstrumentedExecutorService(
        Executors.newWorkStealingPool(prepareUpdatesParallelism),
        metrics.registry,
        metrics.daml.lapi.threadpool.indexBypass.prepareUpdates,
      )
    )
    updateCachesExecutor <- ResourceOwner.forExecutorService(() =>
      new InstrumentedExecutorService(
        Executors.newFixedThreadPool(1),
        metrics.registry,
        metrics.daml.lapi.threadpool.indexBypass.updateInMemoryState,
      )
    )
  } yield new InMemoryStateUpdater(
    prepareUpdatesParallelism = prepareUpdatesParallelism,
    prepareUpdatesExecutionContext = ExecutionContext.fromExecutorService(prepareUpdatesExecutor),
    updateCachesExecutionContext = ExecutionContext.fromExecutorService(updateCachesExecutor),
    metrics = metrics,
  )(
    updateCaches = updateCaches(inMemoryState),
    convertTransactionAccepted = convertTransactionAccepted,
    convertTransactionRejected = convertTransactionRejected,
    updateLedgerEnd = updateLedgerEnd(inMemoryState),
  )

  private def updateCaches(inMemoryState: InMemoryState)(
      updates: Vector[TransactionLogUpdate]
  ): Unit =
    updates.foreach { transaction: TransactionLogUpdate =>
      // TODO LLP: Batch update caches
      inMemoryState.transactionsBuffer.push(transaction.offset, transaction)

      val contractStateEventsBatch = convertToContractStateEvents(transaction)
      if (contractStateEventsBatch.nonEmpty) {
        inMemoryState.contractStateCaches.push(contractStateEventsBatch)
      }
    }

  private def updateLedgerEnd(
      inMemoryState: InMemoryState
  )(lastOffset: Offset, lastEventSequentialId: Long)(implicit
      loggingContext: LoggingContext
  ): Unit = {
    inMemoryState.ledgerEndCache.set((lastOffset, lastEventSequentialId))
    // the order here is very important: first we need to make data available for point-wise lookups
    // and SQL queries, and only then we can make it available on the streams.
    // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
    inMemoryState.dispatcherState.getDispatcher.signalNewHead(lastOffset)
    logger.debug(s"Updated ledger end at offset $lastOffset - $lastEventSequentialId")
  }

  private def convertToContractStateEvents(
      tx: TransactionLogUpdate
  ): Vector[ContractStateEvent] =
    tx match {
      case tx: TransactionLogUpdate.TransactionAccepted =>
        tx.events.iterator.collect {
          case createdEvent: TransactionLogUpdate.CreatedEvent =>
            ContractStateEvent.Created(
              contractId = createdEvent.contractId,
              contract = Contract(
                template = createdEvent.templateId,
                arg = createdEvent.createArgument,
                agreementText = createdEvent.createAgreementText.getOrElse(""),
              ),
              globalKey = createdEvent.contractKey.map(k =>
                Key.assertBuild(createdEvent.templateId, k.unversioned)
              ),
              ledgerEffectiveTime = createdEvent.ledgerEffectiveTime,
              stakeholders = createdEvent.flatEventWitnesses.map(Party.assertFromString),
              eventOffset = createdEvent.eventOffset,
              eventSequentialId = createdEvent.eventSequentialId,
            )
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
            ContractStateEvent.Archived(
              contractId = exercisedEvent.contractId,
              globalKey = exercisedEvent.contractKey.map(k =>
                Key.assertBuild(exercisedEvent.templateId, k.unversioned)
              ),
              stakeholders = exercisedEvent.flatEventWitnesses.map(Party.assertFromString),
              eventOffset = exercisedEvent.eventOffset,
              eventSequentialId = exercisedEvent.eventSequentialId,
            )
        }.toVector
      case _ => Vector.empty
    }

  private def convertTransactionAccepted(
      offset: Offset,
      txAccepted: Update.TransactionAccepted,
  ): TransactionLogUpdate.TransactionAccepted = {
    // TODO LLP: Extract in common functionality together with duplicated code in [[UpdateToDbDto]]
    val rawEvents = txAccepted.transaction.transaction
      .foldInExecutionOrder(List.empty[(NodeId, Node)])(
        exerciseBegin = (acc, nid, node) => ((nid -> node) :: acc, ChildrenRecursion.DoRecurse),
        // Rollback nodes are not indexed
        rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        leaf = (acc, nid, node) => (nid -> node) :: acc,
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
      .reverseIterator

    // TODO LLP: Deduplicate blinding info computation with the work done in [[UpdateToDbDto]]
    val blinding = txAccepted.blindingInfo.getOrElse(Blinding.blind(txAccepted.transaction))

    val events = rawEvents.collect {
      case (nodeId, create: Create) =>
        TransactionLogUpdate.CreatedEvent(
          eventOffset = offset,
          transactionId = txAccepted.transactionId,
          nodeIndex = nodeId.index,
          eventSequentialId = 0L,
          eventId = EventId(txAccepted.transactionId, nodeId),
          contractId = create.coid,
          ledgerEffectiveTime = txAccepted.transactionMeta.ledgerEffectiveTime,
          templateId = create.templateId,
          commandId = txAccepted.optCompletionInfo.map(_.commandId).getOrElse(""),
          workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
          contractKey =
            create.key.map(k => com.daml.lf.transaction.Versioned(create.version, k.key)),
          treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty),
          flatEventWitnesses = create.stakeholders,
          submitters = txAccepted.optCompletionInfo
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
          transactionId = txAccepted.transactionId,
          nodeIndex = nodeId.index,
          eventSequentialId = 0L,
          eventId = EventId(txAccepted.transactionId, nodeId),
          contractId = exercise.targetCoid,
          ledgerEffectiveTime = txAccepted.transactionMeta.ledgerEffectiveTime,
          templateId = exercise.templateId,
          commandId = txAccepted.optCompletionInfo.map(_.commandId).getOrElse(""),
          workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
          contractKey =
            exercise.key.map(k => com.daml.lf.transaction.Versioned(exercise.version, k.key)),
          treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty),
          flatEventWitnesses = if (exercise.consuming) exercise.stakeholders else Set.empty,
          submitters = txAccepted.optCompletionInfo
            .map(_.actAs.toSet)
            .getOrElse(Set.empty),
          choice = exercise.choiceId,
          actingParties = exercise.actingParties,
          children = exercise.children.iterator
            .map(EventId(txAccepted.transactionId, _).toLedgerString)
            .toSeq,
          exerciseArgument = exercise.versionedChosenValue,
          exerciseResult = exercise.versionedExerciseResult,
          consuming = exercise.consuming,
          interfaceId = exercise.interfaceId,
        )
    }

    val completionDetails = txAccepted.optCompletionInfo
      .map { completionInfo =>
        val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
          deduplicationInfo(completionInfo)

        CompletionDetails(
          CompletionFromTransaction.acceptedCompletion(
            recordTime = txAccepted.recordTime,
            offset = offset,
            commandId = completionInfo.commandId,
            transactionId = txAccepted.transactionId,
            applicationId = completionInfo.applicationId,
            optSubmissionId = completionInfo.submissionId,
            optDeduplicationOffset = deduplicationOffset,
            optDeduplicationDurationSeconds = deduplicationDurationSeconds,
            optDeduplicationDurationNanos = deduplicationDurationNanos,
          ),
          submitters = completionInfo.actAs.toSet,
        )
      }

    TransactionLogUpdate.TransactionAccepted(
      transactionId = txAccepted.transactionId,
      commandId = txAccepted.optCompletionInfo.map(_.commandId).getOrElse(""),
      workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
      effectiveAt = txAccepted.transactionMeta.ledgerEffectiveTime,
      offset = offset,
      events = events.toVector,
      completionDetails = completionDetails,
    )
  }

  private def convertTransactionRejected(
      offset: Offset,
      u: Update.CommandRejected,
  ): TransactionLogUpdate.TransactionRejected = {
    val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
      deduplicationInfo(u.completionInfo)

    TransactionLogUpdate.TransactionRejected(
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

  private def deduplicationInfo(
      completionInfo: CompletionInfo
  ): (Option[HexString], Option[Long], Option[Int]) =
    completionInfo.optDeduplicationPeriod
      .map {
        case DeduplicationOffset(offset) =>
          (Some(offset.toHexString), None, None)
        case DeduplicationDuration(duration) =>
          (None, Some(duration.getSeconds), Some(duration.getNano))
      }
      .getOrElse((None, None, None))
}
