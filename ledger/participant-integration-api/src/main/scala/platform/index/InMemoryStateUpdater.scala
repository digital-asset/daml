// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.codahale.metrics.InstrumentedExecutorService
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.participant.state.v2.Update.TransactionAccepted
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Node.{Create, Exercise}
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{Node, NodeId}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.{Contract, Key, ParticipantInMemoryState, Party}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

private[index] class InMemoryStateUpdater(
    prepareUpdatesParallelism: Int,
    prepareUpdatesExecutionContext: ExecutionContext,
    updateCachesExecutionContext: ExecutionContext,
)(
    updateCaches: Vector[TransactionLogUpdate] => Unit,
    updateToTransactionAccepted: (Offset, Update.TransactionAccepted) => TransactionLogUpdate,
    updateLedgerEnd: (Offset, Long) => Unit,
) {

  def flow: Flow[(Vector[(Offset, Update)], Long), Unit, NotUsed] =
    Flow[(Vector[(Offset, Update)], Long)]
      .filter(_._1.nonEmpty)
      .mapAsync(prepareUpdatesParallelism) { case (batch, lastEventSequentialId) =>
        Future {
          val transactionsAcceptedBatch =
            batch.collect { case (offset, u: TransactionAccepted) =>
              updateToTransactionAccepted(offset, u)
            }
          transactionsAcceptedBatch -> (batch.last._1 -> lastEventSequentialId)
        }(prepareUpdatesExecutionContext)
      }
      .async
      .mapAsync(1) { case (updates, ledgerEnd) =>
        Future {
          updateCaches(updates)
          updateLedgerEnd(ledgerEnd._1, ledgerEnd._2)
        }(updateCachesExecutionContext)
      }
}

object InMemoryStateUpdater {
  private val logger = ContextualizedLogger.get(getClass)

  def owner(
      participantInMemoryState: ParticipantInMemoryState,
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
  )(
    updateCaches = updateCaches(participantInMemoryState),
    updateToTransactionAccepted = updateToTransactionAccepted,
    updateLedgerEnd = updateLedgerEnd(participantInMemoryState),
  )

  private def updateCaches(participantInMemoryState: ParticipantInMemoryState)(
      updates: Vector[TransactionLogUpdate]
  ): Unit =
    updates.foreach {
      case transaction: TransactionLogUpdate.Transaction =>
        // TODO LLP: Batch update caches
        participantInMemoryState.transactionsBuffer.push(transaction.offset, transaction)

        val contractStateEventsBatch = convertToContractStateEvents(transaction)
        if (contractStateEventsBatch.nonEmpty) {
          participantInMemoryState.contractStateCaches.push(contractStateEventsBatch)
        }

      case _: TransactionLogUpdate.LedgerEndMarker =>
        // TODO LLP: Remove TransactionLogUpdate.LedgerEndMarker
        ()
    }

  private def updateLedgerEnd(
      participantInMemoryState: ParticipantInMemoryState
  )(lastOffset: Offset, lastEventSequentialId: Long)(implicit
      loggingContext: LoggingContext
  ): Unit = {
    participantInMemoryState.ledgerEndCache.set((lastOffset, lastEventSequentialId))
    // the order here is very important: first we need to make data available for point-wise lookups
    // and SQL queries, and only then we can make it available on the streams.
    // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
    participantInMemoryState.dispatcher().signalNewHead(lastOffset)
    logger.debug(s"Updated ledger end at offset $lastOffset - $lastEventSequentialId")
  }

  private def convertToContractStateEvents(
      tx: TransactionLogUpdate
  ): Vector[ContractStateEvent] =
    tx match {
      case tx: TransactionLogUpdate.Transaction =>
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

  private def updateToTransactionAccepted(
      offset: Offset,
      u: TransactionAccepted,
  ): TransactionLogUpdate.Transaction = {
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
          workflowId = u.transactionMeta.workflowId.getOrElse(""),
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
          workflowId = u.transactionMeta.workflowId.getOrElse(""),
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
            .map(EventId(u.transactionId, _).toLedgerString)
            .toSeq,
          exerciseArgument = exercise.versionedChosenValue,
          exerciseResult = exercise.versionedExerciseResult,
          consuming = exercise.consuming,
          interfaceId = exercise.interfaceId,
        )
    }

    TransactionLogUpdate.Transaction(
      transactionId = u.transactionId,
      workflowId = u.transactionMeta.workflowId.getOrElse(""), // TODO LLP: check
      effectiveAt = u.transactionMeta.ledgerEffectiveTime,
      offset = offset,
      events = events.toVector,
    )
  }
}
