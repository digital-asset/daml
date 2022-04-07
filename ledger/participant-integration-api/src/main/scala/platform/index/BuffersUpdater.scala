// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.index.BuffersUpdater._
import com.daml.platform.store.appendonlydao.events.{Contract, ContractStateEvent, Key, Party}
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.scalautil.Statement.discard

import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Creates and manages a subscription to a transaction log updates source
  *
  * @param subscribeToTransactionLogUpdates Subscribe to the transaction log updates stream
  * @param updateCaches Takes a `(Offset, TransactionLogUpdate)` pair and uses it to update the caches/buffers.
  * @param sysExitWithCode Triggers a system exit (i.e. `sys.exit`) with a specific exit code.
  * @param mat The Akka materializer.
  * @param loggingContext The logging context.
  * @param executionContext The execution context.
  */
class BuffersUpdater(
    subscribeToTransactionLogUpdates: SubscribeToTransactionLogUpdates,
    updateCaches: (Offset, TransactionLogUpdate) => Unit,
    metrics: Metrics,
    sysExitWithCode: Int => Unit,
)(implicit mat: Materializer, loggingContext: LoggingContext, executionContext: ExecutionContext)
    extends AutoCloseable {
  private val buffersUpdaterExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  private val logger = ContextualizedLogger.get(getClass)

  private val (transactionLogUpdatesKillSwitch, transactionLogUpdatesDone) =
    subscribeToTransactionLogUpdates()
      .mapAsync(1) { case ((offset, eventSequentialId), update) =>
        Timed.future(
          metrics.daml.commands.updateBuffers,
          Future {
            updateCaches(offset, update)
            logger.debug(s"Updated caches at offset $offset - $eventSequentialId")
          }(buffersUpdaterExecutionContext),
        )
      }
      .mapError { case NonFatal(e) =>
        logger.error("Error encountered when updating caches", e)
        e
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

  transactionLogUpdatesDone.onComplete {
    // AbruptStageTerminationException is propagated even when streams materializer/actorSystem are terminated normally
    case Success(_) | Failure(_: AbruptStageTerminationException) =>
      logger.info(s"Finished transaction log updates stream")
    case Failure(ex) =>
      logger.error(
        "The transaction log updates stream encountered a non-recoverable error and will shutdown",
        ex,
      )
      sysExitWithCode(1)
  }

  override def close(): Unit = {
    transactionLogUpdatesKillSwitch.shutdown()

    discard(Await.ready(transactionLogUpdatesDone, 10.seconds))
  }
}

object BuffersUpdater {
  type SubscribeToTransactionLogUpdates =
    () => Source[((Offset, Long), TransactionLogUpdate), NotUsed]

  /** [[BuffersUpdater]] convenience builder.
    *
    * @param subscribeToTransactionLogUpdates Subscribe to the transaction log updates stream starting at a specific
    *                                         `(Offset, EventSequentialId)`.
    * @param updateTransactionsBuffer Trigger externally the update of the transactions buffer.
    * @param updateMutableCache Trigger externally the update of the mutable contract state cache.
    *                           (see [[com.daml.platform.store.cache.MutableCacheBackedContractStore]])
    * @param toContractStateEvents Converts [[TransactionLogUpdate]]s to [[ContractStateEvent]]s.
    * @param minBackoffStreamRestart Minimum back-off before restarting the transaction log updates stream.
    * @param sysExitWithCode Triggers a system exit (i.e. `sys.exit`) with a specific exit code.
    * @param mat The Akka materializer.
    * @param loggingContext The logging context.
    * @param executionContext The execution context.
    */
  def apply(
      subscribeToTransactionLogUpdates: SubscribeToTransactionLogUpdates,
      updateTransactionsBuffer: (Offset, TransactionLogUpdate) => Unit,
      updateCompletionsBuffer: (Offset, TransactionLogUpdate) => Unit,
      updateMutableCache: ContractStateEvent => Unit,
      toContractStateEvents: TransactionLogUpdate => Iterator[ContractStateEvent] =
        convertToContractStateEvents,
      updateLedgerApiLedgerEnd: LedgerEnd => Unit,
      executionContext: ExecutionContext,
      metrics: Metrics,
      sysExitWithCode: Int => Unit = sys.exit(_),
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
  ): BuffersUpdater = new BuffersUpdater(
    subscribeToTransactionLogUpdates = subscribeToTransactionLogUpdates,
    updateCaches = (offset, transactionLogUpdate) => {
      updateTransactionsBuffer(offset, transactionLogUpdate)
      updateCompletionsBuffer(offset, transactionLogUpdate)
      toContractStateEvents(transactionLogUpdate).foreach(updateMutableCache)
      transactionLogUpdate match {
        case TransactionLogUpdate.TransactionAccepted(_, _, _, offset, events, _) =>
          updateLedgerApiLedgerEnd(LedgerEnd(offset, events.last.eventSequentialId, -1))
        case TransactionLogUpdate.SubmissionRejected(offset, lastEventSeqId, _) =>
          updateLedgerApiLedgerEnd(LedgerEnd(offset, lastEventSeqId, -1))
        case TransactionLogUpdate.LedgerEndMarker(offset, lastEventSeqId) =>
          updateLedgerApiLedgerEnd(LedgerEnd(offset, lastEventSeqId, -1))
      }
    },
    metrics = metrics,
    sysExitWithCode = sysExitWithCode,
  )(mat, loggingContext, executionContext)

  def convertToContractStateEvents(
      tx: TransactionLogUpdate
  ): Iterator[ContractStateEvent] =
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
        }
      case TransactionLogUpdate.LedgerEndMarker(eventOffset, eventSequentialId) =>
        Iterator(ContractStateEvent.LedgerEndMarker(eventOffset, eventSequentialId))
      case _: TransactionLogUpdate.SubmissionRejected =>
        // Nothing to update the cache on rejections
        Iterator.empty
    }
}
