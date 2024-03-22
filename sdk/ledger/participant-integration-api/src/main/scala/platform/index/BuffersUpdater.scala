// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.index.BuffersUpdater._
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.{Contract, Key, Party}
import com.daml.scalautil.Statement.discard
import scala.util.chaining._

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Creates and manages a subscription to a transaction log updates source
  * (see [[LedgerDaoTransactionsReader.getTransactionLogUpdates]]
  * and uses it for updating:
  *    * The transactions buffer used for in-memory Ledger API serving.
  *    * The mutable contract state cache.
  *
  * @param subscribeToTransactionLogUpdates Subscribe to the transaction log updates stream starting at a specific
  *                                         `(Offset, EventSequentialId)`.
  * @param updateCaches Takes a `(Offset, TransactionLogUpdate)` pair and uses it to update the caches/buffers.
  * @param minBackoffStreamRestart Minimum back-off before restarting the transaction log updates stream.
  * @param sysExitWithCode Triggers a system exit (i.e. `sys.exit`) with a specific exit code.
  * @param mat The Akka materializer.
  * @param loggingContext The logging context.
  */
private[index] class BuffersUpdater(
    subscribeToTransactionLogUpdates: SubscribeToTransactionLogUpdates,
    updateCaches: (Offset, TransactionLogUpdate) => Unit,
    metrics: Metrics,
    minBackoffStreamRestart: FiniteDuration,
    cachesUpdaterExecutionContext: ExecutionContext,
    sysExitWithCode: Int => Unit,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends AutoCloseable {

  private val logger = ContextualizedLogger.get(getClass)

  private[index] val updaterIndex: AtomicReference[Option[(Offset, Long)]] =
    new AtomicReference(None)

  private val (transactionLogUpdatesKillSwitch, transactionLogUpdatesDone) =
    RestartSource
      .withBackoff(
        RestartSettings(
          minBackoff = minBackoffStreamRestart,
          maxBackoff = 10.seconds,
          randomFactor = 0.0,
        )
      )(() => subscribeToTransactionLogUpdates(updaterIndex.get))
      .async
      // The parallelism here must be limited to 1
      // since the order of the updates being applied to the caches must be preserved.
      .mapAsync(1) { case ((offset, eventSequentialId), update) =>
        Timed.future(
          metrics.daml.index.updateCaches,
          Future {
            updateCaches(offset, update)
            updaterIndex.set(Some(offset -> eventSequentialId))
            // The async caches update must be executed on a dedicated threadpool as it is
            // fundamental that it gets high execution priority even under heavy system load when
            // other threadpools are saturated.
          }(cachesUpdaterExecutionContext),
        )
      }
      .mapError { case NonFatal(e) =>
        logger.error("Error encountered when updating caches", e)
        e
      }
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.ignore)(Keep.both[UniqueKillSwitch, Future[Done]])
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
  }(ExecutionContext.parasitic)

  override def close(): Unit = {
    transactionLogUpdatesKillSwitch.shutdown()

    discard(Await.ready(transactionLogUpdatesDone, 10.seconds))
  }
}

private[index] object BuffersUpdater {
  type SubscribeToTransactionLogUpdates =
    Option[(Offset, Long)] => Source[((Offset, Long), TransactionLogUpdate), NotUsed]

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
    */
  def owner(
      subscribeToTransactionLogUpdates: SubscribeToTransactionLogUpdates,
      updateTransactionsBuffer: (Offset, TransactionLogUpdate) => Unit,
      updateMutableCache: Vector[ContractStateEvent] => Unit,
      toContractStateEvents: TransactionLogUpdate => Iterator[ContractStateEvent] =
        convertToContractStateEvents,
      metrics: Metrics,
      minBackoffStreamRestart: FiniteDuration = 100.millis,
      sysExitWithCode: Int => Unit = sys.exit(_),
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[BuffersUpdater] =
    for {
      cachesUpdaterExecutorService <- ResourceOwner.forExecutorService(() =>
        Executors.newSingleThreadExecutor(
          new Thread(_).tap(_.setName("ledger-api-caches-updater-thread"))
        )
      )
      cachesUpdaterExecutionContext = ExecutionContext.fromExecutor(cachesUpdaterExecutorService)
      buffersUpdater <- ResourceOwner.forCloseable(() =>
        new BuffersUpdater(
          subscribeToTransactionLogUpdates = subscribeToTransactionLogUpdates,
          updateCaches = (offset, transactionLogUpdate) => {
            updateTransactionsBuffer(offset, transactionLogUpdate)

            val contractStateEventsBatch = toContractStateEvents(transactionLogUpdate).toVector
            if (contractStateEventsBatch.nonEmpty) {
              updateMutableCache(contractStateEventsBatch)
            }
          },
          metrics = metrics,
          cachesUpdaterExecutionContext = cachesUpdaterExecutionContext,
          minBackoffStreamRestart = minBackoffStreamRestart,
          sysExitWithCode = sysExitWithCode,
        )(mat, loggingContext)
      )
    } yield buffersUpdater

  private[index] def convertToContractStateEvents(
      tx: TransactionLogUpdate
  ): Iterator[ContractStateEvent] =
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
        }
      case TransactionLogUpdate.LedgerEndMarker(eventOffset, eventSequentialId) =>
        Iterator(ContractStateEvent.LedgerEndMarker(eventOffset, eventSequentialId))
    }
}
