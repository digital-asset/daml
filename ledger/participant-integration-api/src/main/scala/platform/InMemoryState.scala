// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
}
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.{StringInterningView, UpdatingStringInterningView}
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

/** This class encapsulates and mutates the caching in-memory data structures used for
  * serving Ledger API requests.
  *
  * TODO Implement one-at-a-time synchronization for the async public methods in this class.
  */
private[platform] class InMemoryState(
    val ledgerEndCache: MutableLedgerEndCache,
    val contractStateCaches: ContractStateCaches,
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer,
    val stringInterningView: StringInterningView,
    val dispatcherState: DispatcherState,
) {
  implicit private val offsetEquals: Equal[Offset] = Equal.equalA[Offset]
  private val logger = ContextualizedLogger.get(getClass)
  @volatile private var _dirty = false

  /** Returns true if the state has been initialized.
    *
    * This method is designed to synchronize start-up of the IndexService only
    * after the first initialization triggered by the Indexer.
    * (see [[com.daml.platform.indexer.JdbcIndexer.Factory.initialized()]]
    */
  final def initialized: Boolean = dispatcherState.isRunning

  /** (Re-)initializes the participant in-memory state to a specific ledger end.
    *
    * The state is conditionally (re-)initialized depending on:
    *    - whether is it not initialized (i.e. the dispatcher is not running)
    *    - whether the ledger end cache or the last interned string id have changed
    *    since the last known state. Note that we additionally check the string interning view's last id,
    *    since it is updated outside this class (see [[com.daml.platform.indexer.parallel.ParallelIndexerSubscription]]).
    *    - whether it has been marked dirty. On a successful (re-)initialization, the state is marked not dirty again.
    *
    * NOTE:
    *   - This method is not thread-safe. Calling it concurrently with itself
    * or with [[update()]] leads to undefined behavior.
    *   - A failure in this method leaves the state dirty.
    */
  final def initializeTo(ledgerEnd: LedgerEnd, executionContext: ExecutionContext)(
      updateStringInterningView: (UpdatingStringInterningView, LedgerEnd) => Future[Unit]
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    implicit val ec: ExecutionContext = executionContext
    conditionallyInitialize(ledgerEnd) { () =>
      dirtyWriteSection {
        for {
          // Ensure only async execution
          _ <- Future.unit
          // First stop the active dispatcher (if exists) to ensure
          // termination of existing Ledger API subscriptions and to also ensure
          // that new Ledger API subscriptions racing with `initializeTo`
          // do not observe an inconsistent state.
          _ <- dispatcherState.stopDispatcher()
          // Reset the string interning view to the latest ledger end
          _ <- updateStringInterningView(stringInterningView, ledgerEnd)
          // Reset the Ledger API caches to the latest ledger end
          _ <- Future {
            contractStateCaches.reset(ledgerEnd.lastOffset)
            inMemoryFanoutBuffer.flush()
            ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
          }
          // Start a new Ledger API offset dispatcher
          _ = dispatcherState.startDispatcher(ledgerEnd.lastOffset)
        } yield ()
      }
    }
  }

  /** Updates the in-memory state.
    *
    * On failure, the state is marked as dirty, ensuring re-initialization on [[InMemoryState.initializeTo()]].
    *
    * NOTE:
    *   - This method is not thread-safe. Calling it concurrently with itself
    * or with [[InMemoryState.update()]] leads to undefined behavior.
    *   - A failure in this method leaves the state dirty.
    *
    * @param updates The update batch used to update the the in-memory fan-out buffer and the contract state cache.
    * @param lastOffset The last offset ingested by the Indexer.
    * @param lastEventSequentialId The last event sequential id ingested by the Indexer.
    * @param executionContext The execution context.
    * @param loggingContext The logging context.
    */
  final def update(
      updates: Vector[(TransactionLogUpdate, Vector[ContractStateEvent])],
      lastOffset: Offset,
      lastEventSequentialId: Long,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    if (!_dirty) {
      dirtyWriteSection {
        Future {
          // Update caches
          updates.foreach { case (transaction, contractStateEventsBatch) =>
            // TODO LLP: Batch update caches
            inMemoryFanoutBuffer.push(transaction.offset, transaction)
            if (contractStateEventsBatch.nonEmpty) {
              contractStateCaches.push(contractStateEventsBatch)
            }
          }

          // Update ledger end
          ledgerEndCache.set((lastOffset, lastEventSequentialId))
          // the order here is very important: first we need to make data available for point-wise lookups
          // and SQL queries, and only then we can make it available on the streams.
          // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
          dispatcherState.getDispatcher.signalNewHead(lastOffset)
          logger.info(s"Updated ledger end cache at offset $lastOffset - $lastEventSequentialId")
        }
      }
    } else {
      dirtyUpdateAttempted(lastOffset, lastEventSequentialId)
    }

  private def dirtyUpdateAttempted(offset: Offset, eventSequentialId: Long)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = {
    val logMessage =
      s"Attempted to update a dirty in-memory state at offset ($offset) and event sequential id ($eventSequentialId)."

    logger.error(logMessage)
    Future.failed(new IllegalStateException(logMessage))
  }

  private def dirtyWriteSection(mutateState: => Future[Unit]): Future[Unit] = {
    // On update failure, we can't make assumptions on which part of the state was correctly updated.
    // Thus, set the in-memory state dirty at the beginning of a mutating operation unset it on success.
    _dirty = true
    mutateState.map { _ =>
      _dirty = false
      ()
    }(ExecutionContext.parasitic)
  }

  private def conditionallyInitialize(
      ledgerEnd: LedgerEnd
  )(initialize: () => Future[Unit])(implicit loggingContext: LoggingContext): Future[Unit] =
    if (!initialized) {
      logger.info(s"Initializing  in-memory state to ledger end: $ledgerEnd.")
      initialize()
    } else if (_dirty) {
      logger.warn(
        s"Dirty state detected on initialization. Re-setting the in-memory state to ledger end: $ledgerEnd."
      )
      initialize()
    } else {
      val currentLedgerEndCache = ledgerEndCache()
      if (
        loggedNonEquality[Offset](
          expected = ledgerEnd.lastOffset,
          actual = currentLedgerEndCache._1,
          name = "ledger end cache offset",
        ) ||
        loggedNonEquality[Long](
          expected = ledgerEnd.lastEventSeqId,
          actual = currentLedgerEndCache._2,
          name = "ledger end cache event sequential id",
        ) ||
        loggedNonEquality[Int](
          expected = ledgerEnd.lastStringInterningId,
          actual = stringInterningView.lastId,
          name = "last string interning id",
        )
      ) {
        logger.warn(
          s"Re-initialization ledger end mismatches in-memory state reference. Re-setting the in-memory state ledger end: $ledgerEnd."
        )
        initialize()
      } else {
        logger.info("Coherency checks passed. In-memory state re-initialization skipped.")
        Future.unit
      }
    }

  private def loggedNonEquality[T](expected: T, actual: T, name: String)(implicit
      loggingContext: LoggingContext,
      safeEquals: Equal[T],
  ): Boolean =
    (!safeEquals.equal(expected, actual))
      .tap { notEqual =>
        if (notEqual)
          logger.warn(
            s"Mismatching state ledger end references for $name: expected ($expected) vs actual ($actual)."
          )
      }
}

object InMemoryState {
  def owner(
      apiStreamShutdownTimeout: Duration,
      bufferedStreamsPageSize: Int,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Int,
      metrics: Metrics,
      executionContext: ExecutionContext,
  )(implicit loggingContext: LoggingContext): ResourceOwner[InMemoryState] = {
    val initialLedgerEnd = LedgerEnd.beforeBegin
    val initialLedgerEndCache = MutableLedgerEndCache()
      .tap(
        _.set((initialLedgerEnd.lastOffset, initialLedgerEnd.lastEventSeqId))
      )

    val contractStateCaches = ContractStateCaches.build(
      initialLedgerEnd.lastOffset,
      maxContractStateCacheSize,
      maxContractKeyStateCacheSize,
      metrics,
    )(executionContext, loggingContext)

    for {
      dispatcherState <- DispatcherState.owner(apiStreamShutdownTimeout)
    } yield new InMemoryState(
      ledgerEndCache = initialLedgerEndCache,
      dispatcherState = dispatcherState,
      contractStateCaches = contractStateCaches,
      inMemoryFanoutBuffer = new InMemoryFanoutBuffer(
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        maxBufferedChunkSize = bufferedStreamsPageSize,
      ),
      stringInterningView = new StringInterningView,
    )
  }
}
