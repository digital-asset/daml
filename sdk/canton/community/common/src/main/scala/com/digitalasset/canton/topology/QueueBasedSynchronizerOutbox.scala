// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.RegisterTopologyTransactionHandle
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.canton.util.{DelayUtil, EitherTUtil, FutureUnlessShutdownUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Promise}

class QueueBasedSynchronizerOutbox(
    synchronizerAlias: SynchronizerAlias,
    val synchronizerId: SynchronizerId,
    val memberId: Member,
    val protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandle,
    val targetClient: SynchronizerTopologyClientWithInit,
    val synchronizerOutboxQueue: SynchronizerOutboxQueue,
    val targetStore: TopologyStore[TopologyStoreId.SynchronizerStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    val crypto: Crypto,
    broadcastBatchSize: PositiveInt,
    maybeObserverCloseable: Option[AutoCloseable] = None,
)(implicit executionContext: ExecutionContext)
    extends SynchronizerOutbox
    with QueueBasedSynchronizerOutboxDispatchHelper
    with FlagCloseable {

  protected def awaitTransactionObserved(
      transaction: GenericSignedTopologyTransaction,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    TopologyStore.awaitTxObserved(targetClient, transaction, targetStore, timeout)

  protected def findPendingTransactions()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    FutureUnlessShutdown.pure(
      synchronizerOutboxQueue
        .dequeue(broadcastBatchSize)
    )

  override protected def onClosed(): Unit = {
    val closeables = maybeObserverCloseable.toList ++ List(handle)
    LifeCycle.close(closeables*)(logger)
    super.onClosed()
  }

  runOnOrAfterClose_(new RunOnClosing {
    override def name: String = "close-participant-topology-outbox"
    override def done: Boolean = idleFuture.get().forall(_.isCompleted)
    override def run()(implicit traceContext: TraceContext): Unit =
      idleFuture.get().foreach(_.trySuccess(UnlessShutdown.AbortedDueToShutdown))
  })(TraceContext.empty)

  def awaitIdle(
      timeout: Duration
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    // first, we wait until the idle future is idle again
    // this is the case when we've updated the queue state such that
    // there are no more topology transactions to read
    idleFuture
      .get()
      .map(x => FutureUnlessShutdown(x.future))
      .getOrElse(FutureUnlessShutdown.unit)
      .flatMap { _ =>
        // now, we've left the last transaction that got dispatched to the synchronizer
        // in the last dispatched reference. we can now wait on the synchronizer topology client
        // and synchronizer store for it to appear.
        // as the transactions get sent sequentially we know that once the last transaction is out
        // we are idle again.
        lastDispatched.get().fold(FutureUnlessShutdown.pure(true)) { last =>
          awaitTransactionObserved(last, timeout)
        }
      }

  private val isRunning = new AtomicBoolean(false)
  private val initialized = new AtomicBoolean(false)

  /** a future we provide that gets fulfilled once we are done dispatching */
  private val idleFuture = new AtomicReference[Option[Promise[UnlessShutdown[Unit]]]](None)
  private val lastDispatched =
    new AtomicReference[Option[GenericSignedTopologyTransaction]](None)
  private def ensureIdleFutureIsSet(): Unit = idleFuture.updateAndGet {
    case None =>
      Some(Promise())
    case x => x
  }.discard

  // reflect both unsent and in-process transactions in the topology queue status
  def queueSize: Int =
    synchronizerOutboxQueue.numUnsentTransactions + synchronizerOutboxQueue.numInProcessTransactions

  private def hasUnsentTransactions: Boolean = synchronizerOutboxQueue.numUnsentTransactions > 0

  def newTransactionsAdded(
      asOf: CantonTimestamp,
      num: Int,
  ): FutureUnlessShutdown[Unit] = {
    ensureIdleFutureIsSet()
    kickOffFlush()
    FutureUnlessShutdown.unit
  }

  protected def notAlreadyPresent(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = {
    val doesNotAlreadyExistPredicate = (tx: GenericSignedTopologyTransaction) =>
      targetStore.providesAdditionalSignatures(tx)
    filterTransactions(transactions, doesNotAlreadyExistPredicate)
  }

  def startup()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    performUnlessClosingEitherUSF(functionFullName) {
      if (hasUnsentTransactions) ensureIdleFutureIsSet()
      logger.debug(
        s"Resuming dispatching, pending=$hasUnsentTransactions"
      )
      // run initial flush
      flush(initialize = true)
    }

  protected def kickOffFlush(): Unit =
    // It's fine to ignore shutdown because we do not await the future anyway.
    if (initialized.get()) {
      TraceContext.withNewTraceContext(implicit tc =>
        EitherTUtil.doNotAwait(
          flush().onShutdown(Either.unit),
          "synchronizer outbox flusher",
          level = Level.WARN,
        )
      )
    }

  protected def flush(initialize: Boolean = false)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    def markDone(delayRetry: Boolean = false): Unit = {
      isRunning.set(false)
      if (!hasUnsentTransactions) {
        idleFuture.getAndSet(None).foreach(_.trySuccess(UnlessShutdown.unit))
      }
      // if anything has been pushed in the meantime, we need to kick off a new flush
      logger.debug(
        s"Marked flush as done. Current queue size: $queueSize. IsClosing: $isClosing"
      )
      if (hasUnsentTransactions && !isClosing) {
        if (delayRetry) {
          val delay = 10.seconds
          logger.debug(s"Kick off a new delayed flush in $delay")
          DelayUtil
            .delay(functionFullName, delay, this)
            .map { _ =>
              if (!isClosing) {
                logger.debug(s"About to kick off a delayed flush scheduled $delay ago")
                kickOffFlush()
              } else {
                logger.debug(
                  s"Queue-based outbox is now closing. Ignoring delayed flushed schedule $delay ago"
                )
              }
            }
            .discard
        } else {
          kickOffFlush()
        }
      }
    }

    val transitionedToRunning = isRunning.compareAndSet(false, true)
    if (transitionedToRunning) {
      ensureIdleFutureIsSet()
    }

    logger.debug(s"Invoked flush with queue size $queueSize")

    if (isClosing) {
      logger.debug("Flush invoked in spite of closing")
      EitherT.rightT(())
    }
    // only flush if we are not running yet
    else if (!transitionedToRunning) {
      logger.debug("Another flush cycle is currently ongoing")
      EitherT.rightT(())
    } else {
      // mark as initialised (it's safe now for a concurrent thread to invoke flush as well)
      if (initialize)
        initialized.set(true)
      if (hasUnsentTransactions) {
        val pendingAndApplicableF = performUnlessClosingUSF(functionFullName)(for {
          // find pending transactions
          pending <- findPendingTransactions()
          // filter out applicable
          applicable <- onlyApplicable(pending)
          _ = if (applicable.sizeIs != pending.size)
            logger.debug(
              s"applicable transactions: $applicable"
            )
          // not already present
          notPresent <- notAlreadyPresent(applicable)
          _ = if (notPresent.sizeIs != applicable.size)
            logger.debug(s"not already present transactions: $notPresent")
        } yield notPresent)

        val ret = for {
          notPresent <- EitherT.right(pendingAndApplicableF)

          _ = lastDispatched.set(notPresent.lastOption)
          // Try to convert if necessary the topology transactions for the required protocol version of the synchronizer
          convertedTxs <- performUnlessClosingEitherUSF(functionFullName) {
            convertTransactions(notPresent)
          }
          // dispatch to synchronizer
          _ <- dispatch(synchronizerAlias, transactions = convertedTxs)
          observed <- EitherT.right[String](
            // for x-nodes, we either receive
            // * TopologyTransactionsBroadcast.State.Accepted: SendTracker returned Success
            // * TopologyTransactionsBroadcast.State.Failed: SendTracker returned Timeout or Error
            // for all transactions in a submission batch.
            // Failed submissions are turned into a Left in dispatch. Therefore it's safe to await without additional checks.
            convertedTxs.headOption
              .map(awaitTransactionObserved(_, timeouts.unbounded.duration))
              // there were no transactions to wait for
              .getOrElse(FutureUnlessShutdown.pure(true))
          )
        } yield {
          if (!observed) {
            logger.warn("Did not observe transactions in target synchronizer store.")
          }

          synchronizerOutboxQueue.completeCycle(observed)
          markDone()
        }

        EitherTUtil.onErrorOrFailureUnlessShutdown[String, Unit](
          errorHandler = either => {
            val errorDetails = either.fold(
              throwable => s"exception ${throwable.getMessage}",
              error => s"error $error",
            )
            logger.info(s"Requeuing and backing off due to $errorDetails")
            synchronizerOutboxQueue.requeue()
            markDone(delayRetry = true)
          },
          shutdownHandler = () => {
            logger.info(s"Requeuing and stopping due to closing/synchronizer-disconnect")
            synchronizerOutboxQueue.requeue()
            markDone()
          },
        )(ret)
      } else {
        logger.debug("Nothing pending. Marking as done.")
        markDone()
        EitherT.rightT(())
      }
    }
  }

  protected def dispatch(
      synchronizerAlias: SynchronizerAlias,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[messages.TopologyTransactionsBroadcast.State]] =
    if (transactions.isEmpty) EitherT.rightT(Seq.empty)
    else {
      implicit val success = retry.Success.always
      val ret = retry
        .Backoff(
          logger,
          this,
          timeouts.unbounded.retries(1.second),
          1.second,
          10.seconds,
          "push topology transaction",
        )
        .unlessShutdown(
          {
            if (logger.underlying.isDebugEnabled()) {
              logger.debug(
                s"Attempting to push ${transactions.size} topology transactions to $synchronizerAlias, specifically: $transactions"
              )
            }
            FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
              handle.submit(transactions),
              s"Pushing topology transactions to $synchronizerAlias",
            )
          },
          AllExceptionRetryPolicy,
        )
        .map { responses =>
          if (responses.sizeCompare(transactions) != 0) {
            logger.error(
              s"Topology request contained ${transactions.length} txs, but I received responses for ${responses.length}"
            )
          }
          val responsesWithTransactions = responses.zip(transactions)
          if (logger.underlying.isDebugEnabled()) {
            logger.debug(
              s"$synchronizerAlias responded the following for the given topology transactions: $responsesWithTransactions"
            )
          }
          val failedResponses =
            responsesWithTransactions.collect {
              case (TopologyTransactionsBroadcast.State.Failed, tx) => tx
            }

          Either.cond(
            failedResponses.isEmpty,
            responses,
            s"The synchronizer $synchronizerAlias failed the following topology transactions: $failedResponses",
          )
        }

      EitherT(ret)
    }
}
