// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.common.domain.RegisterTopologyTransactionHandle
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.{DelayUtil, EitherTUtil, FutureUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, DomainAlias}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

class QueueBasedDomainOutboxX(
    domain: DomainAlias,
    val domainId: DomainId,
    val memberId: Member,
    val protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandle,
    val targetClient: DomainTopologyClientWithInit,
    val domainOutboxQueue: DomainOutboxQueue,
    val targetStore: TopologyStoreX[TopologyStoreId.DomainStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    val crypto: Crypto,
    batchSize: Int = 100,
    maybeObserverCloseable: Option[AutoCloseable] = None,
)(implicit executionContext: ExecutionContext)
    extends DomainOutboxCommon
    with QueueBasedDomainOutboxDispatchHelperX
    with FlagCloseable {

  protected def awaitTransactionObserved(
      transaction: GenericSignedTopologyTransactionX,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    TopologyStoreX.awaitTxObserved(targetClient, transaction, targetStore, timeout)

  protected def findPendingTransactions()(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {
    Future.successful(
      domainOutboxQueue
        .dequeue(batchSize)
    )
  }

  override protected def onClosed(): Unit = {
    maybeObserverCloseable.foreach(_.close())
    Lifecycle.close(handle)(logger)
    super.onClosed()
  }

  runOnShutdown_(new RunOnShutdown {
    override def name: String = "close-participant-topology-outbox"
    override def done: Boolean = idleFuture.get().forall(_.isCompleted)
    override def run(): Unit =
      idleFuture.get().foreach(_.trySuccess(UnlessShutdown.AbortedDueToShutdown))
  })(TraceContext.empty)

  def awaitIdle(
      timeout: Duration
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] = {
    // first, we wait until the idle future is idle again
    // this is the case when we've updated the queue state such that
    // there are no more topology transactions to read
    idleFuture
      .get()
      .map(x => FutureUnlessShutdown(x.future))
      .getOrElse(FutureUnlessShutdown.unit)
      .flatMap { _ =>
        // now, we've left the last transaction that got dispatched to the domain
        // in the last dispatched reference. we can now wait on the domain topology client
        // and domain store for it to appear.
        // as the transactions get sent sequentially we know that once the last transaction is out
        // we are idle again.
        lastDispatched.get().fold(FutureUnlessShutdown.pure(true)) { last =>
          awaitTransactionObserved(last, timeout)
        }
      }
  }

  private val isRunning = new AtomicBoolean(false)
  private val initialized = new AtomicBoolean(false)

  /** a future we provide that gets fulfilled once we are done dispatching */
  private val idleFuture = new AtomicReference[Option[Promise[UnlessShutdown[Unit]]]](None)
  private val lastDispatched =
    new AtomicReference[Option[GenericSignedTopologyTransactionX]](None)
  private def ensureIdleFutureIsSet(): Unit = idleFuture.updateAndGet {
    case None =>
      Some(Promise())
    case x => x
  }.discard

  // reflect both unsent and in-process transactions in the topology queue status
  def queueSize: Int =
    domainOutboxQueue.numUnsentTransactions + domainOutboxQueue.numInProcessTransactions

  private def hasUnsentTransactions: Boolean = domainOutboxQueue.numUnsentTransactions > 0

  def newTransactionsAddedToAuthorizedStore(
      asOf: CantonTimestamp,
      num: Int,
  ): FutureUnlessShutdown[Unit] = {
    ensureIdleFutureIsSet()
    kickOffFlush()
    FutureUnlessShutdown.unit
  }

  protected def notAlreadyPresent(
      transactions: Seq[GenericSignedTopologyTransactionX]
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {
    val doesNotAlreadyExistPredicate = (tx: GenericSignedTopologyTransactionX) =>
      targetStore.providesAdditionalSignatures(tx)
    filterTransactions(transactions, doesNotAlreadyExistPredicate)
  }

  def startup()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    performUnlessClosingEitherUSF(functionFullName) {
      if (hasUnsentTransactions) ensureIdleFutureIsSet()
      logger.debug(
        s"Resuming dispatching, pending=${hasUnsentTransactions}"
      )
      // run initial flush
      flush(initialize = true)
    }
  }

  protected def kickOffFlush(): Unit = {
    // It's fine to ignore shutdown because we do not await the future anyway.
    if (initialized.get()) {
      TraceContext.withNewTraceContext(implicit tc =>
        EitherTUtil.doNotAwait(flush().onShutdown(Either.unit), "domain outbox flusher")
      )
    }
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
        s"Marked flush as done. Current queue size: ${queueSize}. IsClosing: ${isClosing}"
      )
      if (hasUnsentTransactions && !isClosing) {
        if (delayRetry) {
          val delay = 10.seconds
          logger.debug(s"Kick off a new delayed flush in ${delay}")
          DelayUtil
            .delay(functionFullName, delay, this)
            .map { _ =>
              if (!isClosing) {
                logger.debug(s"About to kick off a delayed flush scheduled ${delay} ago")
                kickOffFlush()
              } else {
                logger.debug(
                  s"Queue-based outbox is now closing. Ignoring delayed flushed schedule ${delay} ago"
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

    logger.debug(s"Invoked flush with queue size ${queueSize}")

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
        val pendingAndApplicableF = performUnlessClosingF(functionFullName)(for {
          // find pending transactions
          pending <- findPendingTransactions()
          // filter out applicable
          applicable <- onlyApplicable(pending)
          _ = if (applicable.size != pending.size)
            logger.debug(
              s"applicable transactions: $applicable"
            )
          // not already present
          notPresent <- notAlreadyPresent(applicable)
          _ = if (notPresent.size != applicable.size)
            logger.debug(s"not already present transactions: $notPresent")
        } yield notPresent)

        val ret = for {
          notPresent <- EitherT.right(pendingAndApplicableF)

          _ = lastDispatched.set(notPresent.lastOption)
          // Try to convert if necessary the topology transactions for the required protocol version of the domain
          convertedTxs <- performUnlessClosingEitherU(functionFullName) {
            convertTransactions(notPresent)
          }
          // dispatch to domain
          _ <- dispatch(domain, transactions = convertedTxs)
          observed <- EitherT.right[String](
            // for x-nodes, we either receive
            // * TopologyTransactionsBroadcastX.State.Accepted: SendTracker returned Success
            // * TopologyTransactionsBroadcastX.State.Failed: SendTracker returned Timeout or Error
            // for all transactions in a submission batch.
            // Failed submissions are turned into a Left in dispatch. Therefore it's safe to await without additional checks.
            convertedTxs.headOption
              .map(awaitTransactionObserved(_, timeouts.unbounded.duration))
              // there were no transactions to wait for
              .getOrElse(FutureUnlessShutdown.pure(true))
          )
        } yield {
          if (!observed) {
            logger.warn("Did not observe transactions in target domain store.")
          }

          domainOutboxQueue.completeCycle()
          markDone()
        }

        EitherTUtil.onErrorOrFailureUnlessShutdown[String, Unit](
          errorHandler = either => {
            val errorDetails = either.fold(
              throwable => s"exception ${throwable.getMessage}",
              error => s"error $error",
            )
            logger.info(s"Requeuing and backing off due to $errorDetails")
            domainOutboxQueue.requeue()
            markDone(delayRetry = true)
          },
          shutdownHandler = () => {
            logger.info(s"Requeuing and stopping due to closing/domain-disconnect")
            domainOutboxQueue.requeue()
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
      domain: DomainAlias,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[messages.TopologyTransactionsBroadcastX.State]] =
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
                s"Attempting to push ${transactions.size} topology transactions to $domain, specifically: ${transactions}"
              )
            }
            FutureUtil.logOnFailureUnlessShutdown(
              handle.submit(transactions),
              s"Pushing topology transactions to $domain",
            )
          },
          AllExnRetryable,
        )
        .map { responses =>
          if (responses.length != transactions.length) {
            logger.error(
              s"Topology request contained ${transactions.length} txs, but I received responses for ${responses.length}"
            )
          }
          val responsesWithTransactions = responses.zip(transactions)
          if (logger.underlying.isDebugEnabled()) {
            logger.debug(
              s"$domain responded the following for the given topology transactions: $responsesWithTransactions"
            )
          }
          val failedResponses =
            responsesWithTransactions.collect {
              case (TopologyTransactionsBroadcastX.State.Failed, tx) => tx
            }

          Either.cond(
            failedResponses.isEmpty,
            responses,
            s"The domain $domain failed the following topology transactions: $failedResponses",
          )
        }
      EitherT(
        ret
      )
    }
}
