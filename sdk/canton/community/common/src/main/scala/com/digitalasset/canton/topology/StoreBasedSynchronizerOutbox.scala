// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.{
  RegisterTopologyTransactionHandle,
  SequencerBasedRegisterTopologyTransactionHandle,
}
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{DelayUtil, EitherTUtil, ErrorUtil, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.chaining.*

class StoreBasedSynchronizerOutbox(
    synchronizerAlias: SynchronizerAlias,
    val synchronizerId: SynchronizerId,
    val memberId: Member,
    val protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandle,
    val targetClient: SynchronizerTopologyClientWithInit,
    val authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStore[TopologyStoreId.SynchronizerStore],
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: SynchronizerCrypto,
    broadcastBatchSize: PositiveInt,
    maybeObserverCloseable: Option[AutoCloseable] = None,
    override protected val futureSupervisor: FutureSupervisor,
)(implicit override protected val executionContext: ExecutionContext)
    extends SynchronizerOutbox
    with SynchronizerOutboxDispatch
    with HasFutureSupervision
    with StoreBasedSynchronizerOutboxDispatchHelper {

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
    // this is the case when we've updated the dispatching watermark such that
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

  private case class Watermarks(
      queuedApprox: Int,
      running: Boolean,
      authorized: CantonTimestamp, // last time a transaction was added to the store
      dispatched: CantonTimestamp,
  ) {
    def updateAuthorized(updated: CantonTimestamp, queuedNum: Int): Watermarks = {
      val ret = copy(
        authorized = authorized.max(updated),
        queuedApprox = queuedApprox + queuedNum,
      )
      if (ret.hasPending) {
        idleFuture.updateAndGet {
          case None =>
            Some(Promise())
          case x => x
        }
      }
      ret
    }

    def hasPending: Boolean = authorized != dispatched

    def done(): Watermarks = {
      if (!hasPending) {
        idleFuture.getAndSet(None).foreach(_.trySuccess(UnlessShutdown.unit))
      }
      copy(running = false)
    }

    def setRunning(): Watermarks = {
      if (!running) {
        ensureIdleFutureIsSet()
      }
      copy(running = true)
    }

  }

  private val watermarks =
    new AtomicReference[Watermarks](
      Watermarks(
        0,
        running = false,
        CantonTimestamp.MinValue,
        CantonTimestamp.MinValue,
      )
    )
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

  final def queueSize: Int = watermarks.get().queuedApprox

  final def newTransactionsAdded(
      asOf: CantonTimestamp,
      num: Int,
  ): FutureUnlessShutdown[Unit] = {
    watermarks.updateAndGet(_.updateAuthorized(asOf, num)).discard
    kickOffFlush()
    FutureUnlessShutdown.unit
  }

  final def startup()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val loadWatermarksF = performUnlessClosingUSF(functionFullName)(for {
      // find the current target watermark
      watermarkTsO <- targetStore.currentDispatchingWatermark
      watermarkTs = watermarkTsO.getOrElse(CantonTimestamp.MinValue)
      authorizedTsO <- maxAuthorizedStoreTimestamp()
      authorizedTs = authorizedTsO
        .map { case (_, effectiveTime) => effectiveTime.value }
        .getOrElse(CantonTimestamp.MinValue)
      // update cached watermark
    } yield {
      val cur = watermarks.updateAndGet { c =>
        val newAuthorized = c.authorized.max(authorizedTs)
        val newDispatched = c.dispatched.max(watermarkTs)
        val next = c.copy(
          // queuing statistics during startup will be a bit off, we just ensure that we signal that we have something in our queue
          // we might improve by querying the store, checking for the number of pending tx
          queuedApprox = if (newAuthorized == newDispatched) c.queuedApprox else c.queuedApprox + 1,
          authorized = newAuthorized,
          dispatched = newDispatched,
        )
        if (next.hasPending) ensureIdleFutureIsSet()
        next
      }
      logger.debug(
        s"Resuming dispatching, pending=${cur.hasPending}, authorized=${cur.authorized}, dispatched=${cur.dispatched}"
      )
      ()
    })
    for {
      // load current authorized timestamp and watermark
      _ <- EitherT.right(loadWatermarksF)
      // run initial flush in the background
      _ = flushAsync(initialize = true)
    } yield ()
  }

  private def kickOffFlush(): Unit =
    if (initialized.get()) {
      TraceContext.withNewTraceContext(implicit tc => flushAsync())
    }

  private def flushAsync(initialize: Boolean = false)(implicit
      traceContext: TraceContext
  ): Unit = {
    def markDone(delayRetry: Boolean = false): Unit = {
      val updated = watermarks.getAndUpdate(_.done())
      // if anything has been pushed in the meantime, we need to kick off a new flush
      if (updated.hasPending) {
        if (delayRetry) {
          // kick off new flush in the background
          DelayUtil.delay(functionFullName, 10.seconds, this).map(_ => kickOffFlush()).discard
        } else {
          kickOffFlush()
        }
      }
    }

    val cur = watermarks.getAndUpdate(_.setRunning())

    // only flush if we are not running yet
    if (!cur.running) {
      // mark as initialised (it's safe now for a concurrent thread to invoke flush as well)
      if (initialize)
        initialized.set(true)
      if (cur.hasPending) {
        val pendingAndApplicableF = performUnlessClosingUSF(functionFullName)(for {
          // find pending transactions
          pending <- findPendingTransactions(cur)
          // filter out applicable
          applicablePotentiallyPresent <- onlyApplicable(pending.transactions)
          // not already present
          applicable <- notAlreadyPresent(applicablePotentiallyPresent)
        } yield (pending, applicable))
        val ret = for {
          pendingAndApplicable <- EitherT.right(pendingAndApplicableF)
          (pending, applicable) = pendingAndApplicable
          _ = lastDispatched.set(applicable.lastOption)
          // Try to convert if necessary the topology transactions for the required protocol version of the synchronizer
          convertedTxs <- performUnlessClosingEitherUSF(functionFullName) {
            convertTransactions(applicable)
          }
          // dispatch to synchronizer
          responses <- dispatch(synchronizerAlias, transactions = convertedTxs)
          observed <- EitherT.right(
            // we either receive accepted or failed for all transactions in a submission batch.
            // failed submissions are turned into a Left in dispatch. Therefore it's safe to await without additional checks.
            convertedTxs.headOption
              .map(awaitTransactionObserved(_, timeouts.unbounded.duration))
              // there were no transactions to wait for
              .getOrElse(FutureUnlessShutdown.pure(true))
          )
          _ =
            if (!observed) {
              logger.warn("Did not observe transactions in target synchronizer store.")
            }
          // update watermark according to responses
          _ <- EitherT.right[String](
            updateWatermark(pending, applicable, responses)
          )
        } yield ()

        // It's fine to ignore shutdown because we do not await the future anyway.
        EitherTUtil.doNotAwaitUS(
          ret.transform { x =>
            markDone(delayRetry = x.isLeft)
            x
          },
          "synchronizer outbox flusher",
          failLevel = Level.WARN,
        )
      } else {
        markDone()
      }
    }
  }

  private def updateWatermark(
      found: PendingTransactions,
      applicable: Seq[GenericSignedTopologyTransaction],
      responses: Seq[TopologyTransactionsBroadcast.State],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val valid = applicable.zipWithIndex.zip(responses).foldLeft(true) {
      case (valid, ((item, idx), response)) =>
        if (!isExpectedState(response)) {
          logger.warn(
            s"Topology transaction ${topologyTransaction(item)} got $response. Will not update watermark."
          )
          false
        } else valid
    }
    if (valid) {
      val newWatermark = found.newWatermark
      watermarks.updateAndGet { c =>
        c.copy(
          // this will ensure that we have a queue count of at least 1 during catchup
          queuedApprox =
            if (c.authorized != newWatermark)
              Math.max(c.queuedApprox - found.transactions.length, 1)
            else 0,
          dispatched = newWatermark,
        )
      }.discard
      logger.debug(s"Updating dispatching watermark to $newWatermark")
      performUnlessClosingUSF(functionFullName)(
        targetStore.updateDispatchingWatermark(newWatermark)
      )
    } else {
      FutureUnlessShutdown.unit
    }
  }

  private def awaitTransactionObserved(
      transaction: GenericSignedTopologyTransaction,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    supervisedUS(
      s"Waiting for transaction $transaction to be observed",
      timeouts.topologyChangeWarnDelay.duration,
    )(
      TopologyStore.awaitTxObserved(targetClient, transaction, targetStore, timeout)
    )

  private def findPendingTransactions(watermarks: Watermarks)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[PendingTransactions] =
    authorizedStore
      .findDispatchingTransactionsAfter(
        timestampExclusive = watermarks.dispatched,
        limit = Some(broadcastBatchSize.value),
      )
      .map(storedTransactions =>
        PendingTransactions(
          storedTransactions.result.map(_.transaction),
          storedTransactions.result.map(_.validFrom.value).fold(watermarks.dispatched)(_ max _),
        )
      )

  private def maxAuthorizedStoreTimestamp()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    authorizedStore.maxTimestamp(SequencedTime.MaxValue, includeRejected = true)

  override protected def onClosed(): Unit = {
    val closeables = maybeObserverCloseable.toList ++ List(handle)
    LifeCycle.close(closeables*)(logger)
    super.onClosed()
  }
}

trait SynchronizerOutboxHandle extends FlagCloseable {
  def queueSize: Int
  def startup()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

abstract class SynchronizerOutbox extends SynchronizerOutboxHandle {
  def awaitIdle(
      timeout: Duration
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]

  def targetClient: SynchronizerTopologyClientWithInit

  def newTransactionsAdded(
      asOf: CantonTimestamp,
      num: Int,
  ): FutureUnlessShutdown[Unit]
}

/** Dynamic version of a TopologyManagerObserver allowing observers to be dynamically added or
  * removed while the TopologyManager stays up. (This is helpful for mediator node failover where
  * synchronizer-outboxes are started and closed.)
  */
class SynchronizerOutboxDynamicObserver(val loggerFactory: NamedLoggerFactory)
    extends TopologyManagerObserver
    with NamedLogging {
  private val outboxRef = new AtomicReference[Option[SynchronizerOutbox]](None)

  override def addedNewTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    outboxRef.get.fold(FutureUnlessShutdown.unit)(
      _.newTransactionsAdded(timestamp, transactions.size)
    )

  def addObserver(ob: SynchronizerOutbox)(implicit traceContext: TraceContext): Unit = {
    val previous = outboxRef.getAndSet(ob.some)
    if (previous.nonEmpty) {
      logger.warn("Expecting previously added synchronizer outbox to have been removed")
    }
  }

  def removeObserver(): Unit = outboxRef.set(None)
}

class SynchronizerOutboxFactory(
    synchronizerId: SynchronizerId,
    memberId: Member,
    authorizedTopologyManager: AuthorizedTopologyManager,
    synchronizerTopologyManager: SynchronizerTopologyManager,
    crypto: SynchronizerCrypto,
    topologyConfig: TopologyConfig,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val authorizedObserverRef = new SingleUseCell[SynchronizerOutboxDynamicObserver]
  private val synchronizerObserverRef = new SingleUseCell[SynchronizerOutboxDynamicObserver]

  def create(
      protocolVersion: ProtocolVersion,
      targetTopologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      timeTracker: SynchronizerTimeTracker,
      clock: Clock,
      synchronizerLoggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): SynchronizerOutboxHandle = {
    val handle = new SequencerBasedRegisterTopologyTransactionHandle(
      sequencerClient,
      synchronizerId,
      memberId,
      timeTracker,
      clock,
      topologyConfig,
      protocolVersion,
      timeouts,
      synchronizerLoggerFactory,
    )
    if (authorizedObserverRef.isEmpty) {
      authorizedObserverRef
        .putIfAbsent(
          new SynchronizerOutboxDynamicObserver(loggerFactory)
            .tap(authorizedTopologyManager.addObserver)
        )
        .discard
    }
    val authorizedObserver =
      authorizedObserverRef.getOrElse(throw new IllegalStateException("Must have observer"))

    val storeBasedSynchronizerOutbox = new StoreBasedSynchronizerOutbox(
      SynchronizerAlias(synchronizerId.uid.toLengthLimitedString),
      synchronizerId,
      memberId = memberId,
      protocolVersion = protocolVersion,
      handle = handle,
      targetClient = targetTopologyClient,
      authorizedStore = authorizedTopologyManager.store,
      targetStore = synchronizerTopologyManager.store,
      timeouts = timeouts,
      loggerFactory = synchronizerLoggerFactory,
      crypto = crypto,
      broadcastBatchSize = topologyConfig.broadcastBatchSize,
      maybeObserverCloseable = new AutoCloseable {
        override def close(): Unit = authorizedObserver.removeObserver()
      }.some,
      futureSupervisor = futureSupervisor,
    )
    authorizedObserver.addObserver(storeBasedSynchronizerOutbox)

    if (synchronizerObserverRef.isEmpty) {
      synchronizerObserverRef
        .putIfAbsent(
          new SynchronizerOutboxDynamicObserver(loggerFactory)
            .tap(synchronizerTopologyManager.addObserver)
        )
        .discard
    }
    val synchronizerObserver =
      synchronizerObserverRef.getOrElse(throw new IllegalStateException("Must have observer"))

    val queueBasedSynchronizerOutbox =
      new QueueBasedSynchronizerOutbox(
        SynchronizerAlias(synchronizerId.uid.toLengthLimitedString),
        synchronizerId,
        memberId = memberId,
        protocolVersion = protocolVersion,
        handle = handle,
        targetClient = targetTopologyClient,
        synchronizerOutboxQueue = synchronizerTopologyManager.outboxQueue,
        targetStore = synchronizerTopologyManager.store,
        timeouts = timeouts,
        loggerFactory = synchronizerLoggerFactory,
        crypto = crypto,
        broadcastBatchSize = topologyConfig.broadcastBatchSize,
        maybeObserverCloseable = new AutoCloseable {
          override def close(): Unit = authorizedObserver.removeObserver()
        }.some,
      )

    synchronizerObserver.addObserver(queueBasedSynchronizerOutbox)

    new SynchronizerOutboxHandle {
      override def startup()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, String, Unit] =
        storeBasedSynchronizerOutbox.startup().flatMap(_ => queueBasedSynchronizerOutbox.startup())

      override def queueSize: Int =
        storeBasedSynchronizerOutbox.queueSize + queueBasedSynchronizerOutbox.queueSize

      override protected def onClosed(): Unit =
        LifeCycle.close(storeBasedSynchronizerOutbox, queueBasedSynchronizerOutbox)(
          SynchronizerOutboxFactory.this.logger
        )

      override protected def timeouts: ProcessingTimeout = SynchronizerOutboxFactory.this.timeouts

      override protected def logger: TracedLogger = SynchronizerOutboxFactory.this.logger
    }
  }
}

class SynchronizerOutboxFactorySingleCreate(
    synchronizerId: SynchronizerId,
    memberId: Member,
    authorizedTopologyManager: AuthorizedTopologyManager,
    synchronizerTopologyManager: SynchronizerTopologyManager,
    crypto: SynchronizerCrypto,
    topologyConfig: TopologyConfig,
    override val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
) extends SynchronizerOutboxFactory(
      synchronizerId,
      memberId,
      authorizedTopologyManager,
      synchronizerTopologyManager,
      crypto,
      topologyConfig,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    with FlagCloseable {
  val outboxRef = new SingleUseCell[SynchronizerOutboxHandle]

  def createOnlyOnce(
      protocolVersion: ProtocolVersion,
      targetTopologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      timeTracker: SynchronizerTimeTracker,
      clock: Clock,
      synchronizerLoggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): SynchronizerOutboxHandle = {
    outboxRef.get.foreach { _ =>
      ErrorUtil.invalidState(s"SynchronizerOutbox was already created. This is a bug.")(
        ErrorLoggingContext(
          synchronizerLoggerFactory.getTracedLogger(getClass),
          LoggingContextWithTrace(synchronizerLoggerFactory),
        )
      )
    }

    create(
      protocolVersion,
      targetTopologyClient,
      sequencerClient,
      timeTracker,
      clock,
      synchronizerLoggerFactory,
    ).tap(outbox => outboxRef.putIfAbsent(outbox).discard)
  }

  override protected def onClosed(): Unit =
    LifeCycle.close(outboxRef.get.toList*)(logger)
}

final case class PendingTransactions(
    transactions: Seq[GenericSignedTopologyTransaction],
    newWatermark: CantonTimestamp,
)
