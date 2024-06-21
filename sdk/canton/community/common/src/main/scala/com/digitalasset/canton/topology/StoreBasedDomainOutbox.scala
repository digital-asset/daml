// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.{
  RegisterTopologyTransactionHandle,
  SequencerBasedRegisterTopologyTransactionHandle,
}
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.Crypto
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
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.chaining.*

class StoreBasedDomainOutbox(
    domain: DomainAlias,
    val domainId: DomainId,
    val memberId: Member,
    val protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandle,
    val targetClient: DomainTopologyClientWithInit,
    val authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStore[TopologyStoreId.DomainStore],
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: Crypto,
    broadcastBatchSize: PositiveInt,
    maybeObserverCloseable: Option[AutoCloseable] = None,
    override protected val futureSupervisor: FutureSupervisor,
)(implicit override protected val executionContext: ExecutionContext)
    extends DomainOutbox
    with DomainOutboxDispatch
    with HasFutureSupervision
    with StoreBasedDomainOutboxDispatchHelper {

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
    // this is the case when we've updated the dispatching watermark such that
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
        false,
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

  final def newTransactionsAddedToAuthorizedStore(
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
    val loadWatermarksF = performUnlessClosingF(functionFullName)(for {
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
      // run initial flush
      _ <- flush(initialize = true)
    } yield ()
  }

  private def kickOffFlush(): Unit = {
    // It's fine to ignore shutdown because we do not await the future anyway.
    if (initialized.get()) {
      TraceContext.withNewTraceContext(implicit tc =>
        EitherTUtil.doNotAwait(flush().onShutdown(Either.unit), "domain outbox flusher")
      )
    }
  }

  private def flush(initialize: Boolean = false)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
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
    if (cur.running) {
      EitherT.rightT(())
    } else {
      // mark as initialised (it's safe now for a concurrent thread to invoke flush as well)
      if (initialize)
        initialized.set(true)
      if (cur.hasPending) {
        val pendingAndApplicableF = performUnlessClosingF(functionFullName)(for {
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
          // Try to convert if necessary the topology transactions for the required protocol version of the domain
          convertedTxs <- performUnlessClosingEitherUSF(functionFullName) {
            convertTransactions(applicable)
          }
          // dispatch to domain
          responses <- dispatch(domain, transactions = convertedTxs)
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
              logger.warn("Did not observe transactions in target domain store.")
            }
          // update watermark according to responses
          _ <- EitherT.right[String](
            updateWatermark(pending, applicable, responses)
          )
        } yield ()
        ret.transform {
          case x @ Left(_) =>
            markDone(delayRetry = true)
            x
          case x @ Right(_) =>
            markDone()
            x
        }
      } else {
        markDone()
        EitherT.rightT(())
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
            s"Topology transaction ${topologyTransaction(item)} got ${response}. Will not update watermark."
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
      performUnlessClosingF(functionFullName)(targetStore.updateDispatchingWatermark(newWatermark))
    } else {
      FutureUnlessShutdown.unit
    }
  }

  private def awaitTransactionObserved(
      transaction: GenericSignedTopologyTransaction,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    supervisedUS(s"Waiting for transaction $transaction to be observed")(
      TopologyStore.awaitTxObserved(targetClient, transaction, targetStore, timeout)
    )

  private def findPendingTransactions(watermarks: Watermarks)(implicit
      traceContext: TraceContext
  ): Future[PendingTransactions] =
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
  ): Future[Option[(SequencedTime, EffectiveTime)]] = authorizedStore.maxTimestamp()

  override protected def onClosed(): Unit = {
    maybeObserverCloseable.foreach(_.close())
    Lifecycle.close(handle)(logger)
    super.onClosed()
  }
}

trait DomainOutboxHandle extends FlagCloseable {
  def queueSize: Int
  def startup()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

abstract class DomainOutbox extends DomainOutboxHandle {
  def awaitIdle(
      timeout: Duration
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]

  def targetClient: DomainTopologyClientWithInit

  def newTransactionsAddedToAuthorizedStore(
      asOf: CantonTimestamp,
      num: Int,
  ): FutureUnlessShutdown[Unit]
}

/** Dynamic version of a TopologyManagerObserver allowing observers
  * to be dynamically added or removed while the TopologyManager stays up.
  * (This is helpful for mediator node failover where domain-outboxes are started
  * and closed.)
  */
class DomainOutboxDynamicObserver(val loggerFactory: NamedLoggerFactory)
    extends TopologyManagerObserver
    with NamedLogging {
  private val outboxRef = new AtomicReference[Option[DomainOutbox]](None)

  override def addedNewTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    outboxRef.get.fold(FutureUnlessShutdown.unit)(
      _.newTransactionsAddedToAuthorizedStore(timestamp, transactions.size)
    )
  }

  def addObserver(ob: DomainOutbox)(implicit traceContext: TraceContext): Unit = {
    val previous = outboxRef.getAndSet(ob.some)
    if (previous.nonEmpty) {
      logger.warn("Expecting previously added domain outbox to have been removed")
    }
  }

  def removeObserver(): Unit = outboxRef.set(None)
}

class DomainOutboxFactory(
    domainId: DomainId,
    memberId: Member,
    authorizedTopologyManager: AuthorizedTopologyManager,
    domainTopologyManager: DomainTopologyManager,
    crypto: Crypto,
    topologyConfig: TopologyConfig,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val authorizedObserverRef = new SingleUseCell[DomainOutboxDynamicObserver]
  private val domainObserverRef = new SingleUseCell[DomainOutboxDynamicObserver]

  def create(
      protocolVersion: ProtocolVersion,
      targetTopologyClient: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
      clock: Clock,
      domainLoggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): DomainOutboxHandle = {
    val handle = new SequencerBasedRegisterTopologyTransactionHandle(
      sequencerClient,
      domainId,
      memberId,
      clock,
      topologyConfig,
      protocolVersion,
      timeouts,
      domainLoggerFactory,
    )
    if (authorizedObserverRef.isEmpty) {
      authorizedObserverRef
        .putIfAbsent(
          new DomainOutboxDynamicObserver(loggerFactory).tap(authorizedTopologyManager.addObserver)
        )
        .discard
    }
    val authorizedObserver =
      authorizedObserverRef.getOrElse(throw new IllegalStateException("Must have observer"))

    val storeBasedDomainOutbox = new StoreBasedDomainOutbox(
      DomainAlias(domainId.uid.toLengthLimitedString),
      domainId,
      memberId = memberId,
      protocolVersion = protocolVersion,
      handle = handle,
      targetClient = targetTopologyClient,
      authorizedStore = authorizedTopologyManager.store,
      targetStore = domainTopologyManager.store,
      timeouts = timeouts,
      loggerFactory = domainLoggerFactory,
      crypto = crypto,
      broadcastBatchSize = topologyConfig.broadcastBatchSize,
      maybeObserverCloseable = new AutoCloseable {
        override def close(): Unit = authorizedObserver.removeObserver()
      }.some,
      futureSupervisor = futureSupervisor,
    )
    authorizedObserver.addObserver(storeBasedDomainOutbox)

    if (domainObserverRef.isEmpty) {
      domainObserverRef
        .putIfAbsent(
          new DomainOutboxDynamicObserver(loggerFactory).tap(domainTopologyManager.addObserver)
        )
        .discard
    }
    val domainObserver =
      domainObserverRef.getOrElse(throw new IllegalStateException("Must have observer"))

    val queueBasedDomainOutbox =
      new QueueBasedDomainOutbox(
        DomainAlias(domainId.uid.toLengthLimitedString),
        domainId,
        memberId = memberId,
        protocolVersion = protocolVersion,
        handle = handle,
        targetClient = targetTopologyClient,
        domainOutboxQueue = domainTopologyManager.outboxQueue,
        targetStore = domainTopologyManager.store,
        timeouts = timeouts,
        loggerFactory = domainLoggerFactory,
        crypto = crypto,
        broadcastBatchSize = topologyConfig.broadcastBatchSize,
        maybeObserverCloseable = new AutoCloseable {
          override def close(): Unit = authorizedObserver.removeObserver()
        }.some,
      )

    domainObserver.addObserver(queueBasedDomainOutbox)

    new DomainOutboxHandle {
      override def startup()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, String, Unit] =
        storeBasedDomainOutbox.startup().flatMap(_ => queueBasedDomainOutbox.startup())

      override def queueSize: Int =
        storeBasedDomainOutbox.queueSize + queueBasedDomainOutbox.queueSize

      override protected def onClosed(): Unit = {
        Lifecycle.close(storeBasedDomainOutbox, queueBasedDomainOutbox)(
          DomainOutboxFactory.this.logger
        )
      }

      override protected def timeouts: ProcessingTimeout = DomainOutboxFactory.this.timeouts

      override protected def logger: TracedLogger = DomainOutboxFactory.this.logger
    }
  }
}

class DomainOutboxFactorySingleCreate(
    domainId: DomainId,
    memberId: Member,
    authorizedTopologyManager: AuthorizedTopologyManager,
    domainTopologyManager: DomainTopologyManager,
    crypto: Crypto,
    topologyConfig: TopologyConfig,
    override val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
) extends DomainOutboxFactory(
      domainId,
      memberId,
      authorizedTopologyManager,
      domainTopologyManager,
      crypto,
      topologyConfig,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    with FlagCloseable {
  val outboxRef = new SingleUseCell[DomainOutboxHandle]

  def createOnlyOnce(
      protocolVersion: ProtocolVersion,
      targetTopologyClient: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
      clock: Clock,
      domainLoggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): DomainOutboxHandle = {
    outboxRef.get.foreach { outbox =>
      ErrorUtil.invalidState(s"DomainOutbox was already created. This is a bug.")(
        ErrorLoggingContext(
          domainLoggerFactory.getTracedLogger(getClass),
          LoggingContextWithTrace(domainLoggerFactory),
        )
      )
    }

    create(
      protocolVersion,
      targetTopologyClient,
      sequencerClient,
      clock,
      domainLoggerFactory,
    ).tap(outbox => outboxRef.putIfAbsent(outbox).discard)
  }

  override protected def onClosed(): Unit =
    Lifecycle.close(outboxRef.get.toList*)(logger)
}

final case class PendingTransactions(
    transactions: Seq[GenericSignedTopologyTransaction],
    newWatermark: CantonTimestamp,
)
