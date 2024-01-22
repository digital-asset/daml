// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.common.domain.{
  RegisterTopologyTransactionHandleCommon,
  SequencerBasedRegisterTopologyTransactionHandleX,
}
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyXConfig}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.protocol.messages.{
  RegisterTopologyTransactionResponseResult,
  TopologyTransactionsBroadcastX,
}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.{DomainTopologyClient, DomainTopologyClientWithInit}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreCommon,
  TopologyStoreId,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransactionX,
  TopologyChangeOpX,
  TopologyMappingX,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{DelayUtil, EitherTUtil, ErrorUtil, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, DomainAlias}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.chaining.*

class StoreBasedDomainOutbox(
    domain: DomainAlias,
    domainId: DomainId,
    memberId: Member,
    protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandleCommon[
      GenericSignedTopologyTransaction,
      RegisterTopologyTransactionResponseResult.State,
    ],
    targetClient: DomainTopologyClientWithInit,
    val authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStore[TopologyStoreId.DomainStore],
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    crypto: Crypto,
    // TODO(#9270) clean up how we parameterize our nodes
    batchSize: Int = 100,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends StoreBasedDomainOutboxCommon[
      GenericSignedTopologyTransaction,
      RegisterTopologyTransactionResponseResult.State,
      RegisterTopologyTransactionHandleCommon[
        GenericSignedTopologyTransaction,
        RegisterTopologyTransactionResponseResult.State,
      ],
      TopologyStore[TopologyStoreId.DomainStore],
    ](
      domain,
      domainId,
      memberId,
      protocolVersion,
      targetClient,
      timeouts,
      loggerFactory,
      crypto,
      syncTransactionAfterDispatch = false,
      futureSupervisor,
    )
    with DomainOutboxDispatchHelperOld {
  override protected def awaitTransactionObserved(
      client: DomainTopologyClient,
      transaction: GenericSignedTopologyTransaction,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    supervisedUS(s"Waiting for transaction $transaction to be observed")(
      TopologyStore.awaitTxObserved(targetClient, transaction, targetStore, timeout)
    )

  override protected def findPendingTransactions(
      watermarks: Watermarks
  )(implicit
      traceContext: TraceContext
  ): Future[PendingTransactions[GenericSignedTopologyTransaction]] =
    authorizedStore
      .findDispatchingTransactionsAfter(
        timestampExclusive = watermarks.dispatched,
        limit = Some(batchSize),
      )
      .map(storedTransactions =>
        PendingTransactions(
          storedTransactions.result.map(_.transaction),
          storedTransactions.result.map(_.validFrom.value).fold(watermarks.dispatched)(_ max _),
        )
      )

  override def maxAuthorizedStoreTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] =
    authorizedStore.timestamp(useStateStore =
      false // can't use TopologyStore.maxTimestamp which uses state-store
    )
}

class StoreBasedDomainOutboxX(
    domain: DomainAlias,
    domainId: DomainId,
    memberId: Member,
    protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandleCommon[
      GenericSignedTopologyTransactionX,
      TopologyTransactionsBroadcastX.State,
    ],
    targetClient: DomainTopologyClientWithInit,
    val authorizedStore: TopologyStoreX[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStoreX[TopologyStoreId.DomainStore],
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    crypto: Crypto,
    batchSize: Int = 100,
    maybeObserverCloseable: Option[AutoCloseable] = None,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends StoreBasedDomainOutboxCommon[
      GenericSignedTopologyTransactionX,
      TopologyTransactionsBroadcastX.State,
      RegisterTopologyTransactionHandleCommon[
        GenericSignedTopologyTransactionX,
        TopologyTransactionsBroadcastX.State,
      ],
      TopologyStoreX[TopologyStoreId.DomainStore],
    ](
      domain,
      domainId,
      memberId,
      protocolVersion,
      targetClient,
      timeouts,
      loggerFactory,
      crypto,
      syncTransactionAfterDispatch = true,
      futureSupervisor = futureSupervisor,
    )
    with StoreBasedDomainOutboxDispatchHelperX {
  override protected def awaitTransactionObserved(
      client: DomainTopologyClient,
      transaction: GenericSignedTopologyTransactionX,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    supervisedUS(s"Waiting for transaction $transaction to be observed")(
      TopologyStoreX.awaitTxObserved(targetClient, transaction, targetStore, timeout)
    )

  override protected def findPendingTransactions(watermarks: Watermarks)(implicit
      traceContext: TraceContext
  ): Future[PendingTransactions[GenericSignedTopologyTransactionX]] =
    authorizedStore
      .findDispatchingTransactionsAfter(
        timestampExclusive = watermarks.dispatched,
        limit = Some(batchSize),
      )
      .map(storedTransactions =>
        PendingTransactions(
          storedTransactions.result.map(_.transaction),
          storedTransactions.result.map(_.validFrom.value).fold(watermarks.dispatched)(_ max _),
        )
      )

  override def maxAuthorizedStoreTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = authorizedStore.maxTimestamp()

  override protected def onClosed(): Unit = {
    maybeObserverCloseable.foreach(_.close())
    Lifecycle.close(handle)(logger)
    super.onClosed()
  }
}

trait DomainOutboxHandle extends DomainOutboxStatus with FlagCloseable {
  def startup()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

abstract class DomainOutboxCommon extends DomainOutboxHandle {
  def awaitIdle(
      timeout: Duration
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]

  def targetClient: DomainTopologyClientWithInit

  def newTransactionsAddedToAuthorizedStore(
      asOf: CantonTimestamp,
      num: Int,
  ): FutureUnlessShutdown[Unit]
}

abstract class StoreBasedDomainOutboxCommon[
    TX,
    State,
    +H <: RegisterTopologyTransactionHandleCommon[TX, State],
    +DTS <: TopologyStoreCommon[TopologyStoreId.DomainStore, ?, ?, TX],
](
    domain: DomainAlias,
    val domainId: DomainId,
    val memberId: Member,
    val protocolVersion: ProtocolVersion,
    val targetClient: DomainTopologyClientWithInit,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: Crypto,
    syncTransactionAfterDispatch: Boolean,
    override protected val futureSupervisor: FutureSupervisor,
)(implicit override protected val executionContext: ExecutionContext)
    extends DomainOutboxCommon
    with DomainOutboxDispatch[TX, State, H, DTS]
    with HasFutureSupervision {
  this: DomainOutboxDispatchStoreSpecific[TX, State] =>

  def handle: H
  def targetStore: DTS

  runOnShutdown_(new RunOnShutdown {
    override def name: String = "close-participant-topology-outbox"
    override def done: Boolean = idleFuture.get().forall(_.isCompleted)
    override def run(): Unit =
      idleFuture.get().foreach(_.trySuccess(UnlessShutdown.AbortedDueToShutdown))
  })(TraceContext.empty)

  protected def awaitTransactionObserved(
      client: DomainTopologyClient,
      transaction: TX,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]

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
          awaitTransactionObserved(targetClient, last, timeout)
        }
      }
  }

  protected case class Watermarks(
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
    new AtomicReference[Option[TX]](None)
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

  def maxAuthorizedStoreTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]]

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

  protected final def kickOffFlush(): Unit = {
    // It's fine to ignore shutdown because we do not await the future anyway.
    if (initialized.get()) {
      TraceContext.withNewTraceContext(implicit tc =>
        EitherTUtil.doNotAwait(flush().onShutdown(Either.unit), "domain outbox flusher")
      )
    }
  }

  protected def findPendingTransactions(
      watermarks: Watermarks
  )(implicit traceContext: TraceContext): Future[PendingTransactions[TX]]

  protected final def flush(initialize: Boolean = false)(implicit
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
          convertedTxs <- performUnlessClosingEitherU(functionFullName) {
            convertTransactions(applicable)
          }
          // dispatch to domain
          responses <- dispatch(domain, transactions = convertedTxs)
          observed <- EitherT.right(
            if (syncTransactionAfterDispatch) {
              // this block is only run for x-nodes.
              // for x-nodes, we either receive accepted or failed for all transactions in a submission batch.
              // failed submissions are turned into a Left in dispatch. Therefore it's safe to await without additional checks.
              convertedTxs.headOption
                .map(awaitTransactionObserved(targetClient, _, timeouts.unbounded.duration))
                // there were no transactions to wait for
                .getOrElse(FutureUnlessShutdown.pure(true))
            } else {
              // the domain outbox is configured to not wait for submitted transactions, so we just
              FutureUnlessShutdown.pure(true)
            }
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
      found: PendingTransactions[TX],
      applicable: Seq[TX],
      responses: Seq[State],
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
}

/** Dynamic version of a TopologyManagerObserver allowing observers
  * to be dynamically added or removed while the TopologyManager stays up.
  * (This is helpful for MediatorNodeX failover where domain-outboxes are started
  * and closed.)
  */
class DomainOutboxXDynamicObserver(val loggerFactory: NamedLoggerFactory)
    extends TopologyManagerObserver
    with NamedLogging {
  private val outboxRef = new AtomicReference[Option[DomainOutboxCommon]](None)

  override def addedNewTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    outboxRef.get.fold(FutureUnlessShutdown.unit)(
      _.newTransactionsAddedToAuthorizedStore(timestamp, transactions.size)
    )
  }

  def addObserver(ob: DomainOutboxCommon)(implicit traceContext: TraceContext): Unit = {
    val previous = outboxRef.getAndSet(ob.some)
    if (previous.nonEmpty) {
      logger.warn("Expecting previously added domain outbox-X to have been removed")
    }
  }

  def removeObserver(): Unit = outboxRef.set(None)
}

class DomainOutboxXFactory(
    domainId: DomainId,
    memberId: Member,
    authorizedTopologyManager: AuthorizedTopologyManagerX,
    domainTopologyManager: DomainTopologyManagerX,
    crypto: Crypto,
    topologyXConfig: TopologyXConfig,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val authorizedObserverRef = new SingleUseCell[DomainOutboxXDynamicObserver]
  private val domainObserverRef = new SingleUseCell[DomainOutboxXDynamicObserver]

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
    val handle = new SequencerBasedRegisterTopologyTransactionHandleX(
      sequencerClient,
      domainId,
      memberId,
      clock,
      topologyXConfig,
      protocolVersion,
      timeouts,
      domainLoggerFactory,
    )
    if (authorizedObserverRef.isEmpty) {
      authorizedObserverRef
        .putIfAbsent(
          new DomainOutboxXDynamicObserver(loggerFactory).tap(authorizedTopologyManager.addObserver)
        )
        .discard
    }
    val authorizedObserver =
      authorizedObserverRef.getOrElse(throw new IllegalStateException("Must have observer"))

    val storeBasedDomainOutbox = new StoreBasedDomainOutboxX(
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
      maybeObserverCloseable = new AutoCloseable {
        override def close(): Unit = authorizedObserver.removeObserver()
      }.some,
      futureSupervisor = futureSupervisor,
    )
    authorizedObserver.addObserver(storeBasedDomainOutbox)

    if (domainObserverRef.isEmpty) {
      domainObserverRef
        .putIfAbsent(
          new DomainOutboxXDynamicObserver(loggerFactory).tap(domainTopologyManager.addObserver)
        )
        .discard
    }
    val domainObserver =
      domainObserverRef.getOrElse(throw new IllegalStateException("Must have observer"))

    val queueBasedDomainOutbox =
      new QueueBasedDomainOutboxX(
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
          DomainOutboxXFactory.this.logger
        )
      }

      override protected def timeouts: ProcessingTimeout = DomainOutboxXFactory.this.timeouts

      override protected def logger: TracedLogger = DomainOutboxXFactory.this.logger
    }
  }
}

class DomainOutboxXFactorySingleCreate(
    domainId: DomainId,
    memberId: Member,
    authorizedTopologyManager: AuthorizedTopologyManagerX,
    domainTopologyManager: DomainTopologyManagerX,
    crypto: Crypto,
    topologyXConfig: TopologyXConfig,
    override val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
) extends DomainOutboxXFactory(
      domainId,
      memberId,
      authorizedTopologyManager,
      domainTopologyManager,
      crypto,
      topologyXConfig,
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
    )
      .tap { case outbox =>
        outboxRef.putIfAbsent(outbox).discard
      }
  }

  override protected def onClosed(): Unit =
    Lifecycle.close(outboxRef.get.toList: _*)(logger)
}

final case class PendingTransactions[TX](
    transactions: Seq[TX],
    newWatermark: CantonTimestamp,
)
