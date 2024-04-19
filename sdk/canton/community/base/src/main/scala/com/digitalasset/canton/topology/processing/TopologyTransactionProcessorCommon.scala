// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, ProtocolMessage}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.{Deliver, DeliverError}
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessorX.subscriptionTimestamp
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.topology.{DomainId, TopologyManagerError}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, MonadUtil, SimpleExecutionQueue}

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

trait TopologyTransactionProcessorCommon extends NamedLogging with FlagCloseable {

  /** Inform the topology manager where the subscription starts when using [[processEnvelopes]] rather than [[createHandler]] */
  def subscriptionStartsAt(start: SubscriptionStart, domainTimeTracker: DomainTimeTracker)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def createHandler(domainId: DomainId): UnsignedProtocolEventHandler

  /** process envelopes mostly asynchronously
    *
    * Here, we return a Future[Future[Unit]]. We need to ensure the outer future finishes processing
    * before we tick the record order publisher.
    */
  def processEnvelopes(
      sc: SequencerCounter,
      ts: SequencedTime,
      envelopes: Traced[List[DefaultOpenEnvelope]],
  ): HandlerResult

}

/** Main incoming topology transaction validation and processing
  *
  * The topology transaction processor is subscribed to the event stream and processes
  * the domain topology transactions sent via the sequencer.
  *
  * It validates and then computes the updates to the data store in order to be able
  * to represent the topology state at any point in time.
  *
  * The processor works together with the StoreBasedDomainTopologyClient
  */
abstract class TopologyTransactionProcessorCommonImpl[M](
    domainId: DomainId,
    futureSupervisor: FutureSupervisor,
    store: TopologyStoreX[?],
    acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessorCommon {

  private val initialised = new AtomicBoolean(false)

  protected val listeners = ListBuffer[TopologyTransactionProcessingSubscriber]()

  protected val timeAdjuster =
    new TopologyTimestampPlusEpsilonTracker(timeouts, loggerFactory, futureSupervisor)

  private val serializer = new SimpleExecutionQueue(
    "topology-transaction-processor-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  /** assumption: subscribers don't do heavy lifting */
  final def subscribe(listener: TopologyTransactionProcessingSubscriber): Unit = {
    listeners += listener
  }

  protected def epsilonForTimestamp(
      asOfExclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologyStoreX.Change.TopologyDelay]

  protected def maxTimestampFromStore()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]]

  protected def initializeTopologyTimestampPlusEpsilonTracker(
      processorTs: CantonTimestamp,
      maxStored: Option[SequencedTime],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[EffectiveTime]

  private def initialise(
      start: SubscriptionStart,
      domainTimeTracker: DomainTimeTracker,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    ErrorUtil.requireState(
      !initialised.getAndSet(true),
      "topology processor is already initialised",
    )

    def initClientFromSequencedTs(
        sequencedTs: SequencedTime
    ): FutureUnlessShutdown[NonEmpty[Seq[(EffectiveTime, ApproximateTime)]]] = for {
      // we need to figure out any future effective time. if we had been running, there would be a clock
      // scheduled to poke the domain client at the given time in order to adjust the approximate timestamp up to the
      // effective time at the given point in time. we need to recover these as otherwise, we might be using outdated
      // topology snapshots on startup. (wouldn't be tragic as by getting the rejects, we'd be updating the timestamps
      // anyway).
      upcoming <- performUnlessClosingF(functionFullName)(
        store.findUpcomingEffectiveChanges(sequencedTs.value)
        // find effective time of sequenced Ts (directly from store)
        // merge times
      )
      currentEpsilon <- epsilonForTimestamp(sequencedTs.value)
    } yield {
      // we have (ts+e, ts) and quite a few te in the future, so we create list of upcoming changes and sort them

      val head = (
        EffectiveTime(sequencedTs.value.plus(currentEpsilon.epsilon.unwrap)),
        ApproximateTime(sequencedTs.value),
      )
      val tail = upcoming.map(x => (x.effective, x.effective.toApproximate))

      NonEmpty(Seq, head, tail*).sortBy { case (effectiveTime, _) => effectiveTime.value }
    }

    for {
      stateStoreTsO <- performUnlessClosingF(functionFullName)(
        maxTimestampFromStore()
      )
      (processorTs, clientTs) = subscriptionTimestamp(start, stateStoreTsO)
      _ <- initializeTopologyTimestampPlusEpsilonTracker(processorTs, stateStoreTsO.map(_._1))

      clientInitTimes <- clientTs match {
        case Left(sequencedTs) =>
          // approximate time is sequencedTs
          initClientFromSequencedTs(sequencedTs)
        case Right(effective) =>
          // effective and approximate time are effective time
          FutureUnlessShutdown.pure(NonEmpty(Seq, (effective, effective.toApproximate)))
      }
    } yield {
      logger.debug(
        s"Initializing topology processing for start=$start with effective ts ${clientInitTimes.map(_._1)}"
      )

      // let our client know about the latest known information right now, but schedule the updating
      // of the approximate time subsequently
      val maxEffective = clientInitTimes.map { case (effective, _) => effective }.max1
      val minApproximate = clientInitTimes.map { case (_, approximate) => approximate }.min1
      listenersUpdateHead(maxEffective, minApproximate, potentialChanges = true)

      val directExecutionContext = DirectExecutionContext(noTracingLogger)
      clientInitTimes.foreach { case (effective, _approximate) =>
        // if the effective time is in the future, schedule a clock to update the time accordingly
        domainTimeTracker.awaitTick(effective.value) match {
          case None =>
            // The effective time is in the past. Directly advance our approximate time to the respective effective time
            listenersUpdateHead(effective, effective.toApproximate, potentialChanges = true)
          case Some(tickF) =>
            FutureUtil.doNotAwait(
              tickF.map(_ =>
                listenersUpdateHead(effective, effective.toApproximate, potentialChanges = true)
              )(directExecutionContext),
              "Notifying listeners to the topology processor's head",
            )
        }
      }
    }
  }

  final protected def listenersUpdateHead(
      effective: EffectiveTime,
      approximate: ApproximateTime,
      potentialChanges: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(
      s"Updating listener heads to ${effective} and ${approximate}. Potential changes: ${potentialChanges}"
    )
    listeners.toList.foreach(_.updateHead(effective, approximate, potentialChanges))
  }

  /** Inform the topology manager where the subscription starts when using [[processEnvelopes]] rather than [[createHandler]] */
  override def subscriptionStartsAt(start: SubscriptionStart, domainTimeTracker: DomainTimeTracker)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = initialise(start, domainTimeTracker)

  /** process envelopes mostly asynchronously
    *
    * Here, we return a Future[Future[Unit]]. We need to ensure the outer future finishes processing
    * before we tick the record order publisher.
    */
  override def processEnvelopes(
      sc: SequencerCounter,
      ts: SequencedTime,
      envelopes: Traced[List[DefaultOpenEnvelope]],
  ): HandlerResult =
    envelopes.withTraceContext { implicit traceContext => env =>
      internalProcessEnvelopes(
        sc,
        ts,
        extractTopologyUpdatesAndValidateEnvelope(ts, env),
      )
    }

  protected def extractTopologyUpdatesAndValidateEnvelope(
      ts: SequencedTime,
      value: List[DefaultOpenEnvelope],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[M]]

  private[processing] def process(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      messages: List[M],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  protected def internalProcessEnvelopes(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      updatesF: FutureUnlessShutdown[List[M]],
  )(implicit traceContext: TraceContext): HandlerResult = {
    def computeEffectiveTime(
        updates: List[M]
    ): FutureUnlessShutdown[EffectiveTime] = {
      if (updates.nonEmpty) {
        val effectiveTimeF =
          futureSupervisor.supervisedUS(s"adjust ts=$sequencedTime for update")(
            timeAdjuster.adjustTimestampForUpdate(sequencedTime)
          )

        // we need to inform the acs commitment processor about the incoming change
        effectiveTimeF.map { effectiveTime =>
          // this is safe to do here, as the acs commitment processor `publish` method will only be
          // invoked long after the outer future here has finished processing
          acsCommitmentScheduleEffectiveTime(Traced(effectiveTime))
          effectiveTime
        }
      } else {
        futureSupervisor.supervisedUS(s"adjust ts=$sequencedTime for update")(
          timeAdjuster.adjustTimestampForTick(sequencedTime)
        )
      }
    }

    for {
      updates <- updatesF
      _ <- ErrorUtil.requireStateAsyncShutdown(
        initialised.get(),
        s"Topology client for $domainId is not initialized. Cannot process sequenced event with counter ${sc} at ${sequencedTime}",
      )
      // compute effective time
      effectiveTime <- computeEffectiveTime(updates)
    } yield {
      // the rest, we'll run asynchronously, but sequential
      val scheduledF =
        serializer.executeUS(
          {
            if (updates.nonEmpty) {
              process(sequencedTime, effectiveTime, sc, updates)
            } else {
              tickleListeners(sequencedTime, effectiveTime)
            }
          },
          "processing topology transactions",
        )
      AsyncResult(scheduledF)
    }
  }

  private def tickleListeners(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    this.performUnlessClosingF(functionFullName) {
      Future {
        val approximate = ApproximateTime(sequencedTimestamp.value)
        listenersUpdateHead(effectiveTimestamp, approximate, potentialChanges = false)
      }
    }
  }

  override def createHandler(domainId: DomainId): UnsignedProtocolEventHandler =
    new UnsignedProtocolEventHandler {

      override def name: String = s"topology-processor-$domainId"

      override def apply(
          tracedBatch: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
      ): HandlerResult = {
        MonadUtil.sequentialTraverseMonoid(tracedBatch.value) {
          _.withTraceContext { implicit traceContext =>
            {
              // TODO(#13883) Topology transactions must not specify a topology timestamp. Check this.
              case Deliver(sc, ts, _, _, batch, _) =>
                logger.debug(s"Processing sequenced event with counter $sc and timestamp $ts")
                val sequencedTime = SequencedTime(ts)
                val transactionsF = extractTopologyUpdatesAndValidateEnvelope(
                  sequencedTime,
                  ProtocolMessage.filterDomainsEnvelopes(
                    batch,
                    domainId,
                    (wrongMsgs: List[DefaultOpenEnvelope]) =>
                      TopologyManagerError.TopologyManagerAlarm
                        .Warn(
                          s"received messages with wrong domain ids: ${wrongMsgs.map(_.protocolMessage.domainId)}"
                        )
                        .report(),
                  ),
                )
                internalProcessEnvelopes(sc, sequencedTime, transactionsF)
              case err: DeliverError =>
                internalProcessEnvelopes(
                  err.counter,
                  SequencedTime(err.timestamp),
                  FutureUnlessShutdown.pure(Nil),
                )
            }
          }
        }
      }

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          domainTimeTracker: DomainTimeTracker,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        TopologyTransactionProcessorCommonImpl.this.subscriptionStartsAt(start, domainTimeTracker)
    }

  override def onClosed(): Unit = {
    Lifecycle.close(
      timeAdjuster,
      serializer,
    )(logger)
  }

}

object TopologyTransactionProcessorCommon {
  abstract class Factory {
    def create(
        acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
    )(implicit executionContext: ExecutionContext): TopologyTransactionProcessorCommon
  }
}
