// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Monoid
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.ledger.participant.state.Update.SequencerIndexMoved
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.ConnectedSynchronizerMetrics
import com.digitalasset.canton.participant.protocol.MessageDispatcher.{
  ParticipantTopologyProcessor,
  RequestProcessors,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionSynchronizerTracker
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlProcessor
import com.digitalasset.canton.sequencing.{
  AsyncResult,
  HandlerResult,
  OrdinaryProtocolEvent,
  PossiblyIgnoredProtocolEvent,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
}
import com.digitalasset.canton.topology.processing.SequencedTime
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Dispatches the incoming messages of the
  * [[com.digitalasset.canton.sequencing.client.SequencerClient]] to the different processors. It
  * also informs the
  * [[com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker]] about the
  * passing of time for messages that are not processed by the
  * [[com.digitalasset.canton.participant.protocol.ProtocolProcessor]].
  */
class ParallelMessageDispatcher(
    override protected val protocolVersion: ProtocolVersion,
    override protected val synchronizerId: PhysicalSynchronizerId,
    override protected val participantId: ParticipantId,
    override protected val requestTracker: RequestTracker,
    override protected val requestProcessors: RequestProcessors,
    override protected val topologyProcessor: ParticipantTopologyProcessor,
    override protected val trafficProcessor: TrafficControlProcessor,
    override protected val acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
    override protected val requestCounterAllocator: RequestCounterAllocator,
    override protected val recordOrderPublisher: RecordOrderPublisher,
    override protected val badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
    override protected val inFlightSubmissionSynchronizerTracker: InFlightSubmissionSynchronizerTracker,
    processAsyncronously: ViewType => Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
    override val metrics: ConnectedSynchronizerMetrics,
)(override implicit val ec: ExecutionContext, tracer: Tracer)
    extends MessageDispatcher
    with NamedLogging
    with Spanning {

  import MessageDispatcher.*

  override protected type ProcessingAsyncResult = AsyncResult[TickDecision]
  override protected implicit val processingAsyncResultMonoid: Monoid[ProcessingAsyncResult] =
    new Monoid[ProcessingAsyncResult] {
      override def empty: ProcessingAsyncResult = AsyncResult.pure(TickDecision())
      override def combine(
          x: ProcessingAsyncResult,
          y: ProcessingAsyncResult,
      ): ProcessingAsyncResult =
        AsyncResult(
          for {
            xResult <- x.unwrap
            yResult <- y.unwrap
          } yield xResult combine yResult
        )
    }

  override protected def doProcess(
      kind: MessageKind
  ): ProcessingResult = {
    def runAsynchronously[T](
        run: () => FutureUnlessShutdown[AsyncResult[T]]
    ): FutureUnlessShutdown[AsyncResult[T]] =
      run()

    def runSynchronously[T](
        run: () => FutureUnlessShutdown[T]
    ): FutureUnlessShutdown[AsyncResult[T]] =
      run().map(AsyncResult.pure)

    def forceRunSynchronously[T](
        run: () => FutureUnlessShutdown[AsyncResult[T]]
    ): FutureUnlessShutdown[AsyncResult[T]] =
      runSynchronously(() => run().flatMap(_.unwrap))

    def tick(res: AsyncResult[Unit]): ProcessingAsyncResult =
      res.map(_ => TickDecision())

    // The RecordOrderPublisher will be ticked at result processing
    def noRecordOrderPublisherTick(res: AsyncResult[Unit]): ProcessingAsyncResult =
      res.map(_ =>
        TickDecision(
          tickRecordOrderPublisher = false
        )
      )

    // The TopologyProcessor does not need to be ticked again
    def noTopologyTick(res: AsyncResult[Unit]): ProcessingAsyncResult =
      res.map(_ =>
        TickDecision(
          tickTopologyProcessor = false
        )
      )

    // Explicitly enumerate all cases for type safety
    kind match {
      // The identity processor must run sequential on all delivered events and identity stuff needs to be
      // processed before any other transaction (as it might have changed the topology state the
      // other envelopes are referring to)
      case TopologyTransaction(run) => runAsynchronously(run).map(noTopologyTick)
      case TrafficControlTransaction(run) => runSynchronously(run).map(tick)
      case AcsCommitment(run) => runSynchronously(run).map(tick)
      case CausalityMessageKind(run) => runSynchronously(run).map(tick)
      case MalformedMessage(run) => runSynchronously(run).map(tick)
      case UnspecifiedMessageKind(run) => runSynchronously(run).map(tick)
      case RequestKind(viewType, run) =>
        (if (processAsyncronously(viewType)) runAsynchronously(run)
         else forceRunSynchronously(run)).map(noRecordOrderPublisherTick)
      case ResultKind(viewType, run) =>
        (if (processAsyncronously(viewType)) runAsynchronously(run)
         else forceRunSynchronously(run)).map(tick)
      case DeliveryMessageKind(run) =>
        // TODO(#6914) Figure out the required synchronization to run this asynchronously.
        //  We must make sure that the observation of having been sequenced runs before the tick to the record order publisher.
        // Marton's note:
        //   with an external queue to ensure serial execution for these
        //   with generating an input async message kind for each of the event's in the batch which would be also put in to the single handling to wait for before ticking
        //   we could do this async
        runSynchronously(run).map(tick)
    }
  }

  override def handleAll(
      tracedEvents: Traced[Seq[WithOpeningErrors[PossiblyIgnoredProtocolEvent]]]
  ): HandlerResult =
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      for {
        observeSequencing <- observeSequencing(
          events.collect { case WithOpeningErrors(e: OrdinaryProtocolEvent, _) =>
            e.signedEvent.content
          }
        ).map(_.map(_ => ())) // moving back to HandlerResult / not caring about the tick info
        process <- MonadUtil.sequentialTraverseMonoid(events)(handle)
      } yield Monoid.combine(observeSequencing, process)
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def handle(eventE: WithOpeningErrors[PossiblyIgnoredProtocolEvent]): HandlerResult = {
    implicit val traceContext: TraceContext = eventE.event.traceContext

    withSpan("MessageDispatcher.handle") { implicit traceContext => _ =>
      val processingResult: ProcessingResult = eventE.event match {
        case OrdinarySequencedEvent(sequencerCounter, signedEvent) =>
          val signedEventE = eventE.map(_ => signedEvent)
          processOrdinary(sequencerCounter, signedEventE)

        case _: IgnoredSequencedEvent[_] =>
          pureProcessingResult
      }
      processingResult.map(
        _.flatMapFUS(
          tickTrackers(
            sc = eventE.event.counter,
            ts = eventE.event.timestamp,
          )
        )
      )
    }(traceContext, tracer)
  }

  private def processOrdinary(
      sequencerCounter: SequencerCounter,
      signedEventE: WithOpeningErrors[SignedContent[SequencedEvent[DefaultOpenEnvelope]]],
  )(implicit traceContext: TraceContext): ProcessingResult =
    signedEventE.event.content match {
      case deliver @ Deliver(_pts, ts, _, _, _, _, _) if TimeProof.isTimeProofDeliver(deliver) =>
        logTimeProof(sequencerCounter, ts)
        FutureUnlessShutdown
          .lift(
            recordOrderPublisher.scheduleEmptyAcsChangePublication(sequencerCounter, ts)
          )
          .flatMap(_ => pureProcessingResult)

      case Deliver(_pts, ts, _, msgId, _, _, _) =>
        // TODO(#13883) Validate the topology timestamp
        if (signedEventE.hasNoErrors) {
          logEvent(sequencerCounter, ts, msgId, signedEventE.event)
        } else {
          logFaultyEvent(sequencerCounter, ts, msgId, signedEventE.map(_.content))
        }
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val deliverE =
          signedEventE.asInstanceOf[WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]]]
        processBatch(sequencerCounter, deliverE)
          .transform {
            case success @ Success(_) => success

            case Failure(ex) =>
              logger.error("Synchronous event processing failed.", ex)
              // Do not tick anything as the subscription will close anyway
              // and there is no guarantee that the exception will happen again during a replay.
              Failure(ex)
          }

      case error @ DeliverError(_pts, ts, _, msgId, status, _) =>
        logDeliveryError(sequencerCounter, ts, msgId, status)
        observeDeliverError(error)
    }

  private def tickTrackers(
      sc: SequencerCounter,
      ts: CantonTimestamp,
  )(
      tickDecision: TickDecision
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def topologyProcessorTickAsyncF(): HandlerResult =
      if (tickDecision.tickTopologyProcessor)
        topologyProcessor(sc, SequencedTime(ts), None, Traced(List.empty))
      else
        Monoid[HandlerResult].empty
    def recordOrderPublisherTickF(): FutureUnlessShutdown[Unit] =
      if (tickDecision.tickRecordOrderPublisher) {
        recordOrderPublisher.tick(
          SequencerIndexMoved(
            synchronizerId = synchronizerId.logical,
            recordTime = ts,
          ),
          sequencerCounter = sc,
          rcO = None,
        )
      } else {
        FutureUnlessShutdown.unit
      }
    def requestTrackerTick(): Unit =
      if (tickDecision.tickRequestTracker) requestTracker.tick(sc, ts)
    // Signal to the topology processor that all messages up to timestamp `ts` have arrived
    // Publish the tick only afterwards as this may trigger an ACS commitment computation which accesses the topology state.
    for {
      topologyAsync <- topologyProcessorTickAsyncF()
      _ = requestTrackerTick()
      _ <- recordOrderPublisherTickF()
      // waiting for the topologyAsync as well
      _ <- topologyAsync.unwrap
    } yield ()
  }
}
