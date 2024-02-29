// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Monoid
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.MessageDispatcher.RequestProcessors
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.protocol.*
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
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.traffic.TrafficControlProcessor
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DefaultMessageDispatcher(
    override protected val protocolVersion: ProtocolVersion,
    override protected val domainId: DomainId,
    override protected val participantId: ParticipantId,
    override protected val requestTracker: RequestTracker,
    override protected val requestProcessors: RequestProcessors,
    override protected val topologyProcessor: (
        SequencerCounter,
        SequencedTime,
        Traced[List[DefaultOpenEnvelope]],
    ) => HandlerResult,
    override protected val trafficProcessor: TrafficControlProcessor,
    override protected val acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
    override protected val requestCounterAllocator: RequestCounterAllocator,
    override protected val recordOrderPublisher: RecordOrderPublisher,
    override protected val badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
    override protected val repairProcessor: RepairProcessor,
    override protected val inFlightSubmissionTracker: InFlightSubmissionTracker,
    override protected val loggerFactory: NamedLoggerFactory,
    override val metrics: SyncDomainMetrics,
)(override implicit val ec: ExecutionContext, tracer: Tracer)
    extends MessageDispatcher
    with Spanning
    with NamedLogging {

  import MessageDispatcher.*

  override protected type ProcessingResult = Unit

  override implicit val processingResultMonoid: Monoid[ProcessingResult] = {
    import cats.instances.unit.*
    Monoid[Unit]
  }

  private def runAsyncResult(
      run: FutureUnlessShutdown[AsyncResult]
  ): FutureUnlessShutdown[ProcessingResult] =
    run.flatMap(_.unwrap)

  override protected def doProcess[A](
      kind: MessageKind[A],
      run: => FutureUnlessShutdown[A],
  ): FutureUnlessShutdown[ProcessingResult] = {
    import MessageDispatcher.*
    // Explicitly enumerate all cases for type safety
    kind match {
      case TopologyTransaction => runAsyncResult(run)
      case TrafficControlTransaction => run
      case AcsCommitment => run
      case CausalityMessageKind => run
      case MalformedMessage => run
      case UnspecifiedMessageKind => run
      case RequestKind(_) => runAsyncResult(run)

      case ResultKind(_) => runAsyncResult(run)
      case MalformedMediatorConfirmationRequestMessage => runAsyncResult(run)

      case DeliveryMessageKind => run
    }
  }

  override def handleAll(
      tracedEvents: Traced[Seq[WithOpeningErrors[PossiblyIgnoredProtocolEvent]]]
  ): HandlerResult =
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      for {
        _observeSequencing <- observeSequencing(
          events.collect { case WithOpeningErrors(e: OrdinaryProtocolEvent, _) =>
            e.signedEvent.content
          }
        )
        result <- MonadUtil.sequentialTraverseMonoid(events)(handle)
      } yield result
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def handle(eventE: WithOpeningErrors[PossiblyIgnoredProtocolEvent]): HandlerResult = {
    implicit val traceContext: TraceContext = eventE.event.traceContext

    withSpan(s"MessageDispatcher.handle") { implicit traceContext => _ =>
      val future = eventE.event match {
        case OrdinarySequencedEvent(signedEvent, _) =>
          val signedEventE = eventE.map(_ => signedEvent)
          processOrdinary(signedEventE)
        case IgnoredSequencedEvent(ts, sc, _, _) =>
          tickTrackers(sc, ts, triggerAcsChangePublication = false)
      }

      HandlerResult.synchronous(future)
    }(traceContext, tracer)
  }

  private def processOrdinary(
      signedEventE: WithOpeningErrors[SignedContent[SequencedEvent[DefaultOpenEnvelope]]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    signedEventE.event.content match {
      case deliver @ Deliver(sc, ts, _, _, _, _) if TimeProof.isTimeProofDeliver(deliver) =>
        logTimeProof(sc, ts)
        tickTrackers(sc, ts, triggerAcsChangePublication = true)

      case Deliver(sc, ts, _, msgIdO, _, _) =>
        if (signedEventE.hasNoErrors) {
          logEvent(sc, ts, msgIdO, signedEventE.event)
        } else {
          logFaultyEvent(sc, ts, msgIdO, signedEventE.map(_.content))
        }
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val deliverE =
          signedEventE.asInstanceOf[WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]]]
        processBatch(deliverE)
          .thereafter {
            // Make sure that the tick is not lost unless we're shutting down or getting an exception
            case Success(outcome) =>
              if (outcome != UnlessShutdown.AbortedDueToShutdown)
                requestTracker.tick(sc, ts)
            case Failure(ex) =>
              logger.error("event processing failed.", ex)
          }

      case error @ DeliverError(sc, ts, _, msgId, status) =>
        logDeliveryError(sc, ts, msgId, status)
        logger.debug(s"Received a deliver error at ${sc} / ${ts}")
        for {
          _unit <- observeDeliverError(error)
          _unit <- tickTrackers(sc, ts, triggerAcsChangePublication = false)
        } yield ()
    }
  }

  private def tickTrackers(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      triggerAcsChangePublication: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      // Signal to the topology processor that all messages up to timestamp `ts` have arrived
      // Publish the empty ACS change only afterwards as this may trigger an ACS commitment computation which accesses the topology state.
      _unit <- runAsyncResult(topologyProcessor(sc, SequencedTime(ts), Traced(List.empty)))
    } yield {
      // Make sure that the tick is not lost
      requestTracker.tick(sc, ts)
      if (triggerAcsChangePublication)
        recordOrderPublisher.scheduleEmptyAcsChangePublication(sc, ts)

      recordOrderPublisher.tick(sc, ts)
    }

  @VisibleForTesting
  override def flush(): Future[Unit] = Future.unit
}
