// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.processing.SequencedTime
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DefaultMessageDispatcher(
    override protected val protocolVersion: ProtocolVersion,
    override protected val uniqueContractKeys: Boolean,
    override protected val domainId: DomainId,
    override protected val participantId: ParticipantId,
    override protected val requestTracker: RequestTracker,
    override protected val requestProcessors: RequestProcessors,
    override protected val topologyProcessor: (
        SequencerCounter,
        SequencedTime,
        Traced[List[DefaultOpenEnvelope]],
    ) => HandlerResult,
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
      case AcsCommitment => run
      case CausalityMessageKind => run
      case MalformedMessage => run
      case UnspecifiedMessageKind => run
      case RequestKind(_) => runAsyncResult(run)

      case ResultKind(_) => runAsyncResult(run)
      case MalformedMediatorRequestMessage => runAsyncResult(run)

      case DeliveryMessageKind => run
    }
  }

  override def handleAll(
      tracedEvents: Traced[Seq[Either[
        Traced[EventWithErrors[SequencedEvent[DefaultOpenEnvelope]]],
        PossiblyIgnoredProtocolEvent,
      ]]]
  ): HandlerResult =
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      for {
        _observeSequencing <- observeSequencing(
          events.collect {
            case Left(Traced(EventWithErrors(e, _, /* isIgnored */ false))) => e
            case Right(e: OrdinaryProtocolEvent) => e.signedEvent.content
          }
        )
        result <- MonadUtil.sequentialTraverseMonoid(events)(handle)
      } yield result
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def handle(
      eventE: Either[
        Traced[EventWithErrors[SequencedEvent[DefaultOpenEnvelope]]],
        PossiblyIgnoredProtocolEvent,
      ]
  ): HandlerResult = {
    implicit val traceContext: TraceContext =
      eventE.fold(_.traceContext, _.traceContext)

    def tickTrackers(
        sc: SequencerCounter,
        ts: CantonTimestamp,
        triggerAcsChangePublication: Boolean,
    ): FutureUnlessShutdown[Unit] =
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

    withSpan(s"MessageDispatcher.handle") { implicit traceContext => _ =>
      def processOrdinary(
          signedEventE: Either[
            EventWithErrors[SequencedEvent[DefaultOpenEnvelope]],
            SignedContent[SequencedEvent[DefaultOpenEnvelope]],
          ]
      ): FutureUnlessShutdown[Unit] =
        signedEventE.fold(_.content, _.content) match {
          case deliver @ Deliver(sc, ts, _, _, _) if TimeProof.isTimeProofDeliver(deliver) =>
            tickTrackers(sc, ts, triggerAcsChangePublication = true)

          case Deliver(sc, ts, _, _, _) =>
            val deliverE = signedEventE.fold(
              err => Left(err.asInstanceOf[EventWithErrors[Deliver[DefaultOpenEnvelope]]]),
              evt => Right(evt.asInstanceOf[SignedContent[Deliver[DefaultOpenEnvelope]]]),
            )
            processBatch(deliverE)
              .thereafter {
                // Make sure that the tick is not lost unless we're shutting down or getting an exception
                case Success(outcome) =>
                  if (outcome != UnlessShutdown.AbortedDueToShutdown)
                    requestTracker.tick(sc, ts)
                case Failure(ex) =>
                  logger.error("event processing failed.", ex)
              }

          case error @ DeliverError(sc, ts, _, _, _) =>
            logger.debug(s"Received a deliver error at ${sc} / ${ts}")
            for {
              _unit <- observeDeliverError(error)
              _unit <- tickTrackers(sc, ts, triggerAcsChangePublication = false)
            } yield ()
        }

      val future = eventE match {
        // Ordinary events
        case Left(Traced(e @ EventWithErrors(_content, _openingErrors, /* isIgnored */ false))) =>
          processOrdinary(Left(e))
        case Right(OrdinarySequencedEvent(signedEvent, _)) =>
          processOrdinary(Right(signedEvent))

        // Ignored events
        case Left(Traced(EventWithErrors(content, _openingErrors, /* isIgnored */ true))) =>
          tickTrackers(content.counter, content.timestamp, triggerAcsChangePublication = false)
        case Right(IgnoredSequencedEvent(ts, sc, _, _)) =>
          tickTrackers(sc, ts, triggerAcsChangePublication = false)
      }

      HandlerResult.synchronous(future)
    }(traceContext, tracer)
  }

  @VisibleForTesting
  override def flush(): Future[Unit] = Future.unit
}
