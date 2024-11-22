// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Monoid
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.ledger.participant.state.Update.SequencerIndexMoved
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.MessageDispatcher.{
  ParticipantTopologyProcessor,
  RequestProcessors,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionDomainTracker
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
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.{HasFlushFuture, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Dispatches the incoming messages of the [[com.digitalasset.canton.sequencing.client.SequencerClient]]
  * to the different processors.
  * It also informs the [[com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker]]
  * about the passing of time for messages that are not processed by the
  * [[com.digitalasset.canton.participant.protocol.ProtocolProcessor]].
  */
class ParallelMessageDispatcher(
    override protected val protocolVersion: ProtocolVersion,
    override protected val domainId: DomainId,
    override protected val participantId: ParticipantId,
    override protected val requestTracker: RequestTracker,
    override protected val requestProcessors: RequestProcessors,
    override protected val topologyProcessor: ParticipantTopologyProcessor,
    override protected val trafficProcessor: TrafficControlProcessor,
    override protected val acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
    override protected val requestCounterAllocator: RequestCounterAllocator,
    override protected val recordOrderPublisher: RecordOrderPublisher,
    override protected val badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
    override protected val repairProcessor: RepairProcessor,
    override protected val inFlightSubmissionDomainTracker: InFlightSubmissionDomainTracker,
    processAsyncronously: ViewType => Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
    override val metrics: SyncDomainMetrics,
)(override implicit val ec: ExecutionContext, tracer: Tracer)
    extends MessageDispatcher
    with NamedLogging
    with HasFlushFuture
    with Spanning {

  import MessageDispatcher.*

  override protected type ProcessingResult = AsyncResult

  override implicit val processingResultMonoid: Monoid[ProcessingResult] =
    AsyncResult.monoidAsyncResult

  override protected def doProcess[A](
      kind: MessageKind[A],
      run: => FutureUnlessShutdown[A],
  ): FutureUnlessShutdown[ProcessingResult] = {
    def runSynchronously(implicit ev: A =:= Unit): FutureUnlessShutdown[ProcessingResult] =
      // We technically wouldn't need `ev` here because we throw away the result of `run`,
      // but it's still a function parameter so that we get the compiler to complain if this is used with a non-unit type.
      // A `run.map { case () => ... }` would not catch such problems either as `A` is unknown to the compiler
      // and a Unit pattern match therefore looks OK to the Scala type checker :-(
      HandlerResult.synchronous(run.map(ev))

    def runSequential(implicit ev: A =:= AsyncResult): FutureUnlessShutdown[ProcessingResult] =
      HandlerResult.synchronous(run.flatMap(_.unwrap))

    // Explicitly enumerate all cases for type safety
    kind match {
      // The identity processor must run sequential on all delivered events and identity stuff needs to be
      // processed before any other transaction (as it might have changed the topology state the
      // other envelopes are referring to)
      case TopologyTransaction => run
      case TrafficControlTransaction => runSynchronously
      case AcsCommitment => runSynchronously
      case CausalityMessageKind => runSynchronously
      case MalformedMessage => runSynchronously
      case UnspecifiedMessageKind => runSynchronously
      case RequestKind(viewType) =>
        if (processAsyncronously(viewType)) run
        else runSequential
      case ResultKind(viewType) =>
        if (processAsyncronously(viewType)) run
        else runSequential
      case DeliveryMessageKind =>
        // TODO(#6914) Figure out the required synchronization to run this asynchronously.
        //  We must make sure that the observation of having been sequenced runs before the tick to the record order publisher.
        runSynchronously
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
        )
        process <- MonadUtil.sequentialTraverseMonoid(events)(handle)
      } yield processingResultMonoid.combine(observeSequencing, process)
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def handle(eventE: WithOpeningErrors[PossiblyIgnoredProtocolEvent]): HandlerResult = {
    implicit val traceContext: TraceContext = eventE.event.traceContext

    withSpan("MessageDispatcher.handle") { implicit traceContext => _ =>
      eventE.event match {
        case OrdinarySequencedEvent(signedEvent) =>
          val signedEventE = eventE.map(_ => signedEvent)
          processOrdinary(signedEventE)

        case IgnoredSequencedEvent(ts, sc, _) =>
          tickTrackers(sc, ts, triggerAcsChangePublication = false)
      }
    }(traceContext, tracer)
  }

  private def processOrdinary(
      signedEventE: WithOpeningErrors[SignedContent[SequencedEvent[DefaultOpenEnvelope]]]
  )(implicit traceContext: TraceContext): HandlerResult =
    signedEventE.event.content match {
      case deliver @ Deliver(sc, ts, _, _, _, _, _) if TimeProof.isTimeProofDeliver(deliver) =>
        logTimeProof(sc, ts)
        tickTrackers(sc, ts, triggerAcsChangePublication = true)

      case Deliver(sc, ts, _, msgId, _, _, _) =>
        // TODO(#13883) Validate the topology timestamp
        if (signedEventE.hasNoErrors) {
          logEvent(sc, ts, msgId, signedEventE.event)
        } else {
          logFaultyEvent(sc, ts, msgId, signedEventE.map(_.content))
        }
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val deliverE =
          signedEventE.asInstanceOf[WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]]]
        processBatch(deliverE)
          .transform {
            case Success(asyncUS) =>
              Success(asyncUS.map { asyncF =>
                val asyncFTick = asyncF.thereafter { result =>
                  // Make sure that the tick is not lost unless we're shutting down anyway or getting an exception
                  result.foreach { outcome =>
                    if (outcome != UnlessShutdown.AbortedDueToShutdown)
                      requestTracker.tick(sc, ts)
                  }
                }
                // error logging is the responsibility of the caller
                addToFlushWithoutLogging(s"Deliver event with sequencer counter $sc")(
                  asyncFTick.unwrap.unwrap
                )
                asyncFTick
              })
            case Failure(ex) =>
              logger.error("Synchronous event processing failed.", ex)
              // Do not tick anything as the subscription will close anyway
              // and there is no guarantee that the exception will happen again during a replay.
              Failure(ex)
          }

      case error @ DeliverError(sc, ts, _, msgId, status, _) =>
        logDeliveryError(sc, ts, msgId, status)
        for {
          _ <- observeDeliverError(error)
          identityAsync <- tickTrackers(sc, ts, triggerAcsChangePublication = false)
        } yield identityAsync
    }

  private def tickTrackers(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      triggerAcsChangePublication: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[AsyncResult] =
    // Signal to the topology processor that all messages up to timestamp `ts` have arrived
    // Publish the tick only afterwards as this may trigger an ACS commitment computation which accesses the topology state.
    for {
      topologyAsync <- topologyProcessor(sc, SequencedTime(ts), None, Traced(List.empty))
      _ = {
        // Make sure that the tick is not lost
        requestTracker.tick(sc, ts)
        if (triggerAcsChangePublication)
          recordOrderPublisher.scheduleEmptyAcsChangePublication(sc, ts)
      }
      // ticking the RecordOrderPublisher asynchronously
      recordOrderPublisherTickF = FutureUnlessShutdown.outcomeF(
        recordOrderPublisher.tick(
          SequencerIndexMoved(
            domainId = domainId,
            sequencerCounter = sc,
            recordTime = ts,
            requestCounterO = None,
          )
        )
      )
    } yield topologyAsync.andThenF(_ => recordOrderPublisherTickF)

  @VisibleForTesting
  override def flush(): Future[Unit] = doFlush()
}
