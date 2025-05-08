// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.Monad
import cats.syntax.alternative.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** We process a sequence of sequential events from the sequencer for the mediator in an optimal
  * manner. Crashes can occur at any point during this processing (or even afterwards as it's the
  * persistence in the sequencer client that would move us to following events). Processing should
  * be effectively idempotent to handle this.
  */
private[mediator] class MediatorEventsProcessor(
    identityClientEventHandler: UnsignedProtocolEventHandler,
    handler: MediatorEventHandler,
    deduplicator: MediatorEventDeduplicator,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  def handle(events: Seq[TracedProtocolEvent])(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): HandlerResult =
    NonEmpty.from(events).fold(HandlerResult.done)(handle)

  private def handle(
      events: NonEmpty[Seq[TracedProtocolEvent]]
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): HandlerResult = {
    val identityF = identityClientEventHandler(Traced(events))

    val envelopesForSynchronizer = filterEnvelopesForSynchronizer(events)
    val determinedMediatorEvents = envelopesForSynchronizer.forgetNE.flatMap {
      case (event, envelopes) =>
        determineMediatorEvents(event, envelopes)
    }
    for {
      deduplicatorResult <- MonadUtil
        .sequentialTraverse(determinedMediatorEvents) {
          case traced @ Traced(req: MediatorEvent.Request) =>
            deduplicator
              .rejectDuplicate(
                req.sequencingTimestamp,
                req.requestEnvelope.protocolMessage,
                req.rootHashMessages,
              )(traced.traceContext, callerCloseContext)
              .map { case (isUnique, storeF) => Option.when(isUnique)(traced) -> storeF }
          case traced =>
            FutureUnlessShutdown.pure(Some(traced) -> FutureUnlessShutdown.unit)
        }
        .map(_.separate)
        .map { case (deduplicatedMediatorEvents, storeFs) =>
          (deduplicatedMediatorEvents.flattenOption, storeFs.sequence_)
        }
      (deduplicatedMediatorEvents, storeF) = deduplicatorResult
      lastEventTimestamp = events.last1.value.timestamp

      // we need to advance time on the confirmation response even if there are no relevant mediator events
      _ <- NonEmpty.from(deduplicatedMediatorEvents) match {
        case None =>
          handler.observeTimestampWithoutEvent(lastEventTimestamp)(events.last1.traceContext)
        case Some(mediatorEventsNE) =>
          for {
            _ <- MonadUtil.sequentialTraverseMonoid(mediatorEventsNE)(stage =>
              handler.handleMediatorEvent(
                stage.value
              )(stage.traceContext)
            )
            // if the sequencing timestamp of the last event is higher than the timestamp of the last mediator event,
            // trigger an additional round of timeout detection with that timestamp
            _ <- Monad[FutureUnlessShutdown].whenA(
              mediatorEventsNE.last1.value.sequencingTimestamp < lastEventTimestamp
            )(handler.observeTimestampWithoutEvent(lastEventTimestamp)(events.last1.traceContext))
          } yield ()
      }

      resultIdentity <- identityF
    } yield {
      resultIdentity.andThenF(_ => storeF)
    }
  }

  @VisibleForTesting
  private[mediator] def filterEnvelopesForSynchronizer(
      events: NonEmpty[Seq[TracedProtocolEvent]]
  ): NonEmpty[Seq[(TracedProtocolEvent, Seq[DefaultOpenEnvelope])]] =
    events.map { tracedProtocolEvent =>
      implicit val traceContext: TraceContext = tracedProtocolEvent.traceContext
      val synchronizerEnvelopes = ProtocolMessage.filterSynchronizerEnvelopes(
        tracedProtocolEvent.value.envelopes,
        tracedProtocolEvent.value.synchronizerId,
      ) { wrongMessages =>
        val wrongSynchronizerIds = wrongMessages.map(_.protocolMessage.synchronizerId)
        logger.error(s"Received messages with wrong synchronizer ids: $wrongSynchronizerIds")
      }
      tracedProtocolEvent -> synchronizerEnvelopes
    }

  private def determineMediatorEvents(
      tracedProtocolEvent: TracedProtocolEvent,
      envelopes: Seq[DefaultOpenEnvelope],
  ): Seq[Traced[MediatorEvent]] = {
    implicit val traceContext: TraceContext = tracedProtocolEvent.traceContext
    val event = tracedProtocolEvent.value
    val topologyTimestampO = event match {
      case deliver: Deliver[?] => deliver.topologyTimestampO
      case _ => None
    }
    val stages =
      extractMediatorEvents(
        tracedProtocolEvent.counter,
        event.timestamp,
        topologyTimestampO,
        envelopes,
      )

    stages.map(Traced(_))
  }

  private def extractMediatorEvents(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      topologyTimestamp: Option[CantonTimestamp],
      envelopes: Seq[DefaultOpenEnvelope],
  )(implicit traceContext: TraceContext): Seq[MediatorEvent] = {
    val requests = envelopes.mapFilter(ProtocolMessage.select[MediatorConfirmationRequest])
    val responses =
      envelopes.mapFilter(ProtocolMessage.select[SignedProtocolMessage[ConfirmationResponses]])

    val containsTopologyTransactions = DefaultOpenEnvelopesFilter.containsTopology(
      envelopes = envelopes,
      withExplicitTopologyTimestamp = false, // we do not care about this for mediator
    )

    if (requests.nonEmpty && responses.nonEmpty) {
      MediatorError.MalformedMessage
        .Reject(
          "Received both mediator confirmation requests and confirmation responses."
        )
        .report()
      Seq.empty
    } else if (requests.nonEmpty) {
      requests match {
        case Seq(request) =>
          val rootHashMessages =
            envelopes.mapFilter(
              ProtocolMessage.select[RootHashMessage[SerializedRootHashMessagePayload]]
            )
          Seq(
            MediatorEvent.Request(
              counter,
              timestamp,
              request,
              rootHashMessages.toList,
              batchAlsoContainsTopologyTransaction = containsTopologyTransactions,
            )
          )

        case _ =>
          MediatorError.MalformedMessage
            .Reject("Received more than one mediator confirmation request.")
            .report()
          Seq.empty
      }
    } else if (responses.nonEmpty) {
      responses.map(res =>
        MediatorEvent.Response(
          counter,
          timestamp,
          res.protocolMessage,
          topologyTimestamp,
          res.recipients,
        )
      )
    } else Seq.empty
  }
}

private[mediator] trait MediatorEventHandler {
  def handleMediatorEvent(event: MediatorEvent)(implicit traceContext: TraceContext): HandlerResult

  def observeTimestampWithoutEvent(sequencingTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): HandlerResult
}
