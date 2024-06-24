// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, *}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{TracedProtocolEvent, *}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext

/** We process a sequence of sequential events from the sequencer for the mediator in an optimal manner.
  * Crashes can occur at any point during this processing (or even afterwards as it's the persistence in the sequencer
  * client that would move us to following events). Processing should be effectively idempotent to handle this.
  */
private[mediator] class MediatorEventsProcessor(
    identityClientEventHandler: UnsignedProtocolEventHandler,
    handleMediatorEvents: (
        CantonTimestamp,
        Option[Traced[MediatorEvent]],
        TraceContext,
    ) => HandlerResult,
    deduplicator: MediatorEventDeduplicator,
    metrics: MediatorMetrics,
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

    val envelopesByEvent = envelopesGroupedByEvent(events)
    for {
      deduplicatorResult <-
        deduplicator.rejectDuplicates(envelopesByEvent)
      (uniqueEnvelopesByEvent, storeF) = deduplicatorResult
      lastEvent = events.last1

      determinedStages <- FutureUnlessShutdown.pure(
        uniqueEnvelopesByEvent.flatMap { case (event, envelopes) => determine(event, envelopes) }
      )

      // we need to advance time on the confirmation response even if there is no relevant mediator events
      _ <-
        if (determinedStages.isEmpty)
          handleMediatorEvents(lastEvent.value.timestamp, None, traceContext)
        else FutureUnlessShutdown.unit

      _ <- MonadUtil.sequentialTraverseMonoid(determinedStages)(stage =>
        handleMediatorEvents(stage.value.requestId.unwrap, Some(stage), stage.traceContext)
      )

      resultIdentity <- identityF
    } yield {
      resultIdentity
        .andThenF(_ => storeF)
    }
  }

  private def envelopesGroupedByEvent(
      events: NonEmpty[Seq[TracedProtocolEvent]]
  ): NonEmpty[Seq[(TracedProtocolEvent, Seq[DefaultOpenEnvelope])]] =
    events.map { tracedProtocolEvent =>
      implicit val traceContext: TraceContext = tracedProtocolEvent.traceContext
      val envelopes = tracedProtocolEvent.value match {
        case deliver: Deliver[DefaultOpenEnvelope] =>
          val domainEnvelopes = ProtocolMessage.filterDomainsEnvelopes(
            deliver.batch,
            deliver.domainId,
            (wrongMessages: List[DefaultOpenEnvelope]) => {
              val wrongDomainIds = wrongMessages.map(_.protocolMessage.domainId)
              logger.error(s"Received messages with wrong domain ids: $wrongDomainIds")
            },
          )
          domainEnvelopes
        case _: DeliverError =>
          Seq.empty
      }
      tracedProtocolEvent -> envelopes
    }

  private def determine(
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
      extractMediatorEvents(event.counter, event.timestamp, topologyTimestampO, envelopes)

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
      envelopes.mapFilter(ProtocolMessage.select[SignedProtocolMessage[ConfirmationResponse]])

    val containsTopologyTransactions = DefaultOpenEnvelopesFilter.containsTopology(envelopes)

    if (requests.nonEmpty && responses.nonEmpty) {
      logger.error("Received both mediator confirmation requests and confirmation responses.")
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
              request.protocolMessage,
              rootHashMessages.toList,
              batchAlsoContainsTopologyTransaction = containsTopologyTransactions,
            )
          )

        case _ =>
          logger.error("Received more than one mediator confirmation request.")
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

private[mediator] object MediatorEventsProcessor {
  def apply(
      identityClientEventHandler: UnsignedProtocolEventHandler,
      processor: ConfirmationRequestAndResponseProcessor,
      mediatorEventDeduplicator: MediatorEventDeduplicator,
      metrics: MediatorMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): MediatorEventsProcessor = {
    new MediatorEventsProcessor(
      identityClientEventHandler,
      processor.handleRequestEvents,
      mediatorEventDeduplicator,
      metrics,
      loggerFactory,
    )
  }
}
