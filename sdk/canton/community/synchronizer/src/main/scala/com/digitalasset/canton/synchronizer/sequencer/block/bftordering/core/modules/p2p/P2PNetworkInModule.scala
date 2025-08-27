// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PMetrics.{
  emitReceiveStats,
  receiveMetricsContext,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  P2PNetworkIn,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessageBody.Message
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessage,
  BftOrderingMessageBody,
}
import com.digitalasset.canton.tracing.TraceContext

import java.time.{Duration, Instant}

class P2PNetworkInModule[E <: Env[E]](
    metrics: BftOrderingMetrics,
    override val availability: ModuleRef[Availability.Message[E]],
    override val consensus: ModuleRef[Consensus.Message[E]],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit mc: MetricsContext)
    extends P2PNetworkIn[E] {

  private type OutcomeType = metrics.p2p.receive.labels.source.values.SourceValue

  override def receiveInternal(
      message: BftOrderingMessage
  )(implicit
      context: E#ActorContextT[BftOrderingMessage],
      traceContext: TraceContext,
  ): Unit = {
    val sentBy = BftNodeId(message.sentBy)
    logger.trace(s"Received network message from $sentBy: $message")
    val start = Instant.now
    parseAndForwardBody(
      sentBy,
      message.body,
      start,
    )(TraceContext.fromW3CTraceParent(message.traceContext))
  }

  private def parseAndForwardBody(
      from: BftNodeId,
      body: Option[BftOrderingMessageBody],
      start: Instant,
  )(implicit traceContext: TraceContext): Unit = {
    val outcome: OutcomeType =
      body.fold[OutcomeType]({
        logger.info(s"Received empty message body from $from, dropping")
        metrics.p2p.receive.labels.source.values.Empty(from)
      })(body => handleMessage(from, body.message))

    val end = Instant.now
    val mc1 = receiveMetricsContext(metrics)(outcome)
    locally {
      implicit val mc: MetricsContext = mc1
      updateTimer(metrics.p2p.receive.processingLatency, Duration.between(start, end))
      emitReceiveStats(metrics, size = body.map(_.serializedSize.toLong).getOrElse(0L))
    }
  }

  private def handleMessage(
      from: BftNodeId,
      message: Message,
  )(implicit
      traceContext: TraceContext
  ): OutcomeType =
    message match {
      case Message.Empty =>
        logger.info(s"Received empty message from $from, dropping")
        metrics.p2p.receive.labels.source.values.Empty(from)
      case Message.AvailabilityMessage(availabilityMessage) =>
        AvailabilityModule
          .parseNetworkMessage(availabilityMessage)
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping availability message from $from as it couldn't be parsed: $errorMessage"
              ),
            msg => availability.asyncSend(msg),
          )
        metrics.p2p.receive.labels.source.values.Availability(from)
      case Message.ConsensusMessage(consensusMessage) =>
        IssConsensusModule
          .parseNetworkMessage(consensusMessage)
          .map(msg => Consensus.ConsensusMessage.PbftUnverifiedNetworkMessage(from, msg))
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping consensus message from $from as it couldn't be parsed: $errorMessage"
              ),
            message => {
              val originalSender = message.underlyingNetworkMessage.message.from
              if (originalSender != from) {
                val epoch = message.underlyingNetworkMessage.message.blockMetadata.epochNumber
                logger.debug(
                  s"Received retransmitted message at epoch $epoch from $from originally created by $originalSender"
                )
              }
              consensus.asyncSend(message)
            },
          )
        metrics.p2p.receive.labels.source.values.Consensus(from)
      case Message.RetransmissionMessage(message) =>
        IssConsensusModule
          .parseRetransmissionMessage(from, message)
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping retransmission message from $from as it couldn't be parsed: $errorMessage"
              ),
            message => consensus.asyncSend(message),
          )
        metrics.p2p.receive.labels.source.values.Retransmissions(from)

      case Message.StateTransferMessage(message) =>
        SignedMessage
          .fromProto(v30.StateTransferMessage)(
            IssConsensusModule
              .parseStateTransferMessage(from, _)
          )(message)
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping state transfer message from $from as it couldn't be parsed: $errorMessage"
              ),
            signedMessage =>
              consensus.asyncSend(
                Consensus.StateTransferMessage.UnverifiedStateTransferMessage(signedMessage)
              ),
          )
        metrics.p2p.receive.labels.source.values.StateTransfer(from)

      case Message.ConnectionOpened(_) =>
        logger.debug(s"Received connection opener from $from")
        metrics.p2p.receive.labels.source.values.ConnectionOpener(from)
    }
}
