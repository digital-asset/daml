// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking

import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.v30
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.BftOrderingMessageBody.Message
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  BftOrderingMessageBody,
  BftOrderingServiceReceiveRequest,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.NetworkingMetrics.{
  emitReceiveStats,
  receiveMetricsContext,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  P2PNetworkIn,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext

import java.time.{Duration, Instant}

class BftP2PNetworkIn[E <: Env[E]](
    metrics: BftOrderingMetrics,
    override val availability: ModuleRef[Availability.Message[E]],
    override val consensus: ModuleRef[Consensus.Message[E]],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit mc: MetricsContext)
    extends P2PNetworkIn[E] {

  private type OutcomeType = metrics.p2p.receive.labels.source.values.SourceValue

  override def receiveInternal(
      message: BftOrderingServiceReceiveRequest
  )(implicit
      context: E#ActorContextT[BftOrderingServiceReceiveRequest],
      traceContext: TraceContext,
  ): Unit = {
    logger.debug(s"Received network message $message")
    val start = Instant.now
    val sequencerIdOrError = UniqueIdentifier
      .fromProtoPrimitive(message.sentBySequencerUid, "sent_by_sequencer_uid")
      .map(SequencerId(_))
      .leftMap(_.toString)
    parseAndForwardBody(
      sequencerIdOrError,
      message.body,
      message.signature,
      start,
    )
  }

  private def parseAndForwardBody(
      from: Either[String, SequencerId],
      body: Option[BftOrderingMessageBody],
      protoSignature: Option[v30.Signature],
      start: Instant,
  )(implicit traceContext: TraceContext): Unit = {
    val outcome: OutcomeType = from match {
      case Left(error) =>
        logger.warn(error)
        metrics.p2p.receive.labels.source.values.SourceParsingFailed
      case Right(from) =>
        body.fold[OutcomeType]({
          logger.info(s"Received empty message body from $from, dropping")
          metrics.p2p.receive.labels.source.values.Empty(from)
        })(body => handleMessage(from, body.message, protoSignature))
    }
    val end = Instant.now
    val mc1 = receiveMetricsContext(metrics)(outcome)
    locally {
      implicit val mc: MetricsContext = mc1
      metrics.p2p.receive.processingLatency.update(Duration.between(start, end))
      emitReceiveStats(metrics, size = body.map(_.serializedSize.toLong).getOrElse(0L))
    }
  }

  private def handleMessage(
      from: SequencerId,
      message: Message,
      protoSignature: Option[v30.Signature],
  )(implicit
      traceContext: TraceContext
  ): OutcomeType =
    message match {
      case Message.Empty =>
        logger.info(s"Received empty message from $from, dropping")
        metrics.p2p.receive.labels.source.values.Empty(from)
      case Message.AvailabilityMessage(availabilityMessage) =>
        AvailabilityModule
          .parseNetworkMessage(from, availabilityMessage, protoSignature)
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping availability message from $from as it couldn't be parsed: $errorMessage"
              ),
            availability.asyncSend,
          )
        metrics.p2p.receive.labels.source.values.Availability(from)
      case Message.ConsensusMessage(consensusMessage) =>
        IssConsensusModule
          .parseUnverifiedNetworkMessage(from, consensusMessage)
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping consensus message from $from as it couldn't be parsed: $errorMessage"
              ),
            consensus.asyncSend,
          )
        metrics.p2p.receive.labels.source.values.Consensus(from)
      case Message.StateTransferMessage(message) =>
        IssConsensusModule
          .parseStateTransferMessage(from, message)
          .fold(
            errorMessage =>
              logger.warn(
                s"Dropping state transfer message from $from as it couldn't be parsed: $errorMessage"
              ),
            consensus.asyncSend,
          )
        metrics.p2p.receive.labels.source.values.StateTransfer(from)
    }
}
