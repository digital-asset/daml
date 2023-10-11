// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.TestVerdictSender.Result
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.OpenEnvelope
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class TestVerdictSender(
    crypto: DomainSyncCryptoClient,
    mediatorId: MediatorId,
    sequencerSend: SequencerClientSend,
    protocolVersion: ProtocolVersion,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends DefaultVerdictSender(
      sequencerSend,
      crypto,
      mediatorId,
      protocolVersion,
      loggerFactory,
    ) {

  val sentResultsQueue: java.util.concurrent.BlockingQueue[Result] =
    new java.util.concurrent.LinkedBlockingQueue()

  def sentResults: Iterable[Result] = sentResultsQueue.asScala

  override def sendResult(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    sentResultsQueue.add(Result(requestId, decisionTime, Some(request), Some(verdict)))
    super.sendResult(requestId, request, verdict, decisionTime)
  }

  override def sendReject(
      requestId: RequestId,
      requestO: Option[MediatorRequest],
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      rejectionReason: Verdict.MediatorReject,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    sentResultsQueue.add(Result(requestId, decisionTime, requestO, Some(rejectionReason)))
    super.sendReject(requestId, requestO, rootHashMessages, rejectionReason, decisionTime)
  }
}

object TestVerdictSender {
  final case class Result(
      requestId: RequestId,
      decisionTime: CantonTimestamp,
      request: Option[MediatorRequest],
      verdict: Option[Verdict],
  )
}
