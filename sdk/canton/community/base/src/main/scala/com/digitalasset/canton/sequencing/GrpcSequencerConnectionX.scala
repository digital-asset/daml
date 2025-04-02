// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.ConnectionX.{ConnectionXConfig, ConnectionXError}
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXHealth,
}
import com.digitalasset.canton.sequencing.SequencerConnectionXStub.SequencerConnectionXStubError
import com.digitalasset.canton.sequencing.client.SequencerSubscription
import com.digitalasset.canton.sequencing.client.transports.GrpcClientTransportHelpers
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

/** Sequencer connection specialized for gRPC transport.
  */

class GrpcSequencerConnectionX(
    private val underlying: GrpcInternalSequencerConnectionX,
    override val name: String,
    stub: UserSequencerConnectionXStub,
    override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectionX
    with PrettyPrinting
    with GrpcClientTransportHelpers {

  override val health: SequencerConnectionXHealth = underlying.health

  override val config: ConnectionXConfig = underlying.config

  override val attributes: ConnectionAttributes = underlying.tryAttributes

  override def fail(reason: String)(implicit traceContext: TraceContext): Unit =
    underlying.fail(reason)

  override def fatal(reason: String)(implicit traceContext: TraceContext): Unit =
    underlying.fatal(reason)

  override def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, Unit] = {
    // sends are at-most-once so we cannot retry when unavailable as we don't know if the request has been accepted
    val sendAtMostOnce = retryPolicy(retryOnUnavailable = false)
    stub.sendAsync(request, timeout, retryPolicy = sendAtMostOnce)
  }

  override def acknowledgeSigned(
      signedRequest: SignedContent[AcknowledgeRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, Boolean] = {
    val timestamp = signedRequest.content.timestamp
    stub
      .acknowledgeSigned(
        signedRequest,
        timeout,
        retryPolicy = retryPolicy(retryOnUnavailable = false),
      )
      .map { _ =>
        logger.debug(s"Acknowledged timestamp: $timestamp")
        true
      }
      .recover {
        // if sequencer is not available, we'll return false
        case SequencerConnectionXStubError.ConnectionError(ConnectionXError.TransportError(x))
            if x.status == io.grpc.Status.UNAVAILABLE =>
          false
      }
  }

  override def getTrafficStateForMember(
      request: GetTrafficStateForMemberRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionXStubError,
    GetTrafficStateForMemberResponse,
  ] = stub
    .getTrafficStateForMember(
      request,
      timeout,
      retryPolicy = retryPolicy(retryOnUnavailable = true),
    )
    .map { res =>
      logger.debug(s"Got traffic state ${res.trafficState}")
      res
    }

  override def downloadTopologyStateForInit(
      request: TopologyStateForInitRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, TopologyStateForInitResponse] =
    stub.downloadTopologyStateForInit(request, timeout)

  override def subscribe[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): SequencerSubscription[E] = stub.subscribe(request, handler, timeout)

  override protected def pretty: Pretty[GrpcSequencerConnectionX] =
    prettyOfClass(
      param("name", _.name.singleQuoted),
      param("underlying", _.underlying),
    )
}
