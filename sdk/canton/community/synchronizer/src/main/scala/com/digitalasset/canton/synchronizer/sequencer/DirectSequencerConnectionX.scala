// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXHealth,
}
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SequencerSubscription,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  SendAsyncError,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.sequencing.{SequencedEventHandler, SequencerConnectionX}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import io.grpc.Status

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

/** Sequencer connection meant to be used with [[DirectSequencerConnectionXPool]].
  */
class DirectSequencerConnectionX(
    override val config: ConnectionXConfig,
    sequencer: Sequencer,
    synchronizerId: PhysicalSynchronizerId,
    sequencerId: SequencerId,
    staticParameters: StaticSynchronizerParameters,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectionX
    with PrettyPrinting {

  override def name: String = config.name

  override val health: SequencerConnectionXHealth =
    new SequencerConnectionXHealth.AlwaysValidated(s"$name-health", logger)

  override def fail(reason: String)(implicit traceContext: TraceContext): Unit = ()

  override def fatal(reason: String)(implicit traceContext: TraceContext): Unit = ()

  override def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] =
    sequencer
      .sendAsyncSigned(request)
      .leftMap(err =>
        SendAsyncClientError.RequestRefused(SendAsyncError.SendAsyncErrorDirect(err.cause))
      )

  override def acknowledgeSigned(
      signedRequest: SignedContent[AcknowledgeRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean] =
    sequencer.acknowledgeSigned(signedRequest).map(_ => true)

  override def getTrafficStateForMember(
      request: GetTrafficStateForMemberRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse] =
    sequencer
      .getTrafficStateAt(request.member, request.timestamp)
      .map { trafficStateO =>
        GetTrafficStateForMemberResponse(trafficStateO, staticParameters.protocolVersion)
      }
      .leftMap(_.toString)

  override def downloadTopologyStateForInit(
      request: TopologyStateForInitRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitResponse] =
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        s"$functionFullName is not implemented for DirectSequencerConnectionX"
      )
    )

  override def logout()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Unit] =
    // In-process connection is not authenticated
    EitherTUtil.unitUS

  override def subscribe[E](
      request: SubscriptionRequest,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): Either[String, SequencerSubscription[E]] = ???

  override protected def pretty: Pretty[DirectSequencerConnectionX] =
    prettyOfClass(param("name", _.name.singleQuoted))

  override def attributes: ConnectionAttributes =
    ConnectionAttributes(synchronizerId, sequencerId, staticParameters)

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    SubscriptionErrorRetryPolicy.never
}
