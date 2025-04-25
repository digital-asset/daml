// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.implicits.catsSyntaxEither
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencer.api.v30.{
  AcknowledgeSignedRequest,
  AcknowledgeSignedResponse,
  SendAsyncRequest,
}
import com.digitalasset.canton.sequencing.SequencerConnectionXStub.SequencerConnectionXStubError
import com.digitalasset.canton.sequencing.client.SequencerSubscription
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequestV2,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Channel

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

/** Stub for user interactions with a sequencer, specialized for gRPC transport.
  */
class GrpcUserSequencerConnectionXStub(
    connection: GrpcConnectionX,
    sequencerSvcFactory: Channel => SequencerServiceStub,
)(implicit
    ec: ExecutionContextExecutor
) extends UserSequencerConnectionXStub {
  override def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, Unit] = {
    val messageId = request.content.messageId
    for {
      _ <- connection
        .sendRequest(
          requestDescription = s"send-async-versioned/$messageId",
          stubFactory = sequencerSvcFactory,
          retryPolicy = retryPolicy,
          timeout = timeout,
        )(
          _.sendAsync(
            SendAsyncRequest(signedSubmissionRequest = request.toByteString)
          )
        )
        .leftMap[SequencerConnectionXStubError](
          SequencerConnectionXStubError.ConnectionError.apply
        )
    } yield ()
  }

  override def acknowledgeSigned(
      signedRequest: SignedContent[AcknowledgeRequest],
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, AcknowledgeSignedResponse] = {
    val acknowledgeRequest = AcknowledgeSignedRequest(signedRequest.toByteString)
    for {
      result <- connection
        .sendRequest(
          requestDescription = s"acknowledge-signed/${signedRequest.content.timestamp}",
          stubFactory = sequencerSvcFactory,
          retryPolicy = retryPolicy,
          timeout = timeout,
        )(_.acknowledgeSigned(acknowledgeRequest))
        .leftMap[SequencerConnectionXStubError](
          SequencerConnectionXStubError.ConnectionError.apply
        )
    } yield result
  }

  override def getTrafficStateForMember(
      request: GetTrafficStateForMemberRequest,
      timeout: Duration,
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionXStubError,
    GetTrafficStateForMemberResponse,
  ] = for {
    responseP <- connection
      .sendRequest(
        requestDescription = s"get-traffic-state/${request.member}",
        stubFactory = sequencerSvcFactory,
        retryPolicy = retryPolicy,
        timeout = timeout,
      )(_.getTrafficStateForMember(request.toProtoV30))
      .leftMap[SequencerConnectionXStubError](
        SequencerConnectionXStubError.ConnectionError.apply
      )
    responseE = GetTrafficStateForMemberResponse.fromProtoV30(responseP)
    response <- EitherT.fromEither[FutureUnlessShutdown](
      responseE.leftMap[SequencerConnectionXStubError](err =>
        SequencerConnectionXStubError.DeserializationError(err.message)
      )
    )
  } yield response

  override def downloadTopologyStateForInit(
      request: TopologyStateForInitRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, TopologyStateForInitResponse] =
    ???

  def subscribe[E](
      request: SubscriptionRequestV2,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): SequencerSubscription[E] = ???
}
