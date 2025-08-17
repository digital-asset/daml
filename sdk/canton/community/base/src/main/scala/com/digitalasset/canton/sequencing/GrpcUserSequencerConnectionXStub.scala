// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.implicits.catsSyntaxEither
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasRunOnClosing}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencer.api.v30.{
  AcknowledgeSignedRequest,
  AcknowledgeSignedResponse,
  SendAsyncRequest,
  SubscriptionResponse,
}
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXError
import com.digitalasset.canton.sequencing.SequencerConnectionXStub.SequencerConnectionXStubError
import com.digitalasset.canton.sequencing.client.SequencerSubscription
import com.digitalasset.canton.sequencing.client.transports.{
  ConsumesCancellableGrpcStreamObserver,
  GrpcSequencerSubscription,
}
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
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Context.CancellableContext
import io.grpc.{Channel, StatusRuntimeException}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/** Stub for user interactions with a sequencer, specialized for gRPC transport.
  */
class GrpcUserSequencerConnectionXStub(
    connection: GrpcConnectionX,
    sequencerSvcFactory: Channel => SequencerServiceStub,
    timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
    protocolVersion: ProtocolVersion,
)(implicit
    ec: ExecutionContextExecutor,
    esf: ExecutionSequencerFactory,
    materializer: Materializer,
) extends UserSequencerConnectionXStub {
  override def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError.ConnectionError, Unit] = {
    val messageId = request.content.messageId
    for {
      _ <- connection
        .sendRequest(
          requestDescription = s"send-async-versioned/$messageId",
          stubFactory = sequencerSvcFactory,
          retryPolicy = retryPolicy,
          logPolicy = logPolicy,
          timeout = timeout,
        )(
          _.sendAsync(
            SendAsyncRequest(signedSubmissionRequest = request.toByteString)
          )
        )
        .leftMap(
          SequencerConnectionXStubError.ConnectionError.apply
        )
    } yield ()
  }

  override def acknowledgeSigned(
      signedRequest: SignedContent[AcknowledgeRequest],
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
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
          logPolicy = logPolicy,
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
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
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
        logPolicy = logPolicy,
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
    // TODO(i26281): Maybe use a `KillSwitch` to enforce a timeout
    for {
      source <-
        connection
          .serverStreamingRequestPekko(stubFactory = sequencerSvcFactory)(
            request = request.toProtoV30,
            send = _.downloadTopologyStateForInit,
          )
          .leftMap[SequencerConnectionXStubError](
            SequencerConnectionXStubError.ConnectionError.apply
          )
          .toEitherT[FutureUnlessShutdown]

      result <- EitherT[Future, SequencerConnectionXStubError, TopologyStateForInitResponse](
        source
          .map(TopologyStateForInitResponse.fromProtoV30(_))
          .flatMapConcat { parsingResult =>
            parsingResult.fold(
              err => Source.failed(ProtoDeserializationFailure.Wrap(err).asGrpcError),
              Source.single,
            )
          }
          // TODO(i26281): Maybe use `.runWith(Sink.seq)` to simplify
          .runFold(Vector.empty[GenericStoredTopologyTransaction])((acc, txs) =>
            acc ++ txs.topologyTransactions.value.result
          )
          .map { accumulated =>
            val storedTxs = StoredTopologyTransactions(accumulated)
            TopologyStateForInitResponse(Traced(storedTxs))
          }
          .transformWith {
            case Success(value) => Future.successful(Right(value))

            case Failure(grpcExc: StatusRuntimeException) =>
              logger.debug(
                s"Downloading topology state for initialization failed with gRPC exception",
                grpcExc,
              )
              val grpcError =
                GrpcError("download-topology-state-for-init", connection.name, grpcExc)
              Future.successful(
                Left(
                  SequencerConnectionXStubError
                    .ConnectionError(
                      ConnectionXError.TransportError(grpcError)
                    )
                )
              )

            case Failure(exc) =>
              logger.warn(
                s"Downloading topology state for initialization failed with unexpected exception",
                exc,
              )
              Future.failed(exc)
          }
      )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield result

  override def subscribe[E](
      request: SubscriptionRequest,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionXStubError, SequencerSubscription[E]] = {
    val loggerWithConnection = loggerFactory.append("connection", connection.name)

    def mkSubscription(
        context: CancellableContext,
        hasRunOnClosing: HasRunOnClosing,
    ): ConsumesCancellableGrpcStreamObserver[E, SubscriptionResponse] =
      GrpcSequencerSubscription.fromSubscriptionResponse(
        context,
        handler,
        hasRunOnClosing,
        timeouts,
        loggerWithConnection,
      )(protocolVersion)

    connection
      .serverStreamingRequest(
        stubFactory = sequencerSvcFactory,
        observerFactory = mkSubscription,
      )(getObserver = _.observer)(_.subscribe(request.toProtoV30, _))
      .leftMap[SequencerConnectionXStubError](SequencerConnectionXStubError.ConnectionError.apply)
  }
}
