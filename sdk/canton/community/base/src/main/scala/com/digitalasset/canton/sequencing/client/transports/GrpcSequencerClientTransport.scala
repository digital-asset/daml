// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.lifecycle.Lifecycle.CloseableChannel
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.networking.grpc.GrpcError.{
  GrpcClientGaveUp,
  GrpcServerError,
  GrpcServiceUnavailable,
}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SequencerSubscription,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc, Traced}
import com.digitalasset.canton.util.EitherTUtil.syntax.*
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Context.CancellableContext
import io.grpc.{CallOptions, Context, ManagedChannel}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

private[transports] abstract class GrpcSequencerClientTransportCommon(
    channel: ManagedChannel,
    callOptions: CallOptions,
    clientAuth: GrpcSequencerClientAuth,
    val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    esf: ExecutionSequencerFactory,
    materializer: Materializer,
) extends SequencerClientTransportCommon
    with NamedLogging {

  protected val sequencerServiceClient: SequencerServiceStub = clientAuth(
    new SequencerServiceStub(channel, options = callOptions)
  )
  private val noLoggingShutdownErrorsLogPolicy: GrpcError => TracedLogger => TraceContext => Unit =
    err =>
      logger =>
        traceContext =>
          err match {
            case _: GrpcClientGaveUp | _: GrpcServerError | _: GrpcServiceUnavailable =>
              // avoid logging client errors that typically happen during shutdown (such as grpc context cancelled)
              performUnlessClosing("grpc-client-transport-log")(err.log(logger)(traceContext))(
                traceContext
              ).discard
            case _ => err.log(logger)(traceContext)
          }

  override def sendAsyncSigned(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] = {
    sendInternal(
      stub =>
        stub.sendAsyncVersioned(
          v30.SendAsyncVersionedRequest(signedSubmissionRequest = request.toByteString)
        ),
      "send-async-versioned",
      request.content.messageId,
      timeout,
      SendAsyncVersionedResponse.fromProtoV30,
    )
  }

  private def sendInternal[Resp](
      send: SequencerServiceStub => Future[Resp],
      endpoint: String,
      messageId: MessageId,
      timeout: Duration,
      fromResponseProto: Resp => ParsingResult[SendAsyncVersionedResponse],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] = {
    // sends are at-most-once so we cannot retry when unavailable as we don't know if the request has been accepted
    val sendAtMostOnce = retryPolicy(retryOnUnavailable = false)
    val response =
      CantonGrpcUtil.sendGrpcRequest(sequencerServiceClient, "sequencer")(
        stub => send(stub),
        requestDescription = s"$endpoint/$messageId",
        timeout = timeout,
        logger = logger,
        logPolicy = noLoggingShutdownErrorsLogPolicy,
        retryPolicy = sendAtMostOnce,
      )
    response
      .biflatMap(
        fromGrpcError(_, messageId).toEitherT,
        fromResponse(_, fromResponseProto).toEitherT,
      )
      .mapK(FutureUnlessShutdown.outcomeK)
  }

  private def fromResponse[Proto](
      p: Proto,
      deserializer: Proto => ParsingResult[SendAsyncVersionedResponse],
  ): Either[SendAsyncClientResponseError, Unit] = {
    for {
      response <- deserializer(p)
        .leftMap[SendAsyncClientResponseError](err =>
          SendAsyncClientError.RequestFailed(s"Failed to deserialize response: $err")
        )
      _ <- response.error.toLeft(()).leftMap(SendAsyncClientError.RequestRefused)
    } yield ()
  }

  private def fromGrpcError(error: GrpcError, messageId: MessageId)(implicit
      traceContext: TraceContext
  ): Either[SendAsyncClientResponseError, Unit] = {
    val result = EitherUtil.condUnitE(
      !bubbleSendErrorPolicy(error),
      SendAsyncClientError.RequestFailed(s"Failed to make request to the server: $error"),
    )

    // log that we're swallowing the error
    result.foreach { _ =>
      logger.info(
        s"Send [$messageId] returned an error however may still be possibly sequenced so we are ignoring the error: $error"
      )
    }

    result
  }

  /** We receive grpc errors for a variety of reasons. The send operation is at-most-once and should only be bubbled up
    * and potentially retried if we are absolutely certain the request will never be sequenced.
    */
  private def bubbleSendErrorPolicy(error: GrpcError): Boolean =
    error match {
      // bad request refused by server
      case _: GrpcError.GrpcClientError => true
      // the request was rejected by the server as it wasn't in a state to accept it
      case _: GrpcError.GrpcRequestRefusedByServer => true
      // an internal error happened at the server, this could have been when constructing or sending the response
      // after accepting the request so we cannot safely bubble the error
      case _: GrpcError.GrpcServerError => false
      // the service is unavailable, but this could have been returned after a request was received
      case _: GrpcServiceUnavailable => false
      // there was a timeout meaning we don't know what happened with the request
      case _: GrpcError.GrpcClientGaveUp => false
    }

  /** Retry policy to retry once for authentication failures to allow re-authentication and optionally retry when unavailable. */
  private def retryPolicy(
      retryOnUnavailable: Boolean
  )(implicit traceContext: TraceContext): GrpcError => Boolean = {
    // we allow one retry if the failure was due to an auth token expiration
    // if it's not refresh upon the next call we shouldn't retry again
    val hasRetriedDueToTokenExpiration = new AtomicBoolean(false)

    error =>
      if (isClosing) false // don't even think about retrying if we're closing
      else
        error match {
          case requestRefused: GrpcError.GrpcRequestRefusedByServer
              if !hasRetriedDueToTokenExpiration
                .get() && requestRefused.isAuthenticationTokenMissing =>
            logger.info(
              "Retrying once to give the sequencer the opportunity to refresh the authentication token."
            )
            hasRetriedDueToTokenExpiration.set(true) // don't allow again
            true
          // Retrying to recover from transient failures, e.g.:
          // - network outages
          // - sequencer starting up during integration tests
          case _: GrpcServiceUnavailable => retryOnUnavailable
          // don't retry on anything else as the request may have been received and a subsequent send may cause duplicates
          case _ => false
        }
  }

  override def getTrafficStateForMember(request: GetTrafficStateForMemberRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse] = {
    logger.debug(s"Getting traffic state for: ${request.member}")
    CantonGrpcUtil
      .sendGrpcRequest(sequencerServiceClient, "sequencer")(
        _.getTrafficStateForMember(request.toProtoV30),
        requestDescription = s"get-traffic-state/${request.member}",
        timeout = timeouts.network.duration,
        logger = logger,
        logPolicy = noLoggingShutdownErrorsLogPolicy,
        retryPolicy = retryPolicy(retryOnUnavailable = true),
      )
      .map { res =>
        logger.debug(s"Got traffic state ${res.trafficState}")
        res
      }
      .leftMap(_.toString)
      .flatMap(protoRes =>
        EitherT
          .fromEither[Future](GetTrafficStateForMemberResponse.fromProtoV30(protoRes))
          .leftMap(_.toString)
      )
      .mapK(FutureUnlessShutdown.outcomeK)
  }

  override def acknowledgeSigned(signedRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean] = {
    val request = signedRequest.content
    val timestamp = request.timestamp
    logger.debug(s"Acknowledging timestamp: $timestamp")
    CantonGrpcUtil
      .sendGrpcRequest(sequencerServiceClient, "sequencer")(
        _.acknowledgeSigned(v30.AcknowledgeSignedRequest(signedRequest.toByteString)),
        requestDescription = s"acknowledge-signed/$timestamp",
        timeout = timeouts.network.duration,
        logger = logger,
        logPolicy = noLoggingShutdownErrorsLogPolicy,
        retryPolicy = retryPolicy(retryOnUnavailable = false),
      )
      .map { _ =>
        logger.debug(s"Acknowledged timestamp: $timestamp")
        true
      }
      .recover {
        // if sequencer is not available, we'll return false
        case x if x.status == io.grpc.Status.UNAVAILABLE => false
      }
      .leftMap(_.toString)
      .mapK(FutureUnlessShutdown.outcomeK)
  }

  override def downloadTopologyStateForInit(request: TopologyStateForInitRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, TopologyStateForInitResponse] = {
    logger.debug("Downloading topology state for initialization")

    ClientAdapter
      .serverStreaming(request.toProtoV30, sequencerServiceClient.downloadTopologyStateForInit)
      .map(TopologyStateForInitResponse.fromProtoV30(_))
      .flatMapConcat { parsingResult =>
        parsingResult.fold(
          err => Source.failed(ProtoDeserializationFailure.Wrap(err).asGrpcError),
          Source.single,
        )
      }
      .runFold(Vector.empty[GenericStoredTopologyTransaction])((acc, txs) =>
        acc ++ txs.topologyTransactions.value.result
      )
      .toEitherTRight[String]
      .map { accumulated =>
        val storedTxs = StoredTopologyTransactions(accumulated)
        logger.debug(
          s"Downloaded topology state for initialization with last change timestamp at ${storedTxs.lastChangeTimestamp}:\n${storedTxs.result}"
        )
        TopologyStateForInitResponse(Traced(storedTxs))
      }
  }

  override protected def onClosed(): Unit =
    Lifecycle.close(
      clientAuth,
      new CloseableChannel(channel, logger, "grpc-sequencer-transport"),
    )(logger)
}

class GrpcSequencerClientTransport(
    channel: ManagedChannel,
    callOptions: CallOptions,
    clientAuth: GrpcSequencerClientAuth,
    metrics: SequencerClientMetrics,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    protocolVersion: ProtocolVersion,
)(implicit
    executionContext: ExecutionContext,
    esf: ExecutionSequencerFactory,
    materializer: Materializer,
) extends GrpcSequencerClientTransportCommon(
      channel,
      callOptions,
      clientAuth,
      timeouts,
      loggerFactory,
    )
    with SequencerClientTransport {

  override def subscribe[E](
      subscriptionRequest: SubscriptionRequest,
      handler: SerializedEventHandler[E],
  )(implicit traceContext: TraceContext): SequencerSubscription[E] = {
    // we intentionally don't use `Context.current()` as we don't want to inherit the
    // cancellation scope from upstream requests
    val context: CancellableContext = Context.ROOT.withCancellation()

    val subscription = GrpcSequencerSubscription.fromVersionedSubscriptionResponse(
      context,
      handler,
      metrics,
      timeouts,
      loggerFactory,
    )(protocolVersion)

    context.run(() =>
      TraceContextGrpc.withGrpcContext(traceContext) {
        sequencerServiceClient.subscribeVersioned(
          subscriptionRequest.toProtoV30,
          subscription.observer,
        )
      }
    )

    subscription
  }

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    new GrpcSubscriptionErrorRetryPolicy(loggerFactory)
}
