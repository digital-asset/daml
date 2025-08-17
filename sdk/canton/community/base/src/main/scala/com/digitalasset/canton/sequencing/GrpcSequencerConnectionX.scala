// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.networking.grpc.GrpcError.{
  GrpcClientError,
  GrpcRequestRefusedByServer,
  GrpcServiceUnavailable,
}
import com.digitalasset.canton.sequencing.ConnectionX.{ConnectionXConfig, ConnectionXError}
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXHealth,
}
import com.digitalasset.canton.sequencing.SequencerConnectionXStub.SequencerConnectionXStubError
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.transports.{
  GrpcClientTransportHelpers,
  GrpcSequencerClientAuth,
  GrpcSubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SequencerSubscription,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.protocol.SendAsyncError.SendAsyncErrorGrpc
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  MessageId,
  SequencerErrors,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import io.grpc.Status

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

/** Sequencer connection specialized for gRPC transport.
  */

class GrpcSequencerConnectionX(
    private val underlying: GrpcInternalSequencerConnectionX,
    override val name: String,
    stub: UserSequencerConnectionXStub,
    clientAuth: GrpcSequencerClientAuth,
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

  override def onClosed(): Unit =
    LifeCycle.close(clientAuth)(logger)

  override def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] = {
    val messageId = request.content.messageId

    // sends are at-most-once so we cannot retry when unavailable as we don't know if the request has been accepted
    val sendAtMostOnce = retryPolicy(retryOnUnavailable = false)

    stub
      .sendAsync(request, timeout, retryPolicy = sendAtMostOnce)
      .leftFlatMap { connectionError =>
        maybeFailConnection(connectionError.error)
        fromConnectionError(connectionError.error, messageId).toEitherT
      }
  }

  /** Fail the connection if the gRPC error indicates a likely network issue.
    *
    * This will prevent other clients requesting a connection to receive it until service is
    * restored. The connection pool will take care of periodically restarting the connection and
    * attempting validation.
    */
  private def maybeFailConnection(
      error: ConnectionXError
  )(implicit traceContext: TraceContext): Unit =
    // gRPC status codes are reported for various reasons, which don't always allow to clearly pinpoint an issue as
    // coming from a faulty connection. For example, `DEADLINE_EXCEEDED` or `CANCELLED` can be due to a faulty
    // connection, but also to a client giving up according to this particular request's parameters.
    // We consider the cost of failing connections in cases where other client requests would proceed unimpeded to
    // be higher than doing nothing, and therefore prefer to err on the side of caution and fail the connection only
    // for cases where we are reasonably confident that another client obtaining the connection would encounter the
    // same issue with its request.
    // We will adapt this code after observing how it behaves in the wild.
    error match {
      // This is mapped from `UNAVAILABLE` or `UNIMPLEMENTED` which, outside blatant errors due to a bad URL or TLS
      // (which should have been caught during connection validation), are likely to be due to a faulty connection or
      // a server not yet fully initialized.
      case ConnectionXError.TransportError(grpcError: GrpcServiceUnavailable) =>
        fail(reason = grpcError.toString)
      case _ =>
    }

  private def fromConnectionError(error: ConnectionXError, messageId: MessageId)(implicit
      traceContext: TraceContext
  ): Either[SendAsyncClientResponseError, Unit] =
    // Adapted from GrpcSequencerClientTransportCommon
    Either.cond(
      !bubbleSendErrorPolicy(error), {
        // log that we're swallowing the error
        logger.info(
          s"Send [$messageId] returned an error however may still be possibly sequenced so we are ignoring the error: $error"
        )
        ()
      },
      error match {
        case ConnectionXError.InvalidStateError(_) =>
          SendAsyncClientError.RequestFailed(s"Failed to make request to the server: $error")
        case ConnectionXError.TransportError(grpcError) =>
          grpcError match {
            case SequencerErrors.Overloaded(_) =>
              SendAsyncClientError.RequestRefused(SendAsyncErrorGrpc(grpcError))
            case _: GrpcRequestRefusedByServer =>
              SendAsyncClientError.RequestRefused(SendAsyncErrorGrpc(grpcError))
            case _: GrpcClientError =>
              SendAsyncClientError.RequestFailed(s"Failed to make request to the server: $error")
            case _ =>
              ErrorUtil.invalidState("We should bubble only refused and client errors")
          }
      },
    )

  /** We receive grpc errors for a variety of reasons. The send operation is at-most-once and should
    * only be bubbled up and potentially retried if we are absolutely certain the request will never
    * be sequenced.
    */
  private def bubbleSendErrorPolicy(error: ConnectionXError): Boolean =
    // Adapted from GrpcSequencerClientTransportCommon
    error match {
      // connection not started
      case ConnectionXError.InvalidStateError(_) => true
      case ConnectionXError.TransportError(grpcError) =>
        grpcError match {
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
    }

  override def acknowledgeSigned(
      signedRequest: SignedContent[AcknowledgeRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean] = {
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
      .leftMap(_.toString)
  }

  override def getTrafficStateForMember(
      request: GetTrafficStateForMemberRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse] =
    stub
      .getTrafficStateForMember(
        request,
        timeout,
        retryPolicy = retryPolicy(retryOnUnavailable = true),
      )
      .map { res =>
        logger.debug(s"Got traffic state ${res.trafficState}")
        res
      }
      .leftMap(_.toString)

  override def logout()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Unit] =
    clientAuth.logout()

  override def downloadTopologyStateForInit(
      request: TopologyStateForInitRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitResponse] = {
    logger.debug("Downloading topology state for initialization")

    for {
      result <- stub.downloadTopologyStateForInit(request, timeout).leftMap(_.toString)
      storedTxs = result.topologyTransactions.value
      _ = logger.debug(
        s"Downloaded topology state for initialization with last change timestamp at " +
          s"${storedTxs.lastChangeTimestamp}: ${storedTxs.result.size} transactions"
      )
    } yield result
  }

  override def subscribe[E](
      request: SubscriptionRequest,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): Either[String, SequencerSubscription[E]] =
    stub.subscribe(request, handler, timeout).leftMap(_.toString)

  override protected def pretty: Pretty[GrpcSequencerConnectionX] =
    prettyOfClass(
      param("name", _.name.singleQuoted),
      param("underlying", _.underlying),
    )

  override val subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    new GrpcSubscriptionErrorRetryPolicy(loggerFactory)
}
