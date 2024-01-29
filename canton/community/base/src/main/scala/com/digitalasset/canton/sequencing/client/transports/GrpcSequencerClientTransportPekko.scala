// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.health.HealthComponent.AlwaysHealthyComponent
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcServiceUnavailable
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.client.{
  SequencerSubscriptionPekko,
  SubscriptionErrorRetryPolicyPekko,
}
import com.digitalasset.canton.sequencing.protocol.{SubscriptionRequest, SubscriptionResponse}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{CallOptions, Context, ManagedChannel, Status, StatusRuntimeException}
import org.apache.pekko.stream.scaladsl.{Keep, Source}

import scala.concurrent.ExecutionContext

class GrpcSequencerClientTransportPekko(
    channel: ManagedChannel,
    callOptions: CallOptions,
    clientAuth: GrpcSequencerClientAuth,
    metrics: SequencerClientMetrics,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    protocolVersion: ProtocolVersion,
)(implicit
    executionContext: ExecutionContext,
    executionSequencerFactory: ExecutionSequencerFactory,
)
// TODO(#13789) Extend GrpcSequencerClientTransportCommon and drop support for non-Pekko subscriptions
    extends GrpcSequencerClientTransport(
      channel,
      callOptions,
      clientAuth,
      metrics,
      timeouts,
      loggerFactory,
      protocolVersion,
    )
    with SequencerClientTransportPekko {

  import GrpcSequencerClientTransportPekko.*

  override type SubscriptionError = GrpcSequencerSubscriptionError

  override def subscribe(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SubscriptionError] =
    subscribeInternal(request, requiresAuthentication = true)

  override def subscribeUnauthenticated(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SubscriptionError] =
    subscribeInternal(request, requiresAuthentication = false)

  private def subscribeInternal(
      subscriptionRequest: SubscriptionRequest,
      requiresAuthentication: Boolean,
  )(implicit traceContext: TraceContext): SequencerSubscriptionPekko[SubscriptionError] = {

    val subscriptionRequestP = subscriptionRequest.toProtoV0

    def mkSubscription[Resp: HasProtoTraceContext](
        subscriber: (v0.SubscriptionRequest, StreamObserver[Resp]) => Unit
    )(
        parseResponse: (Resp, TraceContext) => ParsingResult[SubscriptionResponse]
    ): SequencerSubscriptionPekko[SubscriptionError] = {
      val source = ClientAdapter
        .serverStreaming[v0.SubscriptionRequest, Resp](
          subscriptionRequestP,
          stubWithFreshContext(subscriber),
        )
        .map(Right(_))
        .concatLazy(
          // A sequencer subscription should never terminate; it's an endless stream.
          // So if we see a termination, then insert an appropriate error.
          // If there is an actual gRPC error, this source will not be evaluated as
          // `recover` below completes the stream before emitting.
          // See `PekkoUtilTest` for a unit test that this works as expected.
          Source.lazySingle { () =>
            // Info level, as this occurs from time to time due to the invalidation of the authentication token.
            logger.info("The sequencer subscription has been terminated by the server.")
            val error = GrpcError(
              "subscription",
              "sequencer",
              Status.UNAVAILABLE
                .withDescription("Connection terminated by the server.")
                .asRuntimeException(),
            )
            Left(ExpectedGrpcFailure(error))
          }
        )
        .recover(recoverOnError)
        // Everything up to here runs "synchronously" and can deal with cancellations
        // without causing shutdown synchronization problems
        // Technically, everything below until `takeUntilThenDrain` also could deal with
        // cancellations just fine, but we nevertheless establish the pattern here
        // to see how well it scales
        .withUniqueKillSwitchMat()(Keep.right)
        .map(
          _.map(
            _.flatMap(
              deserializeSubscriptionResponse(_)(parseResponse).leftMap(ResponseParseError)
            )
          )
        )
        // Stop emitting after the first parse error
        .takeUntilThenDrain(_.isLeft)
        .watchTermination()(Keep.both)
      SequencerSubscriptionPekko(
        source,
        // Transport does not report its health individually
        new AlwaysHealthyComponent("GrpcSequencerClientTransport", logger),
      )
    }

    if (protocolVersion >= ProtocolVersion.v5) {
      val subscriber =
        if (requiresAuthentication) sequencerServiceClient.subscribeVersioned _
        else sequencerServiceClient.subscribeUnauthenticatedVersioned _

      mkSubscription(subscriber)(SubscriptionResponse.fromVersionedProtoV0(protocolVersion)(_)(_))
    } else {
      val subscriber =
        if (requiresAuthentication) sequencerServiceClient.subscribe _
        else sequencerServiceClient.subscribeUnauthenticated _
      mkSubscription(subscriber)(SubscriptionResponse.fromProtoV0(protocolVersion)(_)(_))
    }

  }

  private def stubWithFreshContext[Req, Resp](
      stub: (Req, StreamObserver[Resp]) => Unit
  )(req: Req, obs: StreamObserver[Resp])(implicit traceContext: TraceContext): Unit = {
    // we intentionally don't use `Context.current()` as we don't want to inherit the
    // cancellation scope from upstream requests
    val context: CancellableContext = Context.ROOT.withCancellation()

    context.run { () =>
      TraceContextGrpc.withGrpcContext(traceContext) {
        stub(req, obs)
      }
    }
  }

  private def deserializeSubscriptionResponse[R: HasProtoTraceContext](subscriptionResponseP: R)(
      fromProto: (R, TraceContext) => ParsingResult[SubscriptionResponse]
  ): ParsingResult[OrdinarySerializedEvent] = {
    // we take the unusual step of immediately trying to deserialize the trace-context
    // so it is available here for logging
    implicit val traceContext: TraceContext = SerializableTraceContext
      .fromProtoSafeV0Opt(noTracingLogger)(
        implicitly[HasProtoTraceContext[R]].traceContext(subscriptionResponseP)
      )
      .unwrap
    logger.debug("Received a message from the sequencer.")
    fromProto(subscriptionResponseP, traceContext).map { response =>
      OrdinarySequencedEvent(response.signedSequencedEvent, response.trafficState)(
        response.traceContext
      )
    }
  }

  private def recoverOnError(implicit
      traceContext: TraceContext
  ): Throwable PartialFunction Either[GrpcSequencerSubscriptionError, Nothing] = {
    case s: StatusRuntimeException =>
      val grpcError = if (s.getStatus.getCode() == io.grpc.Status.CANCELLED) {
        // Since recoverOnError sits before the kill switch in the stream,
        // this error will be passed downstream only if the cancellation came from the server.
        // For if the subscription was cancelled by the client via the kill switch,
        // the kill switch won't let it through any more.
        GrpcServiceUnavailable(
          "subscription",
          "sequencer",
          s.getStatus,
          Option(s.getTrailers),
          None,
        )
      } else
        GrpcError("subscription", "sequencer", s)
      Left(ExpectedGrpcFailure(grpcError))
    case t: Throwable =>
      logger.error("The sequencer subscription failed unexpectedly.", t)
      Left(UnexpectedGrpcFailure(t))
  }

  override def subscriptionRetryPolicyPekko: SubscriptionErrorRetryPolicyPekko[SubscriptionError] =
    new GrpcSubscriptionErrorRetryPolicyPekko()
}

object GrpcSequencerClientTransportPekko {
  sealed trait GrpcSequencerSubscriptionError extends Product with Serializable

  final case class ExpectedGrpcFailure(error: GrpcError) extends GrpcSequencerSubscriptionError
  final case class UnexpectedGrpcFailure(ex: Throwable) extends GrpcSequencerSubscriptionError
  final case class ResponseParseError(error: ProtoDeserializationError)
      extends GrpcSequencerSubscriptionError
}
