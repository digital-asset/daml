// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication.grpc

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.google.common.annotations.VisibleForTesting
import io.grpc.*
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener
import io.grpc.internal.GrpcAttributes
import io.grpc.stub.AbstractStub

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/** Provides call credentials and an interceptor to generate a token for outgoing requests and add the token to the call
  * metadata, then cause the token to be invalidated if an UNAUTHORIZED response is returned.
  */
private[grpc] class SequencerClientTokenAuthentication(
    synchronizerId: SynchronizerId,
    member: Member,
    tokenManagerPerEndpoint: NonEmpty[Map[Endpoint, AuthenticationTokenManager]],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerClientAuthentication
    with NamedLogging {

  /** Apply the sequencer authentication components to a grpc client stub */
  def apply[S <: AbstractStub[S]](client: S): S =
    client.withCallCredentials(callCredentials).withInterceptors(reauthorizationInterceptor)

  private def getTokenManager(maybeEndpoint: Option[Endpoint]) = (for {
    endpoint <- maybeEndpoint
    tokenManager <- tokenManagerPerEndpoint.get(endpoint)
  } yield tokenManager).getOrElse(tokenManagerPerEndpoint.head1._2)

  /** Asks token manager for the current auth token and applies it to outgoing requests */
  @VisibleForTesting
  private[grpc] val callCredentials: CallCredentials = new CallCredentials {
    override def applyRequestMetadata(
        requestInfo: CallCredentials.RequestInfo,
        appExecutor: Executor,
        applier: CallCredentials.MetadataApplier,
    ): Unit = {
      val maybeEndpoint = for {
        clientEagAttrs <- Option(
          requestInfo.getTransportAttrs.get(GrpcAttributes.ATTR_CLIENT_EAG_ATTRS)
        )
        endpoint <- Option(clientEagAttrs.get(Endpoint.ATTR_ENDPOINT))
      } yield endpoint
      val tokenManager = getTokenManager(maybeEndpoint)

      implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
      tokenManager.getToken
        .leftMap(err =>
          Status.PERMISSION_DENIED.withDescription(s"Authentication token refresh error: $err")
        )
        .value
        .onShutdown(
          Left(
            CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
              .Error()
              .asGrpcError
              .getStatus
              .withDescription("Token refresh aborted due to shutdown")
          )
        )
        .recover {
          case grpcError: StatusRuntimeException =>
            // if auth token refresh fails with a grpc error, pass along that status so that the grpc subscription retry
            // mechanism can base the retry decision on it.
            Left(
              grpcError.getStatus
                .withDescription("Authentication token refresh failed with grpc error")
            )
          case NonFatal(ex) =>
            // otherwise indicate internal error
            Left(
              Status.INTERNAL
                .withDescription("Authentication token refresh failed with exception")
                .withCause(ex)
            )
        }
        .foreach {
          case Left(errorStatus) => applier.fail(errorStatus)
          case Right(token) =>
            applier.apply(generateMetadata(token, maybeEndpoint))
        }
    }
  }

  /** Will invalidate the current token if an UNAUTHORIZED response is observed.
    * This will typically happen after a token has expired.
    * Note the caller will still receive the UNAUTHORIZED response,
    * although there are approaches for buffering and retrying the request this would not
    * work for all cases (such as a streamed response).
    * Instead the caller is expected to retry the request which will attempt to fetch
    * a new authorization token as the prior response invalidated the previous token.
    */
  @VisibleForTesting
  private[grpc] val reauthorizationInterceptor = new ClientInterceptor {
    override def interceptCall[ReqT, RespT](
        method: MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions,
        next: Channel,
    ): ClientCall[ReqT, RespT] =
      new ReauthorizeClientCall(next.newCall(method, callOptions))

    private class ReauthorizeClientCall[ReqT, RespT](call: ClientCall[ReqT, RespT])
        extends SimpleForwardingClientCall[ReqT, RespT](call) {

      override def start(responseListener: ClientCall.Listener[RespT], headers: Metadata): Unit =
        super.start(new ReauthorizeClientCallListener(responseListener), headers)

      private class ReauthorizeClientCallListener(responseListener: ClientCall.Listener[RespT])
          extends SimpleForwardingClientCallListener[RespT](responseListener) {
        override def onClose(status: Status, trailers: Metadata): Unit = {
          if (status.getCode == Status.UNAUTHENTICATED.getCode) {
            val tokenManager = Option(trailers.get(Constant.ENDPOINT_METADATA_KEY))
              .flatMap(tokenManagerPerEndpoint.get)
              .getOrElse(tokenManagerPerEndpoint.head1._2)
            Option(trailers.get(Constant.AUTH_TOKEN_METADATA_KEY))
              .foreach(tokenManager.invalidateToken)
          }

          super.onClose(status, trailers)
        }
      }

    }
  }

  private def generateMetadata(
      token: AuthenticationToken,
      maybeEndpoint: Option[Endpoint],
  ): Metadata = {
    val metadata = new Metadata()
    metadata.put(Constant.MEMBER_ID_METADATA_KEY, member.toProtoPrimitive)
    metadata.put(Constant.AUTH_TOKEN_METADATA_KEY, token)
    metadata.put(Constant.SYNCHRONIZER_ID_METADATA_KEY, synchronizerId.toProtoPrimitive)
    maybeEndpoint.foreach(endpoint => metadata.put(Constant.ENDPOINT_METADATA_KEY, endpoint))
    metadata
  }
}

object SequencerClientTokenAuthentication {
  def apply(
      synchronizerId: SynchronizerId,
      member: Member,
      obtainTokenPerEndpoint: NonEmpty[
        Map[
          Endpoint,
          TraceContext => EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry],
        ]
      ],
      isClosed: => Boolean,
      tokenManagerConfig: AuthenticationTokenManagerConfig,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SequencerClientAuthentication = {
    val tokenManagerPerEndpoint = obtainTokenPerEndpoint.transform { case (_, obtainToken) =>
      new AuthenticationTokenManager(
        obtainToken,
        isClosed,
        tokenManagerConfig,
        clock,
        loggerFactory,
      )
    }
    new SequencerClientTokenAuthentication(
      synchronizerId,
      member,
      tokenManagerPerEndpoint,
      loggerFactory,
    )
  }

}

trait SequencerClientAuthentication {

  /** Apply the sequencer authentication components to a grpc client stub */
  def apply[S <: AbstractStub[S]](client: S): S
}
