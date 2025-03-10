// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication.grpc

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.getToken
import com.digitalasset.canton.sequencing.authentication.grpc.SequencerClientTokenAuthentication.{
  ReauthorizationClientInterceptor,
  authenticationMetadata,
}
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth.TokenFetcher
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

trait SequencerClientAuthentication {

  /** Apply the sequencer authentication components to a grpc client stub */
  def apply[S <: AbstractStub[S]](client: S): S
}

/** Provides call credentials and an interceptor to generate a token for outgoing requests and add
  * the token to the call metadata, then cause the token to be invalidated if an UNAUTHORIZED
  * response is returned.
  */
private[grpc] class SequencerClientTokenAuthentication(
    synchronizerId: SynchronizerId,
    member: Member,
    tokenManagerPerEndpoint: NonEmpty[Map[Endpoint, AuthenticationTokenManager]],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerClientAuthentication
    with NamedLogging {

  @VisibleForTesting
  private[grpc] val reauthorizationInterceptor = new ReauthorizationClientInterceptor(
    tokenManagerPerEndpoint
  )

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

      logger.debug(s"Detected client endpoint $maybeEndpoint")(TraceContext.empty)
      val tokenManager = getTokenManager(maybeEndpoint)

      implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
      getToken(tokenManager)
        .fold(
          applier.fail,
          token =>
            applier.apply(
              authenticationMetadata(
                synchronizerId,
                member,
                token,
                maybeEndpoint,
              )
            ),
        )
        .discard
    }
  }
}

object SequencerClientTokenAuthentication {

  def apply(
      synchronizerId: SynchronizerId,
      member: Member,
      obtainTokenPerEndpoint: NonEmpty[Map[Endpoint, TokenFetcher]],
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

  /** Will invalidate the current token if an UNAUTHORIZED response is observed. This will typically
    * happen after a token has expired. Note the caller will still receive the UNAUTHORIZED
    * response, although there are approaches for buffering and retrying the request this would not
    * work for all cases (such as a streamed response). Instead, the caller is expected to retry the
    * request which will attempt to fetch a new authorization token as the prior response
    * invalidated the previous token.
    */
  final class ReauthorizationClientInterceptor(
      tokenManagerPerEndpoint: NonEmpty[Map[Endpoint, AuthenticationTokenManager]]
  ) extends ClientInterceptor {

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

  def authenticationMetadata(
      synchronizerId: SynchronizerId,
      member: Member,
      token: AuthenticationToken,
      maybeEndpoint: Option[Endpoint] = None,
      into: Metadata = new Metadata(),
  ): Metadata = {
    into.put(Constant.SYNCHRONIZER_ID_METADATA_KEY, synchronizerId.toProtoPrimitive)
    into.put(Constant.MEMBER_ID_METADATA_KEY, member.toProtoPrimitive)
    into.put(Constant.AUTH_TOKEN_METADATA_KEY, token)
    maybeEndpoint.foreach(endpoint => into.put(Constant.ENDPOINT_METADATA_KEY, endpoint))
    into
  }
}
