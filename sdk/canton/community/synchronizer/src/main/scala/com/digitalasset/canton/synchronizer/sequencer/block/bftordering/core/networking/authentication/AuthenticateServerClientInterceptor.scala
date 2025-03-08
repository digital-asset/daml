// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.authentication

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencing.authentication.{
  MemberAuthenticationService,
  MemberAuthenticator,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.*
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall

private[networking] class AuthenticateServerClientInterceptor(
    memberAuthenticationService: MemberAuthenticationService,
    onAuthenticationSuccess: SequencerId => Unit,
    onAuthenticationFailure: Throwable => Unit,
    override val loggerFactory: NamedLoggerFactory,
) extends ClientInterceptor
    with NamedLogging {

  import AuthenticateServerClientInterceptor.*

  override def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel,
  ): ClientCall[ReqT, RespT] =
    new AuthenticateServerSingleForwardingCall[ReqT, RespT](
      memberAuthenticationService,
      onAuthenticationSuccess,
      onAuthenticationFailure,
      method,
      callOptions,
      next,
      loggerFactory,
    )
}

object AuthenticateServerClientInterceptor {

  private class AuthenticateServerSingleForwardingCall[ReqT, RespT](
      memberAuthenticationService: MemberAuthenticationService,
      onAuthenticationSuccess: SequencerId => Unit,
      onAuthenticationFailure: Throwable => Unit,
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel,
      override val loggerFactory: NamedLoggerFactory,
  ) extends SimpleForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions))
      with NamedLogging {

    override def start(
        responseListener: ClientCall.Listener[RespT],
        headers: Metadata,
    ): Unit =
      super.start(
        new AuthenticateServerClientCallListener[ReqT, RespT](
          memberAuthenticationService,
          onAuthenticationSuccess,
          onAuthenticationFailure,
          clientCall = this,
          responseListenerDelegate = responseListener,
          loggerFactory,
        ),
        headers,
      )
  }

  private class AuthenticateServerClientCallListener[ReqT, RespT](
      memberAuthenticationService: MemberAuthenticationService,
      onAuthenticationSuccess: SequencerId => Unit,
      onAuthenticationFailure: Throwable => Unit,
      clientCall: ClientCall[ReqT, RespT],
      responseListenerDelegate: ClientCall.Listener[RespT],
      override val loggerFactory: NamedLoggerFactory,
  ) extends ClientCall.Listener[RespT]
      with NamedLogging {

    override def onHeaders(headers: Metadata): Unit = {
      implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
      MemberAuthenticator
        .extractAuthenticationCredentials(headers)
        .fold {
          val errorMessage = "Authentication headers are not set"
          val exception = unauthenticatedException(errorMessage)
          logger.error(errorMessage, exception)
          onAuthenticationFailure(exception)
          clientCall.cancel(errorMessage, exception)
        } { authenticationCredentials =>
          MemberAuthenticator.authenticate(
            memberAuthenticationService,
            authenticationCredentials,
          ) match {
            case Right(_) =>
              SequencerId
                .fromProtoPrimitive(authenticationCredentials.member, "member")
                .fold(
                  { parseError =>
                    val errorMessage = parseError.message
                    val exception = unauthenticatedException(errorMessage)
                    logger.error(errorMessage, exception)
                    onAuthenticationFailure(exception)
                    clientCall.cancel(errorMessage, exception)
                  },
                  { sequencerId =>
                    logger.debug(s"Successfully authenticated server $sequencerId")
                    onAuthenticationSuccess(sequencerId)
                    super.onHeaders(headers)
                  },
                )
            case Left(verifyTokenError) =>
              val errorMessage = verifyTokenError.message
              val exception = unauthenticatedException(errorMessage)
              logger.error(errorMessage, exception)
              onAuthenticationFailure(exception)
              clientCall.cancel(errorMessage, exception)
          }
        }
    }

    private def unauthenticatedException(msg: String) =
      Status.UNAUTHENTICATED.withDescription(msg).asException()

    override def onMessage(message: RespT): Unit = responseListenerDelegate.onMessage(message)

    override def onClose(status: Status, trailers: Metadata): Unit =
      responseListenerDelegate.onClose(status, trailers)

    override def onReady(): Unit = responseListenerDelegate.onReady()
  }
}
