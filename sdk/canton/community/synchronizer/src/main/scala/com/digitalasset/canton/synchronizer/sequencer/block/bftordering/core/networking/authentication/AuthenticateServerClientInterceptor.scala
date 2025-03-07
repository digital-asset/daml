// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.authentication

import com.digitalasset.canton.synchronizer.sequencing.authentication.{
  MemberAuthenticationService,
  MemberAuthenticator,
}
import com.digitalasset.canton.topology.SequencerId
import io.grpc.*
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall

import scala.concurrent.Promise

private[networking] class AuthenticateServerClientInterceptor(
    memberAuthenticationService: MemberAuthenticationService,
    sequencerIdPromise: Promise[SequencerId],
) extends ClientInterceptor {

  import AuthenticateServerClientInterceptor.*

  override def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel,
  ): ClientCall[ReqT, RespT] =
    new AuthenticateServerSingleForwardingCall[ReqT, RespT](
      memberAuthenticationService,
      sequencerIdPromise,
      method,
      callOptions,
      next,
    )
}

object AuthenticateServerClientInterceptor {

  private class AuthenticateServerSingleForwardingCall[ReqT, RespT](
      memberAuthenticationService: MemberAuthenticationService,
      sequencerIdPromise: Promise[SequencerId],
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel,
  ) extends SimpleForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions)) {

    override def start(
        responseListener: ClientCall.Listener[RespT],
        headers: Metadata,
    ): Unit =
      super.start(
        new AuthenticateServerClientCallListener[ReqT, RespT](
          memberAuthenticationService,
          sequencerIdPromise,
          clientCall = this,
          responseListenerDelegate = responseListener,
        ),
        headers,
      )
  }

  private class AuthenticateServerClientCallListener[ReqT, RespT](
      memberAuthenticationService: MemberAuthenticationService,
      sequencerIdPromise: Promise[SequencerId],
      clientCall: ClientCall[ReqT, RespT],
      responseListenerDelegate: ClientCall.Listener[RespT],
  ) extends ClientCall.Listener[RespT] {

    override def onHeaders(headers: Metadata): Unit =
      MemberAuthenticator
        .extractAuthenticationCredentials(headers)
        .fold {
          val msg = "Authentication headers are not set"
          val exception = unauthenticatedException(msg)
          sequencerIdPromise.failure(exception)
          clientCall.cancel(msg, exception)
        } { authenticationCredentials =>
          MemberAuthenticator
            .authenticate(memberAuthenticationService, authenticationCredentials) match {
            case Right(_) =>
              SequencerId
                .fromProtoPrimitive(authenticationCredentials.member, "member")
                .fold(
                  { error =>
                    val errorMessage = error.message
                    val exception = unauthenticatedException(errorMessage)
                    sequencerIdPromise.failure(exception)
                    clientCall.cancel(errorMessage, exception)
                  },
                  { sequencerId =>
                    sequencerIdPromise.success(sequencerId)
                    super.onHeaders(headers)
                  },
                )
            case Left(verifyTokenError) =>
              clientCall.cancel(
                verifyTokenError.message,
                Status.UNAUTHENTICATED
                  .withDescription(verifyTokenError.message)
                  .asRuntimeException(),
              )
          }
        }

    private def unauthenticatedException(msg: String) =
      Status.UNAUTHENTICATED.withDescription(msg).asException(new Metadata)

    override def onMessage(message: RespT): Unit = responseListenerDelegate.onMessage(message)

    override def onClose(status: Status, trailers: Metadata): Unit =
      responseListenerDelegate.onClose(status, trailers)

    override def onReady(): Unit = responseListenerDelegate.onReady()
  }
}
