// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.interceptor

import com.daml.ledger.api.auth.{AuthService, ClaimSet}
import io.grpc.{
  Context,
  Contexts,
  Metadata,
  ServerCall,
  ServerCallHandler,
  ServerInterceptor,
  Status,
}
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** This interceptor uses the given [[AuthService]] to get [[Claims]] for the current request,
  * and then stores them in the current [[Context]].
  */
final class AuthorizationInterceptor(protected val authService: AuthService, ec: ExecutionContext)
    extends ServerInterceptor {

  private val logger: Logger = LoggerFactory.getLogger(AuthorizationInterceptor.getClass)
  private val internalAuthenticationError =
    Status.INTERNAL.withDescription("Failed to get claims from request metadata")

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      nextListener: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    // Note: Context uses ThreadLocal storage, we need to capture it outside of the async block below.
    // Contexts are immutable and safe to pass around.
    val prevCtx = Context.current

    // The method interceptCall() must return a Listener.
    // The target listener is created by calling `Contexts.interceptCall()`.
    // However, this is only done after we have asynchronously received the claims.
    // Therefore, we need to return a listener that buffers all messages until the target listener is available.
    new AsyncForwardingListener[ReqT] {
      FutureConverters
        .toScala(authService.decodeMetadata(headers))
        .onComplete {
          case Failure(exception) =>
            logger.warn(s"Failed to get claims from request metadata: ${exception.getMessage}")
            call.close(internalAuthenticationError, new Metadata())
            new ServerCall.Listener[Nothing]() {}
          case Success(claimSet) =>
            val nextCtx = prevCtx.withValue(AuthorizationInterceptor.contextKeyClaimSet, claimSet)
            // Contexts.interceptCall() creates a listener that wraps all methods of `nextListener`
            // such that `Context.current` returns `nextCtx`.
            val nextListenerWithContext =
              Contexts.interceptCall(nextCtx, call, headers, nextListener)
            setNextListener(nextListenerWithContext)
            nextListenerWithContext
        }(ec)
    }
  }
}

object AuthorizationInterceptor {

  private[auth] val contextKeyClaimSet = Context.key[ClaimSet]("AuthServiceDecodedClaim")

  def extractClaimSetFromContext(): Option[ClaimSet] =
    Option(contextKeyClaimSet.get())

  def apply(authService: AuthService, ec: ExecutionContext): AuthorizationInterceptor =
    new AuthorizationInterceptor(authService, ec)

}
