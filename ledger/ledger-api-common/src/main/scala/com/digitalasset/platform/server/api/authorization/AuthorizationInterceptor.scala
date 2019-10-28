// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization

import com.digitalasset.ledger.api.auth.{AuthService, Claims}
import io.grpc.{
  Context,
  Contexts,
  Metadata,
  ServerCall,
  ServerCallHandler,
  ServerInterceptor,
  Status
}
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * This interceptor uses the given [[AuthService]] to get [[Claims]] for the current request,
  * and then stores them in the current [[Context]].
  *
  * Use [[ApiServiceAuthorization]] to read the claims from the context.
  * */
class AuthorizationInterceptor(protected val authService: AuthService, ec: ExecutionContext)
    extends ServerInterceptor {

  protected val logger: Logger = LoggerFactory.getLogger(AuthorizationInterceptor.getClass)

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      nextListener: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
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
          case Success(claims) =>
            val nextCtx = prevCtx.withValue(AuthorizationInterceptor.contextKeyClaim, claims)
            // Contexts.interceptCall() creates a listener that wraps all methods of `nextListener`
            // such that `Context.current` returns `nextCtx`.
            val nextListenerWithContext =
              Contexts.interceptCall(nextCtx, call, headers, nextListener)
            setNextListener(nextListenerWithContext)
            nextListenerWithContext
          case Failure(exception) =>
            logger.warn(s"Failed to get claims from request metadata: ${exception.getMessage}")
            call.close(
              Status.INTERNAL.withDescription("Failed to get claims from request metadata"),
              new Metadata())
            new ServerCall.Listener[Nothing]() {}
        }(ec)
    }
  }
}

object AuthorizationInterceptor {
  val contextKeyClaim: Context.Key[Claims] = Context.key("AuthServiceDecodedClaim")

  def apply(authService: AuthService, ec: ExecutionContext): AuthorizationInterceptor = {
    new AuthorizationInterceptor(authService, ec)
  }
}
