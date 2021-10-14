// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.interceptor

import com.daml.error.ErrorCode.ApiException
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{DamlErrorCodeLoggingContext, ErrorCodesVersionSwitcher}
import com.daml.ledger.api.auth.{AuthService, ClaimSet}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc._

import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** This interceptor uses the given [[AuthService]] to get [[Claims]] for the current request,
  * and then stores them in the current [[Context]].
  */
final class AuthorizationInterceptor(
    protected val authService: AuthService,
    ec: ExecutionContext,
    errorCodesStatusSwitcher: ErrorCodesVersionSwitcher,
) extends ServerInterceptor {
  private val logger = ContextualizedLogger.get(getClass)

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
            val error = internalAuthenticationError(exception)
            call.close(error.getStatus, error.getTrailers)
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

  private def internalAuthenticationError(
      exception: Throwable
  ): StatusRuntimeException =
    LoggingContext.newLoggingContext { implicit loggingContext: LoggingContext =>
      errorCodesStatusSwitcher.choose(
        v1 = {
          logger.warn(s"Failed to get claims from request metadata: ${exception.getMessage}")
          new ApiException(
            status = Status.INTERNAL.withDescription("Failed to get claims from request metadata"),
            metadata = new Metadata(),
          )
        },
        v2 = LedgerApiErrors.AuthorizationChecks.InternalAuthorizationError
          .ClaimsFromMetadataExtractionFailed(exception)(
            new DamlErrorCodeLoggingContext(logger, loggingContext, None)
          )
          .asGrpcError,
      )
    }
}

object AuthorizationInterceptor {

  private[auth] val contextKeyClaimSet = Context.key[ClaimSet]("AuthServiceDecodedClaim")

  def extractClaimSetFromContext(): Option[ClaimSet] =
    Option(contextKeyClaimSet.get())

  def apply(
      authService: AuthService,
      ec: ExecutionContext,
      errorCodesStatusSwitcher: ErrorCodesVersionSwitcher,
  ): AuthorizationInterceptor =
    new AuthorizationInterceptor(authService, ec, errorCodesStatusSwitcher)
}
