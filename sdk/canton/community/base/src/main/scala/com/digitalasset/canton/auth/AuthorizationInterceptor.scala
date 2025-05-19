// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.AuthorizationInterceptor.PassThroughAuthorizer
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.*

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success, Try}

/** This interceptor uses the given [[AuthService]] to get [[ClaimSet.Claims]] for the current request,
  * and then stores them in the current [[io.grpc.Context]].
  */
class AuthorizationInterceptor(
    authService: AuthService,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
    implicit val ec: ExecutionContext,
    statelessAuthorizer: StatelessAuthorizer = PassThroughAuthorizer,
) extends ServerInterceptor
    with NamedLogging {
  import LoggingContextWithTrace.implicitExtractTraceContext

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      nextListener: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    // Note: Context uses ThreadLocal storage, we need to capture it outside of the async block below.
    // Contexts are immutable and safe to pass around.
    val prevCtx = Context.current

    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)

    val serviceName = call.getMethodDescriptor.getServiceName

    // The method interceptCall() must return a Listener.
    // The target listener is created by calling `Contexts.interceptCall()`.
    // However, this is only done after we have asynchronously received the claims.
    // Therefore, we need to return a listener that buffers all messages until the target listener is available.
    new AsyncForwardingListener[ReqT] {
      private def closeWithError(error: StatusRuntimeException) = {
        call.close(error.getStatus, error.getTrailers)
        new ServerCall.Listener[Nothing]() {}
      }

      headerToClaims(headers, serviceName)
        .transform {
          case Failure(f) => Failure(f)
          case Success(claimSet) => statelessAuthorizer(claimSet)
        }
        .onComplete {
          case Failure(error: StatusRuntimeException) =>
            closeWithError(error)
          case Failure(exception: Throwable) =>
            val error = AuthorizationChecksErrors.InternalAuthorizationError
              .Reject(
                message = "Failed to get claims from request metadata",
                throwable = exception,
              )
              .asGrpcError
            closeWithError(error)
          case Success(claimSet) =>
            val nextCtx = prevCtx.withValue(AuthorizationInterceptor.contextKeyClaimSet, claimSet)
            // Contexts.interceptCall() creates a listener that wraps all methods of `nextListener`
            // such that `Context.current` returns `nextCtx`.
            val nextListenerWithContext =
              Contexts.interceptCall(nextCtx, call, headers, nextListener)
            setNextListener(nextListenerWithContext)
            nextListenerWithContext
        }
    }
  }

  def headerToClaims(
      headers: Metadata,
      serviceName: String,
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): Future[ClaimSet] =
    authService
      .decodeMetadata(headers, serviceName)
      .asScala
}

object AuthorizationInterceptor {

  val contextKeyClaimSet: Context.Key[ClaimSet] = Context.key[ClaimSet]("AuthServiceDecodedClaim")

  def extractClaimSetFromContext(): Try[ClaimSet] = {
    val claimSet = AuthorizationInterceptor.contextKeyClaimSet.get()
    if (claimSet == null)
      Failure(
        new RuntimeException(
          "Thread local context unexpectedly does not store authorization claims. Perhaps a Future was used in some intermediate computation and changed the executing thread?"
        )
      )
    else
      Success(claimSet)
  }

  case object PassThroughAuthorizer extends StatelessAuthorizer {
    override def apply(claimSet: ClaimSet)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Try[ClaimSet] =
      Success(claimSet)
  }
}
