// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.tracing.Telemetry
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.*

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class GrpcAuthInterceptor(
    val genInterceptor: AuthInterceptor,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
    implicit val ec: ExecutionContext,
) extends ServerInterceptor
    with NamedLogging {

  private def getAuthorizationHeader(headers: Metadata): Option[String] =
    Option(headers.get(GrpcAuthInterceptor.AUTHORIZATION_KEY))

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
      val authToken = getAuthorizationHeader(headers)
      genInterceptor
        .extractClaims(authToken, serviceName)
        .onComplete {
          case Failure(error: StatusRuntimeException) =>
            closeWithError(error)
          case Failure(unexpected: Throwable) =>
            val error = AuthorizationChecksErrors.InternalAuthorizationError
              .Reject(
                message = "Unexpected error while extracting claims",
                throwable = unexpected,
              )
              .asGrpcError
            Failure(error)
          case Success(claimSet) =>
            val nextCtx = prevCtx.withValue(AuthInterceptor.contextKeyClaimSet, claimSet)
            // Contexts.interceptCall() creates a listener that wraps all methods of `nextListener`
            // such that `Context.current` returns `nextCtx`.
            val nextListenerWithContext =
              Contexts.interceptCall(nextCtx, call, headers, nextListener)
            setNextListener(nextListenerWithContext)
            nextListenerWithContext
        }
    }
  }

}

object GrpcAuthInterceptor {

  /** The [[io.grpc.Metadata.Key]] to use for looking up the `Authorization` header in the request
    * metadata.
    */
  val AUTHORIZATION_KEY: Metadata.Key[String] =
    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)
}
