// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.auth.AuthInterceptor.PassThroughClaimResolver
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** This interceptor uses the given [[AuthService]] to get [[ClaimSet.Claims]] for the current
  * request, and then stores them in the current [[io.grpc.Context]].
  */
class AuthInterceptor(
    authServices: Seq[AuthService],
    val loggerFactory: NamedLoggerFactory,
    implicit val ec: ExecutionContext,
    claimResolver: ClaimResolver = PassThroughClaimResolver,
) extends NamedLogging {
  import LoggingContextWithTrace.implicitExtractTraceContext

  def extractClaims(
      authToken: Option[String],
      serviceName: String,
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): Future[ClaimSet] =
    // Note: Context uses ThreadLocal storage, we need to capture it outside of the async block below.
    // Contexts are immutable and safe to pass around.

    headerToClaims(authToken, serviceName)
      .flatMap(claimResolver.apply)
      .transform {
        case Failure(error: StatusRuntimeException) =>
          Failure(error)
        case Failure(exception: Throwable) =>
          val error = AuthorizationChecksErrors.InternalAuthorizationError
            .Reject(
              message = "Failed to get claims from request metadata",
              throwable = exception,
            )
            .asGrpcError
          Failure(error)
        case Success(claimSet) => Success(claimSet)
      }

  private val deny = Future.successful(ClaimSet.Unauthenticated: ClaimSet)

  def headerToClaims(
      authToken: Option[String],
      serviceName: String,
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): Future[ClaimSet] =
    authServices
      .foldLeft(deny) { case (acc, elem) =>
        acc.flatMap {
          case ClaimSet.Unauthenticated => elem.decodeToken(authToken, serviceName)
          case authenticated => Future.successful(authenticated)
        }
      }
}

object AuthInterceptor {

  val contextKeyClaimSet: Context.Key[ClaimSet] = Context.key[ClaimSet]("AuthServiceDecodedClaim")

  def extractClaimSetFromContext(): Try[ClaimSet] = {
    val claimSet = contextKeyClaimSet.get()
    if (claimSet == null)
      Failure(
        new RuntimeException(
          "Thread local context unexpectedly does not store authorization claims. Perhaps a Future was used in some intermediate computation and changed the executing thread?"
        )
      )
    else
      Success(claimSet)
  }

  case object PassThroughClaimResolver extends ClaimResolver {
    override def apply(claimSet: ClaimSet)(implicit
        loggingContext: LoggingContextWithTrace,
        errorLoggingContext: ErrorLoggingContext,
    ): Future[ClaimSet] =
      Future.successful(claimSet)
  }
}
