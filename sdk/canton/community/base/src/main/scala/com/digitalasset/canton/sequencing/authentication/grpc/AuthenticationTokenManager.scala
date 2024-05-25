// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication.grpc

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.Thereafter.syntax.*
import io.grpc.Status

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

final case class AuthenticationTokenWithExpiry(
    token: AuthenticationToken,
    expiresAt: CantonTimestamp,
)

/** Attempts to hold a valid authentication token.
  * The first token will not be fetched until `getToken` is called for the first time.
  * Subsequent calls to `getToken` before the token is obtained will be resolved for the first token.
  * `getToken` always returns a `EitherT[Future, ...]` but if a token is already available will be completed immediately with that token.
  */
class AuthenticationTokenManager(
    obtainToken: TraceContext => EitherT[
      FutureUnlessShutdown,
      Status,
      AuthenticationTokenWithExpiry,
    ],
    isClosed: => Boolean,
    config: AuthenticationTokenManagerConfig,
    clock: Clock,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, traceContext: TraceContext)
    extends NamedLogging {

  sealed trait State
  case object NoToken extends State
  case class Refreshing(
      pending: EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry]
  ) extends State
  case class HaveToken(token: AuthenticationToken) extends State

  private val state = new AtomicReference[State](NoToken)

  /** Request a token.
    * If a token is immediately available the returned future will be immediately completed.
    * If there is no token it will cause a token refresh to start and be completed once obtained.
    * If there is a refresh already in progress it will be completed with this refresh.
    */
  def getToken: EitherT[FutureUnlessShutdown, Status, AuthenticationToken] =
    refreshToken(refreshWhenHaveToken = false)

  /** Invalid the current token if it matches the provided value.
    * Although unlikely, the token must be provided here in case a response terminates after a new token has already been generated.
    */
  def invalidateToken(invalidToken: AuthenticationToken): Unit = {
    val _ = state.updateAndGet {
      case HaveToken(token) if invalidToken == token => NoToken
      case other => other
    }
  }

  private def refreshToken(
      refreshWhenHaveToken: Boolean
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationToken] = {
    val refreshTokenPromise =
      new PromiseUnlessShutdown[Either[Status, AuthenticationTokenWithExpiry]](
        "refreshToken",
        FutureSupervisor.Noop,
      )(ecl = ErrorLoggingContext.fromTracedLogger(logger), ec = executionContext)
    val refreshingState = Refreshing(EitherT(refreshTokenPromise.futureUS))

    state.getAndUpdate {
      case NoToken => refreshingState
      case have @ HaveToken(_) => if (refreshWhenHaveToken) refreshingState else have
      case other => other
    } match {
      // we are already refreshing, so pass future result
      case Refreshing(pending) => pending.map(_.token)
      // we have a token, so share it
      case HaveToken(token) =>
        if (refreshWhenHaveToken) createRefreshTokenFuture(refreshTokenPromise)
        else EitherT.rightT[FutureUnlessShutdown, Status](token)
      // there is no token yet, so start refreshing and return pending result
      case NoToken =>
        createRefreshTokenFuture(refreshTokenPromise)
    }
  }

  private def createRefreshTokenFuture(
      promise: PromiseUnlessShutdown[Either[Status, AuthenticationTokenWithExpiry]]
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationToken] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    logger.debug("Refreshing authentication token")

    val currentRefresh = promise.futureUS
    def completeRefresh(result: State): Unit = {
      state.updateAndGet {
        case Refreshing(pending) if pending.value == currentRefresh => result
        case other => other
      }.discard
    }

    // asynchronously update the state once completed, one way or another
    val currentRefreshTransformed = currentRefresh.thereafter {
      case Failure(exception) =>
        exception match {
          case ex: io.grpc.StatusRuntimeException
              if ex.getStatus.getCode == io.grpc.Status.Code.CANCELLED =>
            logger.info("Token refresh cancelled", ex)
          case ex: io.grpc.StatusRuntimeException
              if ex.getStatus.getCode == io.grpc.Status.Code.UNAVAILABLE &&
                ex.getMessage.contains("Channel shutdown invoked") =>
            logger.info("Token refresh aborted due to shutdown", ex)
          case ex: io.grpc.StatusRuntimeException =>
            def collectCause(ex: Throwable): Seq[String] = {
              Seq(ex.getMessage) ++ Option(ex.getCause).toList.flatMap(collectCause)
            }
            val causes = collectCause(ex).mkString(", ")
            logger.warn(s"Token refresh failed with ${ex.getStatus} / $causes")
          case _ => logger.warn("Token refresh failed", exception)
        }
        completeRefresh(NoToken)
      case Success(UnlessShutdown.AbortedDueToShutdown) =>
        logger.warn(s"Token refresh aborted due to shutdown.")
        completeRefresh(NoToken)
      case Success(UnlessShutdown.Outcome(Left(error))) =>
        if (error.getCode == Status.Code.CANCELLED)
          logger.debug("Token refresh cancelled due to shutdown")
        else
          logger.warn(s"Token refresh encountered error: $error")
        completeRefresh(NoToken)
      case Success(
            UnlessShutdown.Outcome(Right(AuthenticationTokenWithExpiry(newToken, expiresAt)))
          ) =>
        logger.debug("Token refresh complete")
        completeRefresh(HaveToken(newToken))
        scheduleRefreshBefore(expiresAt)
    }

    promise.completeWith(obtainToken(traceContext).value)
    EitherT(currentRefreshTransformed).map(_.token)
  }

  private def scheduleRefreshBefore(expiresAt: CantonTimestamp): Unit = {
    if (!isClosed) {
      clock
        .scheduleAt(
          backgroundRefreshToken,
          expiresAt.minus(config.refreshAuthTokenBeforeExpiry.asJava),
        )
        .discard
    }
  }

  private def backgroundRefreshToken(_now: CantonTimestamp): Unit = if (!isClosed) {
    refreshToken(refreshWhenHaveToken = true).discard
  }

}
