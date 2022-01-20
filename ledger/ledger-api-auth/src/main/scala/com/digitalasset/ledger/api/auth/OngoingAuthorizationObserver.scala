// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.StatusRuntimeException
import io.grpc.stub.ServerCallStreamObserver

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

private[auth] final class OngoingAuthorizationObserver[A](
    observer: ServerCallStreamObserver[A],
    originalClaims: ClaimSet.Claims,
    nowF: () => Instant,
    errorFactories: ErrorFactories,
    userManagementStore: UserManagementStore,
    implicit val ec: ExecutionContext,
    claimsFreshnessCheckDelayInSeconds: Int,
)(implicit loggingContext: LoggingContext)
    extends ServerCallStreamObserver[A] {

  private val logger = ContextualizedLogger.get(getClass)
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private var lastUserInfoAccessTime = Instant.EPOCH
  private val shouldAbort = new AtomicBoolean(false)
  private val aborted = new AtomicBoolean(false)

  private val userRightsRefreshInProgress = new AtomicBoolean(false)

  private val userId = originalClaims.applicationId.fold[Ref.UserId](
    throw new RuntimeException(
      "Claims were resolved from a user but userId (applicationId) is missing in the claims."
    )
  )(Ref.UserId.assertFromString)

  override def isCancelled: Boolean = observer.isCancelled

  override def setOnCancelHandler(runnable: Runnable): Unit = observer.setOnCancelHandler(runnable)

  override def setCompression(s: String): Unit = observer.setCompression(s)

  override def isReady: Boolean = observer.isReady

  override def setOnReadyHandler(runnable: Runnable): Unit = observer.setOnReadyHandler(runnable)

  override def disableAutoInboundFlowControl(): Unit = observer.disableAutoInboundFlowControl()

  override def request(i: Int): Unit = observer.request(i)

  override def setMessageCompression(b: Boolean): Unit = observer.setMessageCompression(b)

  override def onNext(v: A): Unit =
    authorize match {
      case _ if aborted.get() => ()
      case Right(_) => observer.onNext(v)
      case Left(authorizationError: AuthorizationError) =>
        val e: StatusRuntimeException =
          errorFactories.permissionDenied(authorizationError.reason)(errorLogger)
        observer.onError(e)
    }

  override def onError(throwable: Throwable): Unit = observer.onError(throwable)

  override def onCompleted(): Unit = observer.onCompleted()

  private def authorize: Either[AuthorizationError, Unit] = {
    val now = nowF()
    for {
      _ <- originalClaims.notExpired(now)
      _ = validateClaimsResolvedFromUser(now)
    } yield {
      ()
    }
  }

  /** Aborts the stream by throwing an exception
    * if any change in compared the original user claims has been detected
    * or if user claims change check times out.
    */
  private def validateClaimsResolvedFromUser(now: Instant): Unit = {
    if (originalClaims.resolvedFromUser) {
      if (shouldAbort.get()) {
        signalError()
      } else if (
        !userRightsRefreshInProgress.get()
        && now.isAfter(
          lastUserInfoAccessTime.plus(claimsFreshnessCheckDelayInSeconds.toLong, ChronoUnit.SECONDS)
        )
      ) {
        userRightsRefreshInProgress.set(true)
        scheduleAuthenticationRefresh()
      } else if (userRightsRefreshInProgress.get()) {
        // Timing out user rights refresh
        if (
          now.isAfter(
            lastUserInfoAccessTime
              .plus(2 * claimsFreshnessCheckDelayInSeconds.toLong, ChronoUnit.SECONDS)
          )
        ) {
          shouldAbort.set(true)
          signalError()
        }
      }

    }
  }

  private def scheduleAuthenticationRefresh(): Unit = {
    userManagementStore
      .listUserRights(userId)
      .onComplete {
        case Failure(_) => shouldAbort.set(true)
        case Success(Left(_)) => shouldAbort.set(true)
        case Success(Right(userRights)) =>
          if (!shouldAbort.get()) {
            val updatedClaims = AuthorizationInterceptor.convertUserRightsToClaims(userRights)
            shouldAbort.set(updatedClaims.toSet != originalClaims.claims.toSet)
            lastUserInfoAccessTime = nowF()
            userRightsRefreshInProgress.set(false)
          }
      }
  }

  private def signalError(): Unit = {
    // Throwing an error that has gRPC status ABORTED so that clients will restart their streams
    // and claims will be rechecked precisely.
    onError(
      LedgerApiErrors.AuthorizationChecks.StaleUserManagementBasedStreamClaims
        .Reject()(errorLogger)
        .asGrpcError
    )
    aborted.set(true)
  }
}
