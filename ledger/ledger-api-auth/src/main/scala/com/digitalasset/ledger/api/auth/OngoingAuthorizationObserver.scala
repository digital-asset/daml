// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import akka.actor.Scheduler
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.jwt.JwtTimestampLeeway
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.error.definitions.LedgerApiErrors
import com.daml.platform.localstore.api.UserManagementStore
import io.grpc.StatusRuntimeException
import io.grpc.stub.ServerCallStreamObserver

import scala.concurrent.ExecutionContext

private[auth] final class OngoingAuthorizationObserver[A](
    observer: ServerCallStreamObserver[A],
    originalClaims: ClaimSet.Claims,
    nowF: () => Instant,
    userRightsCheckerO: Option[UserRightsChangeAsyncChecker],
    userRightsCheckIntervalInSeconds: Int,
    lastUserRightsCheckTime: AtomicReference[Instant],
    jwtTimestampLeeway: Option[JwtTimestampLeeway],
)(implicit loggingContext: LoggingContext)
    extends ServerCallStreamObserver[A] {

  private val logger = ContextualizedLogger.get(getClass)
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)

  // Guards against propagating calls to delegate observer after either
  // [[onComplete]] or [[onError]] has already been called once.
  // We need this because [[onError]] can be invoked two concurrent sources:
  // 1) scheduled user rights state change task (see [[cancellableO]]),
  // 2) upstream component that is translating upstream Akka stream into [[onNext]] and other signals.
  private var afterCompletionOrError = false

  private val cancelUserRightsChecksO =
    userRightsCheckerO.map(_.schedule(() => onError(staleStreamAuthError)))

  override def isCancelled: Boolean = synchronized(observer.isCancelled)

  override def setOnCancelHandler(runnable: Runnable): Unit = synchronized(
    observer.setOnCancelHandler(runnable)
  )

  override def setCompression(s: String): Unit = synchronized(observer.setCompression(s))

  override def isReady: Boolean = synchronized(observer.isReady)

  override def setOnReadyHandler(runnable: Runnable): Unit = synchronized(
    observer.setOnReadyHandler(runnable)
  )

  override def disableAutoInboundFlowControl(): Unit = synchronized(
    observer.disableAutoInboundFlowControl()
  )

  override def request(i: Int): Unit = synchronized(observer.request(i))

  override def setMessageCompression(b: Boolean): Unit = synchronized(
    observer.setMessageCompression(b)
  )

  override def onNext(v: A): Unit = onlyBeforeCompletionOrError {
    val now = nowF()
    (for {
      _ <- checkClaimsExpiry(now)
      _ <- checkUserRightsRefreshTimeout(now)
    } yield ()) match {
      case Right(_) => observer.onNext(v)
      case Left(e) => onError(e)
    }
  }

  override def onError(throwable: Throwable): Unit = onlyBeforeCompletionOrError {
    afterCompletionOrError = true
    cancelUserRightsChecksO.foreach(_.apply())
    observer.onError(throwable)
  }

  override def onCompleted(): Unit = onlyBeforeCompletionOrError {
    afterCompletionOrError = true
    cancelUserRightsChecksO.foreach(_.apply())
    observer.onCompleted()
  }

  private def onlyBeforeCompletionOrError(body: => Unit): Unit =
    synchronized(
      if (!afterCompletionOrError) {
        body
      }
    )

  private def checkUserRightsRefreshTimeout(now: Instant): Either[StatusRuntimeException, Unit] = {
    // Safety switch to abort the stream if the user-rights-state-check task
    // fails to refresh within 2*[[userRightsCheckIntervalInSeconds]] seconds.
    // In normal conditions we expected the refresh delay to be about [[userRightsCheckIntervalInSeconds]] seconds.
    if (
      originalClaims.resolvedFromUser &&
      lastUserRightsCheckTime.get.isBefore(
        now.minusSeconds(2 * userRightsCheckIntervalInSeconds.toLong)
      )
    ) {
      Left(staleStreamAuthError)
    } else Right(())
  }

  private def checkClaimsExpiry(now: Instant): Either[StatusRuntimeException, Unit] =
    originalClaims
      .notExpired(now, jwtTimestampLeeway)
      .left
      .map(authorizationError =>
        LedgerApiErrors.AuthorizationChecks.PermissionDenied
          .Reject(authorizationError.reason)(errorLogger)
          .asGrpcError
      )

  private def staleStreamAuthError: StatusRuntimeException =
    // Terminate the stream, so that clients will restart their streams
    // and claims will be rechecked precisely.
    LedgerApiErrors.AuthorizationChecks.StaleUserManagementBasedStreamClaims
      .Reject()(errorLogger)
      .asGrpcError
}

private[auth] object OngoingAuthorizationObserver {

  /** @param userRightsCheckIntervalInSeconds - determines the interval at which to check whether user rights state has changed.
    *                                          Also, double of this value serves as timeout value for subsequent user rights state checks.
    */
  def apply[A](
      observer: ServerCallStreamObserver[A],
      originalClaims: ClaimSet.Claims,
      nowF: () => Instant,
      userManagementStore: UserManagementStore,
      userRightsCheckIntervalInSeconds: Int,
      akkaScheduler: Scheduler,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  )(implicit loggingContext: LoggingContext, ec: ExecutionContext): ServerCallStreamObserver[A] = {

    val lastUserRightsCheckTime = new AtomicReference(nowF())
    val userRightsCheckerO = if (originalClaims.resolvedFromUser) {
      val checker = new UserRightsChangeAsyncChecker(
        lastUserRightsCheckTime = lastUserRightsCheckTime,
        originalClaims = originalClaims,
        nowF: () => Instant,
        userManagementStore: UserManagementStore,
        userRightsCheckIntervalInSeconds: Int,
        akkaScheduler: Scheduler,
      )
      Some(checker)
    } else {
      None
    }
    new OngoingAuthorizationObserver(
      observer = observer,
      originalClaims = originalClaims,
      nowF = nowF,
      userRightsCheckerO = userRightsCheckerO,
      userRightsCheckIntervalInSeconds = userRightsCheckIntervalInSeconds,
      lastUserRightsCheckTime = lastUserRightsCheckTime,
      jwtTimestampLeeway = jwtTimestampLeeway,
    )
  }

}
