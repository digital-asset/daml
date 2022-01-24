// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

import akka.actor.{Cancellable, Scheduler}
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
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/** @param userRightsCheckIntervalInSeconds - determines the interval at which to check whether user rights state has changed.
  *                                         Also, double of this value serves as timeout value for subsequent user rights state checks.
  */
private[auth] final class OngoingAuthorizationObserver[A](
    observer: ServerCallStreamObserver[A],
    originalClaims: ClaimSet.Claims,
    nowF: () => Instant,
    errorFactories: ErrorFactories,
    userManagementStore: UserManagementStore,
    implicit val ec: ExecutionContext,
    userRightsCheckIntervalInSeconds: Int,
    akkaScheduler: Scheduler,
)(implicit loggingContext: LoggingContext)
    extends ServerCallStreamObserver[A] {
  self =>

  private val logger = ContextualizedLogger.get(getClass)
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)

  @volatile private var mostRecentUserInfoRefreshTime = nowF()

  private lazy val userId = originalClaims.applicationId.fold[Ref.UserId](
    throw new RuntimeException(
      "Claims were resolved from a user but userId (applicationId) is missing in the claims."
    )
  )(Ref.UserId.assertFromString)

  private val cancellableO: Option[Cancellable] = {
    if (originalClaims.resolvedFromUser) {
      val delay = userRightsCheckIntervalInSeconds.seconds
      // Note: https://doc.akka.io/docs/akka/2.6.13/scheduler.html states that:
      // "All scheduled task will be executed when the ActorSystem is terminated, i.e. the task may execute before its timeout."
      val c = akkaScheduler.scheduleWithFixedDelay(initialDelay = delay, delay = delay)(runnable =
        checkUserRights _
      )
      Some(c)
    } else None
  }

  private def checkUserRights(): Unit = {
    userManagementStore
      .listUserRights(userId)
      .onComplete {
        case Failure(_) | Success(Left(_)) =>
          self.synchronized(observer.onError(staleStreamAuthError))
          cancelUserRightsCheckTask()
        case Success(Right(userRights)) =>
          val updatedClaims = AuthorizationInterceptor.convertUserRightsToClaims(userRights)
          if (updatedClaims.toSet != originalClaims.claims.toSet) {
            self.synchronized(observer.onError(staleStreamAuthError))
            cancelUserRightsCheckTask()
          }
          mostRecentUserInfoRefreshTime = nowF()
      }
  }

  override def isCancelled: Boolean = observer.isCancelled

  override def setOnCancelHandler(runnable: Runnable): Unit = observer.setOnCancelHandler(runnable)

  override def setCompression(s: String): Unit = observer.setCompression(s)

  override def isReady: Boolean = observer.isReady

  override def setOnReadyHandler(runnable: Runnable): Unit = observer.setOnReadyHandler(runnable)

  override def disableAutoInboundFlowControl(): Unit = observer.disableAutoInboundFlowControl()

  override def request(i: Int): Unit = observer.request(i)

  override def setMessageCompression(b: Boolean): Unit =
    self.synchronized(observer.setMessageCompression(b))

  override def onNext(v: A): Unit = {
    val now = nowF()
    (for {
      _ <- checkClaimsExpiry(now)
      _ <- checkUserRightsRefreshTimeout(now)
    } yield {
      ()
    }) match {
      case Right(_) => self.synchronized(observer.onNext(v))
      case Left(e) =>
        cancelUserRightsCheckTask()
        self.synchronized(observer.onError(e))
    }
  }

  override def onError(throwable: Throwable): Unit = {
    cancelUserRightsCheckTask()
    self.synchronized(observer.onError(throwable))
  }

  override def onCompleted(): Unit = {
    cancelUserRightsCheckTask()
    self.synchronized(observer.onCompleted())
  }

  private def checkUserRightsRefreshTimeout(now: Instant) = {
    if (
      originalClaims.resolvedFromUser &&
      mostRecentUserInfoRefreshTime.isAfter(
        now.plusSeconds(2 * userRightsCheckIntervalInSeconds.toLong)
      )
    ) {
      Left(staleStreamAuthError)
    } else Right(())
  }

  private def checkClaimsExpiry(now: Instant) = {
    originalClaims
      .notExpired(now)
      .left
      .map(authorizationError =>
        errorFactories.permissionDenied(authorizationError.reason)(errorLogger)
      )
  }

  private def staleStreamAuthError: StatusRuntimeException = {
    // Terminate the stream, so that clients will restart their streams
    // and claims will be rechecked precisely.
    LedgerApiErrors.AuthorizationChecks.StaleUserManagementBasedStreamClaims
      .Reject()(errorLogger)
      .asGrpcError
  }

  private def cancelUserRightsCheckTask(): Unit = {
    cancellableO.foreach { cancellable =>
      cancellable.cancel()
      if (!cancellable.isCancelled) {
        logger.debug(s"Failed to cancel stream authorization task")
      }
    }
  }

}
