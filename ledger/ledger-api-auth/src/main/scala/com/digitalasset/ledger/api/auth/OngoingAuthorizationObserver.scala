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

  private val logger = ContextualizedLogger.get(getClass)
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)

  // This flag is set only by the scheduled user rights check task
  @volatile private var shouldAbort = false
  @volatile private var mostRecentUserInfoRefreshTime = nowF()

  private lazy val userId = originalClaims.applicationId.fold[Ref.UserId](
    throw new RuntimeException(
      "Claims were resolved from a user but userId (applicationId) is missing in the claims."
    )
  )(Ref.UserId.assertFromString)

  private val cancellable: Cancellable = {
    val delay = userRightsCheckIntervalInSeconds.seconds
    // Note: https://doc.akka.io/docs/akka/2.6.13/scheduler.html states that:
    // "All scheduled task will be executed when the ActorSystem is terminated, i.e. the task may execute before its timeout."
    akkaScheduler.scheduleWithFixedDelay(initialDelay = delay, delay = delay)(runnable =
      checkUserRights _
    )
  }

  private def checkUserRights(): Unit = {
    // This check ensures that once decided to abort there is no going back.
    if (!shouldAbort) {
      userManagementStore
        .listUserRights(userId)
        .onComplete {
          case Failure(_) => shouldAbort = true
          case Success(Left(_)) => shouldAbort = true
          case Success(Right(userRights)) =>
            val updatedClaims = AuthorizationInterceptor.convertUserRightsToClaims(userRights)
            shouldAbort = updatedClaims.toSet != originalClaims.claims.toSet
            mostRecentUserInfoRefreshTime = nowF()
        }
    }
  }

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
      case Right(_) => observer.onNext(v)
      case Left(e) => observer.onError(e)
    }

  override def onError(throwable: Throwable): Unit = {
    cancelUserRightsCheckTask()
    observer.onError(throwable)
  }

  override def onCompleted(): Unit = {
    cancelUserRightsCheckTask()
    observer.onCompleted()
  }

  private def authorize: Either[StatusRuntimeException, Unit] = {
    val now = nowF()
    for {
      _ <- originalClaims
        .notExpired(now)
        .left
        .map(authorizationError =>
          errorFactories.permissionDenied(authorizationError.reason)(errorLogger)
        )
      _ <-
        if (
          originalClaims.resolvedFromUser &&
          (shouldAbort ||
            mostRecentUserInfoRefreshTime.isAfter(
              now.plusSeconds(2 * userRightsCheckIntervalInSeconds.toLong)
            ))
        ) {
          cancelUserRightsCheckTask()
          // Terminate the stream, so that clients will restart their streams
          // and claims will be rechecked precisely.
          Left(
            LedgerApiErrors.AuthorizationChecks.StaleUserManagementBasedStreamClaims
              .Reject()(errorLogger)
              .asGrpcError
          )
        } else Right(())
    } yield {
      ()
    }
  }

  private def cancelUserRightsCheckTask(): Unit = {
    cancellable.cancel()
    if (!cancellable.isCancelled) {
      logger.debug(s"Failed to cancel stream authorization task")
    }
  }

}
