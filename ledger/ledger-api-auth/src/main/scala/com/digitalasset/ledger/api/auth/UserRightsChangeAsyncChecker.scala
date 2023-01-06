// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Scheduler
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.domain
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.localstore.api.UserManagementStore

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

private[auth] final class UserRightsChangeAsyncChecker(
    lastUserRightsCheckTime: AtomicReference[Instant],
    originalClaims: ClaimSet.Claims,
    nowF: () => Instant,
    userManagementStore: UserManagementStore,
    userRightsCheckIntervalInSeconds: Int,
    akkaScheduler: Scheduler,
)(implicit ec: ExecutionContext) {

  private val logger = ContextualizedLogger.get(getClass)

  /** Schedules an asynchronous and periodic task to check for user rights' state changes
    * @param userClaimsMismatchCallback - called when user rights' state change has been detected.
    * @return a function to cancel the scheduled task
    */
  def schedule(
      userClaimsMismatchCallback: () => Unit
  )(implicit loggingContext: LoggingContext): () => Unit = {
    val delay = userRightsCheckIntervalInSeconds.seconds
    val userId = originalClaims.applicationId.fold[Ref.UserId](
      throw new RuntimeException(
        "Claims were resolved from a user but userId (applicationId) is missing in the claims."
      )
    )(Ref.UserId.assertFromString)
    assert(
      originalClaims.resolvedFromUser,
      "The claims were not resolved from a user. Expected claims resolved from a user.",
    )
    // Note: https://doc.akka.io/docs/akka/2.6.13/scheduler.html states that:
    // "All scheduled task will be executed when the ActorSystem is terminated, i.e. the task may execute before its timeout."
    val cancellable =
      akkaScheduler.scheduleWithFixedDelay(initialDelay = delay, delay = delay)(() => {
        val t1 = System.currentTimeMillis()
        logger.info(
          s"Checking for stream staleness for userId: $userId. About to fetch user state from user management store"
        )
        val userState
            : Future[Either[UserManagementStore.Error, (domain.User, Set[domain.UserRight])]] =
          for {
            userRightsResult <- userManagementStore.listUserRights(userId)
            userResult <- userManagementStore.getUser(userId)
          } yield {
            for {
              userRights <- userRightsResult
              user <- userResult
            } yield (user, userRights)
          }
        userState
          .onComplete { v =>
            {
              logger.info(
                s"Checking for stream staleness for userId: $userId. About to inspect user state fetch from user management store. Elapsed: ${System
                    .currentTimeMillis() - t1} [ms]"
              )
              v match {
                case Failure(e) =>
                  logger.info(s"Could not retrieve user or user rights for userId: '$userId'", e)
                  userClaimsMismatchCallback()
                case Success(Left(e)) =>
                  logger.info(
                    s"Could not retrieve user or user rights for userId: '$userId'. Cause: $e"
                  )
                  userClaimsMismatchCallback()
                case Success(Right((user, userRights))) =>
                  val updatedClaims = AuthorizationInterceptor.convertUserRightsToClaims(userRights)
                  if (updatedClaims.toSet != originalClaims.claims.toSet || user.isDeactivated) {
                    logger.info(s"User or user rights mismatch detected for userId: '$userId'")
                    userClaimsMismatchCallback()
                  }
                  lastUserRightsCheckTime.set(nowF())
              }
            }
          }
      })
    () => (cancellable.cancel(): Unit)
  }

}
