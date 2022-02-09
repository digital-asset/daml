// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Scheduler
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.lf.data.Ref

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

private[auth] trait UserRightsChecker {
  def schedule(userClaimsMismatchCallback: () => Unit): () => Unit
}

private[auth] final class UserRightsCheckerImpl(
    lastUserRightsCheckTime: AtomicReference[Instant],
    originalClaims: ClaimSet.Claims,
    nowF: () => Instant,
    userManagementStore: UserManagementStore,
    userRightsCheckIntervalInSeconds: Int,
    akkaScheduler: Scheduler,
)(implicit ec: ExecutionContext)
    extends UserRightsChecker {

  def schedule(userClaimsMismatchCallback: () => Unit): () => Unit = {
    if (originalClaims.resolvedFromUser) {
      // Scheduling a task that periodically checks
      // whether user rights state has changed.
      // If user rights state has changed it aborts the stream by calling [[userClaimsMismatchCallback]]
      val delay = userRightsCheckIntervalInSeconds.seconds
      val userId = originalClaims.applicationId.fold[Ref.UserId](
        throw new RuntimeException(
          "Claims were resolved from a user but userId (applicationId) is missing in the claims."
        )
      )(Ref.UserId.assertFromString)
      // Note: https://doc.akka.io/docs/akka/2.6.13/scheduler.html states that:
      // "All scheduled task will be executed when the ActorSystem is terminated, i.e. the task may execute before its timeout."
      val cancellable =
        akkaScheduler.scheduleWithFixedDelay(initialDelay = delay, delay = delay)(() =>
          userManagementStore
            .listUserRights(userId)
            .onComplete {
              case Failure(_) | Success(Left(_)) =>
                userClaimsMismatchCallback()
              case Success(Right(userRights)) =>
                val updatedClaims = AuthorizationInterceptor.convertUserRightsToClaims(userRights)
                if (updatedClaims.toSet != originalClaims.claims.toSet) {
                  userClaimsMismatchCallback()
                }
                lastUserRightsCheckTime.set(nowF())
            }
        )
      () => {
        cancellable.cancel()
        ()
      }
    } else { () =>
      ()
    }
  }

}
