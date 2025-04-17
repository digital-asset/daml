// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.digitalasset.canton.auth.ClaimSet
import com.digitalasset.canton.ledger.api.auth.interceptor.UserBasedAuthInterceptor
import com.digitalasset.canton.ledger.api.{IdentityProviderId, User, UserRight}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.actor.Scheduler

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[auth] final class UserRightsChangeAsyncChecker(
    lastUserRightsCheckTime: AtomicReference[Instant],
    originalClaims: ClaimSet.Claims,
    nowF: () => Instant,
    userManagementStore: UserManagementStore,
    userRightsCheckIntervalInSeconds: Int,
    pekkoScheduler: Scheduler,
)(implicit ec: ExecutionContext) {

  /** Schedules an asynchronous and periodic task to check for user rights' state changes
    * @param userClaimsMismatchCallback
    *   called when user rights' state change has been detected.
    * @return
    *   a function to cancel the scheduled task
    */
  def schedule(
      userClaimsMismatchCallback: () => Unit
  )(implicit loggingContext: LoggingContextWithTrace): () => Unit = {
    val delay = userRightsCheckIntervalInSeconds.seconds
    val identityProviderId = originalClaims.identityProviderId
    val userId = originalClaims.userId.fold[Ref.UserId](
      throw new RuntimeException(
        "Claims were resolved from a user but userId is missing in the claims."
      )
    )(Ref.UserId.assertFromString)
    assert(
      originalClaims.resolvedFromUser,
      "The claims were not resolved from a user. Expected claims resolved from a user.",
    )
    // Note: https://doc.akka.io/docs/akka/2.6.13/scheduler.html states that:
    // "All scheduled task will be executed when the ActorSystem is terminated, i.e. the task may execute before its timeout."
    val cancellable =
      pekkoScheduler.scheduleWithFixedDelay(initialDelay = delay, delay = delay) { () =>
        val idpId = IdentityProviderId.fromOptionalLedgerString(identityProviderId)
        val userState: Future[Either[UserManagementStore.Error, (User, Set[UserRight])]] =
          for {
            userRightsResult <- userManagementStore.listUserRights(userId, idpId)
            userResult <- userManagementStore.getUser(userId, idpId)
          } yield {
            for {
              userRights <- userRightsResult
              user <- userResult
            } yield (user, userRights)
          }
        userState
          .onComplete {
            case Failure(_) | Success(Left(_)) =>
              userClaimsMismatchCallback()
            case Success(Right((user, userRights))) =>
              val updatedClaims =
                UserBasedAuthInterceptor.convertUserRightsToClaims(userRights)
              if (updatedClaims.toSet != originalClaims.claims.toSet || user.isDeactivated) {
                userClaimsMismatchCallback()
              }
              lastUserRightsCheckTime.set(nowF())
          }
      }
    () => (cancellable.cancel(): Unit)
  }

}
