// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.caching.{CaffeineCache, ConcurrentCache}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.domain.UserRight
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{Result, UserNotFound}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.github.benmanes.caffeine.cache.Caffeine

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

/** Validates authorizaton claims against the set of known users and their users.
  */
trait UserManagementBasedAuthorizer {
  def validate(claims: ClaimSet.Claims): Either[AuthorizationError, Unit]
}

// TODO participant user management: Review usages
object NoOpUserManagementBasedAuthorizer extends UserManagementBasedAuthorizer {
  override def validate(claims: ClaimSet.Claims): Either[AuthorizationError, Unit] = Right(
    ()
  )
}

// TODO participant user management: Write tests
class CachedUserManagementBasedAuthorizer(
    delegate: UserManagementBasedAuthorizer,
    expiryAfterWriteInSeconds: Int,
) extends UserManagementBasedAuthorizer {

  private val usersCache: ConcurrentCache[ClaimSet.Claims, Either[AuthorizationError, Unit]] =
    CaffeineCache[ClaimSet.Claims, Either[AuthorizationError, Unit]](
      builder = Caffeine
        .newBuilder()
        .expireAfterWrite(Duration.ofSeconds(expiryAfterWriteInSeconds.toLong))
        // TODO participant user management: Choose cache size
        .maximumSize(10000),
      // TODO participant user management: Use metrics
      metrics = None,
    )

  override def validate(claims: ClaimSet.Claims): Either[AuthorizationError, Unit] = {
    usersCache.getOrAcquire(claims, claims => delegate.validate(claims))
  }
}

// TODO participant user management: Write tests
class UserManagementBasedAuthorizerImpl(userManagementStore: UserManagementStore)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends UserManagementBasedAuthorizer {

  private val logger = ContextualizedLogger.get(getClass)
  // TODO participant user management: Add correlationId if it is known at place of instantiation/call-site
  private implicit val errorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  /** If claims can expire checks
    */
  def validate(claims: ClaimSet.Claims): Either[AuthorizationError, Unit] = {
    if (claims.resolvedFromUser) {
      val f = validateClaims(claims)
      // TODO participant user management: Blocking call
      Await.result(f, FiniteDuration(1, TimeUnit.SECONDS))
    } else Right(())
  }

  private def validateClaims(claims: ClaimSet.Claims): Future[Either[AuthorizationError, Unit]] = {
    for {
      // TODO participant user management: ```userId: Ref.UserId <- ...``` doesn't compile. Why?
      // error: scrutinee is incompatible with pattern type;
      // found   : com.daml.lf.data.Ref.UserId
      //    (which expands to)  com.daml.lf.data.Ref.IdString.UserId
      // required: String
      //      userId: Ref.UserId <- claims.applicationId.fold[Future[Ref.UserId]](
      userId /*: Ref.UserId*/ <- claims.applicationId.fold[Future[Ref.UserId]](
        Future.failed(
          LedgerApiErrors.RequestValidation.InvalidArgument
            .Reject(s"Application id is missing from authorization claims")
            .asGrpcError
        )
      )(userId => Future.successful(Ref.UserId.assertFromString(userId)))
      userRightsResult: Result[Set[UserRight]] <- userManagementStore.listUserRights(userId)
      authorization: Either[AuthorizationError, Unit] <- validateClaims(claims, userRightsResult)
    } yield {
      authorization
    }
  }

  private def validateClaims(
      claims: ClaimSet.Claims,
      userRightsResult: Result[Set[UserRight]],
  ): Future[Either[AuthorizationError, Unit]] = {
    userRightsResult match {
      case Left(error) =>
        error match {
          case UserNotFound(userId) =>
            Future.successful(Left(AuthorizationError.UserNotFoundOnStream(userId = userId)))
          case e =>
            Future.failed(
              LedgerApiErrors.AuthorizationChecks.InternalAuthorizationError
                .Reject2(s"Unexpected user management error ${e}")
                .asGrpcError
            )
        }
      case Right(userRights) =>
        val errorO: Option[AuthorizationError] =
          claims.claims.collectFirst((matchMissingUserRight(userRights) _).unlift)
        val result: Either[AuthorizationError, Unit] =
          errorO.fold[Either[AuthorizationError, Unit]](Right(()))(error => Left(error))
        Future.successful(result)
    }
  }

  /** Checks if a claim is satisfied by given set of rights.
    */
  private def matchMissingUserRight(
      userRights: Set[UserRight]
  )(claim: Claim): Option[AuthorizationError] = {
    claim match {
      case ClaimActAsAnyParty =>
        // Claim currently not supported by UserManagement
        None
      case ClaimPublic =>
        // All users have implicit public claim granted
        None
      case ClaimAdmin =>
        if (userRights.contains(UserRight.ParticipantAdmin)) {
          None
        } else {
          Some(AuthorizationError.UserIsMissingUserRightForClaim(claim))
        }
      case ClaimActAsParty(party) =>
        if (userRights.contains(UserRight.CanActAs(party))) {
          None
        } else {
          Some(AuthorizationError.UserIsMissingUserRightForClaim(claim))
        }
      case ClaimReadAsParty(party) =>
        if (userRights.contains(UserRight.CanReadAs(party))) {
          None
        } else {
          Some(AuthorizationError.UserIsMissingUserRightForClaim(claim))
        }
    }
  }

}
