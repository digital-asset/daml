// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.interceptor

import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.*
import com.digitalasset.canton.ledger.api.{IdentityProviderId, User, UserRight}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.UserId
import io.grpc.*

import scala.concurrent.{ExecutionContext, Future}

/** This interceptor uses the given [[com.digitalasset.canton.auth.AuthService]] to get
  * [[com.digitalasset.canton.auth.ClaimSet.Claims]] for the current request, and then stores them
  * in the current [[io.grpc.Context]].
  *
  * @param userManagementStoreO
  *   use None if user management is disabled
  */
class UserBasedAuthInterceptor(
    authServices: Seq[AuthService],
    userManagementStoreO: Option[UserManagementStore],
    telemetry: Telemetry,
    loggerFactory: NamedLoggerFactory,
    override implicit val ec: ExecutionContext,
) extends AuthInterceptor(authServices, telemetry, loggerFactory, ec)
    with NamedLogging {

  import UserBasedAuthInterceptor.*

  override def headerToClaims(
      headers: Metadata,
      serviceName: String,
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): Future[ClaimSet] = {
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)
    super
      .headerToClaims(headers, serviceName)
      .flatMap(resolveAuthenticatedUserRights)
  }

  private[this] def resolveAuthenticatedUserRights(
      claimSet: ClaimSet
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[ClaimSet] =
    claimSet match {
      case ClaimSet.AuthenticatedUser(identityProviderId, userIdStr, participantId, expiration) =>
        val idpId = IdentityProviderId.fromOptionalLedgerString(identityProviderId)
        for {
          userManagementStore <- getUserManagementStore(userManagementStoreO)
          userId <- getUserId(userIdStr)
          user <- verifyUserIsActive(userManagementStore, userId, idpId)
          _ <- verifyUserIsWithinIdentityProvider(idpId, user)
          userRightsResult <- userManagementStore.listUserRights(userId, idpId)
          claimsSet <- userRightsResult match {
            case Left(msg) =>
              Future.failed(
                AuthorizationChecksErrors.PermissionDenied
                  .Reject(
                    s"Could not resolve rights for user '$userId' due to '$msg'"
                  )
                  .asGrpcError
              )
            case Right(userRights: Set[UserRight]) =>
              Future.successful(
                ClaimSet.Claims(
                  claims = convertUserRightsToClaims(userRights),
                  participantId = participantId,
                  userId = Some(userId),
                  expiration = expiration,
                  resolvedFromUser = true,
                  identityProviderId = identityProviderId,
                )
              )
          }
        } yield {
          claimsSet
        }
      case _ => Future.successful(claimSet)
    }

  private def verifyUserIsWithinIdentityProvider(
      identityProviderId: IdentityProviderId,
      user: User,
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[Unit] =
    if (user.identityProviderId != identityProviderId) {
      Future.failed(
        AuthorizationChecksErrors.PermissionDenied
          .Reject(
            s"User is assigned to another identity provider"
          )
          .asGrpcError
      )
    } else Future.unit

  private def verifyUserIsActive(
      userManagementStore: UserManagementStore,
      userId: UserId,
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[User] =
    for {
      userResult <- userManagementStore.getUser(id = userId, identityProviderId)
      value <- userResult match {
        case Left(msg) =>
          Future.failed(
            AuthorizationChecksErrors.PermissionDenied
              .Reject(
                s"Could not resolve is_deactivated status for user '$userId' and identity_provider_id '$identityProviderId' due to '$msg'"
              )
              .asGrpcError
          )
        case Right(user: User) =>
          if (user.isDeactivated) {
            Future.failed(
              AuthorizationChecksErrors.PermissionDenied
                .Reject(
                  s"User $userId is deactivated"
                )
                .asGrpcError
            )
          } else {
            Future.successful(user)
          }
      }
    } yield value

  private[this] def getUserManagementStore(
      userManagementStoreO: Option[UserManagementStore]
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[UserManagementStore] =
    userManagementStoreO match {
      case None =>
        Future.failed(
          AuthorizationChecksErrors.Unauthenticated
            .UserBasedAuthenticationIsDisabled()
            .asGrpcError
        )
      case Some(userManagementStore) =>
        Future.successful(userManagementStore)
    }

  private[this] def getUserId(
      userIdStr: String
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[Ref.UserId] =
    Ref.UserId.fromString(userIdStr) match {
      case Left(err) =>
        Future.failed(
          AuthorizationChecksErrors.InvalidToken
            .MissingUserId(s"token $err")
            .asGrpcError
        )
      case Right(userId) =>
        Future.successful(userId)
    }

}

object UserBasedAuthInterceptor {

  def convertUserRightsToClaims(userRights: Set[UserRight]): Seq[Claim] =
    userRights.view.map(userRightToClaim).toList.prepended(ClaimPublic)

  private[this] def userRightToClaim(r: UserRight): Claim = r match {
    case UserRight.CanActAs(p) => ClaimActAsParty(Ref.Party.assertFromString(p))
    case UserRight.CanReadAs(p) => ClaimReadAsParty(Ref.Party.assertFromString(p))
    case UserRight.IdentityProviderAdmin => ClaimIdentityProviderAdmin
    case UserRight.ParticipantAdmin => ClaimAdmin
    case UserRight.CanReadAsAnyParty => ClaimReadAsAnyParty
  }
}
