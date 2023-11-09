// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.admin.user_management_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.*
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final class UserManagementServiceAuthorization(
    protected val service: UserManagementServiceGrpc.UserManagementService with AutoCloseable,
    private val authorizer: Authorizer,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends UserManagementServiceGrpc.UserManagementService
    with ProxyCloseable
    with GrpcApiService
    with NamedLogging {

  private implicit val loggingContextWithTrace =
    LoggingContextWithTrace(loggerFactory)(TraceContext.empty)
  private implicit val errorLogger: ContextualizedErrorLogger =
    ErrorLoggingContext(logger, loggingContextWithTrace)

  // Only ParticipantAdmin is allowed to grant ParticipantAdmin right
  private def containsParticipantAdmin(rights: Seq[Right]): Boolean =
    rights.contains(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin())))

  override def createUser(request: CreateUserRequest): Future[CreateUserResponse] = {
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId[CreateUserRequest, CreateUserResponse](
      identityProviderIdL = Lens.unit[CreateUserRequest].user.identityProviderId,
      mustBeParticipantAdmin = containsParticipantAdmin(request.rights),
      call = service.createUser,
    )(request)
  }

  override def getUser(request: GetUserRequest): Future[GetUserResponse] =
    defaultToAuthenticatedUser(request.userId) match {
      case Failure(ex) => Future.failed(ex)
      case Success(Some(userId)) =>
        authorizer.requireMatchingRequestIdpId(
          Lens.unit[GetUserRequest].identityProviderId,
          service.getUser,
        )(
          request.copy(userId = userId)
        )
      case Success(None) =>
        authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
          Lens.unit[GetUserRequest].identityProviderId,
          service.getUser,
        )(
          request
        )
    }

  override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      Lens.unit[DeleteUserRequest].identityProviderId,
      service.deleteUser,
    )(request)

  override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      Lens.unit[ListUsersRequest].identityProviderId,
      service.listUsers,
    )(request)

  override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      Lens.unit[GrantUserRightsRequest].identityProviderId,
      containsParticipantAdmin(request.rights),
      service.grantUserRights,
    )(
      request
    )

  override def revokeUserRights(
      request: RevokeUserRightsRequest
  ): Future[RevokeUserRightsResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      Lens.unit[RevokeUserRightsRequest].identityProviderId,
      containsParticipantAdmin(request.rights),
      service.revokeUserRights,
    )(
      request
    )

  override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] =
    defaultToAuthenticatedUser(request.userId) match {
      case Failure(ex) => Future.failed(ex)
      case Success(Some(userId)) =>
        authorizer.requireMatchingRequestIdpId(
          Lens.unit[ListUserRightsRequest].identityProviderId,
          service.listUserRights,
        )(
          request.copy(userId = userId)
        )
      case Success(None) =>
        authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
          Lens.unit[ListUserRightsRequest].identityProviderId,
          service.listUserRights,
        )(
          request
        )
    }

  override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] =
    request.user match {
      case Some(user) =>
        authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
          Lens.unit[UpdateUserRequest].user.identityProviderId,
          service.updateUser,
        )(
          request
        )
      case None =>
        authorizer.requireIdpAdminClaims(service.updateUser)(request)
    }

  override def updateUserIdentityProviderId(
      request: UpdateUserIdentityProviderRequest
  ): Future[UpdateUserIdentityProviderResponse] = {
    authorizer.requireAdminClaims(
      call = service.updateUserIdentityProviderId
    )(
      request
    )
  }

  override def bindService(): ServerServiceDefinition =
    UserManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

  private def defaultToAuthenticatedUser(userId: String): Try[Option[String]] =
    authorizer.authenticatedUserId().flatMap {
      case Some(authUserId) if userId.isEmpty || userId == authUserId =>
        // We include the case where the request userId is equal to the authenticated userId in the defaulting.
        Success(Some(authUserId))
      case None if userId.isEmpty =>
        // This case can be hit both when running without authentication and when using custom Daml tokens.
        Failure(
          RequestValidationErrors.InvalidArgument
            .Reject(
              "requests with an empty user-id are only supported if there is an authenticated user"
            )
            .asGrpcError
        )
      case _ => Success(None)
    }

}
