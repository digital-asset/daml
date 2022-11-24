// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.auth._
import com.daml.ledger.api.v1.admin.user_management_service._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[daml] final class UserManagementServiceAuthorization(
    protected val service: UserManagementServiceGrpc.UserManagementService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends UserManagementServiceGrpc.UserManagementService
    with ProxyCloseable
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private implicit val errorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def createUser(request: CreateUserRequest): Future[CreateUserResponse] =
    authorizer.requireIDPContext(
      request.user.map(_.identityProviderId).getOrElse(""),
      authorizer.requireAdminOrIDPAdminClaims(service.createUser),
    )((identityProviderId, request) =>
      request.copy(user = request.user.map(_.copy(identityProviderId = identityProviderId)))
    )(
      request
    )

  override def getUser(request: GetUserRequest): Future[GetUserResponse] = {
    defaultToAuthenticatedUser(request.userId) match {
      case Failure(ex) => Future.failed(ex)
      case Success(Some(userId)) =>
        authorizer.requireIDPContext(
          request.identityProviderId,
          { request: GetUserRequest => service.getUser(request.copy(userId = userId)) },
        )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
          request
        )
      case Success(None) =>
        authorizer.requireIDPContext(
          request.identityProviderId,
          authorizer.requireAdminOrIDPAdminClaims(service.getUser),
        )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
          request
        )
    }
  }

  override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
    authorizer.requireIDPContext(
      request.identityProviderId,
      authorizer.requireAdminOrIDPAdminClaims(service.deleteUser),
    )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
      request
    )

  override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
    authorizer.requireIDPContext(
      request.identityProviderId,
      authorizer.requireAdminOrIDPAdminClaims(service.listUsers),
    )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
      request
    )

  override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
    authorizer.requireIDPContext(
      request.identityProviderId,
      authorizer.requireAdminOrIDPAdminClaims(service.grantUserRights),
    )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
      request
    )

  override def revokeUserRights(
      request: RevokeUserRightsRequest
  ): Future[RevokeUserRightsResponse] =
    authorizer.requireIDPContext(
      request.identityProviderId,
      authorizer.requireAdminOrIDPAdminClaims(service.revokeUserRights),
    )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
      request
    )

  override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] =
    defaultToAuthenticatedUser(request.userId) match {
      case Failure(ex) => Future.failed(ex)
      case Success(Some(userId)) =>
        authorizer.requireIDPContext(
          request.identityProviderId,
          { request: ListUserRightsRequest =>
            service.listUserRights(request.copy(userId = userId))
          },
        )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
          request
        )
      case Success(None) =>
        authorizer.requireIDPContext(
          request.identityProviderId,
          authorizer.requireAdminOrIDPAdminClaims(service.listUserRights),
        )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
          request
        )
    }

  override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] =
    authorizer.requireIDPContext(
      request.user.map(_.identityProviderId).getOrElse(""),
      authorizer.requireAdminOrIDPAdminClaims(service.updateUser),
    )((identityProviderId, request) =>
      request.copy(user = request.user.map(_.copy(identityProviderId = identityProviderId)))
    )(
      request
    )

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
          LedgerApiErrors.RequestValidation.InvalidArgument
            .Reject(
              "requests with an empty user-id are only supported if there is an authenticated user"
            )
            .asGrpcError
        )
      case _ => Success(None)
    }
}
