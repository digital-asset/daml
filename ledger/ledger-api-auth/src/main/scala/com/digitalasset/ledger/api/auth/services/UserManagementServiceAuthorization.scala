// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    authorizer.requireAdminClaims(service.createUser)(request)

  override def getUser(request: GetUserRequest): Future[GetUserResponse] =
    defaultToAuthenticatedUser(request.userId) match {
      case Failure(ex) => Future.failed(ex)
      case Success(Some(userId)) => service.getUser(request.copy(userId = userId))
      case Success(None) => authorizer.requireAdminClaims(service.getUser)(request)
    }

  override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
    authorizer.requireAdminClaims(service.deleteUser)(request)

  override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
    authorizer.requireAdminClaims(service.listUsers)(request)

  override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
    authorizer.requireAdminClaims(service.grantUserRights)(request)

  override def revokeUserRights(
      request: RevokeUserRightsRequest
  ): Future[RevokeUserRightsResponse] =
    authorizer.requireAdminClaims(service.revokeUserRights)(request)

  override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] =
    defaultToAuthenticatedUser(request.userId) match {
      case Failure(ex) => Future.failed(ex)
      case Success(Some(userId)) => service.listUserRights(request.copy(userId = userId))
      case Success(None) => authorizer.requireAdminClaims(service.listUserRights)(request)
    }

  override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] = {
    authorizer.requireAdminClaims(service.updateUser)(request)
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
          LedgerApiErrors.RequestValidation.InvalidArgument
            .Reject(
              "requests with an empty user-id are only supported if there is an authenticated user"
            )
            .asGrpcError
        )
      case _ => Success(None)
    }
}
