// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.auth._
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

private[daml] final class UserManagementServiceAuthorization(
    protected val service: proto.UserManagementServiceGrpc.UserManagementService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends proto.UserManagementServiceGrpc.UserManagementService
    with ProxyCloseable
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private implicit val errorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def createUser(request: proto.CreateUserRequest): Future[proto.CreateUserResponse] =
    authorizer.requireAdminClaims(service.createUser)(request)

  override def getUser(request: proto.GetUserRequest): Future[proto.GetUserResponse] =
    defaultToAuthenticatedUser(request.userId) match {
      case Left(e: StatusRuntimeException) => Future.failed(e)
      case Right(Some(userId)) => service.getUser(request.copy(userId = userId))
      case Right(None) => authorizer.requireAdminClaims(service.getUser)(request)
    }

  override def deleteUser(request: proto.DeleteUserRequest): Future[proto.DeleteUserResponse] =
    authorizer.requireAdminClaims(service.deleteUser)(request)

  override def listUsers(request: proto.ListUsersRequest): Future[proto.ListUsersResponse] =
    authorizer.requireAdminClaims(service.listUsers)(request)

  override def grantUserRights(
      request: proto.GrantUserRightsRequest
  ): Future[proto.GrantUserRightsResponse] =
    authorizer.requireAdminClaims(service.grantUserRights)(request)

  override def revokeUserRights(
      request: proto.RevokeUserRightsRequest
  ): Future[proto.RevokeUserRightsResponse] =
    authorizer.requireAdminClaims(service.revokeUserRights)(request)

  override def listUserRights(
      request: proto.ListUserRightsRequest
  ): Future[proto.ListUserRightsResponse] =
    defaultToAuthenticatedUser(request.userId) match {
      case Left(e: StatusRuntimeException) => Future.failed(e)
      case Right(Some(userId)) => service.listUserRights(request.copy(userId = userId))
      case Right(None) => authorizer.requireAdminClaims(service.listUserRights)(request)
    }

  override def updateUser(request: proto.UpdateUserRequest): Future[proto.UpdateUserResponse] = {
    authorizer.requireAdminClaims(service.updateUser)(request)
  }

  override def bindService(): ServerServiceDefinition =
    proto.UserManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

  private def defaultToAuthenticatedUser(
      userId: String
  ): Either[StatusRuntimeException, Option[String]] =
    authorizer.authenticatedUserId().flatMap {
      case Some(authUserId) if userId.isEmpty || userId == authUserId =>
        // We include the case where the request userId is equal to the authenticated userId in the defaulting.
        Right(Some(authUserId))
      case None if userId.isEmpty =>
        // This case can be hit both when running without authentication and when using custom Daml tokens.
        Left(
          LedgerApiErrors.RequestValidation.InvalidArgument
            .Reject(
              "requests with an empty user-id are only supported if there is an authenticated user"
            )
            .asGrpcError
        )
      case _ => Right(None)
    }
}
