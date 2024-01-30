// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.UserManagementServiceAuthorization
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc.UserManagementService
import com.daml.ledger.api.v1.admin.user_management_service._
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.ServerServiceDefinition

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

final class UserManagementServiceImpl extends UserManagementService with FakeAutoCloseable {

  private val log = mutable.ArrayBuffer.empty[Any]

  def requests(): Array[Any] = {
    log.toArray
  }

  private def record[Result](request: Any)(result: Result): Future[Result] = {
    log += request
    Future.successful(result)
  }

  override def createUser(request: CreateUserRequest): Future[CreateUserResponse] =
    record(request)(CreateUserResponse.defaultInstance)

  override def getUser(request: GetUserRequest): Future[GetUserResponse] =
    record(request)(GetUserResponse.defaultInstance)

  override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
    record(request)(DeleteUserResponse.defaultInstance)

  override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
    record(request)(ListUsersResponse.defaultInstance)

  override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
    record(request)(GrantUserRightsResponse.defaultInstance)

  override def revokeUserRights(
      request: RevokeUserRightsRequest
  ): Future[RevokeUserRightsResponse] = record(request)(RevokeUserRightsResponse.defaultInstance)

  override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] =
    record(request)(ListUserRightsResponse.defaultInstance)

  override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] =
    record(request)(UpdateUserResponse.defaultInstance)

  override def updateUserIdentityProviderId(
      request: UpdateUserIdentityProviderRequest
  ): Future[UpdateUserIdentityProviderResponse] = throw new UnsupportedOperationException()
}

object UserManagementServiceImpl {

  // for testing only
  private[helpers] def createWithRef(
      authorizer: Authorizer,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, UserManagementServiceImpl) = {
    val impl = new UserManagementServiceImpl
    val authImpl = new UserManagementServiceAuthorization(impl, authorizer, loggerFactory)
    (UserManagementServiceGrpc.bindService(authImpl, ec), impl)
  }

}
