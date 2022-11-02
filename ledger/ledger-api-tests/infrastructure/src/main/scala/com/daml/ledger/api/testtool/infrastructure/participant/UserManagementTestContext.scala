// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import java.util.concurrent.ConcurrentHashMap

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.testtool.infrastructure.LedgerServices
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc.UserManagementService
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  DeleteUserRequest,
  DeleteUserResponse,
  User,
}

import scala.concurrent.{ExecutionContext, Future}

trait UserManagementTestContext {
  self: ParticipantTestContext =>

  private[participant] def services: LedgerServices

  private[participant] implicit val ec: ExecutionContext

  /** Users created during execution of the test case on this participant.
    */
  private val createdUsersById = new ConcurrentHashMap[String, User]

  def userManagement: UserManagementService =
    services.userManagement // TODO (i12059) perhaps remove and create granular accessors

  /** Creates a new user.
    *
    * Additionally keeps track of the created users so that they can be cleaned up automatically when the test case ends.
    */
  def createUser(createUserRequest: CreateUserRequest): Future[CreateUserResponse] = {
    for {
      response <- services.userManagement.createUser(createUserRequest)
      user = response.user.get
      _ = createdUsersById.put(user.id, user)
    } yield response
  }

  /** Deletes a user.
    *
    * Additionally keeps track of the created users so that they can be cleaned up automatically when the test case ends.
    */
  def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] = {
    for {
      response <- services.userManagement.deleteUser(request)
      _ = createdUsersById.remove(request.userId)
    } yield response
  }

  /** Intended to be called by the infrastructure code after a test case's execution has ended.
    */
  def deleteCreatedUsers(): Future[Unit] = {
    import scala.jdk.CollectionConverters._
    val deletions = createdUsersById
      .keys()
      .asScala
      .map(userId =>
        services.userManagement
          .deleteUser(
            DeleteUserRequest(userId)
          )
          .map(_ => ())
          .recover {
            case e if ErrorDetails.matches(e, LedgerApiErrors.Admin.UserManagement.UserNotFound) =>
              ()
          }
      )
    Future.sequence(deletions).map(_ => ())
  }

}
