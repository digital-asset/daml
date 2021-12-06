// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.ledger.api.UserManagement
import com.daml.ledger.api.v1.admin.user_management_service._
import com.daml.ledger.participant.state.index.v2.UserManagementService
import com.daml.ledger.participant.state.index.v2.UserManagementService._
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiUserManagementService(
    userManagementService: UserManagementService,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit
//    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,

) extends UserManagementServiceGrpc.UserManagementService
    with GrpcApiService {
  import ApiUserManagementService._

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  private val errorFactories = ErrorFactories(errorCodesVersionSwitcher)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)
  private val fieldValidations = FieldValidations(errorFactories)

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    UserManagementServiceGrpc.bindService(this, executionContext)

  override def createUser(request: CreateUserRequest): Future[User] = {
    import fieldValidations._
    val userOrErr =
      for {
        pUser <- requirePresence(request.user, "user")
        pUserId <- requireNonEmptyString(pUser.id, "id")
        pOptPrimaryParty <-
          if (pUser.primaryParty.isEmpty)
            scala.util.Right(None)
          else
            requireParty(pUser.primaryParty).map(Some(_))

        // FIXME: validate rights as well!
        // FIXME: add tests for field validation code
      } yield UserManagement.User(pUserId, pOptPrimaryParty)

    userOrErr match {
      case Left(err) => Future.failed(err)
      case scala.util.Right(user) =>
        userManagementService
          .createUser(
            user = user,
            rights = request.rights.view.map(fromApiRight).toSet,
          )
          .flatMap(handleResult("create user"))
          .map(_ => request.user.get)
    }
  }

  override def getUser(request: GetUserRequest): Future[User] =
    userManagementService
      .getUser(request.userId)
      .flatMap(handleResult("get user"))
      .map(toApiUser)

  override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
    userManagementService
      .deleteUser(request.userId)
      .flatMap(handleResult("delete user"))
      .map(_ => DeleteUserResponse())

  override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
    userManagementService
      .listUsers(/*request.pageSize, request.pageToken*/)
      .flatMap(handleResult("list users"))
      .map(_.map(toApiUser))  // case (users, nextPageToken) => ListUsersResponse(users.map(toApiUser), nextPageToken)
      .map(ListUsersResponse(_))

  override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
    userManagementService
      .grantRights(
        id = request.userId,
        rights = request.rights.view.map(fromApiRight).toSet,
      )
      .flatMap(handleResult("grant user rights"))
      .map(_.view.map(toApiRight).toList)
      .map(GrantUserRightsResponse(_))

  override def revokeUserRights(
      request: RevokeUserRightsRequest
  ): Future[RevokeUserRightsResponse] =
    userManagementService
      .revokeRights(
        id = request.userId,
        rights = request.rights.view.map(fromApiRight).toSet,
      )
      .flatMap(handleResult("revoke user rights"))
      .map(_.view.map(toApiRight).toList)
      .map(RevokeUserRightsResponse(_))

  override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] =
    userManagementService
      .listUserRights(request.userId)
      .flatMap(handleResult("list user rights"))
      .map(_.view.map(toApiRight).toList)
      .map(ListUserRightsResponse(_))

  def handleResult[T](operation: String)(result: Result[T]): Future[T] =
    result match {
      case Left(UserNotFound(id)) =>
        Future.failed(
          LedgerApiErrors.AdminServices.UserNotFound.Reject(operation, id).asGrpcError
        )
      case Left(UserExists(id)) =>
        Future.failed(
          LedgerApiErrors.AdminServices.UserAlreadyExists.Reject(operation, id).asGrpcError
        )
      case scala.util.Right(t) => Future.successful(t)
    }
}

object ApiUserManagementService {
  def toApiUser(user: UserManagement.User): User =
    User(
      id = user.id,
      primaryParty = user.primaryParty.getOrElse(""),
    )

  val toApiRight: UserManagement.UserRight => Right = {
    case UserManagement.UserRight.ParticipantAdmin =>
      Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))
    case UserManagement.UserRight.CanActAs(party) =>
      Right(Right.Kind.CanActAs(Right.CanActAs(party)))
    case UserManagement.UserRight.CanReadAs(party) =>
      Right(Right.Kind.CanReadAs(Right.CanReadAs(party)))
  }

  val fromApiRight: Right => UserManagement.UserRight = {
    case Right(_: Right.Kind.ParticipantAdmin) => UserManagement.UserRight.ParticipantAdmin
    case Right(Right.Kind.CanActAs(x)) =>
      UserManagement.UserRight.CanActAs(Ref.Party.assertFromString(x.party))
    case Right(Right.Kind.CanReadAs(x)) =>
      UserManagement.UserRight.CanReadAs(Ref.Party.assertFromString(x.party))
    case _ => throw new Exception // TODO FIXME validation
  }
}
