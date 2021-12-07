// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.admin.user_management_service._
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore._
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiUserManagementService(
    userManagementService: UserManagementStore,
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

  private def withValidation[A, B](validatedResult: Either[StatusRuntimeException, A])(
      f: A => Future[B]
  ): Future[B] =
    validatedResult.fold(Future.failed, Future.successful).flatMap(f)

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    UserManagementServiceGrpc.bindService(this, executionContext)

  override def createUser(request: CreateUserRequest): Future[User] = {
    withValidation({
      import fieldValidations._
      for {
        pUser <- requirePresence(request.user, "user")
        pUserId <- requireUserId(pUser.id, "id")
        pOptPrimaryParty <-
          if (pUser.primaryParty.isEmpty)
            scala.util.Right(None)
          else
            requireParty(pUser.primaryParty).map(Some(_))

        // FIXME: validate rights as well!
        // FIXME: add tests for field validation code
      } yield domain.User(pUserId, pOptPrimaryParty)
    })(user => {
      userManagementService
        .createUser(
          user = user,
          rights = request.rights.view.map(fromApiRight).toSet,
        )
        .flatMap(handleResult("create user"))
        .map(_ => request.user.get)
    })
  }

  override def getUser(request: GetUserRequest): Future[User] = {
    withValidation(
      fieldValidations.requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementService
        .getUser(userId)
        .flatMap(handleResult("get user"))
        .map(toApiUser)
    )
  }

  override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
    withValidation(
      fieldValidations.requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementService
        .deleteUser(userId)
        .flatMap(handleResult("delete user"))
        .map(_ => DeleteUserResponse())
    )

  override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
    userManagementService
      .listUsers( /*request.pageSize, request.pageToken*/ )
      .flatMap(handleResult("list users"))
      .map(
        _.map(toApiUser)
      ) // case (users, nextPageToken) => ListUsersResponse(users.map(toApiUser), nextPageToken)
      .map(ListUsersResponse(_))

  override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
    withValidation(
      fieldValidations.requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementService
        .grantRights(
          id = userId,
          rights = request.rights.view.map(fromApiRight).toSet,
        )
        .flatMap(handleResult("grant user rights"))
        .map(_.view.map(toApiRight).toList)
        .map(GrantUserRightsResponse(_))
    )

  override def revokeUserRights(
      request: RevokeUserRightsRequest
  ): Future[RevokeUserRightsResponse] =
    withValidation(
      fieldValidations.requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementService
        .revokeRights(
          id = userId,
          rights = request.rights.view.map(fromApiRight).toSet,
        )
        .flatMap(handleResult("revoke user rights"))
        .map(_.view.map(toApiRight).toList)
        .map(RevokeUserRightsResponse(_))
    )

  override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] =
    withValidation(
      fieldValidations.requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementService
        .listUserRights(userId)
        .flatMap(handleResult("list user rights"))
        .map(_.view.map(toApiRight).toList)
        .map(ListUserRightsResponse(_))
    )

  def handleResult[T](operation: String)(result: Result[T]): Future[T] =
    result match {
      case Left(UserNotFound(id)) =>
        Future.failed(
          LedgerApiErrors.AdminServices.UserNotFound.Reject(operation, id.toString).asGrpcError
        )
      case Left(UserExists(id)) =>
        Future.failed(
          LedgerApiErrors.AdminServices.UserAlreadyExists.Reject(operation, id.toString).asGrpcError
        )
      case scala.util.Right(t) => Future.successful(t)
    }
}

object ApiUserManagementService {
  def toApiUser(user: domain.User): User =
    User(
      id = user.id.toString,
      primaryParty = user.primaryParty.getOrElse(""),
    )

  val toApiRight: domain.UserRight => Right = {
    case domain.UserRight.ParticipantAdmin =>
      Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))
    case domain.UserRight.CanActAs(party) =>
      Right(Right.Kind.CanActAs(Right.CanActAs(party)))
    case domain.UserRight.CanReadAs(party) =>
      Right(Right.Kind.CanReadAs(Right.CanReadAs(party)))
  }

  val fromApiRight: Right => domain.UserRight = {
    case Right(_: Right.Kind.ParticipantAdmin) => domain.UserRight.ParticipantAdmin
    case Right(Right.Kind.CanActAs(x)) =>
      domain.UserRight.CanActAs(Ref.Party.assertFromString(x.party))
    case Right(Right.Kind.CanReadAs(x)) =>
      domain.UserRight.CanReadAs(Ref.Party.assertFromString(x.party))
    case _ => throw new Exception // TODO FIXME validation
  }
}
