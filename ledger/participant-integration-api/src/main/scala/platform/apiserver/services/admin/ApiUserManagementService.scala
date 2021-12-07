// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.ledger.api.domain._
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
// DO NOT REMOVE: implicit conversion required for traverse below.
import scalaz.std.either._
import scalaz.syntax.traverse._
import scalaz.std.list._

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiUserManagementService(
    userManagementService: UserManagementStore,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit
//    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends proto.UserManagementServiceGrpc.UserManagementService
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

  private val fromProtoRight: proto.Right => Either[StatusRuntimeException, UserRight] = {
    case proto.Right(_: proto.Right.Kind.ParticipantAdmin) =>
      Right(UserRight.ParticipantAdmin)
    case proto.Right(proto.Right.Kind.CanActAs(r)) =>
      fieldValidations.requireParty(r.party).map(UserRight.CanActAs(_))
    case proto.Right(proto.Right.Kind.CanReadAs(r)) =>
      fieldValidations.requireParty(r.party).map(UserRight.CanReadAs(_))
    case proto.Right(proto.Right.Kind.Empty) =>
      Left(LedgerApiErrors.RequestValidation.InvalidArgument.Reject("unknown kind of right - check that the Ledger API version of the server is recent enough").asGrpcError)
  }

  private def fromProtoRights(rights: Seq[proto.Right]): Either[StatusRuntimeException, Set[UserRight]] = {
    // Note: IntelliJ does not seem to be able to compile the below, but scalac does.
    // We might want to switch the field validation to just raise a 'StatusRuntimeException' instead of
    // taking the pain with `Either`.
    rights.toList.traverse(fromProtoRight).map(_.toSet)
  }


  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    proto.UserManagementServiceGrpc.bindService(this, executionContext)

  override def createUser(request: proto.CreateUserRequest): Future[proto.User] = {
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
        pRights <- fromProtoRights(request.rights)
      } yield (User(pUserId, pOptPrimaryParty), pRights)
    })({ case (user, pRights) =>
      userManagementService
        .createUser(
          user = user,
          rights = pRights,
        )
        .flatMap(handleResult("create user"))
        .map(_ => request.user.get)
    })
  }

  override def getUser(request: proto.GetUserRequest): Future[proto.User] = {
    withValidation(
      fieldValidations.requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementService
        .getUser(userId)
        .flatMap(handleResult("get user"))
        .map(toProtoUser)
    )
  }

  override def deleteUser(request: proto.DeleteUserRequest): Future[proto.DeleteUserResponse] =
    withValidation(
      fieldValidations.requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementService
        .deleteUser(userId)
        .flatMap(handleResult("delete user"))
        .map(_ => proto.DeleteUserResponse())
    )

  override def listUsers(request: proto.ListUsersRequest): Future[proto.ListUsersResponse] =
    userManagementService
      .listUsers( /*request.pageSize, request.pageToken*/ )
      .flatMap(handleResult("list users"))
      .map(
        _.map(toProtoUser)
      ) // case (users, nextPageToken) => ListUsersResponse(users.map(toApiUser), nextPageToken)
      .map(proto.ListUsersResponse(_))

  override def grantUserRights(request: proto.GrantUserRightsRequest): Future[proto.GrantUserRightsResponse] =
    withValidation(
      for {
        userId <- fieldValidations.requireUserId(request.userId, "user_id")
        rights <- fromProtoRights(request.rights)
      } yield (userId, rights)
    )({ case (userId, rights) =>
      userManagementService
        .grantRights(
          id = userId,
          rights = rights,
        )
        .flatMap(handleResult("grant user rights"))
        .map(_.view.map(toProtoRight).toList)
        .map(proto.GrantUserRightsResponse(_))
    })

  override def revokeUserRights(
      request: proto.RevokeUserRightsRequest
  ): Future[proto.RevokeUserRightsResponse] =
    withValidation(
      for {
        userId <- fieldValidations.requireUserId(request.userId, "user_id")
        rights <- fromProtoRights(request.rights)
      } yield (userId, rights)
    )({ case (userId, rights) =>
      userManagementService
        .revokeRights(
          id = userId,
          rights = rights,
        )
        .flatMap(handleResult("revoke user rights"))
        .map(_.view.map(toProtoRight).toList)
        .map(proto.RevokeUserRightsResponse(_))
    })

  override def listUserRights(request: proto.ListUserRightsRequest): Future[proto.ListUserRightsResponse] =
    withValidation(
      fieldValidations.requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementService
        .listUserRights(userId)
        .flatMap(handleResult("list user rights"))
        .map(_.view.map(toProtoRight).toList)
        .map(proto.ListUserRightsResponse(_))
    )

  def handleResult[T](operation: String)(result: UserManagementStore.Result[T]): Future[T] =
    result match {
      case Left(UserManagementStore.UserNotFound(id)) =>
        Future.failed(
          LedgerApiErrors.AdminServices.UserNotFound.Reject(operation, id.toString).asGrpcError
        )
      case Left(UserManagementStore.UserExists(id)) =>
        Future.failed(
          LedgerApiErrors.AdminServices.UserAlreadyExists.Reject(operation, id.toString).asGrpcError
        )
      case scala.util.Right(t) => Future.successful(t)
    }

}

object ApiUserManagementService {
  private def toProtoUser(user: User): proto.User =
    proto.User(
      id = user.id.toString,
      primaryParty = user.primaryParty.getOrElse(""),
    )

  private val toProtoRight: UserRight => proto.Right = {
    case UserRight.ParticipantAdmin =>
      proto.Right(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))
    case UserRight.CanActAs(party) =>
      proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(party)))
    case UserRight.CanReadAs(party) =>
      proto.Right(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(party)))
  }

}
