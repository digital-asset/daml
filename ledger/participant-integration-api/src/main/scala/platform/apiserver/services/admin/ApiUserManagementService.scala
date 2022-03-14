// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain._
import com.daml.ledger.api.v1.admin.user_management_service.{CreateUserResponse, GetUserResponse}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.platform.apiserver.page_tokens.ListUsersPageTokenPayload
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.FieldValidations
import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import scalaz.std.either._
import scalaz.syntax.traverse._
import scalaz.std.list._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[apiserver] final class ApiUserManagementService(
    userManagementStore: UserManagementStore,
    maxUsersPageSize: Int,
    submissionIdGenerator: SubmissionIdGenerator,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends proto.UserManagementServiceGrpc.UserManagementService
    with GrpcApiService {

  import ApiUserManagementService._

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  import FieldValidations._

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    proto.UserManagementServiceGrpc.bindService(this, executionContext)

  override def createUser(request: proto.CreateUserRequest): Future[CreateUserResponse] =
    withSubmissionId { implicit loggingContext =>
      withValidation {
        for {
          pUser <- requirePresence(request.user, "user")
          pUserId <- requireUserId(pUser.id, "id")
          pOptPrimaryParty <- optionalString(pUser.primaryParty)(requireParty)
          pRights <- fromProtoRights(request.rights)
        } yield (User(pUserId, pOptPrimaryParty), pRights)
      } { case (user, pRights) =>
        userManagementStore
          .createUser(
            user = user,
            rights = pRights,
          )
          .flatMap(handleResult("creating user"))
          .map(_ => CreateUserResponse(Some(request.user.get)))
      }
    }

  override def getUser(request: proto.GetUserRequest): Future[GetUserResponse] =
    withValidation(
      requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementStore
        .getUser(userId)
        .flatMap(handleResult("getting user"))
        .map(u => GetUserResponse(Some(toProtoUser(u))))
    )

  override def deleteUser(request: proto.DeleteUserRequest): Future[proto.DeleteUserResponse] =
    withSubmissionId { implicit loggingContext =>
      withValidation(
        requireUserId(request.userId, "user_id")
      )(userId =>
        userManagementStore
          .deleteUser(userId)
          .flatMap(handleResult("deleting user"))
          .map(_ => proto.DeleteUserResponse())
      )
    }

  override def listUsers(request: proto.ListUsersRequest): Future[proto.ListUsersResponse] = {
    withValidation(
      for {
        fromExcl <- decodeUserIdFromPageToken(request.pageToken)
        rawPageSize <- Either.cond(
          request.pageSize >= 0,
          request.pageSize,
          LedgerApiErrors.RequestValidation.InvalidArgument
            .Reject("Max page size must be non-negative")
            .asGrpcError,
        )
        pageSize =
          if (rawPageSize == 0) maxUsersPageSize
          else Math.min(request.pageSize, maxUsersPageSize)
      } yield {
        (fromExcl, pageSize)
      }
    ) { case (fromExcl, pageSize) =>
      userManagementStore
        .listUsers(fromExcl, pageSize)
        .flatMap(handleResult("listing users"))
        .map { page: UserManagementStore.UsersPage =>
          val protoUsers = page.users.map(toProtoUser)
          proto.ListUsersResponse(
            protoUsers,
            encodeNextPageToken(if (page.users.size < pageSize) None else page.lastUserIdOption),
          )
        }
    }
  }

  override def grantUserRights(
      request: proto.GrantUserRightsRequest
  ): Future[proto.GrantUserRightsResponse] = withSubmissionId { implicit loggingContext =>
    withValidation(
      for {
        userId <- requireUserId(request.userId, "user_id")
        rights <- fromProtoRights(request.rights)
      } yield (userId, rights)
    ) { case (userId, rights) =>
      userManagementStore
        .grantRights(
          id = userId,
          rights = rights,
        )
        .flatMap(handleResult("grant user rights"))
        .map(_.view.map(toProtoRight).toList)
        .map(proto.GrantUserRightsResponse(_))
    }
  }

  override def revokeUserRights(
      request: proto.RevokeUserRightsRequest
  ): Future[proto.RevokeUserRightsResponse] = withSubmissionId { implicit loggingContext =>
    withValidation(
      for {
        userId <- FieldValidations.requireUserId(request.userId, "user_id")
        rights <- fromProtoRights(request.rights)
      } yield (userId, rights)
    ) { case (userId, rights) =>
      userManagementStore
        .revokeRights(
          id = userId,
          rights = rights,
        )
        .flatMap(handleResult("revoke user rights"))
        .map(_.view.map(toProtoRight).toList)
        .map(proto.RevokeUserRightsResponse(_))
    }
  }

  override def listUserRights(
      request: proto.ListUserRightsRequest
  ): Future[proto.ListUserRightsResponse] =
    withValidation(
      requireUserId(request.userId, "user_id")
    )(userId =>
      userManagementStore
        .listUserRights(userId)
        .flatMap(handleResult("list user rights"))
        .map(_.view.map(toProtoRight).toList)
        .map(proto.ListUserRightsResponse(_))
    )

  private def handleResult[T](operation: String)(result: UserManagementStore.Result[T]): Future[T] =
    result match {
      case Left(UserManagementStore.UserNotFound(id)) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.UserNotFound
            .Reject(operation, id.toString)
            .asGrpcError
        )

      case Left(UserManagementStore.UserExists(id)) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.UserAlreadyExists
            .Reject(operation, id.toString)
            .asGrpcError
        )

      case Left(UserManagementStore.TooManyUserRights(id)) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.TooManyUserRights
            .Reject(operation, id: String)
            .asGrpcError
        )

      case scala.util.Right(t) =>
        Future.successful(t)
    }

  private def withValidation[A, B](validatedResult: Either[StatusRuntimeException, A])(
      f: A => Future[B]
  ): Future[B] =
    validatedResult.fold(Future.failed, Future.successful).flatMap(f)

  private val fromProtoRight: proto.Right => Either[StatusRuntimeException, UserRight] = {
    case proto.Right(_: proto.Right.Kind.ParticipantAdmin) =>
      Right(UserRight.ParticipantAdmin)

    case proto.Right(proto.Right.Kind.CanActAs(r)) =>
      requireParty(r.party).map(UserRight.CanActAs(_))

    case proto.Right(proto.Right.Kind.CanReadAs(r)) =>
      requireParty(r.party).map(UserRight.CanReadAs(_))

    case proto.Right(proto.Right.Kind.Empty) =>
      Left(
        LedgerApiErrors.RequestValidation.InvalidArgument
          .Reject(
            "unknown kind of right - check that the Ledger API version of the server is recent enough"
          )
          .asGrpcError
      )
  }

  private def fromProtoRights(
      rights: Seq[proto.Right]
  ): Either[StatusRuntimeException, Set[UserRight]] =
    rights.toList.traverse(fromProtoRight).map(_.toSet)

  private def withSubmissionId[A](
      f: LoggingContext => A
  )(implicit loggingContext: LoggingContext): A =
    withEnrichedLoggingContext("submissionId" -> submissionIdGenerator.generate())(f)
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

  def encodeNextPageToken(token: Option[Ref.UserId]): String =
    token
      .map { id =>
        val bytes = Base64.getUrlEncoder.encode(
          ListUsersPageTokenPayload(userIdLowerBoundExcl = id).toByteArray
        )
        new String(bytes, StandardCharsets.UTF_8)
      }
      .getOrElse("")

  def decodeUserIdFromPageToken(pageToken: String)(implicit
      loggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[Ref.UserId]] = {
    if (pageToken.isEmpty) {
      Right(None)
    } else {
      val bytes = pageToken.getBytes(StandardCharsets.UTF_8)
      for {
        decodedBytes <- Try[Array[Byte]](Base64.getUrlDecoder.decode(bytes))
          .map(Right(_))
          .recover { case _: IllegalArgumentException =>
            Left(invalidPageToken)
          }
          .get
        tokenPayload <- Try[ListUsersPageTokenPayload] {
          ListUsersPageTokenPayload.parseFrom(decodedBytes)
        }.map(Right(_))
          .recover { case _: InvalidProtocolBufferException =>
            Left(invalidPageToken)
          }
          .get
        userId <- Ref.UserId
          .fromString(tokenPayload.userIdLowerBoundExcl)
          .map(Some(_))
          .left
          .map(_ => invalidPageToken)
      } yield {
        userId
      }
    }
  }

  private def invalidPageToken(implicit
      errorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    LedgerApiErrors.RequestValidation.InvalidArgument
      .Reject("Invalid page token")
      .asGrpcError
  }
}
