// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.auth.ClaimAdmin
import com.daml.ledger.api.auth.ClaimSet.Claims
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.domain._
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserResponse,
  GetUserResponse,
  UpdateUserRequest,
  UpdateUserResponse,
}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.page_tokens.ListUsersPageTokenPayload
import com.daml.platform.apiserver.update
import com.daml.platform.apiserver.update.UserUpdateMapper
import com.daml.platform.localstore.api.UserManagementStore
import com.daml.platform.server.api.validation.FieldValidations
import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[apiserver] final class ApiUserManagementService(
    userManagementStore: UserManagementStore,
    identityProviderExists: IdentityProviderExists,
    partyRecordExist: PartyRecordsExist,
    maxUsersPageSize: Int,
    submissionIdGenerator: SubmissionIdGenerator,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends proto.UserManagementServiceGrpc.UserManagementService
    with GrpcApiService {

  import ApiUserManagementService._

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  import FieldValidations._

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    proto.UserManagementServiceGrpc.bindService(this, executionContext)

  override def createUser(request: proto.CreateUserRequest): Future[CreateUserResponse] =
    withSubmissionId { implicit loggingContext =>
      // Retrieving the authenticated user context from the thread-local context
      val authorizedUserContextF: Future[AuthenticatedUserContext] =
        resolveAuthenticatedUserContext()
      withValidation {
        for {
          pUser <- requirePresence(request.user, "user")
          pUserId <- requireUserId(pUser.id, "id")
          pMetadata = pUser.metadata.getOrElse(
            com.daml.ledger.api.v1.admin.object_meta.ObjectMeta()
          )
          _ <- requireEmptyString(
            pMetadata.resourceVersion,
            "user.metadata.resource_version",
          )
          pAnnotations <- verifyMetadataAnnotations(
            pMetadata.annotations,
            allowEmptyValues = false,
            "user.metadata.annotations",
          )
          pOptPrimaryParty <- optionalString(pUser.primaryParty)(requireParty)
          pRights <- fromProtoRights(request.rights)
          identityProviderId <- optionalIdentityProviderId(
            pUser.identityProviderId,
            "identity_provider_id",
          )
        } yield (
          User(
            id = pUserId,
            primaryParty = pOptPrimaryParty,
            isDeactivated = pUser.isDeactivated,
            metadata = ObjectMeta(
              resourceVersionO = None,
              annotations = pAnnotations,
            ),
            identityProviderId = identityProviderId,
          ),
          pRights,
        )
      } { case (user, pRights) =>
        for {
          _ <- identityProviderExistsOrError(user.identityProviderId)
          authorizedUserContext <- authorizedUserContextF
          _ <- verifyPartyExistInIdp(
            pRights,
            user.identityProviderId,
            authorizedUserContext.isParticipantAdmin,
          )
          _ <- verifyUserDoesNotExist(user.identityProviderId, user.id, "creating user")
          result <- userManagementStore
            .createUser(
              user = user,
              rights = pRights,
            )
          createdUser <- handleResult("creating user")(result)
        } yield CreateUserResponse(Some(toProtoUser(createdUser)))
      }
    }

  override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] = {
    withSubmissionId { implicit loggingContext =>
      val authorizedUserContextF: Future[AuthenticatedUserContext] =
        resolveAuthenticatedUserContext()
      withValidation {
        for {
          pUser <- requirePresence(request.user, "user")
          pUserId <- requireUserId(pUser.id, "user.id")
          pMetadata = pUser.metadata.getOrElse(
            com.daml.ledger.api.v1.admin.object_meta.ObjectMeta()
          )
          pFieldMask <- requirePresence(request.updateMask, "update_mask")
          pOptPrimaryParty <- optionalString(pUser.primaryParty)(requireParty)
          identityProviderId <- optionalIdentityProviderId(
            pUser.identityProviderId,
            "identity_provider_id",
          )
          pResourceVersion <- optionalString(pMetadata.resourceVersion)(
            FieldValidations.requireResourceVersion(_, "user.metadata.resource_version")
          )
          pAnnotations <- verifyMetadataAnnotations(
            pMetadata.annotations,
            allowEmptyValues = true,
            "user.metadata.annotations",
          )
        } yield (
          User(
            id = pUserId,
            primaryParty = pOptPrimaryParty,
            isDeactivated = pUser.isDeactivated,
            metadata = ObjectMeta(
              resourceVersionO = pResourceVersion,
              annotations = pAnnotations,
            ),
            identityProviderId = identityProviderId,
          ),
          pFieldMask,
        )
      } { case (user, fieldMask) =>
        for {
          userUpdate <- handleUpdatePathResult(user.id, UserUpdateMapper.toUpdate(user, fieldMask))
          _ <- identityProviderExistsOrError(user.identityProviderId)
          authorizedUserContext <- authorizedUserContextF
          _ <- verifyPartyExistInIdp(
            Set(),
            user.identityProviderId,
            authorizedUserContext.isParticipantAdmin,
          )
          _ <- verifyUserExistsAndInIdp(user.identityProviderId, user.id, "updating user")
          _ <-
            if (
              authorizedUserContext.userId
                .contains(userUpdate.id) && userUpdate.isDeactivatedUpdateO.contains(true)
            ) {
              Future.failed(
                LedgerApiErrors.RequestValidation.InvalidArgument
                  .Reject(
                    "Requesting user cannot self-deactivate"
                  )
                  .asGrpcError
              )
            } else {
              Future.unit
            }
          resp <- userManagementStore
            .updateUser(userUpdate = userUpdate)
            .flatMap(handleResult("updating user"))
            .map { u =>
              UpdateUserResponse(user = Some(toProtoUser(u)))
            }
        } yield resp
      }
    }
  }

  private def resolveAuthenticatedUserContext(): Future[AuthenticatedUserContext] = {
    AuthorizationInterceptor
      .extractClaimSetFromContext()
      .fold(
        fa = error =>
          Future.failed(
            LedgerApiErrors.InternalError
              .Generic("Could not extract a claim set from the context", throwableO = Some(error))
              .asGrpcError
          ),
        fb = {
          case claims: Claims =>
            Future.successful(AuthenticatedUserContext(claims))
          case claimsSet =>
            Future.failed(
              LedgerApiErrors.InternalError
                .Generic(
                  s"Unexpected claims when trying to resolve the authenticated user: $claimsSet"
                )
                .asGrpcError
            )
        },
      )
  }

  override def getUser(request: proto.GetUserRequest): Future[GetUserResponse] =
    withValidation {
      for {
        userId <- requireUserId(request.userId, "user_id")
        identityProviderId <- optionalIdentityProviderId(
          request.identityProviderId,
          "identity_provider_id",
        )
      } yield (userId, identityProviderId)
    } { case (userId, identityProviderId) =>
      verifyUserExistsAndInIdp(identityProviderId, userId, "getting user")
        .flatMap { _ => userManagementStore.getUser(userId) }
        .flatMap(handleResult("getting user"))
        .map(u => GetUserResponse(Some(toProtoUser(u))))
    }

  override def deleteUser(request: proto.DeleteUserRequest): Future[proto.DeleteUserResponse] =
    withSubmissionId { implicit loggingContext =>
      withValidation {
        for {
          userId <- requireUserId(request.userId, "user_id")
          identityProviderId <- optionalIdentityProviderId(
            request.identityProviderId,
            "identity_provider_id",
          )
        } yield (userId, identityProviderId)
      } { case (userId, identityProviderId) =>
        verifyUserExistsAndInIdp(identityProviderId, userId, "deleting user")
          .flatMap { _ => userManagementStore.deleteUser(userId) }
          .flatMap(handleResult("deleting user"))
          .map(_ => proto.DeleteUserResponse())
      }
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
        identityProviderId <- optionalIdentityProviderId(
          request.identityProviderId,
          "identity_provider_id",
        )
        pageSize =
          if (rawPageSize == 0) maxUsersPageSize
          else Math.min(request.pageSize, maxUsersPageSize)
      } yield {
        (fromExcl, pageSize, identityProviderId)
      }
    ) { case (fromExcl, pageSize, identityProviderId) =>
      userManagementStore
        .listUsers(fromExcl, pageSize, identityProviderId)
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
    // Retrieving the authenticated user context from the thread-local context
    val authorizedUserContextF: Future[AuthenticatedUserContext] =
      resolveAuthenticatedUserContext()
    withValidation(
      for {
        userId <- requireUserId(request.userId, "user_id")
        rights <- fromProtoRights(request.rights)
        identityProviderId <- optionalIdentityProviderId(
          request.identityProviderId,
          "identity_provider_id",
        )
      } yield (userId, rights, identityProviderId)
    ) { case (userId, rights, identityProviderId) =>
      for {
        _ <- verifyUserExistsAndInIdp(identityProviderId, userId, "grant user rights")
        authorizedUserContext <- authorizedUserContextF
        _ <- verifyPartyExistInIdp(
          rights,
          identityProviderId,
          authorizedUserContext.isParticipantAdmin,
        )
        result <- userManagementStore
          .grantRights(
            id = userId,
            rights = rights,
          )
        handledResult <- handleResult("grant user rights")(result)
      } yield proto.GrantUserRightsResponse(handledResult.view.map(toProtoRight).toList)
    }
  }

  override def revokeUserRights(
      request: proto.RevokeUserRightsRequest
  ): Future[proto.RevokeUserRightsResponse] = withSubmissionId { implicit loggingContext =>
    // Retrieving the authenticated user context from the thread-local context
    val authorizedUserContextF: Future[AuthenticatedUserContext] =
      resolveAuthenticatedUserContext()
    withValidation(
      for {
        userId <- FieldValidations.requireUserId(request.userId, "user_id")
        rights <- fromProtoRights(request.rights)
        identityProviderId <- optionalIdentityProviderId(
          request.identityProviderId,
          "identity_provider_id",
        )
      } yield (userId, rights, identityProviderId)
    ) { case (userId, rights, identityProviderId) =>
      for {
        authorizedUserContext <- authorizedUserContextF
        _ <- verifyPartyExistInIdp(
          rights,
          identityProviderId,
          authorizedUserContext.isParticipantAdmin,
        )
        _ <- verifyUserExistsAndInIdp(identityProviderId, userId, "revoke user rights")
        result <- userManagementStore
          .revokeRights(
            id = userId,
            rights = rights,
          )
        handledResult <- handleResult("revoke user rights")(result)
      } yield proto.RevokeUserRightsResponse(handledResult.view.map(toProtoRight).toList)
    }
  }

  override def listUserRights(
      request: proto.ListUserRightsRequest
  ): Future[proto.ListUserRightsResponse] =
    withValidation {
      for {
        userId <- requireUserId(request.userId, "user_id")
        identityProviderId <- optionalIdentityProviderId(
          request.identityProviderId,
          "identity_provider_id",
        )
      } yield (userId, identityProviderId)
    } { case (userId, identityProviderId) =>
      verifyUserExistsAndInIdp(identityProviderId, userId, "list user rights")
        .flatMap { _ => userManagementStore.listUserRights(userId) }
        .flatMap(handleResult("list user rights"))
        .map(_.view.map(toProtoRight).toList)
        .map(proto.ListUserRightsResponse(_))
    }

  private def handleUpdatePathResult[T](userId: Ref.UserId, result: update.Result[T]): Future[T] =
    result match {
      case Left(e: update.UpdatePathError) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest
            .Reject(userId = userId, e.getReason)
            .asGrpcError
        )
      case scala.util.Right(t) =>
        Future.successful(t)
    }

  private def verifyPartyExistInIdp(
      rights: Set[UserRight],
      identityProviderId: IdentityProviderId,
      isParticipantAdmin: Boolean,
  ): Future[Unit] =
    if (isParticipantAdmin) Future.unit
    else partyExistsOrError(userParties(rights), identityProviderId)

  private def partyExistsOrError(
      parties: Set[Ref.Party],
      identityProviderId: IdentityProviderId,
  ): Future[Unit] =
    partyRecordExist
      .filterPartiesExistingInPartyRecordStore(identityProviderId, parties)
      .flatMap { partiesExist =>
        val partiesNotExist = parties -- partiesExist
        if (partiesNotExist.isEmpty)
          Future.successful(())
        else
          Future.failed(
            LedgerApiErrors.RequestValidation.InvalidArgument
              .Reject(
                s"Provided parties [${partiesNotExist.mkString(",")}] have not been found in identity_provider_id=${identityProviderId.toRequestString}."
              )
              .asGrpcError
          )
      }

  private def identityProviderExistsOrError(id: IdentityProviderId): Future[Unit] =
    identityProviderExists(id)
      .flatMap { idpExists =>
        if (idpExists)
          Future.unit
        else
          Future.failed(
            LedgerApiErrors.RequestValidation.InvalidArgument
              .Reject(s"Provided identity_provider_id $id has not been found.")
              .asGrpcError
          )
      }

  // Check if user either doesn't exist or if it does - it belongs to the requested Identity Provider
  // Alternatively, identity_provider_id could be part of the compound unique key within user database.
  // It was considered as complication for the implementation and for now is simplified.
  private def verifyUserDoesNotExist(
      identityProviderId: IdentityProviderId,
      userId: Ref.UserId,
      operation: String,
  ): Future[Unit] = {
    userManagementStore.getUser(userId).flatMap {
      case Right(user) if user.identityProviderId != identityProviderId =>
        Future.failed(
          LedgerApiErrors.AuthorizationChecks.PermissionDenied
            .Reject(s"User ${user.id} belongs to another Identity Provider")
            .asGrpcError
        )
      case Right(_) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.UserAlreadyExists
            .Reject(operation, userId.toString)
            .asGrpcError
        )
      case _ =>
        Future.unit
    }
  }

  // Check if user exists and belongs to the requested Identity Provider
  private def verifyUserExistsAndInIdp(
      identityProviderId: IdentityProviderId,
      userId: Ref.UserId,
      operation: String,
  ): Future[Unit] = {
    userManagementStore.getUser(userId).flatMap {
      case Right(user) if user.identityProviderId != identityProviderId =>
        Future.failed(
          LedgerApiErrors.AuthorizationChecks.PermissionDenied
            .Reject(s"User ${user.id} belongs to another Identity Provider")
            .asGrpcError
        )
      case Left(UserManagementStore.UserNotFound(id)) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.UserNotFound
            .Reject(operation, id.toString)
            .asGrpcError
        )
      case _ => Future.unit
    }
  }

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
      case Left(e: UserManagementStore.ConcurrentUserUpdate) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.ConcurrentUserUpdateDetected
            .Reject(userId = e.userId)
            .asGrpcError
        )

      case Left(e: UserManagementStore.MaxAnnotationsSizeExceeded) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.MaxUserAnnotationsSizeExceeded
            .Reject(userId = e.userId)
            .asGrpcError
        )

      case scala.util.Right(t) =>
        Future.successful(t)
    }

  private def userParties(rights: Set[UserRight]): Set[Ref.Party] = rights.collect {
    case UserRight.CanActAs(party) => party
    case UserRight.CanReadAs(party) => party
  }

  private def withValidation[A, B](validatedResult: Either[StatusRuntimeException, A])(
      f: A => Future[B]
  ): Future[B] =
    validatedResult.fold(Future.failed, Future.successful).flatMap(f)

  private val fromProtoRight: proto.Right => Either[StatusRuntimeException, UserRight] = {
    case proto.Right(_: proto.Right.Kind.ParticipantAdmin) =>
      Right(UserRight.ParticipantAdmin)

    case proto.Right(_: proto.Right.Kind.IdentityProviderAdmin) =>
      Right(UserRight.IdentityProviderAdmin)

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
  case class AuthenticatedUserContext(userId: Option[String], isParticipantAdmin: Boolean)
  object AuthenticatedUserContext {
    def apply(claims: Claims): AuthenticatedUserContext = claims match {
      case claims: Claims if claims.resolvedFromUser =>
        AuthenticatedUserContext(claims.applicationId, claims.claims.contains(ClaimAdmin))
      case claims: Claims =>
        AuthenticatedUserContext(None, claims.claims.contains(ClaimAdmin))
    }
  }

  private def toProtoUser(user: User): proto.User =
    proto.User(
      id = user.id,
      primaryParty = user.primaryParty.getOrElse(""),
      isDeactivated = user.isDeactivated,
      metadata = Some(Utils.toProtoObjectMeta(user.metadata)),
      identityProviderId = user.identityProviderId.toRequestString,
    )

  private val toProtoRight: UserRight => proto.Right = {
    case UserRight.ParticipantAdmin =>
      proto.Right(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))
    case UserRight.IdentityProviderAdmin =>
      proto.Right(proto.Right.Kind.IdentityProviderAdmin(proto.Right.IdentityProviderAdmin()))
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
