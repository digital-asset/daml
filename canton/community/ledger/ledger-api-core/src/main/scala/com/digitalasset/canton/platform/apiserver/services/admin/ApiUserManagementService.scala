// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserResponse,
  GetUserResponse,
  UpdateUserIdentityProviderRequest,
  UpdateUserIdentityProviderResponse,
  UpdateUserRequest,
  UpdateUserResponse,
}
import com.daml.ledger.api.v1.admin.user_management_service as proto
import com.daml.lf.data.Ref
import com.daml.platform.v1.page_tokens.ListUsersPageTokenPayload
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.auth.ClaimAdmin
import com.digitalasset.canton.ledger.api.auth.ClaimSet.Claims
import com.digitalasset.canton.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.canton.ledger.api.domain.*
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.{FieldValidator, ValueValidator}
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.{
  RequestValidationErrors,
  UserManagementServiceErrors,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexPartyManagementService
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.withEnrichedLoggingContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.apiserver.update
import com.digitalasset.canton.platform.apiserver.update.UserUpdateMapper
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

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
    indexPartyManagementService: IndexPartyManagementService,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends proto.UserManagementServiceGrpc.UserManagementService
    with GrpcApiService
    with NamedLogging {

  import ApiUserManagementService.*
  import FieldValidator.*
  import ValueValidator.*
  import UserManagementStore.handleResult

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    proto.UserManagementServiceGrpc.bindService(this, executionContext)

  override def createUser(request: proto.CreateUserRequest): Future[CreateUserResponse] = {
    withSubmissionId(loggerFactory, telemetry) { implicit loggingContext =>
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContext)
      // Retrieving the authenticated user context from the thread-local context
      val authorizedUserContextF: Future[AuthenticatedUserContext] =
        resolveAuthenticatedUserContext
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
          _ <- verifyPartiesExistInIdp(
            pRights,
            user.identityProviderId,
            authorizedUserContext.isParticipantAdmin,
          )
          result <- userManagementStore
            .createUser(
              user = user,
              rights = pRights,
            )
          createdUser <- handleResult("creating user")(result)
        } yield CreateUserResponse(Some(toProtoUser(createdUser)))
      }
    }
  }
  override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] = {
    withSubmissionId(loggerFactory, telemetry) { implicit loggingContext =>
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContext)
      val authorizedUserContextF: Future[AuthenticatedUserContext] =
        resolveAuthenticatedUserContext
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
            FieldValidator.requireResourceVersion(_, "user.metadata.resource_version")
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
          _ <- verifyPartiesExistInIdp(
            Set(),
            user.identityProviderId,
            authorizedUserContext.isParticipantAdmin,
          )
          _ <-
            if (
              authorizedUserContext.userId
                .contains(userUpdate.id) && userUpdate.isDeactivatedUpdateO.contains(true)
            ) {
              Future.failed(
                RequestValidationErrors.InvalidArgument
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

  private def resolveAuthenticatedUserContext(implicit
      errorLogger: ContextualizedErrorLogger
  ): Future[AuthenticatedUserContext] = {
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

  override def getUser(request: proto.GetUserRequest): Future[GetUserResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)
    withValidation {
      for {
        userId <- requireUserId(request.userId, "user_id")
        identityProviderId <- optionalIdentityProviderId(
          request.identityProviderId,
          "identity_provider_id",
        )
      } yield (userId, identityProviderId)
    } { case (userId, identityProviderId) =>
      userManagementStore
        .getUser(userId, identityProviderId)
        .flatMap(handleResult("getting user"))
        .map(u => GetUserResponse(Some(toProtoUser(u))))
    }
  }

  override def deleteUser(request: proto.DeleteUserRequest): Future[proto.DeleteUserResponse] =
    withSubmissionId(loggerFactory, telemetry) { implicit loggingContext =>
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContext)
      withValidation {
        for {
          userId <- requireUserId(request.userId, "user_id")
          identityProviderId <- optionalIdentityProviderId(
            request.identityProviderId,
            "identity_provider_id",
          )
        } yield (userId, identityProviderId)
      } { case (userId, identityProviderId) =>
        userManagementStore
          .deleteUser(userId, identityProviderId)
          .flatMap(handleResult("deleting user"))
          .map(_ => proto.DeleteUserResponse())
      }
    }

  override def listUsers(request: proto.ListUsersRequest): Future[proto.ListUsersResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)
    withValidation(
      for {
        fromExcl <- decodeUserIdFromPageToken(request.pageToken)
        rawPageSize <- Either.cond(
          request.pageSize >= 0,
          request.pageSize,
          RequestValidationErrors.InvalidArgument
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
        .map { page =>
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
  ): Future[proto.GrantUserRightsResponse] = withSubmissionId(loggerFactory, telemetry) {
    implicit loggingContext =>
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContext)
      // Retrieving the authenticated user context from the thread-local context
      val authorizedUserContextF: Future[AuthenticatedUserContext] =
        resolveAuthenticatedUserContext
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
          authorizedUserContext <- authorizedUserContextF
          _ <- verifyPartiesExistInIdp(
            rights,
            identityProviderId,
            authorizedUserContext.isParticipantAdmin,
          )
          result <- userManagementStore
            .grantRights(
              id = userId,
              rights = rights,
              identityProviderId = identityProviderId,
            )
          handledResult <- handleResult("grant user rights")(result)
        } yield proto.GrantUserRightsResponse(handledResult.view.map(toProtoRight).toList)
      }
  }

  override def revokeUserRights(
      request: proto.RevokeUserRightsRequest
  ): Future[proto.RevokeUserRightsResponse] = withSubmissionId(loggerFactory, telemetry) {
    implicit loggingContext =>
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContext)
      // Retrieving the authenticated user context from the thread-local context
      val authorizedUserContextF: Future[AuthenticatedUserContext] =
        resolveAuthenticatedUserContext
      withValidation(
        for {
          userId <- FieldValidator.requireUserId(request.userId, "user_id")
          rights <- fromProtoRights(request.rights)
          identityProviderId <- optionalIdentityProviderId(
            request.identityProviderId,
            "identity_provider_id",
          )
        } yield (userId, rights, identityProviderId)
      ) { case (userId, rights, identityProviderId) =>
        for {
          authorizedUserContext <- authorizedUserContextF
          _ <- verifyPartiesExistInIdp(
            rights,
            identityProviderId,
            authorizedUserContext.isParticipantAdmin,
          )
          result <- userManagementStore
            .revokeRights(
              id = userId,
              rights = rights,
              identityProviderId = identityProviderId,
            )
          handledResult <- handleResult("revoke user rights")(result)
        } yield proto.RevokeUserRightsResponse(handledResult.view.map(toProtoRight).toList)
      }
  }

  override def listUserRights(
      request: proto.ListUserRightsRequest
  ): Future[proto.ListUserRightsResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)
    withValidation {
      for {
        userId <- requireUserId(request.userId, "user_id")
        identityProviderId <- optionalIdentityProviderId(
          request.identityProviderId,
          "identity_provider_id",
        )
      } yield (userId, identityProviderId)
    } { case (userId, identityProviderId) =>
      userManagementStore
        .listUserRights(userId, identityProviderId)
        .flatMap(handleResult("list user rights"))
        .map(_.view.map(toProtoRight).toList)
        .map(proto.ListUserRightsResponse(_))
    }
  }
  override def updateUserIdentityProviderId(
      request: UpdateUserIdentityProviderRequest
  ): Future[UpdateUserIdentityProviderResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)
    withValidation {
      for {
        userId <- requireUserId(request.userId, "user_id")
        sourceIdentityProviderId <- optionalIdentityProviderId(
          request.sourceIdentityProviderId,
          "source_identity_provider_id",
        )
        targetIdentityProviderId <- optionalIdentityProviderId(
          request.targetIdentityProviderId,
          "target_identity_provider_id",
        )
      } yield (userId, sourceIdentityProviderId, targetIdentityProviderId)
    } { case (userId, sourceIdentityProviderId, targetIdentityProviderId) =>
      for {
        _ <- identityProviderExistsOrError(sourceIdentityProviderId)
        _ <- identityProviderExistsOrError(targetIdentityProviderId)
        result <- userManagementStore
          .updateUserIdp(
            sourceIdp = sourceIdentityProviderId,
            targetIdp = targetIdentityProviderId,
            id = userId,
          )
          .flatMap(handleResult("update user identity provider"))
          .map(_ => proto.UpdateUserIdentityProviderResponse())
      } yield result
    }
  }

  private def handleUpdatePathResult[T](userId: Ref.UserId, result: update.Result[T])(implicit
      errorLogger: ContextualizedErrorLogger
  ): Future[T] =
    result match {
      case Left(e: update.UpdatePathError) =>
        Future.failed(
          UserManagementServiceErrors.InvalidUpdateUserRequest
            .Reject(userId = userId, e.getReason)
            .asGrpcError
        )
      case scala.util.Right(t) =>
        Future.successful(t)
    }

  private def verifyPartiesExistInIdp(
      rights: Set[UserRight],
      identityProviderId: IdentityProviderId,
      isParticipantAdmin: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLogger: ContextualizedErrorLogger,
  ): Future[Unit] = {
    val parties = userParties(rights)
    val partiesExistingInPartyRecordStore =
      if (isParticipantAdmin) partyRecordExist.filterPartiesExistingInPartyRecordStore(parties)
      else
        partyRecordExist
          .filterPartiesExistingInPartyRecordStore(identityProviderId, parties)

    partiesExistingInPartyRecordStore
      .flatMap { partiesExist =>
        val partiesWithoutRecord = parties -- partiesExist
        if (partiesWithoutRecord.isEmpty)
          Future.unit
        else
          verifyPartiesExistsInIdp(partiesWithoutRecord, identityProviderId)
      }
  }

  private def verifyPartiesExistsInIdp(
      partiesWithoutRecord: Set[Ref.Party],
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLogger: ContextualizedErrorLogger,
  ): Future[Unit] =
    indexKnownParties(partiesWithoutRecord.toList).flatMap { partiesKnown =>
      val unknownParties = partiesWithoutRecord -- partiesKnown
      if (unknownParties.isEmpty) Future.unit
      else
        partiesNotExistsError(unknownParties, identityProviderId)
    }

  private def indexKnownParties(
      parties: Seq[Ref.Party]
  )(implicit loggingContext: LoggingContextWithTrace): Future[Set[Ref.Party]] =
    indexPartyManagementService.getParties(parties).map { partyDetails =>
      partyDetails.map(_.party).toSet
    }

  private def partiesNotExistsError(
      unknownParties: Set[Ref.Party],
      identityProviderId: IdentityProviderId,
  )(implicit errorLogger: ContextualizedErrorLogger) = {
    val message =
      s"Provided parties have not been found in " +
        s"identity_provider_id=`${identityProviderId.toRequestString}`: [${unknownParties.mkString(",")}]."
    Future.failed(
      RequestValidationErrors.InvalidArgument
        .Reject(message)
        .asGrpcError
    )
  }

  private def identityProviderExistsOrError(
      id: IdentityProviderId
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLogger: ContextualizedErrorLogger,
  ): Future[Unit] =
    identityProviderExists(id)
      .flatMap { idpExists =>
        if (idpExists)
          Future.unit
        else
          Future.failed(
            RequestValidationErrors.InvalidArgument
              .Reject(s"Provided identity_provider_id $id has not been found.")
              .asGrpcError
          )
      }

  private def userParties(rights: Set[UserRight]): Set[Ref.Party] = rights.collect {
    case UserRight.CanActAs(party) => party
    case UserRight.CanReadAs(party) => party
  }

  private def withValidation[A, B](validatedResult: Either[StatusRuntimeException, A])(
      f: A => Future[B]
  ): Future[B] =
    validatedResult.fold(Future.failed, Future.successful).flatMap(f)

  private def fromProtoRight(
      right: proto.Right
  )(implicit errorLogger: ContextualizedErrorLogger): Either[StatusRuntimeException, UserRight] =
    right match {
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
          RequestValidationErrors.InvalidArgument
            .Reject(
              "unknown kind of right - check that the Ledger API version of the server is recent enough"
            )
            .asGrpcError
        )
    }

  private def fromProtoRights(
      rights: Seq[proto.Right]
  )(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Set[UserRight]] =
    rights.toList.traverse(fromProtoRight).map(_.toSet)

  private def withSubmissionId[A](loggerFactory: NamedLoggerFactory, telemetry: Telemetry)(
      f: LoggingContextWithTrace => A
  ): A = {
    val loggingContext = createLoggingContext(loggerFactory)(identity)
    withEnrichedLoggingContext(telemetry)("submissionId" -> submissionIdGenerator.generate())(f)(
      loggingContext
    )
  }

}

object ApiUserManagementService {
  final case class AuthenticatedUserContext(userId: Option[String], isParticipantAdmin: Boolean)
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
        decodedBytes <- Try[Array[Byte]](Base64.getUrlDecoder.decode(bytes)).toEither.left
          .map(_ => invalidPageToken)
        tokenPayload <- Try[ListUsersPageTokenPayload] {
          ListUsersPageTokenPayload.parseFrom(decodedBytes)
        }.toEither.left
          .map(_ => invalidPageToken)
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
    RequestValidationErrors.InvalidArgument
      .Reject("Invalid page token")
      .asGrpcError
  }
}
