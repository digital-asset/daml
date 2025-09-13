// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.user_management_service
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.client.services.admin.UserManagementClient
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.UserId
import io.circe.Codec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody

import scala.concurrent.Future

class JsUserManagementService(
    userManagementClient: UserManagementClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit val authInterceptor: AuthInterceptor)
    extends Endpoints
    with NamedLogging {
  import JsUserManagementService.*

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  def endpoints() = List(
    asPagedList(
      JsUserManagementService.listUsersEndpoint,
      listUsers,
    ),
    withServerLogic(
      JsUserManagementService.createUserEndpoint,
      createUser,
    ),
    withServerLogic(
      JsUserManagementService.getUserEndpoint,
      getUser,
    ),
    withServerLogic(
      JsUserManagementService.getCurrentUserEndpoint,
      getCurrentUser,
    ),
    withServerLogic(
      JsUserManagementService.updateUserEndpoint,
      updateUser,
    ),
    withServerLogic(
      JsUserManagementService.deleteUserEndpoint,
      deleteUser,
    ),
    withServerLogic(
      JsUserManagementService.grantUserRightsEndpoint,
      grantUserRights,
    ),
    withServerLogic(
      JsUserManagementService.revokeUserRightsEndpoint,
      revokeUserRights,
    ),
    withServerLogic(
      JsUserManagementService.listUserRightsEndpoint,
      listUserRights,
    ),
    withServerLogic(
      JsUserManagementService.updateUserIdentityProviderEndpoint,
      updateUserIdentityProvider,
    ),
  )
  private def createUser(
      callerContext: CallerContext
  ): TracedInput[user_management_service.CreateUserRequest] => Future[
    Either[JsCantonError, user_management_service.CreateUserResponse]
  ] = req =>
    userManagementClient
      .serviceStub(callerContext.token())(req.traceContext)
      .createUser(req.in)
      .resultToRight

  private def listUsers(
      callerContext: CallerContext
  ): TracedInput[PagedList[Unit]] => Future[
    Either[JsCantonError, user_management_service.ListUsersResponse]
  ] = req =>
    userManagementClient
      .serviceStub(callerContext.token())(req.traceContext)
      .listUsers(
        user_management_service
          .ListUsersRequest(req.in.pageToken.getOrElse(""), req.in.pageSize.getOrElse(0), "")
      )
      .resultToRight

  private def getUser(
      callerContext: CallerContext
  ): TracedInput[(String, Option[String])] => Future[
    Either[JsCantonError, user_management_service.GetUserResponse]
  ] = { req =>
    val requestedUserId = (req.in._1)
    val requestedIdentityProviderId = req.in._2.getOrElse("")
    UserId.fromString(requestedUserId) match {
      case Right(userId) =>
        userManagementClient
          .serviceStub(callerContext.token())(req.traceContext)
          .getUser(
            user_management_service.GetUserRequest(
              userId = userId,
              identityProviderId = requestedIdentityProviderId,
            )
          )
          .resultToRight
      case Left(error) => malformedUserId(error)(req.traceContext)
    }
  }

  private def getCurrentUser(
      callerContext: CallerContext
  ): TracedInput[Option[String]] => Future[
    Either[JsCantonError, user_management_service.GetUserResponse]
  ] = { req =>
    val requestedIdentityProviderId = req.in.getOrElse("")
    userManagementClient
      .serviceStub(callerContext.token())(req.traceContext)
      .getUser(
        user_management_service.GetUserRequest(
          userId = "",
          identityProviderId = requestedIdentityProviderId,
        )
      )
      .resultToRight
  }

  private def updateUser(
      callerContext: CallerContext
  ): TracedInput[(String, user_management_service.UpdateUserRequest)] => Future[
    Either[JsCantonError, user_management_service.UpdateUserResponse]
  ] = req =>
    if (req.in._2.user.map(_.id).contains(req.in._1)) {
      userManagementClient
        .serviceStub(callerContext.token())(req.traceContext)
        .updateUser(req.in._2)
        .resultToRight
    } else {
      unmatchedUserId(req.traceContext, req.in._1, req.in._2.user.map(_.id))
    }

  private def deleteUser(
      callerContext: CallerContext
  ): TracedInput[String] => Future[Either[JsCantonError, Unit]] = req =>
    UserId.fromString(req.in) match {
      case Right(userId) =>
        userManagementClient
          .deleteUser(userId, callerContext.token())(req.traceContext)
          .resultToRight
      case Left(errorMsg) =>
        malformedUserId(errorMsg)(req.traceContext)
    }

  private def listUserRights(
      callerContext: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, user_management_service.ListUserRightsResponse]
  ] = req =>
    UserId.fromString(req.in) match {
      case Right(userId) =>
        userManagementClient
          .serviceStub(callerContext.token())(req.traceContext)
          .listUserRights(
            new user_management_service.ListUserRightsRequest(
              userId = userId,
              identityProviderId = "",
            )
          )
          .resultToRight
      case Left(error) => malformedUserId(error)(req.traceContext)
    }

  private def grantUserRights(
      callerContext: CallerContext
  ): TracedInput[(String, user_management_service.GrantUserRightsRequest)] => Future[
    Either[JsCantonError, user_management_service.GrantUserRightsResponse]
  ] =
    req =>
      if (req.in._2.userId == req.in._1) {
        userManagementClient
          .serviceStub(callerContext.token())(req.traceContext)
          .grantUserRights(req.in._2)
          .resultToRight
      } else {
        unmatchedUserId(req.traceContext, req.in._1, Some(req.in._2.userId))
      }

  private def revokeUserRights(
      callerContext: CallerContext
  ): TracedInput[(String, user_management_service.RevokeUserRightsRequest)] => Future[
    Either[JsCantonError, user_management_service.RevokeUserRightsResponse]
  ] =
    req =>
      if (req.in._2.userId == req.in._1) {
        userManagementClient
          .serviceStub(callerContext.token())(req.traceContext)
          .revokeUserRights(req.in._2)
          .resultToRight
      } else {
        unmatchedUserId(req.traceContext, req.in._1, Some(req.in._2.userId))
      }

  private def updateUserIdentityProvider(
      callerContext: CallerContext
  ): TracedInput[(String, user_management_service.UpdateUserIdentityProviderIdRequest)] => Future[
    Either[JsCantonError, user_management_service.UpdateUserIdentityProviderIdResponse]
  ] = req =>
    if (req.in._2.userId == req.in._1) {
      userManagementClient
        .serviceStub(callerContext.token())(req.traceContext)
        .updateUserIdentityProviderId(req.in._2)
        .resultToRight
    } else {
      unmatchedUserId(req.traceContext, req.in._1, Some(req.in._2.userId))
    }

  private def malformedUserId(errorMessage: String)(implicit traceContext: TraceContext) =
    error(
      JsCantonError.fromErrorCode(InvalidArgument.Reject(s"Malformed $userIdPath: $errorMessage"))
    )

  private def unmatchedUserId(implicit
      traceContext: TraceContext,
      userInPath: String,
      userInBody: Option[String],
  ) =
    error(
      JsCantonError.fromErrorCode(
        InvalidArgument.Reject(s"$userInPath does not match user in body: $userInBody ")
      )
    )
}

object JsUserManagementService extends DocumentationEndpoints {
  import Endpoints.*
  import JsUserManagementCodecs.*

  private val users = v2Endpoint.in("users")

  private val authenticatedUser = v2Endpoint.in("authenticated-user")

  private val userIdPath = "user-id"

  val listUsersEndpoint =
    users.get
      .out(jsonBody[user_management_service.ListUsersResponse])
      .inPagedListParams()
      .description("List all users.")

  val createUserEndpoint =
    users.post
      .in(jsonBody[user_management_service.CreateUserRequest])
      .out(jsonBody[user_management_service.CreateUserResponse])
      .description("Create user.")

  val getUserEndpoint =
    users.get
      .in(path[String](userIdPath))
      .in(query[Option[String]]("identity-provider-id"))
      .out(jsonBody[user_management_service.GetUserResponse])
      .description("Get user details.")

  val getCurrentUserEndpoint =
    authenticatedUser.get
      .in(query[Option[String]]("identity-provider-id"))
      .out(jsonBody[user_management_service.GetUserResponse])
      .description("Get current user details (uses user for JWT).")

  val updateUserEndpoint =
    users.patch
      .in(path[String](userIdPath))
      .in(jsonBody[user_management_service.UpdateUserRequest])
      .out(jsonBody[user_management_service.UpdateUserResponse])
      .description("Update  user.")

  val deleteUserEndpoint =
    users.delete
      .in(path[String](userIdPath))
      .out(jsonBody[Unit])
      .description("Delete user.")

  val grantUserRightsEndpoint =
    users.post
      .in(path[String](userIdPath))
      .in("rights")
      .in(jsonBody[user_management_service.GrantUserRightsRequest])
      .out(jsonBody[user_management_service.GrantUserRightsResponse])
      .description("Grant user rights.")
  val revokeUserRightsEndpoint =
    users.patch
      .in(path[String](userIdPath))
      .in("rights")
      .in(jsonBody[user_management_service.RevokeUserRightsRequest])
      .out(jsonBody[user_management_service.RevokeUserRightsResponse])
      .description("Revoke user rights.")

  val listUserRightsEndpoint =
    users.get
      .in(path[String](userIdPath))
      .in("rights")
      .out(jsonBody[user_management_service.ListUserRightsResponse])
      .description("List user rights.")

  val updateUserIdentityProviderEndpoint =
    users.patch
      .in(path[String](userIdPath))
      .in("identity-provider-id")
      .in(jsonBody[user_management_service.UpdateUserIdentityProviderIdRequest])
      .out(jsonBody[user_management_service.UpdateUserIdentityProviderIdResponse])
      .description("Update user identity provider.")

  override def documentation: Seq[AnyEndpoint] = List(
    listUsersEndpoint,
    createUserEndpoint,
    getUserEndpoint,
    getCurrentUserEndpoint,
    updateUserEndpoint,
    deleteUserEndpoint,
    grantUserRightsEndpoint,
    revokeUserRightsEndpoint,
    listUserRightsEndpoint,
    updateUserIdentityProviderEndpoint,
  )
}

object JsUserManagementCodecs {
  import JsSchema.config

  implicit val userRW: Codec[user_management_service.User] = deriveRelaxedCodec
  implicit val participantAdminRW: Codec[user_management_service.Right.ParticipantAdmin] =
    deriveRelaxedCodec
  implicit val canActAsRW: Codec[user_management_service.Right.CanActAs] = deriveRelaxedCodec
  implicit val canReadAsRW: Codec[user_management_service.Right.CanReadAs] = deriveRelaxedCodec
  implicit val canExecuteAsRW: Codec[user_management_service.Right.CanExecuteAs] =
    deriveRelaxedCodec
  implicit val rightKindidentityProviderAdminRW
      : Codec[user_management_service.Right.Kind.IdentityProviderAdmin] =
    deriveRelaxedCodec
  implicit val identityProviderAdminRW: Codec[user_management_service.Right.IdentityProviderAdmin] =
    deriveRelaxedCodec
  implicit val canReadAsAnyPartyRWRW: Codec[user_management_service.Right.CanReadAsAnyParty] =
    deriveRelaxedCodec
  implicit val canExecuteAsAnyPartyRWRW: Codec[user_management_service.Right.CanExecuteAsAnyParty] =
    deriveRelaxedCodec
  implicit val kindCanActAsRWRW: Codec[user_management_service.Right.Kind.CanActAs] =
    deriveRelaxedCodec
  implicit val kindCanReadAsRWRW: Codec[user_management_service.Right.Kind.CanReadAs] =
    deriveRelaxedCodec
  implicit val kindCanReadAsAnyPartyRW
      : Codec[user_management_service.Right.Kind.CanReadAsAnyParty] =
    deriveRelaxedCodec
  implicit val kindCanExecuteAsRWRW: Codec[user_management_service.Right.Kind.CanExecuteAs] =
    deriveRelaxedCodec
  implicit val kindCanExecuteAsAnyPartyRW
      : Codec[user_management_service.Right.Kind.CanExecuteAsAnyParty] =
    deriveRelaxedCodec
  implicit val kindParticipantAdminRWRW
      : Codec[user_management_service.Right.Kind.ParticipantAdmin] =
    deriveRelaxedCodec
  implicit val kindRW: Codec[user_management_service.Right.Kind] = deriveConfiguredCodec // ADT
  implicit val rightRW: Codec[user_management_service.Right] = deriveRelaxedCodec
  implicit val createUserRequestRW: Codec[user_management_service.CreateUserRequest] =
    deriveRelaxedCodec
  implicit val updateUserRequestRW: Codec[user_management_service.UpdateUserRequest] =
    deriveRelaxedCodec
  implicit val listUserResponseRW: Codec[user_management_service.ListUsersResponse] =
    deriveRelaxedCodec
  implicit val createUserResponseRW: Codec[user_management_service.CreateUserResponse] =
    deriveRelaxedCodec
  implicit val updateUserResponseRW: Codec[user_management_service.UpdateUserResponse] =
    deriveRelaxedCodec
  implicit val getUserResponseRW: Codec[user_management_service.GetUserResponse] =
    deriveRelaxedCodec
  implicit val grantUserRightsRequestRW: Codec[user_management_service.GrantUserRightsRequest] =
    deriveRelaxedCodec
  implicit val grantUserRightsResponseRW: Codec[user_management_service.GrantUserRightsResponse] =
    deriveRelaxedCodec
  implicit val revokeUserRightsRequestRW: Codec[user_management_service.RevokeUserRightsRequest] =
    deriveRelaxedCodec
  implicit val revokeUserRightsResponseRW: Codec[user_management_service.RevokeUserRightsResponse] =
    deriveRelaxedCodec

  implicit val listUserRightsRequestRW: Codec[user_management_service.ListUserRightsRequest] =
    deriveRelaxedCodec
  implicit val listUserRightsResponseRW: Codec[user_management_service.ListUserRightsResponse] =
    deriveRelaxedCodec

  implicit val updateIdentityProviderRequestRW
      : Codec[user_management_service.UpdateUserIdentityProviderIdRequest] = deriveRelaxedCodec
  implicit val updateIdentityProviderResponseRW
      : Codec[user_management_service.UpdateUserIdentityProviderIdResponse] = deriveRelaxedCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val kindSchema: Schema[user_management_service.Right.Kind] = Schema.oneOfWrapped
}
