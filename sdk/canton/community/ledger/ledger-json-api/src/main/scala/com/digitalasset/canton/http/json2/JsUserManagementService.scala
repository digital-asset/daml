// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json2

import com.daml.ledger.api.v2.admin.user_management_service
import com.digitalasset.daml.lf.data.Ref.UserId
import com.digitalasset.canton.ledger.client.services.admin.UserManagementClient
import com.digitalasset.canton.http.json2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.generic.semiauto.deriveCodec
import io.circe.Codec
import sttp.model.QueryParams
import sttp.tapir.generic.auto.*
import sttp.tapir.*

import scala.concurrent.Future

class JsUserManagementService(
    userManagementClient: UserManagementClient,
    val loggerFactory: NamedLoggerFactory,
) extends Endpoints
    with NamedLogging {
  import JsUserManagementCodecs.*

  private val users = baseEndpoint.in("users")
  private val userIdPath = "user-id"
  def endpoints() = List(
    json(
      users.get
        .in(queryParams)
        .description("List all users."),
      listUsers,
    ), // TODO (i19538) paging
    jsonWithBody(
      users.post
        .description("Create user."),
      createUser,
    ),
    json(
      users.get
        .in(path[String](userIdPath))
        .description("Get user details."),
      getUser,
    ),
    jsonWithBody(
      users.patch
        .in(path[String](userIdPath))
        .description("Update  user."),
      updateUser,
    ),
    json(
      users.delete
        .in(path[String](userIdPath))
        .description("Delete user."),
      deleteUser,
    ),
    jsonWithBody(
      users.post
        .in(path[String](userIdPath))
        .in("rights")
        .description("Grant user rights."),
      grantUserRights,
    ),
    jsonWithBody(
      users.patch
        .in(path[String](userIdPath))
        .in("rights")
        .description("Revoke user rights."),
      revokeUserRights,
    ),
    json(
      users.get
        .in(path[String](userIdPath))
        .in("rights")
        .description("List user rights."),
      listUserRights,
    ),
    jsonWithBody(
      users.patch
        .in(path[String](userIdPath))
        .in("identity-provider-id")
        .description("Update user identity provider."),
      updateUserIdentityProvider,
    ),
  )
  private def createUser(
      callerContext: CallerContext
  ): (TracedInput[Unit], user_management_service.CreateUserRequest) => Future[
    Either[JsCantonError, user_management_service.CreateUserResponse]
  ] = (req, body) =>
    userManagementClient
      .serviceStub(callerContext.token())(req.traceContext)
      .createUser(body)
      .toRight

  private def listUsers(
      callerContext: CallerContext
  ): TracedInput[QueryParams] => Future[
    Either[JsCantonError, user_management_service.ListUsersResponse]
  ] = req =>
    userManagementClient
      .serviceStub(callerContext.token())(req.traceContext)
      .listUsers(user_management_service.ListUsersRequest())
      .toRight
  // TODO (i19538) paging

  private def getUser(
      callerContext: CallerContext
  ): TracedInput[String] => Future[Either[JsCantonError, user_management_service.GetUserResponse]] =
    req =>
      UserId.fromString(req.in) match {
        case Right(userId) =>
          userManagementClient
            .serviceStub(callerContext.token())(req.traceContext)
            .getUser(user_management_service.GetUserRequest(userId = userId))
            .toRight
        case Left(error) => malformedUserId(error)(req.traceContext)

      }

  private def updateUser(
      callerContext: CallerContext
  ): (TracedInput[String], user_management_service.UpdateUserRequest) => Future[
    Either[JsCantonError, user_management_service.UpdateUserResponse]
  ] = (req, body) =>
    if (body.user.map(_.id) == Some(req.in)) {
      userManagementClient
        .serviceStub(callerContext.token())(req.traceContext)
        .updateUser(body)
        .toRight
    } else {
      unmatchedUserId(req.traceContext, req.in, body.user.map(_.id))
    }

  private def deleteUser(
      callerContext: CallerContext
  ): TracedInput[String] => Future[Either[JsCantonError, Unit]] = req =>
    UserId.fromString(req.in) match {
      case Right(userId) =>
        userManagementClient
          .deleteUser(userId, callerContext.jwt.map(_.token))(req.traceContext)
          .toRight
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
          .listUserRights(new user_management_service.ListUserRightsRequest(userId = userId))
          .toRight
      case Left(error) => malformedUserId(error)(req.traceContext)
    }

  private def grantUserRights(
      callerContext: CallerContext
  ): (TracedInput[String], user_management_service.GrantUserRightsRequest) => Future[
    Either[JsCantonError, user_management_service.GrantUserRightsResponse]
  ] =
    (req, body) =>
      if (body.userId == req.in) {
        userManagementClient
          .serviceStub(callerContext.token())(req.traceContext)
          .grantUserRights(body)
          .toRight
      } else {
        unmatchedUserId(req.traceContext, req.in, Some(body.userId))
      }

  private def revokeUserRights(
      callerContext: CallerContext
  ): (TracedInput[String], user_management_service.RevokeUserRightsRequest) => Future[
    Either[JsCantonError, user_management_service.RevokeUserRightsResponse]
  ] =
    (req, body) =>
      if (body.userId == req.in) {
        userManagementClient
          .serviceStub(callerContext.token())(req.traceContext)
          .revokeUserRights(body)
          .toRight
      } else {
        unmatchedUserId(req.traceContext, req.in, Some(body.userId))
      }

  private def updateUserIdentityProvider(
      callerContext: CallerContext
  ): (TracedInput[String], user_management_service.UpdateUserIdentityProviderIdRequest) => Future[
    Either[JsCantonError, user_management_service.UpdateUserIdentityProviderIdResponse]
  ] = (req, body) =>
    if (body.userId == req.in) {
      userManagementClient
        .serviceStub(callerContext.token())(req.traceContext)
        .updateUserIdentityProviderId(body)
        .toRight
    } else {
      unmatchedUserId(req.traceContext, req.in, Some(body.userId))
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

object JsUserManagementCodecs {

  implicit val user: Codec[user_management_service.User] = deriveCodec
  implicit val participantAdmin: Codec[user_management_service.Right.ParticipantAdmin] =
    deriveCodec
  implicit val canActAs: Codec[user_management_service.Right.CanActAs] = deriveCodec
  implicit val canReadAs: Codec[user_management_service.Right.CanReadAs] = deriveCodec
  implicit val identityProviderAdmin: Codec[user_management_service.Right.IdentityProviderAdmin] =
    deriveCodec
  implicit val canReadAsAnyParty: Codec[user_management_service.Right.CanReadAsAnyParty] =
    deriveCodec
  implicit val king: Codec[user_management_service.Right.Kind] = deriveCodec
  implicit val right: Codec[user_management_service.Right] = deriveCodec
  implicit val createUserRequest: Codec[user_management_service.CreateUserRequest] = deriveCodec
  implicit val updateUserRequest: Codec[user_management_service.UpdateUserRequest] = deriveCodec
  implicit val listUserResponse: Codec[user_management_service.ListUsersResponse] = deriveCodec
  implicit val createUserResponse: Codec[user_management_service.CreateUserResponse] = deriveCodec
  implicit val updateUserResponse: Codec[user_management_service.UpdateUserResponse] = deriveCodec
  implicit val getUserResponse: Codec[user_management_service.GetUserResponse] = deriveCodec
  implicit val grantUserRightsRequest: Codec[user_management_service.GrantUserRightsRequest] =
    deriveCodec
  implicit val grantUserRightsResponse: Codec[user_management_service.GrantUserRightsResponse] =
    deriveCodec
  implicit val revokeUserRightsRequest: Codec[user_management_service.RevokeUserRightsRequest] =
    deriveCodec
  implicit val revokeUserRightsResponse: Codec[user_management_service.RevokeUserRightsResponse] =
    deriveCodec

  implicit val listUserRightsRequest: Codec[user_management_service.ListUserRightsRequest] =
    deriveCodec
  implicit val listUserRightsResponse: Codec[user_management_service.ListUserRightsResponse] =
    deriveCodec

  implicit val updateIdentityProviderRequest
      : Codec[user_management_service.UpdateUserIdentityProviderIdRequest] = deriveCodec
  implicit val updateIdentityProviderResponse
      : Codec[user_management_service.UpdateUserIdentityProviderIdResponse] = deriveCodec

}
