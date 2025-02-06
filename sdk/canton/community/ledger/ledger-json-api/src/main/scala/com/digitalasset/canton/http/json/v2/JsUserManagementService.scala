// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.user_management_service
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.client.services.admin.UserManagementClient
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.UserId
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.model.QueryParams
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody

import scala.concurrent.Future

class JsUserManagementService(
    userManagementClient: UserManagementClient,
    val loggerFactory: NamedLoggerFactory,
) extends Endpoints
    with NamedLogging {
  import JsUserManagementService.*

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  def endpoints() = List(
    withServerLogic(
      JsUserManagementService.listUsersEndpoint,
      listUsers,
    ), // TODO (i19538) paging
    withServerLogic(
      JsUserManagementService.createUserEndpoint,
      createUser,
    ),
    withServerLogic(
      JsUserManagementService.getUserEndpoint,
      getUser,
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
  ): TracedInput[QueryParams] => Future[
    Either[JsCantonError, user_management_service.ListUsersResponse]
  ] = req =>
    userManagementClient
      .serviceStub(callerContext.token())(req.traceContext)
      .listUsers(user_management_service.ListUsersRequest())
      .resultToRight
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
            .resultToRight
        case Left(error) => malformedUserId(error)(req.traceContext)

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
          .deleteUser(userId, callerContext.jwt.map(_.token))(req.traceContext)
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
          .listUserRights(new user_management_service.ListUserRightsRequest(userId = userId))
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
  private val userIdPath = "user-id"
  val listUsersEndpoint =
    users.get
      .in(queryParams)
      .out(jsonBody[user_management_service.ListUsersResponse])
      .description("List all users.")

  val createUserEndpoint =
    users.post
      .in(jsonBody[user_management_service.CreateUserRequest])
      .out(jsonBody[user_management_service.CreateUserResponse])
      .description("Create user.")

  val getUserEndpoint =
    users.get
      .in(path[String](userIdPath))
      .out(jsonBody[user_management_service.GetUserResponse])
      .description("Get user details.")

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
    updateUserEndpoint,
    deleteUserEndpoint,
    grantUserRightsEndpoint,
    revokeUserRightsEndpoint,
    listUserRightsEndpoint,
    updateUserIdentityProviderEndpoint,
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
  implicit val kind: Codec[user_management_service.Right.Kind] = deriveCodec

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

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val kindSchema: Schema[user_management_service.Right.Kind] = Schema.oneOfWrapped
}
