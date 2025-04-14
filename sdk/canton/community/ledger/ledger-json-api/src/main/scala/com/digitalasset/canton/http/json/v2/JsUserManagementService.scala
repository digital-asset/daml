// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.user_management_service
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
) extends Endpoints
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
  ): TracedInput[String] => Future[Either[JsCantonError, user_management_service.GetUserResponse]] =
    req =>
      UserId.fromString(req.in) match {
        case Right(userId) =>
          userManagementClient
            .serviceStub(callerContext.token())(req.traceContext)
            .getUser(
              user_management_service.GetUserRequest(
                userId = userId,
                identityProviderId = "",
              )
            )
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
  import JsSchema.config
  import io.circe.generic.extras.auto.*

  implicit val user: Codec[user_management_service.User] = deriveRelaxedCodec
  implicit val participantAdmin: Codec[user_management_service.Right.ParticipantAdmin] =
    deriveRelaxedCodec
  implicit val canActAs: Codec[user_management_service.Right.CanActAs] = deriveRelaxedCodec
  implicit val canReadAs: Codec[user_management_service.Right.CanReadAs] = deriveRelaxedCodec
  implicit val identityProviderAdmin: Codec[user_management_service.Right.IdentityProviderAdmin] =
    deriveRelaxedCodec
  implicit val canReadAsAnyParty: Codec[user_management_service.Right.CanReadAsAnyParty] =
    deriveRelaxedCodec
  implicit val kind: Codec[user_management_service.Right.Kind] = deriveConfiguredCodec // ADT

  implicit val right: Codec[user_management_service.Right] = deriveRelaxedCodec
  implicit val createUserRequest: Codec[user_management_service.CreateUserRequest] =
    deriveRelaxedCodec
  implicit val updateUserRequest: Codec[user_management_service.UpdateUserRequest] =
    deriveRelaxedCodec
  implicit val listUserResponse: Codec[user_management_service.ListUsersResponse] =
    deriveRelaxedCodec
  implicit val createUserResponse: Codec[user_management_service.CreateUserResponse] =
    deriveRelaxedCodec
  implicit val updateUserResponse: Codec[user_management_service.UpdateUserResponse] =
    deriveRelaxedCodec
  implicit val getUserResponse: Codec[user_management_service.GetUserResponse] = deriveRelaxedCodec
  implicit val grantUserRightsRequest: Codec[user_management_service.GrantUserRightsRequest] =
    deriveRelaxedCodec
  implicit val grantUserRightsResponse: Codec[user_management_service.GrantUserRightsResponse] =
    deriveRelaxedCodec
  implicit val revokeUserRightsRequest: Codec[user_management_service.RevokeUserRightsRequest] =
    deriveRelaxedCodec
  implicit val revokeUserRightsResponse: Codec[user_management_service.RevokeUserRightsResponse] =
    deriveRelaxedCodec

  implicit val listUserRightsRequest: Codec[user_management_service.ListUserRightsRequest] =
    deriveRelaxedCodec
  implicit val listUserRightsResponse: Codec[user_management_service.ListUserRightsResponse] =
    deriveRelaxedCodec

  implicit val updateIdentityProviderRequest
      : Codec[user_management_service.UpdateUserIdentityProviderIdRequest] = deriveRelaxedCodec
  implicit val updateIdentityProviderResponse
      : Codec[user_management_service.UpdateUserIdentityProviderIdResponse] = deriveRelaxedCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val kindSchema: Schema[user_management_service.Right.Kind] = Schema.oneOfWrapped
}
