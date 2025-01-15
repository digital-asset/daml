// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import com.daml.jwt.{Jwt, StandardJWTPayload}
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.EndpointsCompanion.*
import com.digitalasset.canton.http.EndpointsCompanion.CreateFromUserToken.userIdFromToken
import com.digitalasset.canton.http.util.FutureUtil.either
import com.digitalasset.canton.http.{
  CreateUserRequest,
  DeleteUserRequest,
  EndpointsCompanion,
  GetUserRequest,
  GrantUserRightsRequest,
  ListUserRightsRequest,
  OkResponse,
  RevokeUserRightsRequest,
  SyncResponse,
  UserDetails,
  UserRight,
  UserRights,
}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.client.services.admin.UserManagementClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.UserId
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import scalaz.std.scalaFuture.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, Monad, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}

final class UserManagement(
    decodeJwt: EndpointsCompanion.ValidateJwt,
    userManagementClient: UserManagementClient,
)(implicit
    ec: ExecutionContext
) {
  import UserManagement.*

  def getUser(jwt: Jwt, req: GetUserRequest)(implicit
      traceContext: TraceContext
  ): ET[SyncResponse[UserDetails]] =
    for {
      userId <- parseUserId(req.userId)
      user <- EitherT.rightT(userManagementClient.getUser(userId = userId, token = Some(jwt.value)))
    } yield OkResponse(
      UserDetails(user.id, user.primaryParty)
    ): SyncResponse[UserDetails]

  def createUser(
      jwt: Jwt,
      createUserRequest: CreateUserRequest,
  )(implicit traceContext: TraceContext): ET[SyncResponse[spray.json.JsObject]] = {
    import scalaz.std.option.*
    import scalaz.syntax.traverse.*
    import scalaz.syntax.std.either.*
    import com.digitalasset.daml.lf.data.Ref
    val input =
      for {
        username <- UserId.fromString(createUserRequest.userId).disjunction
        primaryParty <- createUserRequest.primaryParty.traverse(it =>
          Ref.Party.fromString(it).disjunction
        )
        rights <- UserRights.toLedgerUserRights(
          createUserRequest.rights.getOrElse(List.empty)
        )
      } yield (username, primaryParty, rights)
    for {
      info <- EitherT.either(input.leftMap(InvalidUserInput.apply)): ET[
        (UserId, Option[Ref.Party], List[domain.UserRight])
      ]
      (username, primaryParty, initialRights) = info
      _ <- EitherT.rightT(
        userManagementClient.createUser(
          domain.User(username, primaryParty),
          initialRights,
          Some(jwt.value),
        )
      )
    } yield emptyObjectResponse
  }

  def deleteUser(
      jwt: Jwt,
      deleteUserRequest: DeleteUserRequest,
  )(implicit traceContext: TraceContext): ET[SyncResponse[spray.json.JsObject]] =
    for {
      userId <- parseUserId(deleteUserRequest.userId)
      _ <- EitherT.rightT(userManagementClient.deleteUser(userId = userId, token = Some(jwt.value)))
    } yield emptyObjectResponse

  def listUserRights(
      jwt: Jwt,
      listUserRightsRequest: ListUserRightsRequest,
  )(implicit traceContext: TraceContext): ET[SyncResponse[List[UserRight]]] =
    for {
      userId <- parseUserId(listUserRightsRequest.userId)
      rights <- EitherT.rightT(
        userManagementClient.listUserRights(userId = userId, token = Some(jwt.value))
      )
    } yield OkResponse(UserRights.fromLedgerUserRights(rights)): SyncResponse[List[
      UserRight
    ]]

  def grantUserRights(
      jwt: Jwt,
      grantUserRightsRequest: GrantUserRightsRequest,
  )(implicit traceContext: TraceContext): ET[SyncResponse[List[UserRight]]] =
    for {
      userId <- parseUserId(grantUserRightsRequest.userId)
      rights <- either(
        UserRights.toLedgerUserRights(grantUserRightsRequest.rights)
      ).leftMap(InvalidUserInput.apply): ET[List[domain.UserRight]]
      grantedUserRights <- EitherT.rightT(
        userManagementClient.grantUserRights(
          userId = userId,
          rights = rights,
          token = Some(jwt.value),
        )
      )
    } yield OkResponse(
      UserRights.fromLedgerUserRights(grantedUserRights)
    ): SyncResponse[List[UserRight]]

  def revokeUserRights(
      jwt: Jwt,
      revokeUserRightsRequest: RevokeUserRightsRequest,
  )(implicit traceContext: TraceContext): ET[SyncResponse[List[UserRight]]] =
    for {
      userId <- parseUserId(revokeUserRightsRequest.userId)
      rights <- either(
        UserRights.toLedgerUserRights(revokeUserRightsRequest.rights)
      ).leftMap(InvalidUserInput.apply): ET[List[domain.UserRight]]
      revokedUserRights <- EitherT.rightT(
        userManagementClient.revokeUserRights(
          userId = userId,
          rights = rights,
          token = Some(jwt.value),
        )
      )
    } yield OkResponse(
      UserRights.fromLedgerUserRights(revokedUserRights)
    ): SyncResponse[List[UserRight]]

  def getAuthenticatedUser(
      jwt: Jwt
  )(implicit traceContext: TraceContext): ET[SyncResponse[UserDetails]] =
    for {
      userId <- getUserIdFromToken(jwt)
      user <- EitherT.rightT(userManagementClient.getUser(userId = userId, token = Some(jwt.value)))
    } yield OkResponse(UserDetails(user.id, user.primaryParty))

  def listAuthenticatedUserRights(
      jwt: Jwt
  )(implicit traceContext: TraceContext): ET[SyncResponse[List[UserRight]]] =
    for {
      userId <- getUserIdFromToken(jwt)
      rights <- EitherT.rightT(
        userManagementClient.listUserRights(userId = userId, token = Some(jwt.value))
      )
    } yield OkResponse(UserRights.fromLedgerUserRights(rights)): SyncResponse[List[
      UserRight
    ]]

  def listUsers(
      jwt: Jwt
  )(implicit
      traceContext: TraceContext
  ): ET[SyncResponse[Source[Error \/ UserDetails, NotUsed]]] = {
    val users = aggregateListUserPages(Some(jwt.value))
    val userDetails = users.map(_ map UserDetails.fromUser)
    EitherT.rightT(Future.successful(OkResponse(userDetails)))
  }

  private def aggregateListUserPages(
      token: Option[String],
      pageSize: Int = 1000, // TODO could be made configurable in the future
  )(implicit traceContext: TraceContext): Source[Error \/ domain.User, NotUsed] = {
    import scalaz.std.option.*
    Source.unfoldAsync(some("")) {
      _ traverse { pageToken =>
        userManagementClient
          .listUsers(token = token, pageToken = pageToken, pageSize = pageSize)
          .map {
            case (users, "") => (None, \/-(users))
            case (users, pageToken) => (Some(pageToken), \/-(users))
          }
          // if a listUsers call fails, stop the stream and emit the error as a "warning"
          .recover(Error.fromThrowable andThen (e => (None, -\/(e))))
      }
    } mapConcat {
      case e @ -\/(_) => Seq(e)
      case \/-(users) => users.view.map(\/-(_))
    }
  }

  private def getUserIdFromToken(jwt: Jwt): ET[UserId] =
    decodeAndParseUserIdFromToken(jwt, decodeJwt).leftMap(identity[Error])
}

object UserManagement {
  private def parseUserId(rawUserId: String)(implicit
      ec: ExecutionContext
  ): ET[UserId] = {
    import scalaz.syntax.std.either.*
    either(
      UserId.fromString(rawUserId).disjunction.leftMap(InvalidUserInput.apply)
    )
  }

  private def decodeAndParseUserIdFromToken(rawJwt: Jwt, decodeJwt: ValidateJwt)(implicit
      mf: Monad[Future]
  ): ET[UserId] =
    EitherT.either(decodeAndParseJwt(rawJwt, decodeJwt).flatMap {
      case token: StandardJWTPayload => userIdFromToken(token)
      case _ =>
        -\/(Unauthorized("A user token was expected but a custom token was given"): Error)
    })

  private val emptyObjectResponse: SyncResponse[spray.json.JsObject] =
    OkResponse(spray.json.JsObject())
}
