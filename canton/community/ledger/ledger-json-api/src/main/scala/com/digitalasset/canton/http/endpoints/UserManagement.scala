// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.digitalasset.canton.http.EndpointsCompanion.*
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.EndpointsCompanion.CreateFromUserToken.userIdFromToken
import com.digitalasset.canton.http.util.FutureUtil.either
import com.daml.jwt.domain.Jwt
import com.digitalasset.canton.ledger.api.auth.StandardJWTPayload
import scalaz.std.scalaFuture.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, Monad, \/, \/-}
import com.digitalasset.canton.http.{EndpointsCompanion, domain}
import com.digitalasset.canton.ledger.api.domain.{User, UserRight}
import com.digitalasset.canton.ledger.client.services.admin.UserManagementClient
import com.daml.lf.data.Ref.UserId

import scala.concurrent.{ExecutionContext, Future}

 final class UserManagement(
    decodeJwt: EndpointsCompanion.ValidateJwt,
    userManagementClient: UserManagementClient,
)(implicit
    ec: ExecutionContext
) {
  import UserManagement._

  def getUser(jwt: Jwt, req: domain.GetUserRequest): ET[domain.SyncResponse[domain.UserDetails]] =
    for {
      userId <- parseUserId(req.userId)
      user <- EitherT.rightT(userManagementClient.getUser(userId = userId, token = Some(jwt.value)))
    } yield domain.OkResponse(
      domain.UserDetails(user.id, user.primaryParty)
    ): domain.SyncResponse[domain.UserDetails]

  def createUser(
      jwt: Jwt,
      createUserRequest: domain.CreateUserRequest,
  ): ET[domain.SyncResponse[spray.json.JsObject]] = {
    import scalaz.std.option._
    import scalaz.syntax.traverse._
    import scalaz.syntax.std.either._
    import com.daml.lf.data.Ref
    val input =
      for {
        username <- UserId.fromString(createUserRequest.userId).disjunction
        primaryParty <- createUserRequest.primaryParty.traverse(it =>
          Ref.Party.fromString(it).disjunction
        )
        rights <- domain.UserRights.toLedgerUserRights(
          createUserRequest.rights.getOrElse(List.empty)
        )
      } yield (username, primaryParty, rights)
    for {
      info <- EitherT.either(input.leftMap(InvalidUserInput)): ET[
        (UserId, Option[Ref.Party], List[UserRight])
      ]
      (username, primaryParty, initialRights) = info
      _ <- EitherT.rightT(
        userManagementClient.createUser(
          User(username, primaryParty),
          initialRights,
          Some(jwt.value),
        )
      )
    } yield emptyObjectResponse
  }

  def deleteUser(
      jwt: Jwt,
      deleteUserRequest: domain.DeleteUserRequest,
  ): ET[domain.SyncResponse[spray.json.JsObject]] = {
    for {
      userId <- parseUserId(deleteUserRequest.userId)
      _ <- EitherT.rightT(userManagementClient.deleteUser(userId = userId, token = Some(jwt.value)))
    } yield emptyObjectResponse
  }

  def listUserRights(
      jwt: Jwt,
      listUserRightsRequest: domain.ListUserRightsRequest,
  ): ET[domain.SyncResponse[List[domain.UserRight]]] = {
    for {
      userId <- parseUserId(listUserRightsRequest.userId)
      rights <- EitherT.rightT(
        userManagementClient.listUserRights(userId = userId, token = Some(jwt.value))
      )
    } yield domain
      .OkResponse(domain.UserRights.fromLedgerUserRights(rights)): domain.SyncResponse[List[
      domain.UserRight
    ]]
  }

  def grantUserRights(
      jwt: Jwt,
      grantUserRightsRequest: domain.GrantUserRightsRequest,
  ): ET[domain.SyncResponse[List[domain.UserRight]]] = {
    for {
      userId <- parseUserId(grantUserRightsRequest.userId)
      rights <- either(
        domain.UserRights.toLedgerUserRights(grantUserRightsRequest.rights)
      ).leftMap(InvalidUserInput): ET[List[UserRight]]
      grantedUserRights <- EitherT.rightT(
        userManagementClient.grantUserRights(
          userId = userId,
          rights = rights,
          token = Some(jwt.value),
        )
      )
    } yield domain.OkResponse(
      domain.UserRights.fromLedgerUserRights(grantedUserRights)
    ): domain.SyncResponse[List[domain.UserRight]]
  }

  def revokeUserRights(
      jwt: Jwt,
      revokeUserRightsRequest: domain.RevokeUserRightsRequest,
  ): ET[domain.SyncResponse[List[domain.UserRight]]] = {
    for {
      userId <- parseUserId(revokeUserRightsRequest.userId)
      rights <- either(
        domain.UserRights.toLedgerUserRights(revokeUserRightsRequest.rights)
      ).leftMap(InvalidUserInput): ET[List[UserRight]]
      revokedUserRights <- EitherT.rightT(
        userManagementClient.revokeUserRights(
          userId = userId,
          rights = rights,
          token = Some(jwt.value),
        )
      )
    } yield domain.OkResponse(
      domain.UserRights.fromLedgerUserRights(revokedUserRights)
    ): domain.SyncResponse[List[domain.UserRight]]
  }

  def getAuthenticatedUser(jwt: Jwt): ET[domain.SyncResponse[domain.UserDetails]] =
    for {
      userId <- getUserIdFromToken(jwt)
      user <- EitherT.rightT(userManagementClient.getUser(userId = userId, token = Some(jwt.value)))
    } yield domain.OkResponse(domain.UserDetails(user.id, user.primaryParty))

  def listAuthenticatedUserRights(jwt: Jwt): ET[domain.SyncResponse[List[domain.UserRight]]] = {
    for {
      userId <- getUserIdFromToken(jwt)
      rights <- EitherT.rightT(
        userManagementClient.listUserRights(userId = userId, token = Some(jwt.value))
      )
    } yield domain
      .OkResponse(domain.UserRights.fromLedgerUserRights(rights)): domain.SyncResponse[List[
      domain.UserRight
    ]]
  }

  def listUsers(
      jwt: Jwt
  ): ET[domain.SyncResponse[Source[Error \/ domain.UserDetails, NotUsed]]] = {
    val users = aggregateListUserPages(Some(jwt.value))
    val userDetails = users.map(_ map domain.UserDetails.fromUser)
    EitherT.rightT(Future.successful(domain.OkResponse(userDetails)))
  }

  private def aggregateListUserPages(
      token: Option[String],
      pageSize: Int = 1000, // TODO could be made configurable in the future
  ): Source[Error \/ User, NotUsed] = {
    import scalaz.std.option._
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
    import scalaz.syntax.std.either._
    either(
      UserId.fromString(rawUserId).disjunction.leftMap(InvalidUserInput)
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

  private val emptyObjectResponse: domain.SyncResponse[spray.json.JsObject] =
    domain.OkResponse(spray.json.JsObject())
}
