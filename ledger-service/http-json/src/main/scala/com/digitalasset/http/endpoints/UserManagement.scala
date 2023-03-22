// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.NotUsed
import akka.stream.scaladsl.Source
import EndpointsCompanion._
import Endpoints.ET
import com.daml.http.EndpointsCompanion.CreateFromUserToken.userIdFromToken
import util.FutureUtil.either
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.StandardJWTPayload
import scalaz.std.scalaFuture._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, Monad, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.client.services.admin.UserManagementClient
import com.daml.lf.data.Ref.UserId

private[http] final class UserManagement(
    decodeJwt: EndpointsCompanion.ValidateJwt,
    userManagementClient: UserManagementClient,
)(implicit
    ec: ExecutionContext
) {
  import UserManagement._

  def getUser(jwt: Jwt, req: domain.GetUserRequest): ET[domain.SyncResponse[domain.UserDetails]] =
    for {
      userId <- parseUserId(req.userId)
      user <- EitherT.rightT(userManagementClient.getUser(userId, Some(jwt.value)))
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
      _ <- EitherT.rightT(userManagementClient.deleteUser(userId, Some(jwt.value)))
    } yield emptyObjectResponse
  }

  def updateUser(
      jwt: Jwt,
      updateUserRequest: domain.UpdateUserRequest,
  ): ET[domain.SyncResponse[spray.json.JsObject]] = {
    import scalaz.std.option._
    import scalaz.syntax.traverse._
    import scalaz.syntax.std.either._
    import com.daml.lf.data.Ref
    val input =
      for {
        id <- UserId.fromString(updateUserRequest.user.id).disjunction
        primaryParty <- updateUserRequest.user.primaryParty.traverse(it =>
          Ref.Party.fromString(it).disjunction
        )
      } yield (id, primaryParty, updateUserRequest.user)
    for {
      info <- EitherT.either(input.leftMap(InvalidUserInput)): ET[
        (UserId, Option[Ref.Party], User)
      ]
      (id, primaryParty, user) = info
      _ <- EitherT.rightT(
        userManagementClient.updateUser(
          User(id, primaryParty, user.isDeactivated, user.metadata, user.identityProviderId),
          Some(jwt.value),
          Some(updateUserRequest.updateMask),
        )
      )
      // TODO BH: return proper update user object
    } yield emptyObjectResponse
  }

  def listUserRights(
      jwt: Jwt,
      listUserRightsRequest: domain.ListUserRightsRequest,
  ): ET[domain.SyncResponse[List[domain.UserRight]]] = {
    for {
      userId <- parseUserId(listUserRightsRequest.userId)
      rights <- EitherT.rightT(
        userManagementClient.listUserRights(userId, Some(jwt.value))
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
        userManagementClient.grantUserRights(userId, rights, Some(jwt.value))
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
        userManagementClient.revokeUserRights(userId, rights, Some(jwt.value))
      )
    } yield domain.OkResponse(
      domain.UserRights.fromLedgerUserRights(revokedUserRights)
    ): domain.SyncResponse[List[domain.UserRight]]
  }

  def getAuthenticatedUser(jwt: Jwt): ET[domain.SyncResponse[domain.UserDetails]] =
    for {
      userId <- getUserIdFromToken(jwt)
      user <- EitherT.rightT(userManagementClient.getUser(userId, Some(jwt.value)))
    } yield domain.OkResponse(domain.UserDetails(user.id, user.primaryParty))

  def listAuthenticatedUserRights(jwt: Jwt): ET[domain.SyncResponse[List[domain.UserRight]]] = {
    for {
      userId <- getUserIdFromToken(jwt)
      rights <- EitherT.rightT(
        userManagementClient.listUserRights(userId, Some(jwt.value))
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
          .listUsers(token, pageToken, pageSize)
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

private[http] object UserManagement {
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
