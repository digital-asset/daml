// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.stream.scaladsl.Source
import EndpointsCompanion._
import Endpoints.ET
import com.daml.http.EndpointsCompanion.CreateFromUserToken.userIdFromToken
import util.FutureUtil.{either, eitherT}
import util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.StandardJWTPayload
import scalaz.std.scalaFuture._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, Monad, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.LoggingContextOf
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.client.services.admin.UserManagementClient
import com.daml.lf.data.Ref.UserId

private[http] final class UserManagement(
    routeSetup: RouteSetup,
    decodeJwt: EndpointsCompanion.ValidateJwt,
    userManagementClient: UserManagementClient,
)(implicit
    ec: ExecutionContext
) {
  import UserManagement._
  import routeSetup._
  import json.JsonProtocol._

  def getUser(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[domain.UserDetails]] =
    proxyWithCommandET { (jwt, getUserRequest: domain.GetUserRequest) =>
      for {
        userId <- parseUserId(getUserRequest.userId)
        user <- EitherT.rightT(userManagementClient.getUser(userId, Some(jwt.value)))
      } yield domain.OkResponse(
        domain.UserDetails(user.id, user.primaryParty)
      ): domain.SyncResponse[domain.UserDetails]
    }(req)

  def getUser2(jwt: Jwt, req: domain.GetUserRequest): ET[domain.SyncResponse[domain.UserDetails]] =
    for {
      userId <- parseUserId(req.userId)
      user <- EitherT.rightT(userManagementClient.getUser(userId, Some(jwt.value)))
    } yield domain.OkResponse(
      domain.UserDetails(user.id, user.primaryParty)
    ): domain.SyncResponse[domain.UserDetails]

  def createUser(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[spray.json.JsObject]] =
    proxyWithCommand { (jwt, createUserRequest: domain.CreateUserRequest) =>
      {
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
      }.run
    }(req)

  def deleteUser(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[spray.json.JsObject]] =
    proxyWithCommandET { (jwt, deleteUserRequest: domain.DeleteUserRequest) =>
      for {
        userId <- parseUserId(deleteUserRequest.userId)
        _ <- EitherT.rightT(userManagementClient.deleteUser(userId, Some(jwt.value)))
      } yield emptyObjectResponse
    }(req)

  def listUserRights(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserRight]]] =
    proxyWithCommandET { (jwt, listUserRightsRequest: domain.ListUserRightsRequest) =>
      for {
        userId <- parseUserId(listUserRightsRequest.userId)
        rights <- EitherT.rightT(
          userManagementClient.listUserRights(userId, Some(jwt.value))
        )
      } yield domain
        .OkResponse(domain.UserRights.fromLedgerUserRights(rights)): domain.SyncResponse[
        List[domain.UserRight]
      ]
    }(req)

  def grantUserRights(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserRight]]] =
    proxyWithCommandET { (jwt, grantUserRightsRequest: domain.GrantUserRightsRequest) =>
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
    }(req)

  def revokeUserRights(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserRight]]] =
    proxyWithCommandET { (jwt, revokeUserRightsRequest: domain.RevokeUserRightsRequest) =>
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
    }(req)

  def getAuthenticatedUser(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[domain.UserDetails]] =
    for {
      jwt <- eitherT(input(req)).bimap(identity[Error], _._1)
      userId <- getUserIdFromToken(jwt)
      user <- EitherT.rightT(userManagementClient.getUser(userId, Some(jwt.value)))
    } yield domain.OkResponse(domain.UserDetails(user.id, user.primaryParty))

  def listAuthenticatedUserRights(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[List[domain.UserRight]]] =
    for {
      jwt <- eitherT(input(req)).bimap(identity[Error], _._1)
      userId <- getUserIdFromToken(jwt)
      rights <- EitherT.rightT(
        userManagementClient.listUserRights(userId, Some(jwt.value))
      )
    } yield domain
      .OkResponse(domain.UserRights.fromLedgerUserRights(rights)): domain.SyncResponse[List[
      domain.UserRight
    ]]

  def listUsers(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[Source[Error \/ domain.UserDetails, NotUsed]]] =
    for {
      jwt <- eitherT(input(req)).bimap(identity[Error], _._1)
      users = aggregateListUserPages(Some(jwt.value))
    } yield domain.OkResponse(users.map(_ map domain.UserDetails.fromUser))

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
