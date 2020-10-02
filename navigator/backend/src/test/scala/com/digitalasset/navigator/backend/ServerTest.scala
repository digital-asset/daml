// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.{Cookie, `Set-Cookie`}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.daml.api.util.TimeProvider.UTC
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.SessionJsonProtocol._
import com.daml.navigator.config.{Arguments, Config, UserConfig}
import com.daml.navigator.model.PartyState
import com.daml.navigator.store.Store.{
  ApplicationStateConnected,
  ApplicationStateConnecting,
  ApplicationStateFailed,
  ApplicationStateInfo
}
import com.daml.navigator.time.TimeProviderType.Static
import com.daml.navigator.time.TimeProviderWithType
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers, OptionValues}
import scalaz.syntax.tag._
import spray.json._

import scala.concurrent.Future
import scala.util.Success

class ServerTest
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest
    with LazyLogging
    with OptionValues {

  val userId = "userId"
  val role = "role"
  val party = ApiTypes.Party("party")
  val password = "password"
  val user = User(userId, new PartyState(party, false), Some(role), true)
  val userConfig = UserConfig(Some(password), new PartyState(party, false), Some(role))

  val userJson = JsObject(
    "id" -> JsString(userId),
    "role" -> JsString(role),
    "party" -> JsString(party.unwrap),
    "canAdvanceTime" -> JsBoolean(true))

  val sessionJson = JsObject(
    "type" -> JsString("session"),
    "user" -> userJson
  )

  case object TestInfoHandler extends InfoHandler {
    override def getInfo: Future[JsValue] = Future.successful(JsString("test"))
  }

  private def route(state: ApplicationStateInfo): Route =
    NavigatorBackend.getRoute(
      system = ActorSystem("da-ui-backend-test"),
      arguments = Arguments.default,
      config = new Config(users = Map(userId -> userConfig)),
      graphQL = DefaultGraphQLHandler(Set.empty, None),
      info = TestInfoHandler,
      getAppState = () => Future.successful(state)
    )

  private[this] val connected =
    route(
      ApplicationStateConnected(
        "localhost",
        6865,
        true,
        "n/a",
        "0",
        TimeProviderWithType(UTC, Static),
        List.empty))

  private[this] val unauthorized =
    route(
      ApplicationStateFailed(
        "localhost",
        6865,
        true,
        "n/a",
        io.grpc.Status.PERMISSION_DENIED.asException
      ))

  private[this] val failed =
    route(
      ApplicationStateFailed(
        "localhost",
        6865,
        true,
        "n/a",
        io.grpc.Status.INVALID_ARGUMENT.asException
      ))

  private[this] val connecting =
    route(
      ApplicationStateConnecting(
        "localhost",
        6865,
        true,
        "n/a"
      ))

  def sessionCookie(): String = {
    val cookies = headers.collect { case `Set-Cookie`(x) if x.name == "session-id" => x }
    cookies should have size 1
    cookies.head.value
  }

  def withCleanSessions[T](f: => T): T = {
    Session.clean()
    f
  }

  "SelectMode GET /api/session/" should "respond SignIn with method SignInSelect with the available users" in withCleanSessions {
    Get("/api/session/") ~> connected ~> check {
      responseAs[SignIn] shouldEqual SignIn(method = SignInSelect(userIds = Set(userId)))
    }
  }

  it should "respond with the Session when already signed-in" in withCleanSessions {
    val sessionId = "session-id-value"
    Session.open(sessionId, userId, userConfig)
    Get("/api/session/") ~> Cookie("session-id" -> sessionId) ~> connected ~> check {
      Unmarshal(response.entity).to[String].value.map(_.map(_.parseJson)) shouldEqual Some(
        Success((sessionJson)))
    }
  }

  "SelectMode POST /api/session/" should "allow to SignIn with an existing user" in withCleanSessions {
    Post("/api/session/", LoginRequest(userId, None)) ~> connected ~> check {
      Unmarshal(response.entity).to[String].value.map(_.map(_.parseJson)) shouldEqual Some(
        Success((sessionJson)))
      val sessionId = sessionCookie()
      Session.current(sessionId).value shouldEqual Session(user)
    }
  }

  it should "forbid to SignIn with a non existing user" in withCleanSessions {
    Post("/api/session/", LoginRequest(userId + " ", None)) ~> connected ~> check {
      responseAs[SignIn] shouldEqual SignIn(
        method = SignInSelect(userIds = Set(userId)),
        Some(InvalidCredentials))
    }
  }

  it should "forbid to SignIn with when unauthorized and report the error" in withCleanSessions {
    Post("/api/session/", LoginRequest(userId, None)) ~> unauthorized ~> check {
      responseAs[SignIn] shouldEqual SignIn(
        method = SignInSelect(userIds = Set(userId)),
        Some(InvalidCredentials))
    }
  }

  it should "forbid to SignIn with when it's impossible to connect to the ledger" in withCleanSessions {
    Post("/api/session/", LoginRequest(userId, None)) ~> failed ~> check {
      responseAs[SignIn] shouldEqual SignIn(
        method = SignInSelect(userIds = Set(userId)),
        Some(Unknown))
    }
  }

  it should "forbid to SignIn with when still connecting to a ledger" in withCleanSessions {
    Post("/api/session/", LoginRequest(userId, None)) ~> connecting ~> check {
      responseAs[SignIn] shouldEqual SignIn(
        method = SignInSelect(userIds = Set(userId)),
        Some(NotConnected))
    }
  }

  "SelectMode DELETE /api/session/" should "delete a given Session when signed-in" in withCleanSessions {
    val sessionId = "session-id-value-2"
    Session.open(sessionId, userId, userConfig)
    Delete("/api/session/") ~> Cookie("session-id", sessionId) ~> connected ~> check {
      Session.current(sessionId) shouldBe None
    }
  }
}
