// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.{Cookie, `Set-Cookie`}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.digitalasset.api.util.TimeProvider.UTC
import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.navigator.SessionJsonProtocol._
import com.digitalasset.navigator.config.{Arguments, Config, UserConfig}
import com.digitalasset.navigator.model.PartyState
import com.digitalasset.navigator.store.Store.ApplicationStateConnected
import com.digitalasset.navigator.time.TimeProviderType.Static
import com.digitalasset.navigator.time.TimeProviderWithType
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

  private[this] val route: Route =
    NavigatorBackend.getRoute(
      system = ActorSystem("da-ui-backend-test"),
      arguments = Arguments.default,
      config = new Config(users = Map(userId -> userConfig)),
      graphQL = DefaultGraphQLHandler(Set.empty, None),
      info = TestInfoHandler,
      getAppState = () =>
        Future.successful(
          ApplicationStateConnected(
            "localhost",
            6865,
            true,
            "n/a",
            "0",
            TimeProviderWithType(UTC, Static),
            List.empty))
    )

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
    Get("/api/session/") ~> route ~> check {
      responseAs[SignIn] shouldEqual SignIn(method = SignInSelect(userIds = Set(userId)))
    }
  }

  it should "respond with the Session when already signed-in" in withCleanSessions {
    val sessionId = "session-id-value"
    Session.open(sessionId, userId, userConfig)
    Get("/api/session/") ~> Cookie("session-id" -> sessionId) ~> route ~> check {
      Unmarshal(response.entity).to[String].value.map(_.map(_.parseJson)) shouldEqual Some(
        Success((sessionJson)))
    }
  }

  "SelectMode POST /api/session/" should "allow to SignIn with an existing user" in withCleanSessions {
    Post("/api/session/", LoginRequest(userId, None)) ~> route ~> check {
      Unmarshal(response.entity).to[String].value.map(_.map(_.parseJson)) shouldEqual Some(
        Success((sessionJson)))
      val sessionId = sessionCookie()
      Session.current(sessionId).value shouldEqual Session(user)
    }
  }

  it should "forbid to SignIn with a non existing user" in withCleanSessions {
    Post("/api/session/", LoginRequest(userId + " ", None)) ~> route ~> check {
      responseAs[SignIn] shouldEqual SignIn(
        method = SignInSelect(userIds = Set(userId)),
        Some(InvalidCredentials))
    }
  }

  "SelectMode DELETE /api/session/" should "delete a given Session when signed-in" in withCleanSessions {
    val sessionId = "session-id-value-2"
    Session.open(sessionId, userId, userConfig)
    Delete("/api/session/") ~> Cookie("session-id", sessionId) ~> route ~> check {
      Session.current(sessionId) shouldBe None
    }
  }
}
