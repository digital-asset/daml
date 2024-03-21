// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.api

import org.apache.pekko.http.scaladsl.marshalling.Marshaller
import org.apache.pekko.http.scaladsl.model.headers.HttpChallenge
import org.apache.pekko.http.scaladsl.model.{HttpHeader, Uri, headers}
import org.apache.pekko.http.scaladsl.server.Directive1
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import scalaz.{@@, Tag}
import spray.json._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.util.Try
import scala.language.postfixOps

object Tagged {
  sealed trait AccessTokenTag
  type AccessToken = String @@ AccessTokenTag
  val AccessToken = Tag.of[AccessTokenTag]

  sealed trait RefreshTokenTag
  type RefreshToken = String @@ RefreshTokenTag
  val RefreshToken = Tag.of[RefreshTokenTag]
}

object Request {
  import Tagged._

  // applicationId = None makes no guarantees about the application ID. You can use this
  // if you don’t use the token for requests that use the application ID.
  // applicationId = Some(appId) will return a token that is valid for
  // appId, i.e., either a wildcard token or a token with applicationId set to appId.
  case class Claims(
      admin: Boolean,
      actAs: List[Party],
      readAs: List[Party],
      applicationId: Option[ApplicationId],
  ) {
    def toQueryString() = {
      val adminS = if (admin) LazyList("admin") else LazyList()
      val actAsS = actAs.to(LazyList).map(party => s"actAs:$party")
      val readAsS = readAs.to(LazyList).map(party => s"readAs:$party")
      val applicationIdS = applicationId.toList.to(LazyList).map(appId => s"applicationId:$appId")
      (adminS ++ actAsS ++ readAsS ++ applicationIdS).mkString(" ")
    }
  }
  object Claims {
    def apply(
        admin: Boolean = false,
        actAs: List[Party] = List(),
        readAs: List[Party] = List(),
        applicationId: Option[ApplicationId] = None,
    ): Claims =
      new Claims(admin, actAs, readAs, applicationId)
    def apply(s: String): Claims = {
      var admin = false
      val actAs = ArrayBuffer[Party]()
      val readAs = ArrayBuffer[Party]()
      var applicationId: Option[ApplicationId] = None
      s.split(' ').foreach { w =>
        if (w == "admin") {
          admin = true
        } else if (w.startsWith("actAs:")) {
          actAs.append(Party(w.stripPrefix("actAs:")))
        } else if (w.startsWith("readAs:")) {
          readAs.append(Party(w.stripPrefix("readAs:")))
        } else if (w.startsWith("applicationId:")) {
          applicationId match {
            case None =>
              applicationId = Some(ApplicationId(w.stripPrefix("applicationId:")))
            case Some(_) =>
              throw new IllegalArgumentException(
                "applicationId claim can only be specified once"
              )
          }
        } else {
          throw new IllegalArgumentException(s"Expected claim but got $w")
        }
      }
      Claims(admin, actAs.toList, readAs.toList, applicationId)
    }
    implicit val marshalRequestEntity: Marshaller[Claims, String] =
      Marshaller.opaque(_.toQueryString())
    implicit val unmarshalHttpEntity: Unmarshaller[String, Claims] =
      Unmarshaller { _ => s => Future.fromTry(Try(apply(s))) }
  }

  /** Auth endpoint query parameters
    */
  case class Auth(claims: Claims) {
    def toQuery: Uri.Query = Uri.Query("claims" -> claims.toQueryString())
  }

  /** Login endpoint query parameters
    *
    * @param redirectUri Redirect target after the login flow completed. I.e. the original request URI on the trigger service.
    * @param claims Required ledger claims.
    * @param state State that will be forwarded to the callback URI after authentication and authorization.
    */
  case class Login(redirectUri: Option[Uri], claims: Claims, state: Option[String]) {
    def toQuery: Uri.Query = {
      var params = Seq("claims" -> claims.toQueryString())
      redirectUri.foreach(x => params ++= Seq("redirect_uri" -> x.toString()))
      state.foreach(x => params ++= Seq("state" -> x))
      Uri.Query(params: _*)
    }
  }

  /** Refresh endpoint request entity
    */
  case class Refresh(refreshToken: RefreshToken)

}

object Response {
  import Tagged._

  case class Authorize(accessToken: AccessToken, refreshToken: Option[RefreshToken])

  sealed abstract class Login
  final case class LoginError(error: String, errorDescription: Option[String]) extends Login
  object LoginSuccess extends Login

  object Login {
    val callbackParameters: Directive1[Login] =
      parameters(Symbol("error"), Symbol("error_description") ?)
        .as[LoginError](LoginError)
        .or(provide(LoginSuccess))
  }

  val authenticateChallengeName: String = "DamlAuthMiddleware"

  case class AuthenticateChallenge(
      realm: Request.Claims,
      login: Uri,
      auth: Uri,
  ) {
    def toHeader: HttpHeader = headers.`WWW-Authenticate`(
      HttpChallenge(
        authenticateChallengeName,
        realm.toQueryString(),
        Map(
          "login" -> login.toString(),
          "auth" -> auth.toString(),
        ),
      )
    )
  }

}

object JsonProtocol extends DefaultJsonProtocol {
  import Tagged._

  implicit object UriFormat extends JsonFormat[Uri] {
    def read(value: JsValue) = value match {
      case JsString(s) => Uri(s)
      case _ => deserializationError(s"Expected Uri string but got $value")
    }
    def write(uri: Uri) = JsString(uri.toString)
  }
  implicit object AccessTokenJsonFormat extends JsonFormat[AccessToken] {
    def write(x: AccessToken) = {
      JsString(AccessToken.unwrap(x))
    }
    def read(value: JsValue) = value match {
      case JsString(x) => AccessToken(x)
      case x => deserializationError(s"Expected AccessToken as JsString, but got $x")
    }
  }
  implicit object RefreshTokenJsonFormat extends JsonFormat[RefreshToken] {
    def write(x: RefreshToken) = {
      JsString(RefreshToken.unwrap(x))
    }
    def read(value: JsValue) = value match {
      case JsString(x) => RefreshToken(x)
      case x => deserializationError(s"Expected RefreshToken as JsString, but got $x")
    }
  }
  implicit object RequestClaimsFormat extends JsonFormat[Request.Claims] {
    def write(claims: Request.Claims) = {
      JsString(claims.toQueryString())
    }
    def read(value: JsValue) = value match {
      case JsString(s) =>
        try {
          Request.Claims(s)
        } catch {
          case ex: IllegalArgumentException =>
            deserializationError(ex.getMessage, ex)
        }
      case x => deserializationError(s"Expected Claims as JsString, but got $x")
    }
  }
  implicit val requestRefreshFormat: RootJsonFormat[Request.Refresh] =
    jsonFormat(Request.Refresh, "refresh_token")
  implicit val responseAuthorizeFormat: RootJsonFormat[Response.Authorize] =
    jsonFormat(Response.Authorize, "access_token", "refresh_token")

  implicit object ResponseLoginFormat extends RootJsonFormat[Response.Login] {
    implicit private val errorFormat: RootJsonFormat[Response.LoginError] = jsonFormat2(
      Response.LoginError
    )
    def write(login: Response.Login) = login match {
      case error: Response.LoginError => error.toJson
      case Response.LoginSuccess => JsNull
    }
    def read(value: JsValue) = value.convertTo(safeReader[Response.LoginError]) match {
      case Right(error) => error
      case Left(_) =>
        value match {
          case JsNull => Response.LoginSuccess
          case _ => deserializationError(s"Expected null or error object but got $value")
        }
    }
  }

  implicit val responseAuthenticateChallengeFormat: RootJsonFormat[Response.AuthenticateChallenge] =
    jsonFormat(Response.AuthenticateChallenge, "realm", "login", "auth")
}
