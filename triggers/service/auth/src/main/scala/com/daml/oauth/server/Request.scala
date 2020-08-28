// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Query
import spray.json.{
  DefaultJsonProtocol,
  JsString,
  JsValue,
  JsonFormat,
  RootJsonFormat,
  deserializationError
}

object Request {

  // https://tools.ietf.org/html/rfc6749#section-4.1.1
  case class Authorize(
      responseType: String,
      clientId: String,
      redirectUri: Uri, // optional in oauth but we require it
      scope: Option[String],
      state: Option[String]) {
    def toQuery: Query = {
      var params: Seq[(String, String)] =
        Seq(
          ("response_type", responseType),
          ("client_id", clientId),
          ("redirect_uri", redirectUri.toString))
      scope.foreach { scope =>
        params ++= Seq(("scope", scope))
      }
      state.foreach { state =>
        params ++= Seq(("state", state))
      }
      Query(params: _*)
    }
  }

  // https://tools.ietf.org/html/rfc6749#section-4.1.3
  case class Token(
      grantType: String,
      code: String,
      redirectUri: Uri,
      clientId: String,
      clientSecret: String)

}

object Response {

  // https://tools.ietf.org/html/rfc6749#section-4.1.2
  case class Authorize(code: String, state: Option[String]) {
    def toQuery: Query = state match {
      case None => Query(("code", code))
      case Some(state) => Query(("code", code), ("state", state))
    }
  }

  // https://tools.ietf.org/html/rfc6749#section-5.1
  case class Token(
      accessToken: String,
      tokenType: String,
      expiresIn: Option[String],
      refreshToken: Option[String],
      scope: Option[String])

}

object JsonProtocol extends DefaultJsonProtocol {
  implicit object UriFormat extends JsonFormat[Uri] {
    def read(value: JsValue) = value match {
      case JsString(s) => Uri(s)
      case _ => deserializationError(s"Expected Uri string but got $value")
    }
    def write(uri: Uri) = JsString(uri.toString)
  }
  implicit val tokenReqFormat: RootJsonFormat[Request.Token] =
    jsonFormat(Request.Token, "grant_type", "code", "redirect_uri", "client_id", "client_secret")
  implicit val tokenRespFormat: RootJsonFormat[Response.Token] =
    jsonFormat(Response.Token, "access_token", "token_type", "expires_in", "refresh_token", "scope")
}
