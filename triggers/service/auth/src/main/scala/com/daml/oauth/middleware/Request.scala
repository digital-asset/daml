// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.model.Uri
import spray.json.{DefaultJsonProtocol, JsString, JsValue, JsonFormat, RootJsonFormat, deserializationError}

object Request {

  /** Auth endpoint query parameters
    */
  case class Auth(claims: String) // TODO[AH] parse ledger claims

  /** Login endpoint query parameters
    *
    * @param redirectUri Redirect target after the login flow completed. I.e. the original request URI on the trigger service.
    * @param claims Required ledger claims.
    */
  case class Login(redirectUri: Uri, claims: String) // TODO[AH] parse ledger claims

}

object Response {

  case class Authorize(
    accessToken: String,
    refreshToken: Option[String])

}

object JsonProtocol extends DefaultJsonProtocol {
  implicit object UriFormat extends JsonFormat[Uri] {
    def read(value: JsValue) = value match {
      case JsString(s) => Uri(s)
      case _ => deserializationError(s"Expected Uri string but got $value")
    }
    def write(uri: Uri) = JsString(uri.toString)
  }
  implicit val responseAuthorizeFormat: RootJsonFormat[Response.Authorize] =
    jsonFormat(Response.Authorize, "access_token", "refresh_token")
}