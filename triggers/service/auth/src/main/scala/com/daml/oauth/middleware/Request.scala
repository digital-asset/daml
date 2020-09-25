// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.model.Uri
import spray.json.{DefaultJsonProtocol, JsString, JsValue, JsonFormat, deserializationError}

object Request {

  case class Login(redirectUri: Uri, claims: String) // TODO[AH] parse ledger claims

}

object Response {}

object JsonProtocol extends DefaultJsonProtocol {
  implicit object UriFormat extends JsonFormat[Uri] {
    def read(value: JsValue) = value match {
      case JsString(s) => Uri(s)
      case _ => deserializationError(s"Expected Uri string but got $value")
    }
    def write(uri: Uri) = JsString(uri.toString)
  }
}
