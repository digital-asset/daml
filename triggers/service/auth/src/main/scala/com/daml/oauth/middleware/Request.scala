// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.unmarshalling.Unmarshaller
import com.daml.lf.data.Ref.Party
import spray.json.{
  DefaultJsonProtocol,
  JsString,
  JsValue,
  JsonFormat,
  RootJsonFormat,
  deserializationError
}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.util.Try

object Request {

  case class Claims(admin: Boolean, actAs: List[Party], readAs: List[Party]) {
    def toQueryString() = {
      val adminS = if (admin) Stream("admin") else Stream()
      val actAsS = actAs.toStream.map(party => s"actAs:$party")
      val readAsS = readAs.toStream.map(party => s"readAs:$party")
      (adminS ++ actAsS ++ readAsS).mkString(" ")
    }
  }
  object Claims {
    def apply(
        admin: Boolean = false,
        actAs: List[Party] = List(),
        readAs: List[Party] = List()): Claims =
      new Claims(admin, actAs, readAs)
    implicit val marshalRequestEntity: Marshaller[Claims, String] =
      Marshaller.opaque(_.toQueryString)
    implicit val unmarshalHttpEntity: Unmarshaller[String, Claims] =
      Unmarshaller { _ => s =>
        var admin = false
        val actAs = ArrayBuffer[Party]()
        val readAs = ArrayBuffer[Party]()
        Future.fromTry(Try {
          s.split(' ').foreach { w =>
            if (w == "admin") {
              admin = true
            } else if (w.startsWith("actAs:")) {
              actAs.append(Party.assertFromString(w.stripPrefix("actAs:")))
            } else if (w.startsWith("readAs:")) {
              readAs.append(Party.assertFromString(w.stripPrefix("readAs:")))
            } else {
              throw new IllegalArgumentException(s"Expected claim but got $w")
            }
          }
          Claims(admin, actAs.toList, readAs.toList)
        })
      }
  }

  /** Auth endpoint query parameters
    */
  case class Auth(claims: Claims)

  /** Login endpoint query parameters
    *
    * @param redirectUri Redirect target after the login flow completed. I.e. the original request URI on the trigger service.
    * @param claims Required ledger claims.
    */
  case class Login(redirectUri: Uri, claims: Claims)

}

object Response {

  case class Authorize(accessToken: String, refreshToken: Option[String])

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
