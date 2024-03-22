// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.oauth2.api

import java.util.Base64

import akka.http.scaladsl.model.{FormData, HttpEntity, RequestEntity, Uri}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.unmarshalling._
import spray.json._

import scala.util.Try

object Request {

  // https://tools.ietf.org/html/rfc6749#section-4.1.1
  case class Authorize(
      responseType: String,
      clientId: String,
      redirectUri: Uri, // optional in oauth but we require it
      scope: Option[String],
      state: Option[String],
      audience: Option[Uri],
  ) { // required by auth0 to obtain an access_token
    def toQuery: Query = {
      var params: Seq[(String, String)] =
        Seq(
          ("response_type", responseType),
          ("client_id", clientId),
          ("redirect_uri", redirectUri.toString),
        )
      scope.foreach { scope =>
        params ++= Seq(("scope", scope))
      }
      state.foreach { state =>
        params ++= Seq(("state", state))
      }
      audience.foreach { audience =>
        params ++= Seq(("audience", audience.toString))
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
      clientSecret: String,
  )

  object Token {
    implicit val marshalRequestEntity: Marshaller[Token, RequestEntity] =
      Marshaller.combined { token =>
        FormData(
          "grant_type" -> token.grantType,
          "code" -> token.code,
          "redirect_uri" -> token.redirectUri.toString,
          "client_id" -> token.clientId,
          "client_secret" -> token.clientSecret,
        )
      }
    implicit val unmarshalHttpEntity: Unmarshaller[HttpEntity, Token] =
      Unmarshaller.defaultUrlEncodedFormDataUnmarshaller.map { form =>
        Token(
          grantType = form.fields.get("grant_type").get,
          code = form.fields.get("code").get,
          redirectUri = form.fields.get("redirect_uri").get,
          clientId = form.fields.get("client_id").get,
          clientSecret = form.fields.get("client_secret").get,
        )
      }
  }

  // https://tools.ietf.org/html/rfc6749#section-6
  case class Refresh(
      grantType: String,
      refreshToken: String,
      clientId: String,
      clientSecret: String,
  )

  object Refresh {
    implicit val marshalRequestEntity: Marshaller[Refresh, RequestEntity] =
      Marshaller.combined { refresh =>
        FormData(
          "grant_type" -> refresh.grantType,
          "refresh_token" -> refresh.refreshToken,
          "client_id" -> refresh.clientId,
          "client_secret" -> refresh.clientSecret,
        )
      }
    implicit val unmarshalHttpEntity: Unmarshaller[HttpEntity, Refresh] =
      Unmarshaller.defaultUrlEncodedFormDataUnmarshaller.map { form =>
        Refresh(
          grantType = form.fields.get("grant_type").get,
          refreshToken = form.fields.get("refresh_token").get,
          clientId = form.fields.get("client_id").get,
          clientSecret = form.fields.get("client_secret").get,
        )
      }
  }

}

object Response {

  // https://tools.ietf.org/html/rfc6749#section-4.1.2
  case class Authorize(code: String, state: Option[String]) {
    def toQuery: Query = state match {
      case None => Query(("code", code))
      case Some(state) => Query(("code", code), ("state", state))
    }
  }

  // https://tools.ietf.org/html/rfc6749#section-4.1.2.1
  case class Error(
      error: String,
      errorDescription: Option[String],
      errorUri: Option[Uri],
      state: Option[String],
  ) {
    def toQuery: Query = {
      var params: Seq[(String, String)] = Seq("error" -> error)
      errorDescription.foreach(x => params ++= Seq("error_description" -> x))
      errorUri.foreach(x => params ++= Seq("error_uri" -> x.toString))
      state.foreach(x => params ++= Seq("state" -> x))
      Query(params: _*)
    }
  }

  // https://tools.ietf.org/html/rfc6749#section-5.1
  case class Token(
      accessToken: String,
      tokenType: String,
      expiresIn: Option[Int],
      refreshToken: Option[String],
      scope: Option[String],
  ) {
    def toCookieValue: String = {
      import JsonProtocol._
      Base64.getUrlEncoder().encodeToString(this.toJson.compactPrint.getBytes)
    }
  }

  object Token {
    def fromCookieValue(s: String): Option[Token] = {
      import JsonProtocol._
      for {
        bytes <- Try(Base64.getUrlDecoder().decode(s))
        json <- Try(new String(bytes).parseJson)
        token <- Try(json.convertTo[Token])
      } yield token
    }.toOption
  }

}

object JsonProtocol extends DefaultJsonProtocol {
  implicit object UriFormat extends JsonFormat[Uri] {
    def read(value: JsValue) = value match {
      case JsString(s) => Uri(s)
      case _ => deserializationError(s"Expected Uri string but got $value")
    }
    def write(uri: Uri) = JsString(uri.toString)
  }
  implicit val tokenRespFormat: RootJsonFormat[Response.Token] = {
    jsonFormat(
      Response.Token.apply,
      "access_token",
      "token_type",
      "expires_in",
      "refresh_token",
      "scope",
    )
  }
  implicit val errorRespFormat: RootJsonFormat[Response.Error] = {
    jsonFormat(
      Response.Error.apply,
      "error",
      "error_description",
      "error_uri",
      "state",
    )
  }
}
