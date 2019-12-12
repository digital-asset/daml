// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpMethod,
  HttpRequest,
  HttpResponse,
  StatusCode,
  StatusCodes,
  Uri
}
import akka.util.ByteString
import com.digitalasset.http.domain.JwtPayload
import com.digitalasset.http.json.{ResponseFormats, SprayJson}
import com.digitalasset.jwt.domain.{DecodedJwt, Jwt}
import spray.json.{JsObject, JsValue}
import scalaz.{Show, \/}
import scalaz.syntax.show._
import com.digitalasset.http.json.JsonProtocol._

import scala.concurrent.Future

object EndpointsCompanion {

  type ValidateJwt = Jwt => Unauthorized \/ DecodedJwt[String]

  sealed abstract class Error(message: String) extends Product with Serializable

  final case class InvalidUserInput(message: String) extends Error(message)

  final case class Unauthorized(message: String) extends Error(message)

  final case class ServerError(message: String) extends Error(message)

  final case class NotFound(message: String) extends Error(message)

  object Error {
    implicit val ShowInstance: Show[Error] = Show shows {
      case InvalidUserInput(e) => s"Endpoints.InvalidUserInput: ${e: String}"
      case ServerError(e) => s"Endpoints.ServerError: ${e: String}"
      case Unauthorized(e) => s"Endpoints.Unauthorized: ${e: String}"
      case NotFound(e) => s"Endpoints.NotFound: ${e: String}"
    }
  }

  lazy val notFound: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case HttpRequest(method, uri, _, _, _) =>
      Future.successful(httpResponseError(NotFound(s"${method: HttpMethod}, uri: ${uri: Uri}")))
  }

  private[http] def httpResponseOk(data: JsValue): HttpResponse =
    httpResponse(StatusCodes.OK, ResponseFormats.resultJsObject(data))

  private[http] def httpResponseError(error: Error): HttpResponse = {
    val (status, jsObject) = errorsJsObject(error)
    httpResponse(status, jsObject)
  }

  private[http] def errorsJsObject(error: Error): (StatusCode, JsObject) = {
    val (status, errorMsg): (StatusCode, String) = error match {
      case InvalidUserInput(e) => StatusCodes.BadRequest -> e
      case ServerError(e) => StatusCodes.InternalServerError -> e
      case Unauthorized(e) => StatusCodes.Unauthorized -> e
      case NotFound(e) => StatusCodes.NotFound -> e
    }
    (status, ResponseFormats.errorsJsObject(status, errorMsg))
  }

  private[http] def httpResponse(status: StatusCode, data: JsValue): HttpResponse = {
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, format(data)))
  }

  private[http] def format(a: JsValue): ByteString = ByteString(a.compactPrint)

  private[http] def decodeAndParsePayload(
      jwt: Jwt,
      decodeJwt: ValidateJwt): Unauthorized \/ (Jwt, JwtPayload) =
    for {
      a <- decodeJwt(jwt): Unauthorized \/ DecodedJwt[String]
      p <- parsePayload(a)
    } yield (jwt, p)

  private[http] def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ JwtPayload =
    SprayJson.decode[JwtPayload](jwt.payload).leftMap(e => Unauthorized(e.shows))

  private[http] def encodeList(as: Seq[JsValue]): ServerError \/ JsValue =
    SprayJson.encode(as).leftMap(e => ServerError(e.shows))

}
