// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model._
import akka.util.ByteString
import com.daml.http.domain.{JwtPayload, JwtWritePayload}
import com.daml.http.json.SprayJson
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.auth.AuthServiceJWTCodec
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import scalaz.syntax.std.option._
import scalaz.{-\/, NonEmptyList, Show, \/, \/-}
import spray.json.JsValue

import scala.concurrent.Future

object EndpointsCompanion {

  type ValidateJwt = Jwt => Unauthorized \/ DecodedJwt[String]

  sealed abstract class Error extends Product with Serializable

  final case class InvalidUserInput(message: String) extends Error

  final case class Unauthorized(message: String) extends Error

  final case class ServerError(message: String) extends Error

  final case class NotFound(message: String) extends Error

  object Error {
    implicit val ShowInstance: Show[Error] = Show shows {
      case InvalidUserInput(e) => s"Endpoints.InvalidUserInput: ${e: String}"
      case ServerError(e) => s"Endpoints.ServerError: ${e: String}"
      case Unauthorized(e) => s"Endpoints.Unauthorized: ${e: String}"
      case NotFound(e) => s"Endpoints.NotFound: ${e: String}"
    }
  }

  trait ParsePayload[A] {
    def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ A
  }

  object ParsePayload {
    @inline def apply[A](implicit ev: ParsePayload[A]): ParsePayload[A] = ev

    private[http] implicit val jwtWriteParsePayload: ParsePayload[JwtWritePayload] =
      new ParsePayload[JwtWritePayload] {
        override def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ JwtWritePayload = {
          // AuthServiceJWTCodec is the JWT reader used by the sandbox and some DAML-on-X ledgers.
          // Most JWT fields are optional for the sandbox, but not for the JSON API.
          AuthServiceJWTCodec
            .readFromString(jwt.payload)
            .fold(
              e => -\/(Unauthorized(e.getMessage)),
              payload =>
                for {
                  ledgerId <- payload.ledgerId.toRightDisjunction(
                    Unauthorized("ledgerId missing in access token"))
                  applicationId <- payload.applicationId.toRightDisjunction(
                    Unauthorized("applicationId missing in access token"))
                  actAs <- payload.actAs match {
                    case p +: ps => \/-(NonEmptyList(p, ps: _*))
                    case _ =>
                      -\/(Unauthorized(s"Expected one or more parties in actAs but got none"))
                  }
                } yield
                  JwtWritePayload(
                    lar.LedgerId(ledgerId),
                    lar.ApplicationId(applicationId),
                    lar.Party.subst(actAs),
                    lar.Party.subst(payload.readAs),
                )
            )
        }
      }

    private[http] implicit val jwtParsePayload: ParsePayload[JwtPayload] =
      new ParsePayload[JwtPayload] {
        override def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ JwtPayload = {
          // AuthServiceJWTCodec is the JWT reader used by the sandbox and some DAML-on-X ledgers.
          // Most JWT fields are optional for the sandbox, but not for the JSON API.
          AuthServiceJWTCodec
            .readFromString(jwt.payload)
            .fold(
              e => -\/(Unauthorized(e.getMessage)),
              payload =>
                for {
                  ledgerId <- payload.ledgerId.toRightDisjunction(
                    Unauthorized("ledgerId missing in access token"))
                  applicationId <- payload.applicationId.toRightDisjunction(
                    Unauthorized("applicationId missing in access token"))
                  payload <- JwtPayload(
                    lar.LedgerId(ledgerId),
                    lar.ApplicationId(applicationId),
                    actAs = payload.actAs.map(lar.Party(_)),
                    readAs = payload.readAs.map(lar.Party(_))
                  ).toRightDisjunction(Unauthorized("No parties in actAs and readAs"))
                } yield payload
            )
        }

      }
  }

  lazy val notFound: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case HttpRequest(method, uri, _, _, _) =>
      Future.successful(httpResponseError(NotFound(s"${method: HttpMethod}, uri: ${uri: Uri}")))
  }

  private[http] def httpResponseError(error: Error): HttpResponse = {
    import com.daml.http.json.JsonProtocol._
    val resp = errorResponse(error)
    httpResponse(resp.status, SprayJson.encodeUnsafe(resp))
  }

  private[http] def errorResponse(error: Error): domain.ErrorResponse = {
    val (status, errorMsg): (StatusCode, String) = error match {
      case InvalidUserInput(e) => StatusCodes.BadRequest -> e
      case ServerError(e) => StatusCodes.InternalServerError -> e
      case Unauthorized(e) => StatusCodes.Unauthorized -> e
      case NotFound(e) => StatusCodes.NotFound -> e
    }
    domain.ErrorResponse(errors = List(errorMsg), warnings = None, status = status)
  }

  private[http] def httpResponse(status: StatusCode, data: JsValue): HttpResponse = {
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, format(data)))
  }

  private[http] def format(a: JsValue): ByteString = ByteString(a.compactPrint)

  private[http] def decodeAndParsePayload[A](jwt: Jwt, decodeJwt: ValidateJwt)(
      implicit parse: ParsePayload[A]): Unauthorized \/ (jwt.type, A) = {
    for {
      a <- decodeJwt(jwt): Unauthorized \/ DecodedJwt[String]
      p <- parse.parsePayload(a)
    } yield (jwt, p)
  }
}
