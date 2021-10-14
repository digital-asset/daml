// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.util.ByteString
import com.daml.http.domain.{JwtPayload, JwtPayloadLedgerIdOnly, JwtWritePayload}
import com.daml.http.json.SprayJson
import util.GrpcHttpErrorCodes._
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.auth.AuthServiceJWTCodec
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.scalautil.ExceptionOps._
import io.grpc.Status.{Code => GrpcCode}
import scalaz.syntax.std.option._
import scalaz.{-\/, NonEmptyList, Show, \/, \/-}
import spray.json.JsValue

import scala.concurrent.Future
import scala.util.control.NonFatal

object EndpointsCompanion {

  type ValidateJwt = Jwt => Unauthorized \/ DecodedJwt[String]

  sealed abstract class Error extends Product with Serializable

  final case class InvalidUserInput(message: String) extends Error

  final case class Unauthorized(message: String) extends Error

  final case class ServerError(message: String) extends Error

  final case class ParticipantServerError(grpcStatus: GrpcCode, description: Option[String])
      extends Error

  final case class NotFound(message: String) extends Error

  object Error {
    implicit val ShowInstance: Show[Error] = Show shows {
      case InvalidUserInput(e) => s"Endpoints.InvalidUserInput: ${e: String}"
      case ParticipantServerError(s, d) =>
        s"Endpoints.ParticipantServerError: ${s: GrpcCode}${d.cata((": " + _), "")}"
      case ServerError(e) => s"Endpoints.ServerError: ${e: String}"
      case Unauthorized(e) => s"Endpoints.Unauthorized: ${e: String}"
      case NotFound(e) => s"Endpoints.NotFound: ${e: String}"
    }

    def fromThrowable: Throwable PartialFunction Error = {
      case LedgerClientJwt.Grpc.StatusEnvelope(status) =>
        ParticipantServerError(status.getCode, Option(status.getDescription))
      case NonFatal(t) => ServerError(t.description)
    }
  }

  trait ParsePayload[A] {
    def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ A
  }

  object ParsePayload {
    @inline def apply[A](implicit ev: ParsePayload[A]): ParsePayload[A] = ev

    private[http] implicit val jwtWriteParsePayload: ParsePayload[JwtWritePayload] =
      (jwt: DecodedJwt[String]) => {
        // AuthServiceJWTCodec is the JWT reader used by the sandbox and some Daml-on-X ledgers.
        // Most JWT fields are optional for the sandbox, but not for the JSON API.
        AuthServiceJWTCodec
          .readFromString(jwt.payload)
          .fold(
            e => -\/(Unauthorized(e.getMessage)),
            payload =>
              for {
                ledgerId <- payload.ledgerId
                  .toRightDisjunction(Unauthorized("ledgerId missing in access token"))
                applicationId <- payload.applicationId
                  .toRightDisjunction(Unauthorized("applicationId missing in access token"))
                actAs <- payload.actAs match {
                  case p +: ps => \/-(NonEmptyList(p, ps: _*))
                  case _ =>
                    -\/(Unauthorized(s"Expected one or more parties in actAs but got none"))
                }
              } yield JwtWritePayload(
                lar.LedgerId(ledgerId),
                lar.ApplicationId(applicationId),
                lar.Party.subst(actAs),
                lar.Party.subst(payload.readAs),
              ),
          )
      }
    private[http] implicit val jwtParsePayloadLedgerIdOnly: ParsePayload[JwtPayloadLedgerIdOnly] =
      (jwt: DecodedJwt[String]) => {
        import spray.json._
        val asJsObject: JsValue => Option[Map[String, JsValue]] = {
          case JsObject(fields) => Some(fields)
          case _ => None
        }
        val asString: JsValue => Option[String] = {
          case JsString(value) => Some(value)
          case _ => None
        }
        val ast = jwt.payload.parseJson
        for {
          obj <- asJsObject(ast).toRightDisjunction(Unauthorized("invalid access token"))
          namespace <- obj
            .get(AuthServiceJWTCodec.oidcNamespace)
            .flatMap(asJsObject)
            .toRightDisjunction(Unauthorized("namespace missing in access token"))
          ledgerId <- namespace
            .get("ledgerId")
            .flatMap(asString)
            .toRightDisjunction(Unauthorized("ledgerId missing in access token"))
        } yield JwtPayloadLedgerIdOnly(lar.LedgerId(ledgerId))
      }

    private[http] implicit val jwtParsePayload: ParsePayload[JwtPayload] =
      (jwt: DecodedJwt[String]) => {
        // AuthServiceJWTCodec is the JWT reader used by the sandbox and some Daml-on-X ledgers.
        // Most JWT fields are optional for the sandbox, but not for the JSON API.
        AuthServiceJWTCodec
          .readFromString(jwt.payload)
          .fold(
            e => -\/(Unauthorized(e.getMessage)),
            payload =>
              for {
                ledgerId <- payload.ledgerId
                  .toRightDisjunction(Unauthorized("ledgerId missing in access token"))
                applicationId <- payload.applicationId
                  .toRightDisjunction(Unauthorized("applicationId missing in access token"))
                payload <- JwtPayload(
                  lar.LedgerId(ledgerId),
                  lar.ApplicationId(applicationId),
                  actAs = payload.actAs.map(lar.Party(_)),
                  readAs = payload.readAs.map(lar.Party(_)),
                ).toRightDisjunction(Unauthorized("No parties in actAs and readAs"))
              } yield payload,
          )
      }
  }

  lazy val notFound: Route = (ctx: RequestContext) =>
    ctx.request match {
      case HttpRequest(method, uri, _, _, _) =>
        Future.successful(
          Complete(httpResponseError(NotFound(s"${method: HttpMethod}, uri: ${uri: Uri}")))
        )
    }

  private[http] def httpResponseError(error: Error): HttpResponse = {
    import com.daml.http.json.JsonProtocol._
    val resp = errorResponse(error)
    httpResponse(resp.status, SprayJson.encodeUnsafe(resp))
  }

  private[http] def errorResponse(error: Error): domain.ErrorResponse = {
    val (status, errorMsg): (StatusCode, String) = error match {
      case InvalidUserInput(e) => StatusCodes.BadRequest -> e
      case ParticipantServerError(grpcStatus, d) =>
        grpcStatus.asAkkaHttp -> s"$grpcStatus${d.cata((": " + _), "")}"
      // TODO SC suppress message in this case
      case ServerError(e) => StatusCodes.InternalServerError -> e
      case Unauthorized(e) => StatusCodes.Unauthorized -> e
      case NotFound(e) => StatusCodes.NotFound -> e
    }
    domain.ErrorResponse(errors = List(errorMsg), warnings = None, status = status)
  }

  private[http] def httpResponse(status: StatusCode, data: JsValue): HttpResponse = {
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, format(data)),
    )
  }

  private[http] def format(a: JsValue): ByteString = ByteString(a.compactPrint)

  private[http] def decodeAndParsePayload[A](jwt: Jwt, decodeJwt: ValidateJwt)(implicit
      parse: ParsePayload[A]
  ): Unauthorized \/ (Jwt, A) = {
    for {
      a <- decodeJwt(jwt): Unauthorized \/ DecodedJwt[String]
      p <- parse.parsePayload(a)
    } yield (jwt, p)
  }
}
