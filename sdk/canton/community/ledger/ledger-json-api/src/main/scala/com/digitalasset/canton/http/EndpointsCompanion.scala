// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.RouteResult.Complete
import org.apache.pekko.http.scaladsl.server.{RequestContext, Route}
import org.apache.pekko.util.ByteString
import util.GrpcHttpErrorCodes.*
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.digitalasset.canton.ledger.api.auth.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  StandardJWTPayload,
}
import com.digitalasset.canton.ledger.api.domain.UserRight
import UserRight.{CanActAs, CanReadAs}
import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.ErrorDetails.ErrorDetail
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.canton.http.json.SprayJson
import com.digitalasset.canton.http.util.Logging.{
  InstanceUUID,
  RequestID,
  extendWithRequestIdLogCtx,
}
import com.digitalasset.canton.ledger.client.services.admin.UserManagementClient
import com.digitalasset.canton.ledger.service.Grpc.StatusEnvelope
import com.digitalasset.daml.lf.data.Ref.UserId
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.domain.{JwtPayload, JwtWritePayload, LedgerApiError}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.NoTracing
import com.google.rpc.Code as GrpcCode
import com.google.rpc.Status
import scalaz.{-\/, EitherT, Monad, NonEmptyList, Show, \/, \/-}
import spray.json.JsValue
import scalaz.syntax.std.either.*

import scala.concurrent.Future
import scala.util.control.NonFatal

object EndpointsCompanion extends NoTracing {

  type ValidateJwt = Jwt => Unauthorized \/ DecodedJwt[String]

  sealed abstract class Error extends Product with Serializable

  final case class InvalidUserInput(message: String) extends Error

  final case class Unauthorized(message: String) extends Error

  final case class ServerError(message: Throwable) extends Error

  final case class ParticipantServerError(
      grpcStatus: GrpcCode,
      description: String,
      details: Seq[ErrorDetail],
  ) extends Error

  object ParticipantServerError {
    def apply(status: Status): ParticipantServerError =
      ParticipantServerError(
        com.google.rpc.Code.forNumber(status.getCode),
        status.getMessage,
        ErrorDetails.from(status),
      )
  }

  final case class NotFound(message: String) extends Error

  object ServerError {
    // We want stack traces also in the case of simple error messages.
    def fromMsg(message: String): ServerError = ServerError(new Exception(message))
  }

  object Error {
    implicit val ShowInstance: Show[Error] = Show shows {
      case InvalidUserInput(e) => s"Endpoints.InvalidUserInput: ${e: String}"
      case ParticipantServerError(grpcStatus, description, _) =>
        s"Endpoints.ParticipantServerError: $grpcStatus: $description"
      case ServerError(e) => s"Endpoints.ServerError: ${e.getMessage: String}"
      case Unauthorized(e) => s"Endpoints.Unauthorized: ${e: String}"
      case NotFound(e) => s"Endpoints.NotFound: ${e: String}"
    }

    def fromThrowable: PartialFunction[Throwable, Error] = {
      case StatusEnvelope(status) => ParticipantServerError(status)
      case NonFatal(t) => ServerError(t)
    }
  }

  trait CreateFromUserToken[A] {
    def apply(
        jwt: StandardJWTPayload,
        listUserRights: UserId => Future[Seq[UserRight]],
    ): EitherT[Future, Unauthorized, A]
  }

  object CreateFromUserToken {

    import com.digitalasset.canton.http.util.FutureUtil.either

    trait FromUser[A, B] {
      def apply(userId: String, actAs: List[String], readAs: List[String]): A \/ B
    }

    def userIdFromToken(
        jwt: StandardJWTPayload
    ): Unauthorized \/ UserId =
      UserId
        .fromString(jwt.userId)
        .disjunction
        .leftMap(Unauthorized)

    private def transformUserTokenTo[B](
        jwt: StandardJWTPayload,
        listUserRights: UserId => Future[Seq[UserRight]],
    )(
        fromUser: FromUser[Unauthorized, B]
    )(implicit
        mf: Monad[Future]
    ): EitherT[Future, Unauthorized, B] =
      for {
        userId <- either(userIdFromToken(jwt))
        rights <- EitherT.rightT(listUserRights(userId))

        actAs = rights.collect { case CanActAs(party) =>
          party
        }
        readAs = rights.collect { case CanReadAs(party) =>
          party
        }
        res <- either(fromUser(userId, actAs.toList, readAs.toList))
      } yield res

    implicit def jwtWritePayloadFromUserToken(implicit
        mf: Monad[Future]
    ): CreateFromUserToken[JwtWritePayload] =
      (
          jwt,
          listUserRights,
      ) =>
        transformUserTokenTo(jwt, listUserRights)((userId, actAs, readAs) =>
          for {
            actAsNonEmpty <-
              if (actAs.isEmpty)
                -\/ apply Unauthorized(
                  "ActAs list of user was empty, this is an invalid state for converting it to a JwtWritePayload"
                )
              else \/-(NonEmptyList(actAs.head: String, actAs.tail: _*))
          } yield JwtWritePayload(
            lar.ApplicationId(userId),
            lar.Party.subst(actAsNonEmpty),
            lar.Party.subst(readAs),
          )
        )

    implicit def jwtPayloadFromUserToken(implicit
        mf: Monad[Future]
    ): CreateFromUserToken[JwtPayload] =
      (
          jwt,
          listUserRights,
      ) =>
        transformUserTokenTo(jwt, listUserRights)((userId, actAs, readAs) =>
          \/ fromEither JwtPayload(
            lar.ApplicationId(userId),
            actAs = lar.Party.subst(actAs),
            readAs = lar.Party.subst(readAs),
          ).toRight(Unauthorized("Unable to convert user token into a set of claims"))
        )

  }

  def notFound(
      logger: TracedLogger
  )(implicit lc: LoggingContextOf[InstanceUUID]): Route = (ctx: RequestContext) =>
    ctx.request match {
      case HttpRequest(method, uri, _, _, _) =>
        extendWithRequestIdLogCtx(implicit lc =>
          Future.successful(
            Complete(
              httpResponseError(NotFound(s"${method: HttpMethod}, uri: ${uri: Uri}"), logger)
            )
          )
        )
    }

  def httpResponseError(
      error: Error,
      logger: TracedLogger,
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): HttpResponse = {
    import com.digitalasset.canton.http.json.JsonProtocol.*
    val resp = errorResponse(error, logger)
    httpResponse(resp.status, SprayJson.encodeUnsafe(resp))
  }

  def errorResponse(
      error: Error,
      logger: TracedLogger,
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): domain.ErrorResponse = {
    def mkErrorResponse(
        status: StatusCode,
        error: String,
        ledgerApiError: Option[LedgerApiError] = None,
    ) =
      domain.ErrorResponse(
        errors = List(error),
        warnings = None,
        status = status,
        ledgerApiError = ledgerApiError,
      )
    error match {
      case InvalidUserInput(e) => mkErrorResponse(StatusCodes.BadRequest, e)
      case ParticipantServerError(grpcStatus, description, details) =>
        val ledgerApiError =
          domain.LedgerApiError(
            code = grpcStatus.getNumber,
            message = description,
            details = details.map(domain.ErrorDetail.fromErrorUtils),
          )
        mkErrorResponse(
          grpcStatus.asPekkoHttpForJsonApi,
          s"$grpcStatus: $description",
          Some(ledgerApiError),
        )
      case ServerError(reason) =>
        logger.error(s"Internal server error occured, ${lc.makeString}", reason)
        mkErrorResponse(StatusCodes.InternalServerError, "HTTP JSON API Server Error")
      case Unauthorized(e) => mkErrorResponse(StatusCodes.Unauthorized, e)
      case NotFound(e) => mkErrorResponse(StatusCodes.NotFound, e)
    }
  }

  def httpResponse(status: StatusCode, data: JsValue): HttpResponse = {
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, format(data)),
    )
  }

  def format(a: JsValue): ByteString = ByteString(a.compactPrint)

  def decodeAndParseJwt(
      jwt: Jwt,
      decodeJwt: ValidateJwt,
  ): Error \/ AuthServiceJWTPayload =
    decodeJwt(jwt)
      .flatMap(a =>
        AuthServiceJWTCodec
          .readFromString(a.payload)
          .toEither
          .disjunction
          .leftMap(Error.fromThrowable)
      )

  def decodeAndParsePayload[A](
      jwt: Jwt,
      decodeJwt: ValidateJwt,
      userManagementClient: UserManagementClient,
  )(implicit
      createFromUserToken: CreateFromUserToken[A],
      fm: Monad[Future],
  ): EitherT[Future, Error, (Jwt, A)] = {
    for {
      token <- EitherT.either(decodeAndParseJwt(jwt, decodeJwt))
      p <- token match {
        case standardToken: StandardJWTPayload =>
          createFromUserToken(
            standardToken,
            userId => userManagementClient.listUserRights(userId = userId, token = Some(jwt.value)),
          ).leftMap(identity[Error])
      }
    } yield (jwt, p: A)
  }
}
