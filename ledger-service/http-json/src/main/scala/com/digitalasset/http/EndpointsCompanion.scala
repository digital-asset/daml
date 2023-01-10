// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.util.ByteString
import com.daml.http.domain.{JwtPayload, JwtPayloadLedgerIdOnly, JwtWritePayload, LedgerApiError}
import com.daml.http.json.SprayJson
import com.daml.http.util.Logging.{InstanceUUID, RequestID, extendWithRequestIdLogCtx}
import util.GrpcHttpErrorCodes._
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.auth.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  CustomDamlJWTPayload,
  StandardJWTPayload,
}
import com.daml.ledger.api.domain.UserRight
import UserRight.{CanActAs, CanReadAs}
import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.ErrorDetails.ErrorDetail
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.client.services.admin.UserManagementClient
import com.daml.ledger.client.services.identity.LedgerIdentityClient
import com.daml.lf.data.Ref.UserId
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.google.rpc.{Code => GrpcCode}
import com.google.rpc.Status
import scalaz.syntax.std.option._
import scalaz.{-\/, EitherT, Monad, NonEmptyList, Show, \/, \/-}
import spray.json.JsValue
import scalaz.syntax.std.either._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object EndpointsCompanion {

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
      case LedgerClientJwt.Grpc.StatusEnvelope(status) => ParticipantServerError(status)
      case NonFatal(t) => ServerError(t)
    }
  }

  trait CreateFromCustomToken[A] {
    def apply(
        jwt: CustomDamlJWTPayload
    ): Unauthorized \/ A
  }

  trait CreateFromUserToken[A] {
    def apply(
        jwt: StandardJWTPayload,
        listUserRights: UserId => Future[Seq[UserRight]],
        getLedgerId: () => Future[String],
    ): EitherT[Future, Unauthorized, A]
  }

  object CreateFromUserToken {

    import com.daml.http.util.FutureUtil.either

    trait FromUser[A, B] {
      def apply(userId: String, actAs: List[String], readAs: List[String], ledgerId: String): A \/ B
    }

    private[http] def userIdFromToken(
        jwt: StandardJWTPayload
    ): Unauthorized \/ UserId =
      UserId
        .fromString(jwt.userId)
        .disjunction
        .leftMap(Unauthorized)

    private def transformUserTokenTo[B](
        jwt: StandardJWTPayload,
        listUserRights: UserId => Future[Seq[UserRight]],
        getLedgerId: () => Future[String],
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
        ledgerId <- EitherT.rightT(getLedgerId())
        res <- either(fromUser(userId, actAs.toList, readAs.toList, ledgerId))
      } yield res

    private[http] implicit def jwtWritePayloadFromUserToken(implicit
        mf: Monad[Future]
    ): CreateFromUserToken[JwtWritePayload] =
      (
          jwt,
          listUserRights,
          getLedgerId,
      ) =>
        transformUserTokenTo(jwt, listUserRights, getLedgerId)((userId, actAs, readAs, ledgerId) =>
          for {
            actAsNonEmpty <-
              if (actAs.isEmpty)
                -\/ apply Unauthorized(
                  "ActAs list of user was empty, this is an invalid state for converting it to a JwtWritePayload"
                )
              else \/-(NonEmptyList(actAs.head: String, actAs.tail: _*))
          } yield JwtWritePayload(
            lar.LedgerId(ledgerId),
            lar.ApplicationId(userId),
            lar.Party.subst(actAsNonEmpty),
            lar.Party.subst(readAs),
          )
        )

    private[http] implicit def jwtPayloadLedgerIdOnlyFromUserToken(implicit
        mf: Monad[Future]
    ): CreateFromUserToken[JwtPayloadLedgerIdOnly] =
      (_, _, getLedgerId: () => Future[String]) =>
        EitherT
          .rightT(getLedgerId())
          .map(ledgerId => JwtPayloadLedgerIdOnly(lar.LedgerId(ledgerId)))

    private[http] implicit def jwtPayloadFromUserToken(implicit
        mf: Monad[Future]
    ): CreateFromUserToken[JwtPayload] =
      (
          jwt,
          listUserRights,
          getLedgerId,
      ) =>
        transformUserTokenTo(jwt, listUserRights, getLedgerId)((userId, actAs, readAs, ledgerId) =>
          \/ fromEither JwtPayload(
            lar.LedgerId(ledgerId),
            lar.ApplicationId(userId),
            actAs = lar.Party.subst(actAs),
            readAs = lar.Party.subst(readAs),
          ).toRight(Unauthorized("Unable to convert user token into a set of claims"))
        )

  }

  object CreateFromCustomToken {

    private[http] implicit val jwtWritePayloadFromCustomToken
        : CreateFromCustomToken[JwtWritePayload] =
      (
        jwt: CustomDamlJWTPayload,
      ) =>
        for {
          ledgerId <- jwt.ledgerId
            .toRightDisjunction(Unauthorized("ledgerId missing in access token"))
          applicationId <- jwt.applicationId
            .toRightDisjunction(Unauthorized("applicationId missing in access token"))
          actAs <- jwt.actAs match {
            case p +: ps => \/-(NonEmptyList(p, ps: _*))
            case _ =>
              -\/(Unauthorized(s"Expected one or more parties in actAs but got none"))
          }
        } yield JwtWritePayload(
          lar.LedgerId(ledgerId),
          lar.ApplicationId(applicationId),
          lar.Party.subst(actAs),
          lar.Party.subst(jwt.readAs),
        )

    private[http] implicit val jwtPayloadLedgerIdOnlyFromCustomToken
        : CreateFromCustomToken[JwtPayloadLedgerIdOnly] =
      (jwt: CustomDamlJWTPayload) =>
        jwt.ledgerId
          .toRightDisjunction(Unauthorized("ledgerId missing in access token"))
          .map(ledgerId => JwtPayloadLedgerIdOnly(lar.LedgerId(ledgerId)))

    private[http] implicit val jwtPayloadFromCustomToken: CreateFromCustomToken[JwtPayload] =
      (jwt: CustomDamlJWTPayload) =>
        for {
          ledgerId <- jwt.ledgerId
            .toRightDisjunction(Unauthorized("ledgerId missing in access token"))
          applicationId <- jwt.applicationId
            .toRightDisjunction(Unauthorized("applicationId missing in access token"))
          payload <- JwtPayload(
            lar.LedgerId(ledgerId),
            lar.ApplicationId(applicationId),
            actAs = lar.Party.subst(jwt.actAs),
            readAs = lar.Party.subst(jwt.readAs),
          ).toRightDisjunction(Unauthorized("No parties in actAs and readAs"))
        } yield payload
  }

  def notFound(implicit lc: LoggingContextOf[InstanceUUID]): Route = (ctx: RequestContext) =>
    ctx.request match {
      case HttpRequest(method, uri, _, _, _) =>
        extendWithRequestIdLogCtx(implicit lc =>
          Future.successful(
            Complete(httpResponseError(NotFound(s"${method: HttpMethod}, uri: ${uri: Uri}")))
          )
        )
    }

  private[http] def httpResponseError(
      error: Error
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): HttpResponse = {
    import com.daml.http.json.JsonProtocol._
    val resp = errorResponse(error)
    httpResponse(resp.status, SprayJson.encodeUnsafe(resp))
  }
  private[this] val logger = ContextualizedLogger.get(getClass)

  private[http] def errorResponse(
      error: Error
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
          grpcStatus.asAkkaHttpForJsonApi,
          s"$grpcStatus: $description",
          Some(ledgerApiError),
        )
      case ServerError(reason) =>
        logger.error(s"Internal server error occured", reason)
        mkErrorResponse(StatusCodes.InternalServerError, "HTTP JSON API Server Error")
      case Unauthorized(e) => mkErrorResponse(StatusCodes.Unauthorized, e)
      case NotFound(e) => mkErrorResponse(StatusCodes.NotFound, e)
    }
  }

  private[http] def httpResponse(status: StatusCode, data: JsValue): HttpResponse = {
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, format(data)),
    )
  }

  private[http] def format(a: JsValue): ByteString = ByteString(a.compactPrint)

  private[http] def decodeAndParseJwt(
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

  private[http] def decodeAndParsePayload[A](
      jwt: Jwt,
      decodeJwt: ValidateJwt,
      userManagementClient: UserManagementClient,
      ledgerIdentityClient: LedgerIdentityClient,
  )(implicit
      createFromCustomToken: CreateFromCustomToken[A],
      createFromUserToken: CreateFromUserToken[A],
      fm: Monad[Future],
      ec: ExecutionContext,
  ): EitherT[Future, Error, (Jwt, A)] = {
    for {
      token <- EitherT.either(decodeAndParseJwt(jwt, decodeJwt))
      p <- token match {
        case standardToken: StandardJWTPayload =>
          createFromUserToken(
            standardToken,
            userId => userManagementClient.listUserRights(userId, Some(jwt.value)),
            () => ledgerIdentityClient.getLedgerId(Some(jwt.value)),
          ).leftMap(identity[Error])
        case customToken: CustomDamlJWTPayload =>
          EitherT.either(createFromCustomToken(customToken): Error \/ A)
      }
    } yield (jwt, p: A)
  }
}
