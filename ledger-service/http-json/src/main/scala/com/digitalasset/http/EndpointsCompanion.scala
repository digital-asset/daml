// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.util.ByteString
import com.daml.http.domain.{JwtPayload, JwtPayloadLedgerIdOnly, JwtWritePayload}
import com.daml.http.json.SprayJson
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import util.GrpcHttpErrorCodes._
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, CustomDamlJWTPayload, StandardJWTPayload}
import AuthServiceJWTCodec.{readPayload => readJWTPayload}
import com.daml.ledger.api.domain.UserRight
import UserRight.{CanActAs, CanReadAs}
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.client.services.admin.UserManagementClient
import com.daml.ledger.client.services.identity.LedgerIdentityClient
import com.daml.lf.data.Ref.UserId
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.scalautil.ExceptionOps._
import io.grpc.Status.{Code => GrpcCode}
import scalaz.syntax.std.option._
import scalaz.{-\/, EitherT, Monad, NonEmptyList, Show, \/, \/-}
import spray.json.JsValue
import scalaz.syntax.std.either._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
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
  private type ET[A] = EitherT[Future, Unauthorized, A]
  trait ParsePayload[A] {
    def parsePayload(
        jwt: DecodedJwt[String]
    ): Error \/ A
  }

  trait CreateFromUserToken[A] {
    def apply(
        jwt: DecodedJwt[String],
        listUserRights: UserId => Future[Vector[UserRight]],
        getLedgerId: () => Future[String],
    ): ET[A]
  }

  object CreateFromUserToken {

    import com.daml.http.util.FutureUtil.either

    trait FromUser[A, B] {
      def apply(userId: String, actAs: List[String], readAs: List[String], ledgerId: String): A \/ B
    }

    private def parseAndDecodeUserToken(jwt: DecodedJwt[String]) = {
      import spray.json._
      for {
        jsValue <- Try(jwt.payload.parseJson).toEither.disjunction
          .leftMap(it => Unauthorized(it.getMessage))
        decodedToken <-
          \/.attempt(readJWTPayload(jsValue)) {
            case _: spray.json.DeserializationException => Unauthorized("Invalid token supplied")
            case e => throw e
          }
      } yield decodedToken
    }

    private[http] def parseUserIdFromToken(
        jwt: DecodedJwt[String]
    ): Unauthorized \/ UserId =
      for {
        parsedToken <- parseAndDecodeUserToken(jwt)
        userId <- (parsedToken match {
          case d: CustomDamlJWTPayload =>
            d.applicationId toRight "This user token contains no applicationId/userId"
          case s: StandardJWTPayload => Right(s.userId)
        })
          .flatMap(UserId.fromString)
          .disjunction
          .leftMap(Unauthorized)
      } yield userId

    private def transformUserTokenTo[B](
        jwt: DecodedJwt[String],
        listUserRights: UserId => Future[Vector[UserRight]],
        getLedgerId: () => Future[String],
    )(
        fromUser: FromUser[Unauthorized, B]
    )(implicit
        mf: Monad[Future]
    ): EitherT[Future, Unauthorized, B] =
      for {
        userId <- either(parseUserIdFromToken(jwt))
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
      (jwt, _, getLedgerId: () => Future[String]) =>
        for {
          // Just testing whether this is a valid user token,
          // to make sure that we don't see the incoming token
          // as an user token which actually isn't
          _ <- either(parseAndDecodeUserToken(jwt))
          res <- EitherT
            .rightT(getLedgerId())
            .map(ledgerId => JwtPayloadLedgerIdOnly(lar.LedgerId(ledgerId)))
        } yield res

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

  object ParsePayload {
    @inline def apply[A](implicit ev: ParsePayload[A]): ParsePayload[A] = ev

    private[http] implicit val jwtWriteParsePayload: ParsePayload[JwtWritePayload] =
      (
        jwt: DecodedJwt[String],
      ) => {
        // AuthServiceJWTCodec is the JWT reader used by the sandbox and some Daml-on-X ledgers.
        // Most JWT fields are optional for the sandbox, but not for the JSON API.
        AuthServiceJWTCodec
          .readFromString(jwt.payload)
          .fold(
            e => -\/ apply Unauthorized(e.getMessage),
            mPayload =>
              for {
                payload <- mPayload match {
                  case d: CustomDamlJWTPayload => \/-(d)
                  case _: StandardJWTPayload =>
                    -\/(Unauthorized("ledgerId and actAs missing in access token"))
                }
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
          namespace = obj
            .get(AuthServiceJWTCodec.oidcNamespace)
            .flatMap(asJsObject)
            .getOrElse(obj)
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
            e => -\/ apply Unauthorized(e.getMessage),
            mPayload =>
              for {
                lpcr <- mPayload match {
                  case d: CustomDamlJWTPayload =>
                    \/-((d.ledgerId, d.applicationId, d.actAs, d.readAs))
                  case _: StandardJWTPayload =>
                    -\/(Unauthorized("ledgerId, actAs, readAs missing in access token"))
                }
                (mLedgerId, mApplicationId, mActAs, mReadAs) = lpcr
                ledgerId <- mLedgerId
                  .toRightDisjunction(Unauthorized("ledgerId missing in access token"))
                applicationId <- mApplicationId
                  .toRightDisjunction(Unauthorized("applicationId missing in access token"))
                payload <- JwtPayload(
                  lar.LedgerId(ledgerId),
                  lar.ApplicationId(applicationId),
                  actAs = lar.Party.subst(mActAs),
                  readAs = lar.Party.subst(mReadAs),
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
  private[this] val logger = ContextualizedLogger.get(getClass)

  private[http] def errorResponse(error: Error): domain.ErrorResponse = {
    val (status, errorMsg): (StatusCode, String) = error match {
      case InvalidUserInput(e) => StatusCodes.BadRequest -> e
      case ParticipantServerError(grpcStatus, d) =>
        grpcStatus.asAkkaHttpForJsonApi -> s"$grpcStatus${d.cata((": " + _), "")}"
      case ServerError(_) => StatusCodes.InternalServerError -> "HTTP JSON API Server Error"
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

  private[http] def customDecodeAndParsePayload[A](jwt: Jwt, decodeJwt: ValidateJwt)(implicit
      parse: ParsePayload[A]
  ) =
    for {
      a <- decodeJwt(jwt): Unauthorized \/ DecodedJwt[String]
      p <- parse.parsePayload(a)
    } yield (jwt, p)

  private[http] def decodeAndParseUserIdFromToken(rawJwt: Jwt, decodeJwt: ValidateJwt)(implicit
      mf: Monad[Future]
  ): ET[UserId] =
    for {
      decodedJwt <- EitherT.either(decodeJwt(rawJwt): Unauthorized \/ DecodedJwt[String])
      result <- EitherT.either(CreateFromUserToken.parseUserIdFromToken(decodedJwt))
    } yield result

  private[http] def decodeAndParsePayload[A](
      jwt: Jwt,
      decodeJwt: ValidateJwt,
      userManagementClient: UserManagementClient,
      ledgerIdentityClient: LedgerIdentityClient,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      customParse: ParsePayload[A],
      createFromUserToken: CreateFromUserToken[A],
      fm: Monad[Future],
      ec: ExecutionContext,
  ): ET[(Jwt, A)] = {
    for {
      a <- EitherT.either(decodeJwt(jwt): Unauthorized \/ DecodedJwt[String])
      p <- customParse
        .parsePayload(a)
        .fold(
          { _ =>
            logger.debug("No custom token found, trying to process the token as user token.")
            createFromUserToken(
              a,
              userId => userManagementClient.listUserRights(userId, Some(jwt.value)),
              () => ledgerIdentityClient.getLedgerId(Some(jwt.value)),
            )
          },
          EitherT.pure(_): ET[A],
        )
    } yield (jwt, p)
  }
}
