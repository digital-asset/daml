// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.NotUsed
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.util.ByteString
import com.digitalasset.daml.lf
import com.digitalasset.http.Statement.discard
import com.digitalasset.http.domain.JwtPayload
import com.digitalasset.http.json.ResponseFormats
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder, SprayJson}
import com.digitalasset.http.util.FutureUtil.{either, eitherT}
import com.digitalasset.http.util.{ApiValueToLfValueConverter, FutureUtil}
import com.digitalasset.jwt.domain.{DecodedJwt, Jwt}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, Show, \/, \/-}
import spray.json._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import akka.stream.Materializer
import akka.stream.scaladsl.{Source, Flow}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class Endpoints(
    ledgerId: lar.LedgerId,
    decodeJwt: Endpoints.ValidateJwt,
    commandService: CommandService,
    contractsService: ContractsService,
    partiesService: PartiesService,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    maxTimeToCollectRequest: FiniteDuration = FiniteDuration(5, "seconds"))(
    implicit ec: ExecutionContext,
    mat: Materializer)
    extends StrictLogging {

  import Endpoints._
  import json.JsonProtocol._

  lazy val all: PartialFunction[HttpRequest, Future[HttpResponse]] =
    command orElse contracts orElse parties

  lazy val command: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req @ HttpRequest(POST, Uri.Path("/command/create"), _, _, _) =>
      val et: ET[JsValue] = for {
        t3 <- FutureUtil.eitherT(input(req)): ET[(Jwt, JwtPayload, String)]

        (jwt, jwtPayload, reqBody) = t3

        cmd <- either(
          decoder
            .decodeR[domain.CreateCommand](reqBody)
            .leftMap(e => InvalidUserInput(e.shows))
        ): ET[domain.CreateCommand[lav1.value.Record]]

        ac <- eitherT(
          handleFutureFailure(commandService.create(jwt, jwtPayload, cmd))
        ): ET[domain.ActiveContract[lav1.value.Value]]

        jsVal <- either(encoder.encodeV(ac).leftMap(e => ServerError(e.shows))): ET[JsValue]

      } yield jsVal

      httpResponse(et)

    case req @ HttpRequest(POST, Uri.Path("/command/exercise"), _, _, _) =>
      val et: ET[JsValue] = for {
        t3 <- eitherT(input(req)): ET[(Jwt, JwtPayload, String)]

        (jwt, jwtPayload, reqBody) = t3

        cmd <- either(
          decoder
            .decodeR[domain.ExerciseCommand](reqBody)
            .leftMap(e => InvalidUserInput(e.shows))
        ): ET[domain.ExerciseCommand[lav1.value.Record]]

        cs <- eitherT(
          handleFutureFailure(commandService.exercise(jwt, jwtPayload, cmd))
        ): ET[List[domain.Contract[lav1.value.Value]]]

        jsVal <- either(
          cs.traverse(a => encoder.encodeV(a))
            .leftMap(e => ServerError(e.shows))
            .flatMap(as => encodeList(as))): ET[JsValue]

      } yield jsVal

      httpResponse(et)
  }

  private def handleFutureFailure[A: Show, B](fa: Future[A \/ B]): Future[ServerError \/ B] =
    fa.map(a => a.leftMap(e => ServerError(e.shows))).recover {
      case NonFatal(e) =>
        logger.error("Future failed", e)
        -\/(ServerError(e.getMessage))
    }

  private def handleFutureFailure[A](fa: Future[A]): Future[ServerError \/ A] =
    fa.map(a => \/-(a)).recover {
      case NonFatal(e) =>
        logger.error("Future failed", e)
        -\/(ServerError(e.getMessage))
    }

  private def handleSourceFailure[E: Show, A]: Flow[E \/ A, ServerError \/ A, NotUsed] =
    Flow
      .fromFunction((_: E \/ A).leftMap(e => ServerError(e.shows)))
      .recover {
        case NonFatal(e) =>
          logger.error("Source failed", e)
          -\/(ServerError(e.getMessage))
      }

  private def encodeList(as: Seq[JsValue]): ServerError \/ JsValue =
    SprayJson.encode(as).leftMap(e => ServerError(e.shows))

  lazy val contracts: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req @ HttpRequest(GET, Uri.Path("/contracts/lookup"), _, _, _) =>
      val et: ET[JsValue] = for {
        input <- FutureUtil.eitherT(input(req)): ET[(Jwt, JwtPayload, String)]

        (jwt, jwtPayload, reqBody) = input

        cmd <- either(
          decoder
            .decodeV[domain.ContractLookupRequest](reqBody)
            .leftMap(e => InvalidUserInput(e.shows))
        ): ET[domain.ContractLookupRequest[ApiValue]]

        ac <- eitherT(
          handleFutureFailure(contractsService.lookup(jwt, jwtPayload, cmd))
        ): ET[Option[domain.ActiveContract[LfValue]]]

        jsVal <- either(
          ac.cata(x => lfAcToJsValue(x).leftMap(e => ServerError(e.shows)), \/-(JsObject()))
        ): ET[JsValue]

      } yield jsVal

      httpResponse(et)

    case req @ HttpRequest(GET, Uri.Path("/contracts/search"), _, _, _) =>
      val sourceF: Future[Error \/ Source[Error \/ JsValue, NotUsed]] = input(req).map {
        _.map {
          case (jwt, jwtPayload, _) =>
            contractsService
              .retrieveAll(jwt, jwtPayload)
              .via(handleSourceFailure)
              .map(_.flatMap(lfAcToJsValue)): Source[Error \/ JsValue, NotUsed]
        }
      }

      httpResponse(sourceF)

    case req @ HttpRequest(POST, Uri.Path("/contracts/search"), _, _, _) =>
      val sourceF: Future[Error \/ Source[Error \/ JsValue, NotUsed]] = input(req).map {
        _.flatMap {
          case (jwt, jwtPayload, reqBody) =>
            SprayJson
              .decode[domain.GetActiveContractsRequest](reqBody)
              .leftMap(e => InvalidUserInput(e.shows))
              .map { cmd =>
                contractsService
                  .search(jwt, jwtPayload, cmd)
                  .via(handleSourceFailure)
                  .map(_.flatMap(lfAcToJsValue)): Source[Error \/ JsValue, NotUsed]
              }
        }
      }

      httpResponse(sourceF)
  }

  lazy val parties: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req @ HttpRequest(GET, Uri.Path("/parties"), _, _, _) =>
      val et: ET[JsValue] = for {
        _ <- FutureUtil.eitherT(input(req)): ET[(Jwt, JwtPayload, String)]
        ps <- FutureUtil.rightT(partiesService.allParties()): ET[List[domain.PartyDetails]]
        jsVal <- either(SprayJson.encode(ps)).leftMap(e => ServerError(e.shows)): ET[JsValue]
      } yield jsVal

      httpResponse(et)
  }

  private def apiValueToLfValue(a: ApiValue): Error \/ LfValue =
    ApiValueToLfValueConverter.apiValueToLfValue(a).leftMap(e => ServerError(e.shows))

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.fromTryCatchNonFatal(LfValueCodec.apiValueToJsValue(a)).leftMap(e =>
      ServerError(e.getMessage))

  private def collectActiveContracts(
      predicates: Map[domain.TemplateId.RequiredPkg, LfValue => Boolean]): PartialFunction[
    Error \/ domain.ActiveContract[LfValue],
    Error \/ domain.ActiveContract[LfValue]
  ] = {
    case e @ -\/(_) => e
    case a @ \/-(ac) if predicates.get(ac.templateId).forall(f => f(ac.argument)) => a
  }

  private def errorToJsValue(e: Error): JsValue = errorsJsObject(e)._2

  private def lfAcToJsValue(a: domain.ActiveContract[LfValue]): Error \/ JsValue = {
    for {
      b <- a.traverse(lfValueToJsValue): Error \/ domain.ActiveContract[JsValue]
      c <- SprayJson.encode(b).leftMap(e => ServerError(e.shows))
    } yield c
  }

  private def httpResponse(output: ET[JsValue]): Future[HttpResponse] = {
    val fa: Future[Error \/ JsValue] = output.run
    fa.map {
        case \/-(a) => httpResponseOk(a)
        case -\/(e) => httpResponseError(e)
      }
      .recover {
        case NonFatal(e) => httpResponseError(ServerError(e.getMessage))
      }
  }

  private def httpResponse(
      output: Future[Error \/ Source[Error \/ JsValue, NotUsed]]): Future[HttpResponse] =
    output
      .map {
        case -\/(e) => httpResponseError(e)
        case \/-(source) => httpResponseFromSource(source)
      }
      .recover {
        case NonFatal(e) => httpResponseError(ServerError(e.getMessage))
      }

  private def httpResponseOk(data: JsValue): HttpResponse =
    httpResponse(StatusCodes.OK, ResponseFormats.resultJsObject(data))

  private def httpResponseError(error: Error): HttpResponse = {
    val (status, jsObject) = errorsJsObject(error)
    httpResponse(status, jsObject)
  }

  private def errorsJsObject(error: Error): (StatusCode, JsObject) = {
    val (status, errorMsg): (StatusCode, String) = error match {
      case InvalidUserInput(e) => StatusCodes.BadRequest -> e
      case ServerError(e) => StatusCodes.InternalServerError -> e
      case Unauthorized(e) => StatusCodes.Unauthorized -> e
      case NotFound(e) => StatusCodes.NotFound -> e
    }
    (status, ResponseFormats.errorsJsObject(status, errorMsg))
  }

  private def httpResponse(status: StatusCode, data: JsValue): HttpResponse = {
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, format(data)))
  }

  private def httpResponseFromSource(data: Source[Error \/ JsValue, NotUsed]): HttpResponse =
    HttpResponse(
      status = StatusCodes.OK,
      entity = HttpEntity
        .CloseDelimited(ContentTypes.`application/json`, ResponseFormats.resultJsObject(data))
    )

  lazy val notFound: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case HttpRequest(method, uri, _, _, _) =>
      Future.successful(httpResponseError(NotFound(s"${method: HttpMethod}, uri: ${uri: Uri}")))
  }

  private def format(a: JsValue): ByteString = ByteString(a.compactPrint)

  private[http] def input(req: HttpRequest): Future[Unauthorized \/ (Jwt, JwtPayload, String)] = {
    findJwt(req).flatMap(decodeAndParsePayload) match {
      case e @ -\/(_) =>
        discard { req.entity.discardBytes(mat) }
        Future.successful(e)
      case \/-((j, p)) =>
        req.entity
          .toStrict(maxTimeToCollectRequest)
          .map(b => \/-((j, p, b.data.utf8String)))
    }
  }

  private[http] def findJwt(req: HttpRequest): Unauthorized \/ Jwt =
    req.headers
      .collectFirst {
        case Authorization(OAuth2BearerToken(token)) => Jwt(token)
      }
      .toRightDisjunction(Unauthorized("missing Authorization header with OAuth 2.0 Bearer Token"))

  private def decodeAndParsePayload(jwt: Jwt): Unauthorized \/ (Jwt, JwtPayload) =
    for {
      a <- decodeJwt(jwt): Unauthorized \/ DecodedJwt[String]
      p <- parsePayload(a)
    } yield (jwt, p)

  private def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ JwtPayload =
    SprayJson.decode[JwtPayload](jwt.payload).leftMap(e => Unauthorized(e.shows))
}

object Endpoints {

  private type ET[A] = EitherT[Future, Error, A]

  private type ApiValue = lav1.value.Value

  private type LfValue = lf.value.Value[lf.value.Value.AbsoluteContractId]

  private type ActiveContractStream[A] = Source[A, NotUsed]

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
}
