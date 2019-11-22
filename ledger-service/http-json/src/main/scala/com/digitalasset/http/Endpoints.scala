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
import akka.stream.scaladsl.Source

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
    command orElse contracts orElse parties orElse notFound

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

//  private def handleSourceFailure[E: Show, A, M](
//      soure: Source[E \/ A, M]): Source[ServerError \/ A, M] =
//    source

  private def handleSourceFailure[A, M](soure: Source[A, M]): Source[ServerError \/ A, M] =
    soure.map(a => \/-(a)).recover {
      case NonFatal(e) =>
        logger.error("Source failed", e)
        -\/(ServerError(e.getMessage))
    }

  private def encodeList(as: Seq[JsValue]): ServerError \/ JsValue =
    SprayJson.encode(as).leftMap(e => ServerError(e.shows))

  private val emptyGetActiveContractsRequest =
    domain.GetActiveContractsRequest(Set.empty, Map.empty)

  lazy val contracts: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req @ HttpRequest(GET, Uri.Path("/contracts/lookup"), _, _, _) =>
      val et: ET[JsValue] = for {
        input <- FutureUtil.eitherT(input(req)): ET[(Jwt, JwtPayload, String)]

        (jwt, jwtPayload, reqBody) = input

        cmd <- either(
          decoder
            .decodeV[domain.ContractLookupRequest](reqBody)
            .leftMap(e => InvalidUserInput(e.shows))
        ): ET[domain.ContractLookupRequest[lav1.value.Value]]

        ac <- eitherT(
          handleFutureFailure(contractsService.lookup(jwt, jwtPayload, cmd))
        ): ET[Option[domain.ActiveContract[lav1.value.Value]]]

        jsVal <- either(
          ac match {
            case None => \/-(JsObject())
            case Some(x) => encoder.encodeV(x).leftMap(e => ServerError(e.shows))
          }
        ): ET[JsValue]

      } yield jsVal

      httpResponse(et)

    case req @ HttpRequest(GET, Uri.Path("/contracts/search"), _, _, _) =>
      val sourceF: Future[Error \/ Source[JsValue, NotUsed]] = input(req).map { inputOrError =>
        for {
          x <- inputOrError: Unauthorized \/ (Jwt, JwtPayload, String)
          (jwt, jwtPayload, _) = x

          x <- contractsService
            .search(jwt, jwtPayload, emptyGetActiveContractsRequest)
            .leftMap(e => ServerError(e.shows))
          (source, _) = x

          jsValueSource = toJsValueSource(handleSourceFailure(source)): Source[JsValue, NotUsed]

        } yield jsValueSource
      }

      httpResponse(sourceF)

    /*
    case req @ HttpRequest(POST, Uri.Path("/contracts/search"), _, _, _) =>
      val et: ET[JsValue] = for {
        input <- FutureUtil.eitherT(input(req)): ET[(Jwt, JwtPayload, String)]

        (jwt, jwtPayload, reqBody) = input

        cmd <- either(
          SprayJson
            .decode[domain.GetActiveContractsRequest](reqBody)
            .leftMap(e => InvalidUserInput(e.shows))
        ): ET[domain.GetActiveContractsRequest]

        as <- eitherT(
          handleFutureFailure(contractsService.search(jwt, jwtPayload, cmd))
        ): ET[Seq[contractsService.Result]]

        xs <- either(
          as.toList.traverse { case (a, p) => a.traverse(v => apValueToLfValue(v)).map((_, p)) }
        ): ET[List[(domain.ActiveContract[LfValue], contractsService.CompiledPredicates)]]

        ys = contractsService
          .filterSearch(as._2, xs): Seq[domain.ActiveContract[LfValue]]

        js <- either(
          ys.toList.traverse(_.traverse(v => lfValueToJsValue(v)))
        ): ET[Seq[domain.ActiveContract[JsValue]]]

        j <- either(SprayJson.encode(js).leftMap(e => ServerError(e.shows))): ET[JsValue]

      } yield j

      httpResponse(et)
   */
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

  private def apValueToLfValue(a: ApiValue): Error \/ LfValue =
    ApiValueToLfValueConverter.apiValueToLfValue(a).leftMap(e => ServerError(e.shows))

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.fromTryCatchNonFatal(LfValueCodec.apiValueToJsValue(a)).leftMap(e =>
      ServerError(e.getMessage))

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
      output: Future[Error \/ Source[JsValue, NotUsed]]): Future[HttpResponse] =
    output
      .map {
        case \/-(source) => httpResponseFromSource(StatusCodes.OK, source)
        case -\/(e) => httpResponseError(e)
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

  private def httpResponseFromSource(
      status: StatusCode,
      data: Source[JsValue, NotUsed]): HttpResponse =
    HttpResponse(
      status = status,
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

  private def toJsValueSource(as: Source[ServerError \/ domain.ActiveContract[ApiValue], NotUsed])
    : Source[JsValue, NotUsed] = {
    as.map {
      case -\/(e) =>
        errorsJsObject(e)._2: JsObject
      case \/-(a) =>
        encoder
          .encodeV[domain.ActiveContract](a)
          .fold(e => errorsJsObject(ServerError(e.shows))._2, identity): JsValue
    }
  }
}

object Endpoints {

  private type ET[A] = EitherT[Future, Error, A]

  private type ApiValue = lav1.value.Value

  private type LfValue = lf.value.Value[lf.value.Value.AbsoluteContractId]

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
