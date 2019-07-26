// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.digitalasset.http.json.HttpCodec._
import com.digitalasset.http.json.ResponseFormats._
import com.digitalasset.http.json.SprayJson.decode
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.std.list._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{-\/, Show, \/, \/-}
import spray.json._
import com.digitalasset.http.json.SprayJson

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class Endpoints(
    ledgerId: lar.LedgerId,
    commandService: CommandService,
    contractsService: ContractsService,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    parallelism: Int = 8)(implicit ec: ExecutionContext)
    extends StrictLogging {

  import Endpoints._
  import json.JsonProtocol._

  implicit def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case NonFatal(e) =>
        extractUri { uri =>
          logger.error(s"Request to $uri could not be handled normally", e)
          complete(
            HttpResponse(
              StatusCodes.InternalServerError,
              entity = format(errorsJsObject(StatusCodes.InternalServerError)(e.getMessage))))
        }
    }

  // TODO(Leo) read it from the header
  private val jwtPayload =
    domain.JwtPayload(ledgerId, lar.ApplicationId("applicationId"), lar.Party("Alice"))

//  private val extractJwtPayload: Directive1[domain.JwtPayload] =
//    Directive(_(Tuple1(jwtPayload))) // TODO from header

  private val ok = HttpEntity(ContentTypes.`application/json`, """{"status": "OK"}""")

  lazy val all: PartialFunction[HttpRequest, HttpResponse] =
    command orElse contracts orElse notFound

//  lazy val all2: Route = command2 ~ contracts2

  lazy val command: PartialFunction[HttpRequest, HttpResponse] = {
    case req @ HttpRequest(POST, Uri.Path("/command/create"), _, _, _) =>
      httpResponse(
        input(req)
          .map(decoder.decodeR[domain.CreateCommand])
          .mapAsync(1) {
            case -\/(e) => invalidUserInput(e)
            case \/-(c) => handleFutureFailure(commandService.create(jwtPayload, c))
          }
          .map { fa =>
            fa.flatMap { a: domain.ActiveContract[lav1.value.Value] =>
              encoder.encodeV(a).leftMap(e => ServerError(e.shows))
            }
          }
          .map(encodeResult)
      )

    case req @ HttpRequest(POST, Uri.Path("/command/exercise"), _, _, _) =>
      httpResponse(
        input(req)
          .map(decoder.decodeR[domain.ExerciseCommand])
          .mapAsync(1) {
            case -\/(e) => invalidUserInput(e)
            case \/-(c) => handleFutureFailure(commandService.exercise(jwtPayload, c))
          }
          .map { fa =>
            fa.flatMap { as: List[domain.ActiveContract[lav1.value.Value]] =>
              as.traverse(a => encoder.encodeV(a))
                .leftMap(e => ServerError(e.shows))
                .flatMap(as => encodeList(as))
            }
          }
          .map(encodeResult)
      )
  }

  private def invalidUserInput[A: Show, B](a: A): Future[InvalidUserInput \/ B] =
    Future.successful(-\/(InvalidUserInput(a.shows)))

  private def handleFutureFailure[A: Show, B](fa: Future[A \/ B]): Future[ServerError \/ B] =
    fa.map(a => a.leftMap(e => ServerError(e.shows))).recover {
      case NonFatal(e) =>
        logger.error("Future failed", e)
        -\/(ServerError(e.getMessage))
    }

  private def encodeList(as: List[JsValue]): ServerError \/ JsValue =
    SprayJson.encode(as).leftMap(e => ServerError(e.shows))

  private def encodeResult(a: Error \/ JsValue): ByteString = a match {
    case \/-(a) => format(resultJsObject(a: JsValue))
    case -\/(InvalidUserInput(e)) => format(errorsJsObject(StatusCodes.BadRequest)(e))
    case -\/(ServerError(e)) => format(errorsJsObject(StatusCodes.InternalServerError)(e))
  }

  lazy val command2: Route =
    post {
      path("/command/create") {
        entity(as[JsValue]) { data =>
          complete(data)
        }
      } ~
        path("/command/exercise") {
          entity(as[JsValue]) { data =>
            complete(data)
          }
        }
    }

  private val emptyGetActiveContractsRequest = domain.GetActiveContractsRequest(Set.empty)

  lazy val contracts: PartialFunction[HttpRequest, HttpResponse] = {
    case HttpRequest(GET, Uri.Path("/contracts/lookup"), _, _, _) =>
      HttpResponse(entity = ok) // TODO(Leo): hookup contracts lookup service

    case HttpRequest(GET, Uri.Path("/contracts/search"), _, _, _) =>
      httpResponse(
        contractsService
          .search(jwtPayload, emptyGetActiveContractsRequest)
          .map(encodeResultJsObject)
      )

    case HttpRequest(POST, Uri.Path("/contracts/search"), _, HttpEntity.Strict(_, input), _) => // TODO(Leo): get rid of Strict
      decode[domain.GetActiveContractsRequest](input.utf8String) match {
        case -\/(e) =>
          httpResponse(
            StatusCodes.BadRequest,
            format(errorsJsObject(StatusCodes.BadRequest)(e.shows)))
        case \/-(a) =>
          httpResponse(contractsService.search(jwtPayload, a).map(encodeResultJsObject))
      }
  }

  private def encodeResultJsObject(
      as: Seq[domain.GetActiveContractsResponse[lav1.value.Value]]): ByteString = {
    as.toList.traverse(a => encoder.encodeV(a)) match {
      case -\/(e) => format(errorsJsObject(StatusCodes.InternalServerError)(e.shows))
      case \/-(bs) => format(resultJsObject(bs: List[JsValue]))
    }
  }
  /*
  lazy val contracts2: Route =
    path("/contracts/lookup") {
      post {
        extractJwtPayload { jwt =>
          entity(as[domain.ContractLookupRequest[JsValue] @@ JsonApi]) {
            case JsonApi(clr) =>
              // TODO SC: the result gets labelled "result" not "contract"; does this matter?
              complete(
                JsonApi.subst(
                  contractsService
                    .lookup(jwt, clr map placeholderLfValueDec)
                    .map(oac => resultJsObject(oac.map(_.map(placeholderLfValueEnc))))))
          }
        }
      }
    } ~
      path("contracts/search") {
        get {
          extractJwtPayload { jwt =>
            complete(
              JsonApi.subst(
                contractsService
                  .search(jwt, emptyGetActiveContractsRequest)
                  .map(sgacr => resultJsObject(sgacr.map(_.map(placeholderLfValueEnc))))))
          }
        } ~
          post {
            extractJwtPayload { jwt =>
              entity(as[domain.GetActiveContractsRequest @@ JsonApi]) {
                case JsonApi(gacr) =>
                  complete(
                    JsonApi.subst(contractsService
                      .search(jwt, gacr)
                      .map(sgacr => resultJsObject(sgacr.map(_.map(placeholderLfValueEnc))))))
              }
            }
          }
      }
   */
  private def httpResponse(status: StatusCode, output: ByteString): HttpResponse =
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, output))

  private def httpResponse(output: Future[ByteString]): HttpResponse =
    HttpResponse(entity =
      HttpEntity.CloseDelimited(ContentTypes.`application/json`, Source.fromFuture(output)))

  private def httpResponse(output: Source[ByteString, _]): HttpResponse =
    HttpResponse(entity = HttpEntity.CloseDelimited(ContentTypes.`application/json`, output))

  // TODO SC: this is a placeholder because we can't do this accurately
  // without type context
  private def placeholderLfValueEnc(v: lav1.value.Value): JsValue =
    JsString(v.sum.toString)

  // TODO SC: this is a placeholder because we can't do this accurately
  // without type context
  private def placeholderLfValueDec(v: JsValue): lav1.value.Value =
    lav1.value.Value(lav1.value.Value.Sum.Text(v.toString))

  lazy val notFound: PartialFunction[HttpRequest, HttpResponse] = {
    case HttpRequest(_, _, _, _, _) => HttpResponse(status = StatusCodes.NotFound)
  }

  private def format(a: JsValue): ByteString =
    ByteString(a.compactPrint)
}

object Endpoints {

  sealed abstract class Error(message: String) extends Product with Serializable

  final case class InvalidUserInput(message: String) extends Error(message)

  final case class ServerError(message: String) extends Error(message)

  object Error {
    implicit val ShowInstance: Show[Error] = new Show[Error] {
      override def shows(f: Error): String = f match {
        case InvalidUserInput(message) => s"InvalidUserInput: ${message: String}"
        case ServerError(message) => s"ServerError: ${message: String}"
      }
    }
  }

  private[http] def input(req: HttpRequest): Source[String, _] =
    req.entity.dataBytes.fold(ByteString.empty)(_ ++ _).map(_.utf8String).take(1L)
}
