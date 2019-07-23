// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive, Directive1, Route}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.digitalasset.http.json.HttpCodec._
import com.digitalasset.http.json.ResponseFormats._
import com.digitalasset.http.json.SprayJson.parse
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.std.list._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{-\/, @@, Show, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class Endpoints(
    commandService: CommandService,
    contractsService: ContractsService,
    decoder: DomainJsonDecoder,
    encoder: DomainJsonEncoder,
    parallelism: Int = 8)(implicit ec: ExecutionContext)
    extends StrictLogging {

  import Endpoints._
  import json.JsonProtocol._

  // TODO(Leo) read it from the header
  private val jwtPayload =
    domain.JwtPayload(
      lar.LedgerId("ledgerId"),
      lar.ApplicationId("applicationId"),
      lar.Party("Alice"))

  private val extractJwtPayload: Directive1[domain.JwtPayload] =
    Directive(_(Tuple1(jwtPayload))) // TODO from header

  private val ok = HttpEntity(ContentTypes.`application/json`, """{"status": "OK"}""")

  lazy val all: PartialFunction[HttpRequest, HttpResponse] =
    command orElse contracts orElse notFound

  lazy val all2: Route = command2 ~ contracts2

  lazy val command: PartialFunction[HttpRequest, HttpResponse] = {
    case req @ HttpRequest(POST, Uri.Path("/command/create"), _, _, _) =>
      httpResponse(
        input(req)
          .map(decoder.decode[domain.CreateCommand])
          .mapAsync(parallelism) {
            case -\/(e) =>
              // TODO(Leo): we need to set status code in the result JSON
              Future.failed(InvalidUserInput(e.message))
            case \/-(c) =>
              commandService.create(jwtPayload, c)
          }
          .map { a: domain.ActiveContract[lav1.value.Value] =>
            encoder.encodeV(a) match {
              case -\/(e) => format(errorsJsObject(StatusCodes.InternalServerError)(e.shows))
              case \/-(b) => format(resultJsObject(b: JsValue))
            }
          }
      )

    case req @ HttpRequest(POST, Uri.Path("/command/exercise"), _, _, _) =>
      httpResponse(
        input(req)
          .map(decoder.decode[domain.ExerciseCommand])
          .mapAsync(parallelism) {
            case -\/(e) =>
              // TODO(Leo): we need to set status code in the result JSON
              Future.failed(InvalidUserInput(e.message))
            case \/-(c) =>
              commandService.exercise(jwtPayload, c)
          }
          .map { as: List[domain.ActiveContract[lav1.value.Value]] =>
            as.traverse(a => encoder.encodeV(a)) match {
              case -\/(e) => format(errorsJsObject(StatusCodes.InternalServerError)(e.shows))
              case \/-(bs) => format(resultJsObject(bs: List[JsValue]))
            }
          }
      )
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
      HttpResponse(entity = ok)

    case HttpRequest(GET, Uri.Path("/contracts/search"), _, _, _) =>
      httpResponse(
        contractsService
          .search(jwtPayload, emptyGetActiveContractsRequest)
          .map(x => format(resultJsObject(x.toString)))
      )

    case HttpRequest(POST, Uri.Path("/contracts/search"), _, HttpEntity.Strict(_, input), _) =>
      parse[domain.GetActiveContractsRequest](input) match {
        case -\/(e) =>
          httpResponse(
            StatusCodes.BadRequest,
            format(errorsJsObject(StatusCodes.BadRequest)(e.shows)))
        case \/-(a) =>
          httpResponse(
            contractsService.search(jwtPayload, a).map(x => format(resultJsObject(x.toString)))
          )
      }
  }

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

  sealed abstract class Error(message: String) extends RuntimeException(message)

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

  private[http] def input(req: HttpRequest): Source[ByteString, _] =
    req.entity.dataBytes.fold(ByteString.empty)(_ ++ _)
}
