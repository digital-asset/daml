// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive, Directive1, Route}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.digitalasset.http.Endpoints.InvalidUserInput
import com.digitalasset.http.json.HttpCodec._
import com.digitalasset.http.json.ResponseFormats._
import com.digitalasset.http.json.SprayJson.parse
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.v1.value.Value
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{-\/, @@, Traverse, \/, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds

class Endpoints(
    commandService: CommandService,
    contractsService: ContractsService,
    apiValueToLfValue: ApiValueToLfValueConverter.ApiValueToLfValue,
    parallelism: Int = 8)(implicit ec: ExecutionContext)
    extends StrictLogging {

  import json.JsonProtocol._
  implicit val apiValueWriter = valueWriter(apiValueToLfValue)

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
        parseInput[domain.CreateCommand[JsValue]](req)
          .map(x => x.flatMap(parseValue[domain.CreateCommand]))
          .mapAsync(parallelism) {
            case -\/(e) =>
              Future.failed(e) // TODO(Leo): we need to set status code in the result JSON
            case \/-(c) =>
              commandService.create(jwtPayload, c)
          }
          .map { a: domain.ActiveContract[lav1.value.Value] =>
            format(resultJsObject(toJsValue(a): domain.ActiveContract[JsValue]))
          }
      )

    case req @ HttpRequest(POST, Uri.Path("/command/exercise"), _, _, _) =>
      httpResponse(
        parseInput[domain.ExerciseCommand[JsValue]](req)
          .map(x => x.flatMap(parseValue[domain.ExerciseCommand]))
          .mapAsync(parallelism) {
            case -\/(e) =>
              Future.failed(e) // TODO(Leo): we need to set status code in the result JSON
            case \/-(c) =>
              commandService.exercise(jwtPayload, c)
          }
          .map { as: List[domain.ActiveContract[lav1.value.Value]] =>
            format(resultJsObject(as.map(toJsValue): List[domain.ActiveContract[JsValue]]))
          }
      )
  }

  private def parseInput[A: JsonReader](req: HttpRequest): Source[InvalidUserInput \/ A, _] =
    req.entity.dataBytes
      .fold(ByteString.empty)(_ ++ _)
      .map(data => parse[A](data).leftMap(e => InvalidUserInput(e.shows)))

  private def parseValue[F[_]: Traverse](
      fa: F[JsValue]): InvalidUserInput \/ F[lav1.value.Value] = {
    fa.traverseU(jsValue =>
      json.JsonProtocol.jsValueToApiValue(jsValue).leftMap(e => InvalidUserInput(e.shows)))
  }

  private def toJsValue(
      a: domain.ActiveContract[lav1.value.Value]): domain.ActiveContract[JsValue] = {
    a.map(_.toJson)
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
  private def placeholderLfValueEnc(v: Value): JsValue =
    JsString(v.sum.toString)

  // TODO SC: this is a placeholder because we can't do this accurately
  // without type context
  private def placeholderLfValueDec(v: JsValue): Value =
    Value(Value.Sum.Text(v.toString))

  lazy val notFound: PartialFunction[HttpRequest, HttpResponse] = {
    case HttpRequest(_, _, _, _, _) => HttpResponse(status = StatusCodes.NotFound)
  }

  private def format(a: JsValue): ByteString =
    ByteString(a.compactPrint)
}

object Endpoints {
  final case class InvalidUserInput(message: String) extends RuntimeException(message)
}
