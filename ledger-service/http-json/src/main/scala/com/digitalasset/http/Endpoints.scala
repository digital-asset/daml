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
import com.digitalasset.http.json.JsonProtocol._
import com.digitalasset.http.json.ResponseFormats._
import com.digitalasset.http.json.SprayJson.parse
import com.digitalasset.ledger.api.v1.value.Value
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.functor._
import scalaz.{-\/, @@, \/-}
import spray.json._
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}

import scala.concurrent.{ExecutionContext, Future}

class Endpoints(contractsService: ContractsService)(implicit ec: ExecutionContext)
    extends StrictLogging {

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
    case HttpRequest(
        POST,
        Uri.Path("/command/create"),
        _,
        HttpEntity.Strict(ContentTypes.`application/json`, data),
        _) =>
      HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, data))

    case HttpRequest(
        POST,
        Uri.Path("/command/exercise"),
        _,
        HttpEntity.Strict(ContentTypes.`application/json`, data),
        _) =>
      HttpResponse(entity = HttpEntity.Strict(ContentTypes.`application/json`, data))
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
          httpResponse(StatusCodes.BadRequest, format(errorsJsObject(StatusCodes.BadRequest)(e)))
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
