// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.digitalasset.http.json.JsonProtocol._
import com.typesafe.scalalogging.StrictLogging
import scalaz.{-\/, \/, \/-}
import spray.json._
import json.HttpCodec._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class Endpoints(contractsService: ContractsService)(implicit ec: ExecutionContext)
    extends StrictLogging {

  // TODO(Leo) read it from the header
  private val jwtPayload =
    domain.JwtPayload(ledgerId = "ledgerId", applicationId = "applicationId", party = "Alice")

  private val ok = HttpEntity(ContentTypes.`application/json`, """{"status": "OK"}""")

  lazy val all: PartialFunction[HttpRequest, HttpResponse] =
    command orElse contracts orElse notFound

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

  lazy val command2 =
    post {
      path("/command/create") {
        entity(as[JsValue]) { data =>
          complete(data)
        }
      } ~
        path("/command/create") {
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

  private def httpResponse(status: StatusCode, output: ByteString): HttpResponse =
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, output))

  private def httpResponse(output: Future[ByteString]): HttpResponse =
    HttpResponse(entity =
      HttpEntity.CloseDelimited(ContentTypes.`application/json`, Source.fromFuture(output)))

  lazy val notFound: PartialFunction[HttpRequest, HttpResponse] = {
    case HttpRequest(_, _, _, _, _) => HttpResponse(status = StatusCodes.NotFound)
  }

  private def parse[A: JsonFormat](str: ByteString): String \/ A =
    Try {
      val jsonAst: JsValue = str.utf8String.parseJson
      jsonAst.convertTo[A]
    } fold (t => \/.left(s"JSON parser error: ${t.getMessage}"), a => \/.right(a))

  private def format(a: JsValue): ByteString =
    ByteString(a.compactPrint)

  private def errorsJsObject(status: StatusCode)(es: String*): JsObject = {
    val errors = JsArray(es.map(JsString(_)).toVector)
    JsObject(statusField(status), ("errors", errors))
  }

  private def resultJsObject[A: JsonFormat](a: A): JsObject = {
    val result: JsValue = a.toJson
    JsObject(statusField(StatusCodes.OK), ("result", result))
  }

  private def statusField(status: StatusCode): (String, JsNumber) =
    ("status", JsNumber(status.intValue()))
}
