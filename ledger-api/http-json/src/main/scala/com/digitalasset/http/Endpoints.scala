// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.digitalasset.http.json.JsonProtocol._
import com.typesafe.scalalogging.StrictLogging
import scalaz.\/
import spray.json._

import scala.util.Try

class Endpoints extends StrictLogging {

  private val ok = HttpEntity(ContentTypes.`application/json`, """{"status": "OK"}""")

  lazy val all = command orElse contracts orElse notFound

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

  lazy val contracts: PartialFunction[HttpRequest, HttpResponse] = {

    case HttpRequest(GET, Uri.Path("/contracts/lookup"), _, _, _) =>
      HttpResponse(entity = ok)

    case HttpRequest(GET, Uri.Path("/contracts/search"), _, _, _) =>
      HttpResponse(entity = ok)

    case req @ HttpRequest(POST, Uri.Path("/contracts/search"), _, _, _) =>
      val flow: Flow[ByteString, ByteString, _] =
        Flow[ByteString].fold(ByteString.empty)(_ ++ _).map { s: ByteString =>
          parse[domain.GetActiveContractsRequest](s) fold (
            e => format(errorResult(e)),
            a => format(successResult(a))
          )
        }
      HttpResponse(
        status = StatusCodes.BadRequest,
        entity =
          req.entity.transformDataBytes(flow).withContentType(ContentTypes.`application/json`))
  }

  lazy val notFound: PartialFunction[HttpRequest, HttpResponse] = {
    case HttpRequest(_, _, _, _, _) => HttpResponse(status = StatusCodes.NotFound)
  }

  private def parse[A: JsonFormat](str: ByteString): String \/ A =
    Try {
      val jsonAst: JsValue = str.utf8String.parseJson
      jsonAst.convertTo[A]
    } fold (t => \/.left(s"JSON parser error: ${t.getMessage}"), a => \/.right(a))

  private def format[A: JsonFormat](a: A): ByteString = {
    val jsonAst: JsValue = a.toJson
    val str: String = jsonAst.compactPrint
    ByteString(str)
  }

  private def format(a: JsValue): ByteString =
    ByteString(a.compactPrint)

  private def errorResult(es: String*): JsValue = {
    val errors = JsArray(es.map(JsString(_)).toVector)
    JsObject(("errors", errors))
  }

  private def successResult[A: JsonFormat](a: A): JsValue = {
    val result: JsValue = a.toJson
    JsObject(("result", result))
  }
}
