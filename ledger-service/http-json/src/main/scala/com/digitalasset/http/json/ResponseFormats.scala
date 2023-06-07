// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import akka.NotUsed
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import scalaz.syntax.show._
import scalaz.{Show, -\/, \/, \/-}
import spray.json.DefaultJsonProtocol._
import spray.json._

private[http] object ResponseFormats {
  def errorsJsObject(status: StatusCode, es: String*): JsObject = {
    val errors = es.toJson
    JsObject(statusField(status), ("errors", errors))
  }

  def resultJsObject[A: JsonWriter](a: A): JsObject = {
    resultJsObject(a.toJson)
  }

  def resultJsObject(a: JsValue): JsObject = {
    JsObject(statusField(StatusCodes.OK), ("result", a))
  }

  def resultJsObject[E: Show](
      jsVals: Source[E \/ JsValue, NotUsed],
      warnings: Option[JsValue],
  ): Source[ByteString, NotUsed] = {
    jsVals
      // Collapse the stream of `E \/ JsValue` into a single pair of errors and results,
      // only one of which may be non-empty.
      .fold((Vector.empty[E], Vector.empty[JsValue])) {
        case ((errors, results), \/-(r)) if errors.isEmpty => (Vector.empty, results :+ r)
        case ((errors, _), \/-(_)) => (errors, Vector.empty)
        case ((errors, _), -\/(e)) => (errors :+ e, Vector.empty)
      }
      // Convert that into the appropriate JSON response object
      .map {
        case (errors, results) => {
          val response = JsObject(
            (
              if (errors.isEmpty)
                Map[String, JsValue](
                  "result" -> JsArray(results),
                  statusField(StatusCodes.OK),
                )
              else
                Map[String, JsValue](
                  "errors" -> JsArray(errors.map(e => JsString(e.shows))),
                  statusField(StatusCodes.InternalServerError),
                )
            )
              ++
                warnings.toList.map("warnings" -> _).toMap
          )

          ByteString(response.compactPrint)
        }
      }
  }

  def statusField(status: StatusCode): (String, JsNumber) =
    ("status", JsNumber(status.intValue()))
}
