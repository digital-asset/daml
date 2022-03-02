// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model._
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

// The HTTP service can `complete` using one of these functions to construct a
// a response with a JSON object and status code matching the one in the body.
object Response {
  def successResponse[A: JsonWriter](a: A): (StatusCode, JsObject) = {
    (StatusCodes.OK, resultJsObject(a))
  }

  def errorResponse(status: StatusCode, es: String*): (StatusCode, JsObject) = {
    (status, errorsJsObject(status, es))
  }

  // These functions are borrowed from the HTTP JSON ledger API but I haven't
  // factored them out for now as they are fairly small.
  private[this] def errorsJsObject(status: StatusCode, es: Seq[String]): JsObject = {
    val errors = es.toJson
    JsObject(statusField(status), ("errors", errors))
  }

  private[this] def resultJsObject[A: JsonWriter](a: A): JsObject = {
    resultJsObject(a.toJson)
  }

  private[this] def resultJsObject(a: JsValue): JsObject = {
    JsObject(statusField(StatusCodes.OK), ("result", a))
  }

  private[this] def statusField(status: StatusCode): (String, JsNumber) =
    ("status", JsNumber(status.intValue()))

  // Trigger status messages have timestamps for which this is the
  // formatter.
  implicit val LocalDateTimeJsonFormat: RootJsonFormat[LocalDateTime] =
    new RootJsonFormat[LocalDateTime] {
      override def write(dt: LocalDateTime) =
        JsString(dt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

      override def read(json: JsValue): LocalDateTime = json match {
        case JsString(s) =>
          try LocalDateTime.parse(s, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          catch {
            case e: DateTimeParseException =>
              deserializationError("Decode local datetime failed", e)
          }
        case _ => deserializationError("Decode local datetime failed")
      }
    }

}
