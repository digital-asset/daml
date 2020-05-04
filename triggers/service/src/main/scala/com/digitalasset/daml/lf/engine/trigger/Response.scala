// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model._
import spray.json.DefaultJsonProtocol._
import spray.json._

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
  def errorsJsObject(status: StatusCode, es: Seq[String]): JsObject = {
    val errors = es.toJson
    JsObject(statusField(status), ("errors", errors))
  }

  def resultJsObject[A: JsonWriter](a: A): JsObject = {
    resultJsObject(a.toJson)
  }

  def resultJsObject(a: JsValue): JsObject = {
    JsObject(statusField(StatusCodes.OK), ("result", a))
  }

  def statusField(status: StatusCode): (String, JsNumber) =
    ("status", JsNumber(status.intValue()))
}
