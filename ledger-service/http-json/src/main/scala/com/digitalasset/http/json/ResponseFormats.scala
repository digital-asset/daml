// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import akka.http.scaladsl.model._
import spray.json._
import spray.json.DefaultJsonProtocol._

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

  def statusField(status: StatusCode): (String, JsNumber) =
    ("status", JsNumber(status.intValue()))
}
