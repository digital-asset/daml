// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Concat, Source}
import akka.util.ByteString
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

  private val start: Source[ByteString, NotUsed] = Source.single(ByteString("{result=["))

  private val end: Source[ByteString, NotUsed] = Source.single(ByteString("]}"))

  def resultJsObject(jsVals: Source[JsValue, NotUsed]): Source[ByteString, NotUsed] = {
    val csv: Source[ByteString, NotUsed] = jsVals.zipWithIndex.map {
      case (a, i) =>
        if (i == 0L) ByteString(a.compactPrint)
        else ByteString("," + a.compactPrint)
    }

    Source.combine(start, csv, end)(Concat.apply)
  }

  def statusField(status: StatusCode): (String, JsNumber) =
    ("status", JsNumber(status.intValue()))
}
