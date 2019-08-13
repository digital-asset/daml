// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import scalaz.{-\/, Show, \/, \/-}
import spray.json.{JsValue, JsonReader, _}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object SprayJson {
  sealed abstract class Error extends Product with Serializable
  final case class JsonReaderError(value: String, message: String) extends Error
  final case class JsonWriterError(value: Any, message: String) extends Error

  object Error {
    implicit val show: Show[Error] = Show shows {
      case a: JsonReaderError => JsonReaderError.ShowInstance.shows(a)
      case a: JsonWriterError => JsonWriterError.ShowInstance.shows(a)
    }
  }

  object JsonReaderError {
    implicit val ShowInstance: Show[JsonReaderError] = Show shows { f =>
      s"JsonReaderError. Cannot read JSON: <${f.value}>. Cause: ${f.message}"
    }
  }

  object JsonWriterError {
    implicit val ShowInstance: Show[JsonWriterError] = Show shows { f =>
      s"JsonWriterError. Cannot write value as JSON: <${f.value}>. Cause: ${f.message}"
    }
  }

  def parse(str: String): JsonReaderError \/ JsValue =
    \/.fromTryCatchNonFatal(JsonParser(str)).leftMap(e => JsonReaderError(str, e.getMessage))

  def decode[A: JsonReader](str: String): JsonReaderError \/ A =
    for {
      jsValue <- parse(str)
      a <- decode(jsValue)
    } yield a

  def decode[A: JsonReader](a: JsValue): JsonReaderError \/ A =
    \/.fromTryCatchNonFatal(a.convertTo[A]).leftMap(e => JsonReaderError(a.toString, e.getMessage))

  def encode[A: JsonWriter](a: A): JsonWriterError \/ JsValue = {
    import spray.json._
    \/.fromTryCatchNonFatal(a.toJson).leftMap(e => JsonWriterError(a, e.getMessage))
  }

  def mustBeJsObject(a: JsValue): JsonError \/ JsObject = a match {
    case b: JsObject => \/-(b)
    case _ => -\/(JsonError(s"Expected JsObject, got: ${a: JsValue}"))
  }
}
