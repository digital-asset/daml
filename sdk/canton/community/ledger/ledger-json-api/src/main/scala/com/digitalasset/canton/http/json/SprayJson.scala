// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.scalautil.ExceptionOps.*
import scalaz.{Show, \/}
import spray.json.{JsValue, JsonParser, JsonWriter, enrichAny as `sj enrichAny`}

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
    \/.attempt(JsonParser(str))(e => JsonReaderError(str, e.description))

  def encodeUnsafe[A: JsonWriter](a: A): JsValue =
    a.toJson

}
