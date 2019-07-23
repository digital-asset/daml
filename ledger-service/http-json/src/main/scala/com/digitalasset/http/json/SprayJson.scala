// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import akka.util.ByteString
import scalaz.{Show, \/}
import spray.json.{JsValue, JsonReader, _}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object SprayJson {
  sealed abstract class Error extends Product with Serializable
  final case class JsonReaderError(value: String, message: String) extends Error
  final case class JsonWriterError(value: Any, message: String) extends Error

  object Error {
    implicit val show: Show[Error] = new Show[Error] {
      override def shows(f: Error): String = f match {
        case a: JsonReaderError => JsonReaderError.ShowInstance.shows(a)
        case a: JsonWriterError => JsonWriterError.ShowInstance.shows(a)
      }
    }
  }

  object JsonReaderError {
    implicit val ShowInstance: Show[JsonReaderError] = new Show[JsonReaderError] {
      override def shows(f: JsonReaderError): String =
        s"JsonReaderError. Cannot read JSON: <${f.value}>. Cause: ${f.message}"
    }
  }

  object JsonWriterError {
    implicit val ShowInstance: Show[JsonWriterError] = new Show[JsonWriterError] {
      override def shows(f: JsonWriterError): String =
        s"JsonWriterError. Cannot write value as JSON: <${f.value}>. Cause: ${f.message}"
    }
  }

  def parse[A: JsonReader](str: ByteString): JsonReaderError \/ A =
    parse(str.utf8String)

  def parse[A: JsonReader](str: String): JsonReaderError \/ A =
    for {
      jsValue <- \/.fromTryCatchNonFatal(str.parseJson).leftMap(e =>
        JsonReaderError(str, e.getMessage))
      a <- parse(jsValue)
    } yield a

  def parse[A: JsonReader](a: JsValue): JsonReaderError \/ A =
    \/.fromTryCatchNonFatal(a.convertTo[A]).leftMap(e => JsonReaderError(a.toString, e.getMessage))

  def toJson[A: JsonWriter](a: A): JsonWriterError \/ JsValue = {
    import spray.json._
    \/.fromTryCatchNonFatal(a.toJson).leftMap(e => JsonWriterError(a, e.getMessage))
  }
}
