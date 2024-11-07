// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.scalautil.ExceptionOps.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, Show, Traverse, \/, \/-}
import spray.json.{
  JsObject,
  JsValue,
  JsonParser,
  JsonReader,
  JsonWriter,
  enrichAny as `sj enrichAny`,
}

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

  def decode[A: JsonReader](str: String): JsonReaderError \/ A =
    for {
      jsValue <- parse(str)
      a <- decode(jsValue)
    } yield a

  def decode[A: JsonReader](a: JsValue): JsonReaderError \/ A =
    \/.attempt(a.convertTo[A])(e => JsonReaderError(a.toString, e.description))

  def decode1[F[_], A](str: String)(implicit
      ev1: JsonReader[F[JsValue]],
      ev2: Traverse[F],
      ev3: JsonReader[A],
  ): JsonReaderError \/ F[A] =
    parse(str).flatMap(decode1[F, A])

  def decode1[F[_], A](a: JsValue)(implicit
      ev1: JsonReader[F[JsValue]],
      ev2: Traverse[F],
      ev3: JsonReader[A],
  ): JsonReaderError \/ F[A] =
    for {
      fj <- decode[F[JsValue]](a)
      fa <- fj.traverse(decode[A](_))
    } yield fa

  def encode[A: JsonWriter](a: A): JsonWriterError \/ JsValue =
    \/.attempt(a.toJson)(e => JsonWriterError(a, e.description))

  def encodeUnsafe[A: JsonWriter](a: A): JsValue =
    a.toJson

  def encode1[F[_], A](fa: F[A])(implicit
      ev1: JsonWriter[F[JsValue]],
      ev2: Traverse[F],
      ev3: JsonWriter[A],
  ): JsonWriterError \/ JsValue =
    for {
      fj <- fa.traverse(encode[A](_))
      jsVal <- encode[F[JsValue]](fj)
    } yield jsVal

  def mustBeJsObject(a: JsValue): JsonError \/ JsObject = a match {
    case b: JsObject => \/-(b)
    case _ => -\/(JsonError(s"Expected JsObject, got: ${a: JsValue}"))
  }
}
