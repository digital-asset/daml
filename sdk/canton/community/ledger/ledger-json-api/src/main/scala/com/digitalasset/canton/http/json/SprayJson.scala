// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.scalautil.ExceptionOps._
import scalaz.syntax.bitraverse._
import scalaz.syntax.traverse._
import scalaz.{-\/, Bitraverse, Show, Traverse, \/, \/-}
import spray.json.{
  JsValue,
  JsObject,
  JsonParser,
  JsonReader,
  JsonWriter,
  enrichAny => `sj enrichAny`,
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

  def decode2[F[_, _], A, B](str: String)(implicit
      ev1: JsonReader[F[JsValue, JsValue]],
      ev2: Bitraverse[F],
      ev3: JsonReader[A],
      ev4: JsonReader[B],
  ): JsonReaderError \/ F[A, B] =
    parse(str).flatMap(decode2[F, A, B])

  def decode2[F[_, _], A, B](a: JsValue)(implicit
      ev1: JsonReader[F[JsValue, JsValue]],
      ev2: Bitraverse[F],
      ev3: JsonReader[A],
      ev4: JsonReader[B],
  ): JsonReaderError \/ F[A, B] =
    for {
      fjj <- decode[F[JsValue, JsValue]](a)
      fab <- fjj.bitraverse(decode[A](_), decode[B](_))
    } yield fab

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

  def encode2[F[_, _], A, B](fab: F[A, B])(implicit
      ev1: JsonWriter[F[JsValue, JsValue]],
      ev2: Bitraverse[F],
      ev3: JsonWriter[A],
      ev4: JsonWriter[B],
  ): JsonWriterError \/ JsValue =
    for {
      fjj <- fab.bitraverse(encode[A](_), encode[B](_))
      jsVal <- encode[F[JsValue, JsValue]](fjj)
    } yield jsVal

  def mustBeJsObject(a: JsValue): JsonError \/ JsObject = a match {
    case b: JsObject => \/-(b)
    case _ => -\/(JsonError(s"Expected JsObject, got: ${a: JsValue}"))
  }

  def objectField(o: JsValue, f: String): Option[JsValue] = o match {
    case JsObject(fields) => fields.get(f)
    case _ => None
  }
}
