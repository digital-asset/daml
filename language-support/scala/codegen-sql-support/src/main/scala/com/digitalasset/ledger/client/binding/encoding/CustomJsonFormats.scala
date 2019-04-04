// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId}
import com.digitalasset.ledger.client.binding.{Primitive => P}

import spray.json.{
  AdditionalFormats,
  DefaultJsonProtocol,
  JsArray,
  JsObject,
  JsString,
  JsValue,
  JsonFormat,
  StandardFormats,
  deserializationError
}

import scala.util.control.NonFatal

trait CustomJsonFormats extends StandardFormats { this: StandardFormats with AdditionalFormats =>
  import CustomJsonFormats.NestableOptionJsonFormat

  // spray's standard Option format based on null-punning won't correctly
  // distinguish between Some(None) and None; we let only the innermost
  // Option be represented in that way
  implicit override def optionFormat[A](
      implicit element: JsonFormat[A]): JsonFormat[P.Optional[A]] = element match {
    case _: NestableOptionJsonFormat[_] | _: StandardFormats#OptionFormat[_] =>
      new NestableOptionJsonFormat(element)
    case _ => super.optionFormat(element)
  }
}

object CustomJsonFormats extends DefaultJsonProtocol with CustomJsonFormats {

  val dateJsonFormat: JsonFormat[P.Date] = new JsonFormat[P.Date] {
    private val dateFormatter =
      DateTimeFormatter.ofPattern("uuuu-MM-dd").withZone(ZoneId.of("UTC"))

    override def write(d: P.Date): JsValue = JsString(dateFormatter.format(d))

    override def read(json: JsValue): P.Date = json match {
      case JsString(str) =>
        read(str).getOrElse(deserializationError(s"Cannot convert $str to Primitive.Date"))
      case x => deserializationError(s"Expected Date as JsString, but got $x")
    }

    private def read(str: String): Option[P.Date] =
      try {
        val localDate = LocalDate.from(dateFormatter.parse(str))
        P.Date.fromLocalDate(localDate)
      } catch { case NonFatal(e) => deserializationError("ill-formed date", cause = e) }
  }

  val timestampJsonFormat: JsonFormat[P.Timestamp] = new JsonFormat[P.Timestamp] {
    private val timestampFormatter =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.nX").withZone(ZoneId.of("UTC"))

    override def write(t: P.Timestamp): JsValue = JsString(timestampFormatter.format(t))

    override def read(json: JsValue): P.Timestamp = json match {
      case JsString(str) =>
        read(str).getOrElse(deserializationError(s"Cannot convert $str to Primitive.Timestamp"))
      case x => deserializationError(s"Expected Timestamp as JsString, but got $x")
    }

    private def read(str: String): Option[P.Timestamp] =
      try {
        val instant = Instant.from(timestampFormatter.parse(str))
        P.Timestamp.discardNanos(instant)
      } catch { case NonFatal(e) => deserializationError("ill-formed timestamp", cause = e) }
  }

  private class NestableOptionJsonFormat[A](element: JsonFormat[A])
      extends JsonFormat[P.Optional[A]] {
    override def write(x: P.Optional[A]): JsValue =
      JsArray(x.map(element.write).toList.toVector)

    override def read(json: JsValue): P.Optional[A] = json match {
      case JsArray(Seq(some)) => Some(element read some)
      case JsArray(Seq()) => None
      case x => deserializationError(s"Expected array when deserializing Optional, but got $x")
    }
  }

  // slick uses 1 for unit by default, use {} instead for consistency with records
  val unitJsonFormat: JsonFormat[P.Unit] = new JsonFormat[P.Unit] {
    override def write(x: P.Unit): JsValue = JsObject.empty

    override def read(json: JsValue): P.Unit = json match {
      case JsObject(fields) if fields.isEmpty => ()
      case x => deserializationError(s"Expected empty object when deserializing Unit, but got $x")
    }
  }

  implicit def optionJsonFormat[A: JsonFormat]: JsonFormat[P.Optional[A]] =
    new JsonFormat[P.Optional[A]] {
      private val evA = implicitly[JsonFormat[A]]

      override def read(json: JsValue): P.Optional[A] = json match {
        case JsArray(Vector()) => None
        case JsArray(Vector(x: JsValue)) => Some(evA.read(x))
        case x => deserializationError(s"Expected JsArray with 0 or 1 elements, but got $x")
      }

      override def write(obj: P.Optional[A]): JsValue = obj match {
        case None => JsArray()
        case Some(a) => JsArray(evA.write(a))
      }
    }
}
