// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import com.daml.logging.entries.LoggingValue
import com.fasterxml.jackson.core.JsonGenerator
import spray.json.{
  JsArray,
  JsBoolean,
  JsFalse,
  JsNull,
  JsNumber,
  JsObject,
  JsString,
  JsTrue,
  JsValue,
}

private[logging] object LoggingValueSerializer {
  def writeJsValue(jsValue: JsValue, generator: JsonGenerator): Unit = {
    def write(jsValue: JsValue): Unit =
      jsValue match {
        case JsNull =>
          generator.writeNull()
        case JsTrue =>
          generator.writeBoolean(true)
        case JsFalse =>
          generator.writeBoolean(false)
        case JsBoolean(value) =>
          generator.writeBoolean(value)
        case JsNumber(value) =>
          generator.writeNumber(value.bigDecimal)
        case JsString(value) =>
          generator.writeString(value)
        case JsObject(fields) =>
          generator.writeStartObject()
          fields.foreach { case (key, value) =>
            generator.writeFieldName(key)
            write(value)
          }
          generator.writeEndObject()
        case JsArray(elements) =>
          generator.writeStartArray()
          elements.foreach(value => write(value))
          generator.writeEndArray()
      }
    write(jsValue)
  }

  def writeValue(value: LoggingValue, generator: JsonGenerator): Unit = {
    value match {
      case LoggingValue.Empty =>
        generator.writeNull()
      case LoggingValue.False =>
        generator.writeBoolean(false)
      case LoggingValue.True =>
        generator.writeBoolean(true)
      case LoggingValue.OfString(value) =>
        generator.writeString(value)
      case LoggingValue.OfInt(value) =>
        generator.writeNumber(value)
      case LoggingValue.OfLong(value) =>
        generator.writeNumber(value)
      case LoggingValue.OfIterable(sequence) =>
        generator.writeStartArray()
        sequence.foreach(writeValue(_, generator))
        generator.writeEndArray()
      case LoggingValue.OfJson(jsValue) =>
        writeJsValue(jsValue, generator)
      case LoggingValue.Nested(entries) =>
        generator.writeStartObject()
        new LoggingMarker(entries.contents).writeTo(generator)
        generator.writeEndObject()
    }
  }
}

object LoggingValueStringSerializer {

  def makeString(loggingValue: LoggingValue): String = loggingValue match {
    case LoggingValue.Empty => ""
    case LoggingValue.False => "false"
    case LoggingValue.True => "true"
    case LoggingValue.OfString(value) => s"'$value'"
    case LoggingValue.OfInt(value) => value.toString
    case LoggingValue.OfLong(value) => value.toString
    case LoggingValue.OfIterable(sequence) =>
      sequence.map(makeString).mkString("[", ", ", "]")
    case LoggingValue.Nested(entries) =>
      entries.contents.view
        .map { case (key, value) => s"$key: ${makeString(value)}" }
        .mkString("{", ", ", "}")
    case LoggingValue.OfJson(json) => json.toString()
  }

}
