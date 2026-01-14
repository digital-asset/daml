// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import com.daml.logging.entries.LoggingValue
import com.fasterxml.jackson.core.JsonGenerator

private[logging] object LoggingValueSerializer {
  def writeValue(value: LoggingValue, generator: JsonGenerator): Unit =
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
      case LoggingValue.Nested(entries) =>
        generator.writeStartObject()
        new LoggingMarker(entries.contents).writeTo(generator)
        generator.writeEndObject()
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
  }

}
