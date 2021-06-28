// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.io.StringWriter

import com.daml.logging.LoggingEntries._
import com.fasterxml.jackson.core.json.JsonWriteFeature
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter
import com.fasterxml.jackson.core.{JsonFactoryBuilder, JsonGenerator}
import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.marker.LogstashMarker
import org.slf4j.Marker

final class LoggingEntries private (
    private[logging] val contents: Map[LoggingKey, LoggingValue]
) extends AnyVal {
  def isEmpty: Boolean =
    contents.isEmpty

  def :+(entry: LoggingEntry): LoggingEntries =
    new LoggingEntries(contents + entry)

  def ++(other: LoggingEntries): LoggingEntries =
    new LoggingEntries(contents ++ other.contents)

  private[logging] def loggingMarker: Marker with StructuredArgument =
    new LoggingMarker(contents)
}

object LoggingEntries {
  val empty: LoggingEntries = new LoggingEntries(Map.empty)

  private val toStringJsonFactory =
    new JsonFactoryBuilder().disable(JsonWriteFeature.QUOTE_FIELD_NAMES).build()

  def apply(entries: LoggingEntry*): LoggingEntries =
    new LoggingEntries(entries.toMap)

  def fromIterator(entries: Iterator[LoggingEntry]): LoggingEntries =
    new LoggingEntries(entries.toMap)

  private final class LoggingMarker(contents: Map[LoggingKey, LoggingValue])
      extends LogstashMarker(LogstashMarker.MARKER_NAME_PREFIX + "LOGGING_ENTRIES")
      with StructuredArgument {
    override def writeTo(generator: JsonGenerator): Unit = {
      contents.foreach { case (key, value) =>
        generator.writeFieldName(key)
        value.writeTo(generator)
      }
    }

    override def toStringSelf: String = {
      val writer = new StringWriter
      val generator =
        toStringJsonFactory.createGenerator(writer).setPrettyPrinter(SpaceSeparatedPrettyPrinter)
      generator.writeStartObject()
      writeTo(generator)
      generator.writeEndObject()
      generator.flush()
      writer.toString
    }
  }

  private object SpaceSeparatedPrettyPrinter extends MinimalPrettyPrinter {
    override def writeObjectFieldValueSeparator(g: JsonGenerator): Unit = {
      super.writeObjectFieldValueSeparator(g)
      g.writeRaw(' ')
    }

    override def writeObjectEntrySeparator(g: JsonGenerator): Unit = {
      super.writeObjectEntrySeparator(g)
      g.writeRaw(' ')
    }
  }
}
