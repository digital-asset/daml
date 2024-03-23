// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import com.daml.logging.entries.{LoggingKey, LoggingValue}
import com.fasterxml.jackson.core.JsonGenerator
import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.marker.LogstashMarker

private[logging] final class LoggingMarker(contents: Map[LoggingKey, LoggingValue])
    extends LogstashMarker(LogstashMarker.MARKER_NAME_PREFIX + "LOGGING_ENTRIES")
    with StructuredArgument {
  override def writeTo(generator: JsonGenerator): Unit = {
    contents.foreach { case (key, value) =>
      generator.writeFieldName(key)
      LoggingValueSerializer.writeValue(value, generator)
    }
  }

  override def toStringSelf: String =
    JsonStringSerializer.serialize { generator =>
      generator.writeStartObject()
      writeTo(generator)
      generator.writeEndObject()
    }
}
