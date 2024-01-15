// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.io.StringWriter

import com.fasterxml.jackson.core.json.JsonWriteFeature
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter
import com.fasterxml.jackson.core.{JsonFactoryBuilder, JsonGenerator}
import net.logstash.logback.argument.StructuredArgument

object JsonStringSerializer {
  private val toStringJsonFactory =
    new JsonFactoryBuilder().disable(JsonWriteFeature.QUOTE_FIELD_NAMES).build()

  def serialize(value: StructuredArgument): String = {
    val writer = new StringWriter
    val generator =
      toStringJsonFactory.createGenerator(writer).setPrettyPrinter(SpaceSeparatedPrettyPrinter)
    value.writeTo(generator)
    generator.flush()
    writer.toString
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

    override def writeArrayValueSeparator(g: JsonGenerator): Unit = {
      super.writeArrayValueSeparator(g)
      g.writeRaw(' ')
    }
  }
}
