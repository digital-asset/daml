// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import java.io.IOException;
import java.io.Writer;

public class JsonLfWriter {
  private final Writer writer;

  public JsonLfWriter(Writer w) {
    this.writer = w;
  }

  void writeStartObject() throws IOException {
    write("{");
  }

  void writeEndObject() throws IOException {
    write("}");
  }

  void writeFieldName(String name) throws IOException {
    write("\"" + name + "\": ");
  }

  void writeStartArray() throws IOException {
    write("[");
  }

  void writeEndArray() throws IOException {
    write("]");
  }

  void writeComma() throws IOException {
    write(", ");
  }

  void write(String value) throws IOException {
    writer.write(value);
  }
}
