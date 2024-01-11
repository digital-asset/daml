// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen.json;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;

public class JsonLfWriter {
  private final Writer writer;
  private final Options options;

  public JsonLfWriter(Writer w, Options options) {
    this.writer = w;
    this.options = options;
  }

  public JsonLfWriter(Writer w) {
    this(w, opts());
  }

  void writeStartObject() throws IOException {
    write("{");
  }

  void writeEndObject() throws IOException {
    write("}");
  }

  void writeFieldName(String name) throws IOException {
    write(quoted(name) + ": ");
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

  void writeNumeric(BigDecimal num) throws IOException {
    if (options.encodeNumericAsString) {
      writer.write(quoted(num));
    } else {
      writer.write(num.toString());
    }
  }

  void writeInt64(Long num) throws IOException {
    if (options.encodeInt64AsString) {
      writer.write(quoted(num));
    } else {
      writer.write(num.toString());
    }
  }

  private String quoted(Object val) {
    return "\"" + val.toString() + "\"";
  }

  public static Options opts() {
    return new Options(true, true);
  }

  public static final class Options {
    public final boolean encodeNumericAsString;
    public final boolean encodeInt64AsString;

    public Options encodeNumericAsString(boolean encodeNumericAsString) {
      return new Options(encodeNumericAsString, encodeInt64AsString);
    }

    public Options encodeInt64AsString(boolean encodeInt64AsString) {
      return new Options(encodeNumericAsString, encodeInt64AsString);
    }

    private Options(boolean encodeNumericAsString, boolean encodeInt64AsString) {
      this.encodeNumericAsString = encodeNumericAsString;
      this.encodeInt64AsString = encodeInt64AsString;
    }
  }
}
