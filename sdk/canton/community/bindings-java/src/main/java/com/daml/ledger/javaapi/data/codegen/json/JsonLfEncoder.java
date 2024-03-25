// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen.json;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;

@FunctionalInterface
public interface JsonLfEncoder {
  public void encode(JsonLfWriter w) throws IOException;

  public default String intoString() {
    var w = new StringWriter();
    try {
      this.encode(new JsonLfWriter(w));
    } catch (IOException e) {
      // Not expected with StringWriter
      throw new UncheckedIOException(e);
    }
    return w.toString();
  }
}
