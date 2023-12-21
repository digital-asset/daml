// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;

/**
 * The codegen-decoded form of any of these:
 *
 * <ol>
 *   <li>what {@link DamlRecord} describes,
 *   <li>a {@link Variant} without type parameters, or
 *   <li>any {@link DamlEnum}.
 * </ol>
 *
 * <p>Its encoded counterpart is {@link com.daml.ledger.javaapi.data.Value}, which can be produced
 * with {@link #toValue}.
 *
 * @param <T> A "self type", some subclass of this interface that {@code T} implements.
 */
public interface DefinedDataType<T> {
  /** Produce the encoded form. */
  Value toValue();

  // To avoid breaking code generated by old code-generators that link against updated java-bindings
  // we provide a default implementation. Calling toJson will fail but old code-gen won't use it.
  // TODO(i15641) Remove this once we can expect users of codegen to have caught up.
  default JsonLfEncoder jsonEncoder() {
    throw new UnsupportedOperationException(
        "no jsonEncoder implementation was provided for "
            + this.getClass()
            + ". Does the SDK version you are using for codegen match your version of the"
            + " java-bindings library?");
  }

  default String toJson() {
    var w = new StringWriter();
    try {
      this.jsonEncoder().encode(new JsonLfWriter(w));
    } catch (IOException e) {
      // Not expected with StringWriter
      throw new UncheckedIOException(e);
    }
    return w.toString();
  }
}
