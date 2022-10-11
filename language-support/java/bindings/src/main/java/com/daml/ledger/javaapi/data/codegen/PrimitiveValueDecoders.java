// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.*;
import java.time.Instant;
import java.time.LocalDate;

/**
 * {@link ValueDecoder}s for Daml types that are not code-generated.
 *
 * @see ValueDecoder
 */
public final class PrimitiveValueDecoders {
  // constructing not allowed
  private PrimitiveValueDecoders() {}

  public static final ValueDecoder<Boolean> fromBool =
      value -> value.asBool().orElseThrow(() -> mismatched(Bool.class)).getValue();
  public static final ValueDecoder<Long> fromInt64 =
      value -> value.asInt64().orElseThrow(() -> mismatched(Int64.class)).getValue();
  public static final ValueDecoder<String> fromText =
      value -> value.asText().orElseThrow(() -> mismatched(Text.class)).getValue();
  public static final ValueDecoder<Instant> fromTimestamp =
      value -> value.asTimestamp().orElseThrow(() -> mismatched(Timestamp.class)).getValue();
  public static final ValueDecoder<String> fromParty =
      value -> value.asParty().orElseThrow(() -> mismatched(Party.class)).getValue();
  public static final ValueDecoder<Unit> fromUnit =
      value -> value.asUnit().orElseThrow(() -> mismatched(Unit.class));
  public static final ValueDecoder<LocalDate> fromDate =
      value -> value.asDate().orElseThrow(() -> mismatched(Date.class)).getValue();

  public static <T> ValueDecoder<T> impossible() {
    return x -> {
      throw new IllegalArgumentException("Expected type to be unused, but was used for " + x);
    };
  }

  // TODO CL error context String field
  private static IllegalArgumentException mismatched(Class<?> clazz) {
    String typeName = clazz.getName();
    return new IllegalArgumentException(String.format("Expected field to be of type %s", typeName));
  }
}
