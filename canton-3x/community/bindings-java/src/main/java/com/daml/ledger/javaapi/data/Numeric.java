// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.math.BigDecimal;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class Numeric extends Value {

  private final BigDecimal value;

  public Numeric(@NonNull BigDecimal value) {
    this.value = value;
  }

  public static Numeric fromProto(String numeric) {
    return new Numeric(new BigDecimal(numeric));
  }

  @Override
  public ValueOuterClass.Value toProto() {
    return ValueOuterClass.Value.newBuilder().setNumeric(this.value.toPlainString()).build();
  }

  @NonNull
  public BigDecimal getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "Numeric{" + "value=" + value + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Numeric numeric = (Numeric) o;
    return Objects.equals(value, numeric.value);
  }

  @Override
  public int hashCode() {

    return Objects.hash(value);
  }
}
