// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.Objects;

public final class Bool extends Value {

  private final boolean value;

  public static final Bool TRUE = new Bool(true);
  public static final Bool FALSE = new Bool(false);

  // TODO i15639 make private; delete equals/hashCode
  /** @deprecated Use {@link #of} instead; since Daml 2.5.0 */
  @Deprecated
  public Bool(boolean value) {
    this.value = value;
  }

  public static Bool of(boolean value) {
    return value ? TRUE : FALSE;
  }

  @Override
  public ValueOuterClass.Value toProto() {
    return ValueOuterClass.Value.newBuilder().setBool(this.value).build();
  }

  public boolean isValue() {
    return value;
  }

  public boolean getValue() {
    return isValue();
  }

  @Override
  public String toString() {
    return "Bool{" + "value=" + value + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Bool bool = (Bool) o;
    return value == bool.value;
  }

  @Override
  public int hashCode() {

    return Objects.hash(value);
  }
}
