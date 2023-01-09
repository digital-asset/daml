// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.Objects;

public final class Int64 extends Value {

  private long value;

  public Int64(long int64) {
    this.value = int64;
  }

  public long getValue() {
    return value;
  }

  @Override
  public ValueOuterClass.Value toProto() {
    return ValueOuterClass.Value.newBuilder().setInt64(this.value).build();
  }

  @Override
  public String toString() {
    return "Int64{" + "value=" + value + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Int64 int64 = (Int64) o;
    return value == int64.value;
  }

  @Override
  public int hashCode() {

    return Objects.hash(value);
  }
}
