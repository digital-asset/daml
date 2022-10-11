// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class Party extends Value {

  private final String value;

  public Party(@NonNull String value) {
    this.value = value;
  }

  @Override
  public ValueOuterClass.Value toProto() {
    return ValueOuterClass.Value.newBuilder().setParty(this.value).build();
  }

  @NonNull
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "Party{" + "value='" + value + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Party party = (Party) o;
    return Objects.equals(value, party.value);
  }

  @Override
  public int hashCode() {

    return Objects.hash(value);
  }
}
