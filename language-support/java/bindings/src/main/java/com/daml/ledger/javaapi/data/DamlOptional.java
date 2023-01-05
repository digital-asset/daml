// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.*;
import java.util.function.Function;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class DamlOptional extends Value {

  public static DamlOptional EMPTY = new DamlOptional(Optional.empty());

  private final Value value;

  DamlOptional(Value value) {
    this.value = value;
  }

  @Deprecated // use DamlOptional.of
  public DamlOptional(Optional<@NonNull Value> value) {
    this(value.orElse(null));
  }

  public static DamlOptional of(@NonNull Optional<@NonNull Value> value) {
    if (value.isPresent()) return new DamlOptional((value.get()));
    else return EMPTY;
  }

  public static DamlOptional of(Value value) {
    return new DamlOptional(value);
  }

  public java.util.Optional<Value> getValue() {
    return java.util.Optional.ofNullable(value);
  }

  public @NonNull <V> Optional<V> toOptional(Function<@NonNull Value, @NonNull V> valueMapper) {
    return (value == null) ? Optional.empty() : Optional.of(valueMapper.apply(value));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DamlOptional optional = (DamlOptional) o;
    return Objects.equals(value, optional.value);
  }

  public boolean isEmpty() {
    return value == null;
  }

  @Override
  public int hashCode() {
    return (value == null) ? 0 : value.hashCode();
  }

  @Override
  public @NonNull String toString() {
    return "Optional{" + "value=" + value + '}';
  }

  @Deprecated // use DamlOptional::EMPTY
  public static @NonNull DamlOptional empty() {
    return EMPTY;
  }

  @Override
  public ValueOuterClass.Value toProto() {
    ValueOuterClass.Optional.Builder ob = ValueOuterClass.Optional.newBuilder();
    if (value != null) ob.setValue(value.toProto());
    return ValueOuterClass.Value.newBuilder().setOptional(ob.build()).build();
  }

  public static DamlOptional fromProto(ValueOuterClass.Optional optional) {
    return (optional.hasValue()) ? new DamlOptional(fromProto(optional.getValue())) : EMPTY;
  }
}
