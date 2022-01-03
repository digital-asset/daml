// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class DamlList extends Value {

  private List<Value> values;

  private DamlList() {}

  /** The list that is passed to this constructor must not be change once passed. */
  static @NonNull DamlList fromPrivateList(@NonNull List<@NonNull Value> values) {
    DamlList damlList = new DamlList();
    damlList.values = Collections.unmodifiableList(values);
    return damlList;
  }

  private static DamlList EMPTY = fromPrivateList(Collections.EMPTY_LIST);

  public static DamlList of(@NonNull List<@NonNull Value> values) {
    return fromPrivateList(new ArrayList<>(values));
  }

  @SafeVarargs
  public static DamlList of(@NonNull Value... values) {
    return fromPrivateList(Arrays.asList(values));
  }

  @Deprecated // use DamlList:of
  public DamlList(@NonNull List<@NonNull Value> values) {
    this.values = values;
  }

  @Deprecated // use DamlMap:of
  @SafeVarargs
  public DamlList(@NonNull Value... values) {
    this(Arrays.asList(values));
  }

  @Deprecated // use DamlMap::stream or DamlMap::toListf
  public @NonNull List<@NonNull Value> getValues() {
    return toList(Function.identity());
  }

  public @NonNull Stream<Value> stream() {
    return values.stream();
  }

  public @NonNull <T> List<T> toList(Function<Value, T> valueMapper) {
    return stream().map(valueMapper).collect(Collectors.toList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DamlList list = (DamlList) o;
    return Objects.equals(values, list.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  @Override
  public String toString() {
    return "DamlList{" + "values=" + values + '}';
  }

  @Override
  public ValueOuterClass.Value toProto() {
    ValueOuterClass.List.Builder builder = ValueOuterClass.List.newBuilder();
    for (Value value : this.values) {
      builder.addElements(value.toProto());
    }
    return ValueOuterClass.Value.newBuilder().setList(builder.build()).build();
  }

  public static @NonNull DamlList fromProto(ValueOuterClass.List list) {
    return list.getElementsList().stream().collect(DamlCollectors.toDamlList(Value::fromProto));
  }
}
