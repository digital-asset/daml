// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class DamlGenMap extends Value {

  private final Map<Value, Value> map;

  private DamlGenMap(@NonNull Map<@NonNull Value, @NonNull Value> map) {
    this.map = map;
  }

  /** The map that is passed to this constructor must not be changed once passed. */
  static @NonNull DamlGenMap fromPrivateMap(@NonNull Map<@NonNull Value, @NonNull Value> map) {
    return new DamlGenMap(Collections.unmodifiableMap(map));
  }

  public static DamlGenMap of(@NonNull Map<@NonNull Value, @NonNull Value> map) {
    return fromPrivateMap(new LinkedHashMap<>(map));
  }

  public Stream<Map.Entry<Value, Value>> stream() {
    return map.entrySet().stream();
  }

  public @NonNull <K, V> Map<@NonNull K, @NonNull V> toMap(
      @NonNull Function<@NonNull Value, @NonNull K> keyMapper,
      @NonNull Function<@NonNull Value, @NonNull V> valueMapper) {
    return stream()
        .collect(
            Collectors.toMap(
                e -> keyMapper.apply(e.getKey()),
                e -> valueMapper.apply(e.getValue()),
                (left, right) -> right,
                LinkedHashMap::new));
  }

  public @NonNull <V> Map<@NonNull V, @NonNull V> toMap(
      @NonNull Function<@NonNull Value, @NonNull V> valueMapper) {
    return toMap(valueMapper, valueMapper);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DamlGenMap other = (DamlGenMap) o;
    return Objects.equals(map, other.map);
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  @Override
  public @NonNull String toString() {
    StringJoiner sj = new StringJoiner(", ", "GenMap{", "}");
    map.forEach((key, value) -> sj.add(key.toString() + " -> " + value.toString()));
    return sj.toString();
  }

  @Override
  public ValueOuterClass.Value toProto() {
    ValueOuterClass.GenMap.Builder mb = ValueOuterClass.GenMap.newBuilder();
    map.forEach(
        (key, value) ->
            mb.addEntries(
                ValueOuterClass.GenMap.Entry.newBuilder()
                    .setKey(key.toProto())
                    .setValue(value.toProto())));
    return ValueOuterClass.Value.newBuilder().setGenMap(mb).build();
  }

  public static @NonNull DamlGenMap fromProto(ValueOuterClass.GenMap map) {
    return map.getEntriesList().stream()
        .collect(
            DamlCollectors.toDamlGenMap(
                entry -> fromProto(entry.getKey()), entry -> fromProto(entry.getValue())));
  }
}
