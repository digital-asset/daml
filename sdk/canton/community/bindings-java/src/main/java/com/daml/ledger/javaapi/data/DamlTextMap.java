// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ValueOuterClass;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class DamlTextMap extends Value {

  private final Map<String, Value> map;

  DamlTextMap(Map<String, Value> textMap) {
    this.map = textMap;
  }

  /** The textMap that is passed to this constructor must not be changed once passed. */
  static @NonNull DamlTextMap fromPrivateMap(Map<@NonNull String, @NonNull Value> value) {
    return new DamlTextMap(Collections.unmodifiableMap(value));
  }

  private static @NonNull DamlTextMap EMPTY = fromPrivateMap(Collections.emptyMap());

  public static DamlTextMap of(@NonNull Map<@NonNull String, @NonNull Value> value) {
    return fromPrivateMap(new HashMap<>(value));
  }

  public Stream<Map.Entry<String, Value>> stream() {
    return map.entrySet().stream();
  }

  public final @NonNull <K, V> Map<K, V> toMap(
      @NonNull Function<@NonNull String, @NonNull K> keyMapper,
      @NonNull Function<@NonNull Value, @NonNull V> valueMapper) {
    return stream()
        .collect(
            Collectors.toMap(
                e -> keyMapper.apply(e.getKey()), e -> valueMapper.apply(e.getValue())));
  }

  public final @NonNull <V> Map<@NonNull String, @NonNull V> toMap(
      @NonNull Function<@NonNull Value, @NonNull V> valueMapper) {
    return toMap(Function.identity(), valueMapper);
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DamlTextMap other = (DamlTextMap) o;
    return Objects.equals(map, other.map);
  }

  @Override
  public final int hashCode() {
    return map.hashCode();
  }

  @Override
  public final @NonNull String toString() {
    StringJoiner sj = new StringJoiner(", ", "TextMap{", "}");
    map.forEach((key, value) -> sj.add(key + "->" + value.toString()));
    return sj.toString();
  }

  @Override
  public final ValueOuterClass.Value toProto() {
    ValueOuterClass.TextMap.Builder mb = ValueOuterClass.TextMap.newBuilder();
    map.forEach(
        (k, v) ->
            mb.addEntries(
                ValueOuterClass.TextMap.Entry.newBuilder().setKey(k).setValue(v.toProto())));
    return ValueOuterClass.Value.newBuilder().setTextMap(mb).build();
  }

  public static @NonNull DamlTextMap fromProto(ValueOuterClass.TextMap textMap) {
    return textMap.getEntriesList().stream()
        .collect(
            DamlCollectors.toDamlTextMap(
                ValueOuterClass.TextMap.Entry::getKey, entry -> fromProto(entry.getValue())));
  }
}
