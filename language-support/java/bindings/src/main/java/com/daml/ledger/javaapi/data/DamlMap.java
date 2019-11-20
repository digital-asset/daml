// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;

public class DamlMap extends Value {

    private static DamlMap EMPTY = new DamlMap(Collections.emptyMap());

    private final Map<String, Value> value;

    public DamlMap(Map<String, Value> value) {
        this.value = value;
    }

    public static <T> Collector<T, Map<String, Value>, DamlMap> collector(
            Function<T, String> keyMapper,
            Function<T, Value> valueMapper) {

        return Collector.of(
                HashMap::new,
                (acc, entry) -> acc.put(keyMapper.apply(entry), valueMapper.apply(entry)),
                (left, right) -> {
                    left.putAll(right);
                    return left;
                },
                DamlMap::new
        );
    }

    public @Nonnull
    Map<String, Value> getMap() { return value; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DamlMap optional = (DamlMap) o;
        return Objects.equals(value, optional.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public @NonNull String toString() {
        StringJoiner sj = new StringJoiner(", ", "Map{", "}");
        value.forEach((k, v) -> sj.add(k + "->" + v.toString()));
        return sj.toString();
    }

    @Override
    public @Nonnull ValueOuterClass.Value toProto() {
        ValueOuterClass.Map.Builder mb = ValueOuterClass.Map.newBuilder();
        value.forEach((k, v) ->
                mb.addEntries(ValueOuterClass.Map.Entry.newBuilder()
                        .setKey(k)
                        .setValue(v.toProto())
                )
        );

        return ValueOuterClass.Value.newBuilder().setMap(mb).build();
    }

    public static DamlMap fromProto(ValueOuterClass.Map map) {
        return map.getEntriesList().stream().collect(collector(
                ValueOuterClass.Map.Entry::getKey,
                entry -> fromProto(entry.getValue())
        ));
    }
}
