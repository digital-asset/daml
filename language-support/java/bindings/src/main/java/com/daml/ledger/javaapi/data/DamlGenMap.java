// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;

public final class DamlGenMap extends Value {

    private static DamlGenMap EMPTY = new DamlGenMap(Collections.EMPTY_MAP);

    private final java.util.Map<Value, Value> value;

    public DamlGenMap(java.util.Map<Value, Value> value) {
        this.value = value;
    }

    public static <T> Collector<T, Map<Value, Value>, DamlGenMap> collector(
            Function<T, Value> keyMapper,
            Function<T, Value> valueMapper) {

        return Collector.of(
                LinkedHashMap::new,
                (acc, entry) -> acc.put(keyMapper.apply(entry), valueMapper.apply(entry)),
                (left, right) -> { left.putAll(right); return left; },
                DamlGenMap::new
        );
    }

    public @Nonnull
    java.util.Map<Value, Value> getMap() { return value; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DamlGenMap other = (DamlGenMap) o;
        return Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public @NonNull String toString() {
        StringJoiner sj = new StringJoiner(", ", "GenMap{", "}");
        value.forEach((key, value) -> sj.add(key.toString()+ " -> " + value.toString()));
        return sj.toString();
    }

    @Override
    public @Nonnull ValueOuterClass.Value toProto() {
        ValueOuterClass.GenMap.Builder mb = ValueOuterClass.GenMap.newBuilder();
        value.forEach((key, value) ->
                mb.addEntries(ValueOuterClass.GenMap.Entry.newBuilder()
                        .setKey(key.toProto())
                        .setValue(value.toProto())
                ));
        return ValueOuterClass.Value.newBuilder().setGenMap(mb).build();
    }

    public static DamlGenMap fromProto(ValueOuterClass.GenMap map){
        return map.getEntriesList().stream().collect(collector(
                entry -> fromProto(entry.getKey()),
                entry -> fromProto(entry.getValue())
        ));
    }
}
