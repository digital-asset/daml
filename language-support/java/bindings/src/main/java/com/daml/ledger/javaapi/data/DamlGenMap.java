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

    private final Map<Value, Value> map;

    /**
     * The map that is passed to this constructor must not be changed
     * once passed.
     */
    private DamlGenMap(@NonNull Map<@NonNull Value, @NonNull Value> value) {
        this.map = value;
    }

    private static @Nonnull DamlGenMap fromPrivateMap(@NonNull Map<@NonNull Value, @NonNull Value> map){
        return new DamlGenMap(Collections.unmodifiableMap(map));
    }

    private static DamlGenMap EMPTY = fromPrivateMap(Collections.EMPTY_MAP);

    public static DamlGenMap of(@NonNull Map<@NonNull Value, @NonNull Value> map){
       return new DamlGenMap(new LinkedHashMap<>(map));
    }

    public static <T> Collector<T, Map<Value, Value>, DamlGenMap> collector(
            Function<T, Value> keyMapper,
            Function<T, Value> valueMapper) {

        return Collector.of(
                LinkedHashMap::new,
                (acc, entry) -> acc.put(keyMapper.apply(entry), valueMapper.apply(entry)),
                (left, right) -> { left.putAll(right); return left; },
                DamlGenMap::fromPrivateMap
        );
    }

    public @Nonnull Map<@NonNull Value, @NonNull Value> getMap() { return map; }

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
        map.forEach((key, value) -> sj.add(key.toString()+ " -> " + value.toString()));
        return sj.toString();
    }

    @Override
    public @Nonnull ValueOuterClass.Value toProto() {
        ValueOuterClass.GenMap.Builder mb = ValueOuterClass.GenMap.newBuilder();
        map.forEach((key, value) ->
                mb.addEntries(ValueOuterClass.GenMap.Entry.newBuilder()
                        .setKey(key.toProto())
                        .setValue(value.toProto())
                ));
        return ValueOuterClass.Value.newBuilder().setGenMap(mb).build();
    }

    public static @Nonnull DamlGenMap fromProto(@Nonnull ValueOuterClass.GenMap map){
        return map.getEntriesList().stream().collect(collector(
                entry -> fromProto(entry.getKey()),
                entry -> fromProto(entry.getValue())
        ));
    }
}
