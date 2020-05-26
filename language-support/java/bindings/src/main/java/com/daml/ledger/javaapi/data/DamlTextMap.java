// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DamlTextMap extends Value {

    private final Map<String, Value> map;

    DamlTextMap(Map<String, Value> map){
        this.map = map;
    }

    /**
     * The map that is passed to this constructor must not be changed once passed.
     */
    static @NonNull DamlTextMap fromPrivateMap(Map<@NonNull String, @NonNull Value> value){
        return new DamlTextMap(Collections.unmodifiableMap(value));
    }

    private static @NonNull DamlTextMap EMPTY = fromPrivateMap(Collections.emptyMap());

    public static DamlTextMap of(@NonNull Map<@NonNull String, @NonNull Value> value){
        return fromPrivateMap(new HashMap<>(value));
    }

    @Deprecated // use DamlTextMap::toMap or DamlTextMap::stream
    public final @NonNull Map<@NonNull String, @NonNull Value> getMap() {
        return toMap(Function.identity());
    }

    public Stream<Map.Entry<String, Value>> stream(){
        return map.entrySet().stream();
    }

    public final @NonNull <K, V> Map< K,  V> toMap(
            @NonNull Function<@NonNull String, @NonNull K> keyMapper,
            @NonNull Function<@NonNull Value, @NonNull V> valueMapper
    ){
        return stream().collect(Collectors.toMap(
                e -> keyMapper.apply(e.getKey()),
                e -> valueMapper.apply(e.getValue())
        ));
    }

    public final @NonNull <V> Map<@NonNull String,@NonNull V> toMap(
            @NonNull Function<@NonNull Value, @NonNull V> valueMapper
    ){
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
        ValueOuterClass.Map.Builder mb = ValueOuterClass.Map.newBuilder();
        map.forEach((k, v) ->
                mb.addEntries(ValueOuterClass.Map.Entry.newBuilder()
                        .setKey(k)
                        .setValue(v.toProto())
                )
        );
        return ValueOuterClass.Value.newBuilder().setMap(mb).build();
    }

    public static @NonNull DamlTextMap fromProto(ValueOuterClass.Map map) {
        return map.getEntriesList().stream().collect(DamlCollectors.toDamlTextMap(
                ValueOuterClass.Map.Entry::getKey,
                entry -> fromProto(entry.getValue())
        ));
    }
}
