// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;
import java.util.stream.Collectors;

public final class DamlGenMap extends Value {

    private final Map<Value, Value> map;

    private DamlGenMap(@NonNull Map<@NonNull Value, @NonNull Value> value) {
        this.map = value;
    }

    /**
     * The map that is passed to this constructor must not be changed
     * once passed.
     */
    protected static @NonNull DamlGenMap fromPrivateMap(@NonNull Map<@NonNull Value, @NonNull Value> map){
        return new DamlGenMap(Collections.unmodifiableMap(map));
    }

    private static DamlGenMap EMPTY = fromPrivateMap(Collections.EMPTY_MAP);

    public static DamlGenMap of(@NonNull Map<@NonNull Value, @NonNull Value> map){
       return new DamlGenMap(new LinkedHashMap<>(map));
    }

    public @NonNull Map<@NonNull Value, @NonNull Value> getMap() { return map; }

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
    public ValueOuterClass.Value toProto() {
        ValueOuterClass.GenMap.Builder mb = ValueOuterClass.GenMap.newBuilder();
        map.forEach((key, value) ->
                mb.addEntries(ValueOuterClass.GenMap.Entry.newBuilder()
                        .setKey(key.toProto())
                        .setValue(value.toProto())
                ));
        return ValueOuterClass.Value.newBuilder().setGenMap(mb).build();
    }

    public static @NonNull DamlGenMap fromProto(ValueOuterClass.GenMap map){
        return map.getEntriesList().stream().collect(DamlCollectors.toDamlGenMap(
                entry -> fromProto(entry.getKey()),
                entry -> fromProto(entry.getValue())
        ));
    }
}
