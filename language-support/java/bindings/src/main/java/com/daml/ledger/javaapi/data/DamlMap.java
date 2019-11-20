// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;

public final class DamlMap extends Value {

    private Map<String, Value> value;

    private DamlMap(){ }

    protected static DamlMap fromPrivateMap(Map<@NonNull String, @NonNull Value> value){
        DamlMap damlMap = new DamlMap();
        damlMap.value = Collections.unmodifiableMap(value);
        return damlMap;
    }

    private static @NonNull DamlMap EMPTY = fromPrivateMap(Collections.emptyMap());

    public static DamlMap of(@NonNull Map<@NonNull String, @NonNull Value> value){
        return fromPrivateMap(new HashMap<>(value));
    }

    @Deprecated // use DamlMap:of
    public DamlMap(Map<String, Value> value) {
        this.value = Collections.unmodifiableMap(new HashMap<>(value));
    }

    public @NonNull Map<@NonNull String, @NonNull Value> getMap() { return value; }

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
    public ValueOuterClass.Value toProto() {
        ValueOuterClass.Map.Builder mb = ValueOuterClass.Map.newBuilder();
        value.forEach((k, v) ->
                mb.addEntries(ValueOuterClass.Map.Entry.newBuilder()
                        .setKey(k)
                        .setValue(v.toProto())
                )
        );

        return ValueOuterClass.Value.newBuilder().setMap(mb).build();
    }

    public static @NonNull DamlMap fromProto(ValueOuterClass.Map map) {
        return map.getEntriesList().stream().collect(DamlCollectors.toDamlMap(
                ValueOuterClass.Map.Entry::getKey,
                entry -> fromProto(entry.getValue())
        ));
    }
}
