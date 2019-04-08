// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class DamlMap extends Value {

    private static DamlMap EMPTY = new DamlMap(Collections.emptyMap());

    private final java.util.Map<String, Value> value;

    public DamlMap(java.util.Map<String, Value> value) {
        this.value = value;
    }

    public @Nonnull
    java.util.Map<String, Value> getMap() { return value; }

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
                        .setKey(ValueOuterClass.Value.newBuilder().setText(k))
                        .setValue(v.toProto())
                )
        );

        return ValueOuterClass.Value.newBuilder().setMap(mb).build();
    }
}
