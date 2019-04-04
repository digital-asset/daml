// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import java.util.Objects;

public class DamlOptional extends Value {

    private static DamlOptional EMPTY = new DamlOptional(java.util.Optional.empty());

    private final Value value;

    private DamlOptional(Value value) {
        this.value = value;
    }

    public DamlOptional(java.util.Optional<Value> value) {
        this.value = value.orElse(null);
    }

    public @Nonnull
    java.util.Optional<Value> getValue() {
        return java.util.Optional.ofNullable(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DamlOptional optional = (DamlOptional) o;
        return Objects.equals(value, optional.value);
    }

    public boolean isEmpty() {
        return value == null;
    }

    @Override
    public int hashCode() {
        return (value == null) ? 0 : value.hashCode();
    }

    @Override
    public @NonNull String toString() {
        return "Optional{" +
                "value=" + value +
                '}';
    }

    public static @Nonnull  DamlOptional of(Value value) {
        return value == null ? EMPTY : new DamlOptional(value);
    }

    public static @Nonnull DamlOptional empty() {
        return (DamlOptional) EMPTY;
    }

    @Override
    public @Nonnull ValueOuterClass.Value toProto() {
        ValueOuterClass.Optional.Builder ob = ValueOuterClass.Optional.newBuilder();
        if (value != null) ob.setValue(value.toProto());
        return ValueOuterClass.Value.newBuilder()
                .setOptional(ob.build())
                .build();
    }
}
