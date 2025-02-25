// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ValueOuterClass;
import com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public class Bytes extends Value {
    private final ByteString value;

    public Bytes(@NonNull ByteString value) {
        this.value = value;
    }

    @NonNull
    public ByteString getValue() {
        return value;
    }

    @Override
    public ValueOuterClass.Value toProto() {
        return ValueOuterClass.Value.newBuilder().setBytes(this.value).build();
    }

    @Override
    public String toString() {
        return "Bytes{" + "value='" + value.toStringUtf8() + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteString bytes = (ByteString) o;
        return Objects.equals(value, bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
