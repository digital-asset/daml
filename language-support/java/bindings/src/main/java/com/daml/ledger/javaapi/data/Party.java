// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public class Party extends Value {

    private final String value;

    public Party(@NonNull String value) {
        this.value = value;
    }

    @Override
    public ValueOuterClass.Value toProto() {
        return ValueOuterClass.Value.newBuilder().setParty(this.value).build();
    }

    @NonNull
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Party{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Party party = (Party) o;
        return Objects.equals(value, party.value);
    }

    @Override
    public int hashCode() {

        return Objects.hash(value);
    }
}
