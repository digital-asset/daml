// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.LocalDate;
import java.util.Objects;

public class Date extends Value {

    private final LocalDate value;

    public Date(int value) {
        this.value = LocalDate.ofEpochDay(value);
    }

    @Override
    public ValueOuterClass.Value toProto() {
        return ValueOuterClass.Value.newBuilder().setDate((int)this.value.toEpochDay()).build();
    }

    @NonNull
    public LocalDate getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Date{" +
                "value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Date date = (Date) o;
        return Objects.equals(value, date.value);
    }

    @Override
    public int hashCode() {

        return Objects.hash(value);
    }
}
