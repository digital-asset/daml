// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;

import java.math.BigDecimal;
import java.util.Objects;

public class Decimal extends Value {

    private final BigDecimal value;

    public Decimal(BigDecimal value) {
        this.value = value;
    }

    public static Decimal fromProto(String decimal) {
        return new Decimal(new BigDecimal(decimal));
    }

    @Override
    public ValueOuterClass.Value toProto() {
        return ValueOuterClass.Value.newBuilder().setDecimal(this.value.toPlainString()).build();
    }

    public BigDecimal getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Decimal{" +
                "value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Decimal decimal = (Decimal) o;
        return Objects.equals(value, decimal.value);
    }

    @Override
    public int hashCode() {

        return Objects.hash(value);
    }
}
