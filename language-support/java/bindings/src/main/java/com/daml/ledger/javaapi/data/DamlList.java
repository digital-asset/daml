// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public final class DamlList extends Value {

    private final List<Value> values;

    /**
     * The list that is passed to this constructor must not be changed
     * once passed.
     */
    public DamlList(List<Value> values) {
        this.values = values;
    }

    @SafeVarargs
    public DamlList(Value...values) {
        this(Arrays.asList(values));
    }

    public List<Value> getValues() {
        return values;
    }

    @Override
    public ValueOuterClass.Value toProto() {
        ValueOuterClass.List.Builder builder = ValueOuterClass.List.newBuilder();
        for (Value value : this.values) {
            builder.addElements(value.toProto());
        }
        return ValueOuterClass.Value.newBuilder().setList(builder.build()).build();
    }

    public static DamlList fromProto(ValueOuterClass.List list) {
        ArrayList<Value> values = new ArrayList<>(list.getElementsCount());
        for (ValueOuterClass.Value value : list.getElementsList()) {
            values.add(Value.fromProto(value));
        }
        return new DamlList(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DamlList list = (DamlList) o;
        return Objects.equals(values, list.values);
    }

    @Override
    public int hashCode() {

        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return "DamlList{" +
                "values=" + values +
                '}';
    }
}

class ElementOfWrongTypeFound extends RuntimeException {

    public ElementOfWrongTypeFound(ValueOuterClass.List list, ValueOuterClass.Value v, ValueOuterClass.Value.SumCase found, ValueOuterClass.Value.SumCase expected) {
        super(String.format("Expecting list of types %s but found a value of type %s (value: %s, list: %s)", expected, found, v, list));
    }
}