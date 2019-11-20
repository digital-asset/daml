// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;

public final class DamlList extends Value {

    private final List<Value> values;

    /**
     * The list that is passed to this constructor must not be changed
     * once passed.
     */
    public DamlList(@NonNull List<@NonNull Value> values) {
        this.values = values;
    }

    @SafeVarargs
    public DamlList(@NonNull Value...values) {
        this(Arrays.asList(values));
    }

    public @NonNull List<@NonNull Value> getValues() {
        return values;
    }

    public static <T> Collector<T, ArrayList<Value>, DamlList> collector(Function<T, Value> valueMapper) {
        return Collector.of(
                ArrayList::new,
                (acc, entry) -> acc.add(valueMapper.apply(entry)),
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                DamlList::new
        );
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
        return list.getElementsList().stream().collect(collector(Value::fromProto));
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