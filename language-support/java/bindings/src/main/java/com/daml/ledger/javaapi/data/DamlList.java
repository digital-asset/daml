// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;
import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.stream.Collector;

public final class DamlList extends Value {

    private List<Value> values;

    private DamlList(){ }

    private static DamlList fromPrivateList(@NonNull List<@NonNull Value> values){
        DamlList damlList = new DamlList();
        damlList.values =  Collections.unmodifiableList(values);
        return damlList;
    }

    private static DamlList EMPTY = fromPrivateList(Collections.EMPTY_LIST);

    public static DamlList of(@NonNull List<@NonNull Value> values){
        return fromPrivateList(new ArrayList<>(values));
    }

    @SafeVarargs
    public static DamlList of(@NonNull Value...values){
        return fromPrivateList(Arrays.asList(values));
    }

    @Deprecated // use DamlList:of
    public DamlList(@NonNull List<@NonNull Value> values) {
        this.values = values;
    }

    @Deprecated // use DamlMap:of
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
                (left, right) -> { left.addAll(right); return left; },
                DamlList::fromPrivateList
        );
    }

    @Override
    public @Nonnull ValueOuterClass.Value toProto() {
        ValueOuterClass.List.Builder builder = ValueOuterClass.List.newBuilder();
        for (Value value : this.values) {
            builder.addElements(value.toProto());
        }
        return ValueOuterClass.Value.newBuilder().setList(builder.build()).build();
    }

    public static @Nonnull DamlList fromProto(@Nonnull ValueOuterClass.List list) {
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
