// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

public final class DamlCollectors {

    public static <T> Collector<T, ArrayList<Value>, DamlList> toDamlList(Function<T, Value> valueMapper) {
        return Collector.of(
                ArrayList::new,
                (acc, entry) -> acc.add(valueMapper.apply(entry)),
                (left, right) -> { left.addAll(right); return left; },
                DamlList::fromPrivateList
        );
    }

    public static <T> Collector<T, Map<String, Value>, DamlMap> toDamlMap(
            Function<T, String> keyMapper,
            Function<T, Value> valueMapper) {

        return Collector.of(
                HashMap::new,
                (acc, entry) -> acc.put(keyMapper.apply(entry), valueMapper.apply(entry)),
                (left, right) -> { left.putAll(right); return left; },
                DamlMap::fromPrivateMap
        );
    }

    public static <T> Collector<T, Map<Value, Value>, DamlGenMap> toDamlGenMap(
            Function<T, Value> keyMapper,
            Function<T, Value> valueMapper) {

        return Collector.of(
                LinkedHashMap::new,
                (acc, entry) -> acc.put(keyMapper.apply(entry), valueMapper.apply(entry)),
                (left, right) -> { left.putAll(right); return left; },
                DamlGenMap::fromPrivateMap
        );
    }

}
