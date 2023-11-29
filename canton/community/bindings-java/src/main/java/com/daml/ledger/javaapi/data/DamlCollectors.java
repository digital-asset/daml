// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;

public final class DamlCollectors {

  // no instantiation
  private DamlCollectors() {}

  public static <T> Collector<T, List<Value>, DamlList> toDamlList(Function<T, Value> valueMapper) {
    return Collector.of(
        ArrayList::new,
        (acc, entry) -> acc.add(valueMapper.apply(entry)),
        (left, right) -> {
          left.addAll(right);
          return left;
        },
        DamlList::fromPrivateList);
  }

  public static Collector<Value, List<Value>, DamlList> toDamlList() {
    return toDamlList(Function.identity());
  }

  public static <T> Collector<T, Map<String, Value>, DamlTextMap> toDamlTextMap(
      Function<T, String> keyMapper, Function<T, Value> valueMapper) {

    return Collector.of(
        HashMap::new,
        (acc, entry) -> acc.put(keyMapper.apply(entry), valueMapper.apply(entry)),
        (left, right) -> {
          left.putAll(right);
          return left;
        },
        DamlTextMap::fromPrivateMap);
  }

  public static Collector<Map.Entry<String, Value>, Map<String, Value>, DamlTextMap>
      toDamlTextMap() {
    return toDamlTextMap(Map.Entry::getKey, Map.Entry::getValue);
  }

  public static <T> Collector<T, Map<Value, Value>, DamlGenMap> toDamlGenMap(
      Function<T, Value> keyMapper, Function<T, Value> valueMapper) {

    return Collector.of(
        LinkedHashMap::new,
        (acc, entry) -> acc.put(keyMapper.apply(entry), valueMapper.apply(entry)),
        (left, right) -> {
          left.putAll(right);
          return left;
        },
        DamlGenMap::fromPrivateMap);
  }

  public static Collector<Map.Entry<Value, Value>, Map<Value, Value>, DamlGenMap> toMap() {
    return toDamlGenMap(Map.Entry::getKey, Map.Entry::getValue);
  }
}
