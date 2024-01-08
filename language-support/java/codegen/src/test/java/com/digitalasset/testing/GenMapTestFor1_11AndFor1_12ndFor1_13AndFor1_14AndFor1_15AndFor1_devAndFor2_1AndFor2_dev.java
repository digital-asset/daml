// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.genmapmod.Box;
import test.recordmod.Pair;
import test.variantmod.Either;
import test.variantmod.either.*;

@RunWith(JUnitPlatform.class)
public
class GenMapTestFor1_11AndFor1_12ndFor1_13AndFor1_14AndFor1_15AndFor1_devAndFor2_1AndFor2_dev {

  private BigDecimal bg1() {
    return new BigDecimal("1.0000000000");
  }

  private BigDecimal bg2() {
    return new BigDecimal("-2.2222222222");
  }

  private BigDecimal bg3() {
    return new BigDecimal("3.3333333333");
  }

  private Pair<Long, BigDecimal> pair1() {
    return new Pair<>(1L, bg1());
  }

  private Pair<Long, BigDecimal> pair2() {
    return new Pair<>(2L, bg2());
  }

  private Pair<Long, BigDecimal> pair3() {
    return new Pair<>(3L, bg3());
  }

  private Box box() {
    Map<Pair<Long, BigDecimal>, Either<Long, BigDecimal>> map = new LinkedHashMap<>();
    map.put(pair1(), new Right<>(bg1()));
    map.put(pair2(), new Left<>(2L));
    map.put(pair3(), new Right<>(bg3()));
    return new Box(map, "alice");
  }

  @Test
  void genMap2Value2GenMap() {
    Box b = box();
    assertEquals(Box.fromValue(b.toValue()), b);
  }

  @Test
  void genMap2Value2GenMapJson() throws JsonLfDecoder.Error {
    Box b = box();
    assertEquals(Box.fromJson(b.toJson()), b);
  }

  @Test
  void toValuePreservesOrder() {
    Box b = box();
    Object[] keys = b.x.keySet().toArray();
    assertEquals(keys.length, 3);
    assertEquals(keys[0], pair1());
    assertEquals(keys[1], pair2());
    assertEquals(keys[2], pair3());
  }

  @Test
  void fromJson() throws IOException {
    Box b =
        Box.fromJson(
            "{"
                + "\"party\": \"alice\", "
                + "\"x\": [ "
                + "  [ [1, \"1.0000000000\"], {\"tag\": \"Right\", \"value\": \"1.0000000000\"} ], "
                + "  [ [2, \"-2.2222222222\"], {\"tag\": \"Left\", \"value\": 2} ], "
                + "  [ [3, \"3.3333333333\"], {\"tag\": \"Right\", \"value\": \"3.3333333333\"} ] "
                + "]"
                + "}");
    assertEquals(box(), b);
  }

  private DamlRecord pair(Long fst, BigDecimal snd) {
    return new DamlRecord(
        new DamlRecord.Field("fst", new Int64(fst)), new DamlRecord.Field("snd", new Numeric(snd)));
  }

  private Variant left(Long l) {
    return new Variant("Left", new Int64(l));
  }

  private Variant right(BigDecimal r) {
    return new Variant("Right", new Numeric(r));
  }

  private DamlRecord pairValue1() {
    return pair(1L, bg1());
  }

  private DamlRecord pairValue2() {
    return pair(-2L, bg2());
  }

  private DamlRecord pairValue3() {
    return pair(3L, bg3());
  }

  private DamlRecord value() {
    Map<Value, Value> value = new LinkedHashMap<>();
    value.put(pairValue1(), left(1L));
    value.put(pairValue2(), right(bg2()));
    value.put(pairValue3(), left(3L));
    Value map = DamlGenMap.of(value);
    return new DamlRecord(
        new DamlRecord.Field("x", map), new DamlRecord.Field("party", new Party("alice")));
  }

  @Test
  void value2GenMap2value() {
    DamlRecord b = value();
    assertEquals(Box.fromValue(b).toValue(), b);
  }

  @Test
  void fromValuePreservesOrder() {
    DamlRecord b = value();
    Object[] keys =
        b.getFieldsMap().get("x").asGenMap().get().stream().map(Map.Entry::getKey).toArray();
    Object[] expected = {pairValue1(), pairValue2(), pairValue3()};
    assertArrayEquals(keys, expected);
  }
}
