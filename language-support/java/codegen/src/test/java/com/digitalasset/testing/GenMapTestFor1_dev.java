// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;


import com.daml.ledger.javaapi.data.*;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.genmapmod.Box;
import test.recordmod.Pair;
import test.variantmod.Either;
import test.variantmod.either.*;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnitPlatform.class)
public class GenMapTestFor1_dev {

    private BigDecimal bg1() { return new BigDecimal("1.0000000000"); }
    private BigDecimal bg2() { return new BigDecimal("-2.2222222222"); }
    private BigDecimal bg3() { return new BigDecimal("3.3333333333"); }
    private Pair<Long, BigDecimal> pair1() { return new Pair<>(1L, bg1()); }
    private Pair<Long, BigDecimal> pair2() { return new Pair<>(2L, bg2()); }
    private Pair<Long, BigDecimal> pair3() { return new Pair<>(3L, bg3()); }


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
    void toValuePreservesOrder(){
        Box b = box();
        Object[] keys = b.x.keySet().toArray();
        assertEquals(keys.length, 3);
        assertEquals(keys[0], pair1());
        assertEquals(keys[1], pair2());
        assertEquals(keys[2], pair3());
    }

    private Record pair(Long fst, BigDecimal snd) {
        return new Record(
                new Record.Field("fst", new Int64(fst)),
                new Record.Field("snd", new Numeric(snd))
        );
    }

    private Variant left(Long l) {
        return new Variant("Left", new Int64(l));
    }

    private Variant right(BigDecimal r) {
        return new Variant("Right", new Numeric(r));
    }

    private Record pairValue1() { return pair(1L, bg1());   }
    private Record pairValue2() { return pair(-2L, bg2());  }
    private Record pairValue3() { return pair(3L, bg3());   }

    private Record value() {
        Map<Value, Value> value = new LinkedHashMap<>();
        value.put(pairValue1(), left(1L));
        value.put(pairValue2(), right(bg2()));
        value.put(pairValue3(), left(3L));
        Value map = DamlGenMap.of(value);
        return new Record(
                new Record.Field("x", map),
                new Record.Field("party", new Party("alice"))
        );
    }

    @Test
    void value2GenMap2value() {
        Record b = value();
        assertEquals(Box.fromValue(b).toValue(), b);
    }

    @Test
    void fromValuePreservesOrder(){
        Record b = value();
        Object[] keys = b.getFieldsMap().get("x").asGenMap().get().stream().map(Map.Entry::getKey).toArray();
        Object[] expected = {pairValue1(), pairValue2(), pairValue3()};
        assertArrayEquals(keys, expected);
    }


}
