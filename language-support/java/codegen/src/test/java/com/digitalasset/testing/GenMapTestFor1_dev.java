// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing;


import com.daml.ledger.javaapi.data.*;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.genmapmod.Box;
import test.recordmod.Pair;
import test.variantmod.Either;
import test.variantmod.either.*;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnitPlatform.class)
public class GenMapTestFor1_dev {

    private BigDecimal bg1 = new BigDecimal("1.0000000000");
    private BigDecimal bg2 = new BigDecimal("-2.2222222222");
    private BigDecimal bg3 = new BigDecimal("3.3333333333");

    @Test
    void genMap2Value2GenMap() {
        HashMap<Pair<Long, BigDecimal>, Either<Long, BigDecimal>> map = new HashMap<>();
        map.put(new Pair<>(1L, bg1), new Right<>(bg1));
        map.put(new Pair<>(2L, bg2), new Left<>(2L));
        map.put(new Pair<>(3L, bg3), new Right<>(bg3));
        Box b = new Box(map, "alice");
        assertEquals(Box.fromValue(b.toValue()), b);
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

    @Test
    void value2GenMap2value() {
        Map<Value, Value> value = new HashMap<Value, Value>();
        value.put(pair(1L, bg1), left(1L));
        value.put(pair(-2L,bg2 ), right(bg2));
        value.put(pair(3L, bg3), left(3L));
        Value map = new DamlGenMap(value);
        Record b = new Record(
                new Record.Field("x", map),
                new Record.Field("party", new Party("alice"))
        );
        assertEquals(Box.fromValue(b).toValue(), b);
    }

}
