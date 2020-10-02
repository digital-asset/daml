// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;


import com.daml.ledger.javaapi.data.Numeric;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Record;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.numericmod.Box;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnitPlatform.class)
public class NumericTestFor1_7AndFor1_8AndFor1_dev {

    @Test
    void numeric2Value2Numeric() {

        Box b = new Box(
                new BigDecimal(0),
                new BigDecimal(10),
                new BigDecimal(17),
                new BigDecimal("0.37"),
                "alice"
        );
        assertEquals(Box.fromValue(b.toValue()), b);
    }

    @Test
    void value2Decimal2value() {
        Record record = new Record(
                new Record.Field("x0", new Numeric(new BigDecimal(0))),
                new Record.Field("x10", new Numeric(new BigDecimal(10))),
                new Record.Field("x17", new Numeric(new BigDecimal(17))),
                new Record.Field("x37", new Numeric(new BigDecimal("0.37"))),
                new Record.Field("party", new Party("alice"))
            );
        assertEquals(Box.fromValue(record).toValue(), record);
    }

}
