// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing;


import com.daml.ledger.javaapi.data.Decimal;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Record;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.decimal.Box;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnitPlatform.class)
public class DecimalTestFor$All$ {

    private String[] goodValues = {
            "-9999999999999999999999999999.9999999999",
            "-1.0",
            "0.0",
            "1.0",
            "3.1415926536",
            "42.0",
            "9999999999999999999999999999.9999999999",
    };

    @Test
    void decimal2Value2Decimal() {
        for(String s : goodValues) {
            Box b = new Box(new BigDecimal(s), "alice");;
            assertEquals(Box.fromValue(b.toValue()), b);
        }
    }

    @Test
    void value2Decimal2value() {
        Record.Field partiField = new Record.Field("party", new Party("alice"));
        for(String s : goodValues) {
            Record record = new Record(
                    new Record.Field("x", new Decimal(new BigDecimal(s))),
                    partiField
            );
            assertEquals(Box.fromValue(record).toValue(), record);
        }
    }

}
