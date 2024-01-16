// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Numeric;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.numericmod.Box;

@RunWith(JUnitPlatform.class)
public class NumericTestForAll {

  @Test
  void numeric2Value2Numeric() {

    Box b =
        new Box(
            new BigDecimal(0),
            new BigDecimal(10),
            new BigDecimal(17),
            new BigDecimal("0.37"),
            "alice");
    assertEquals(Box.fromValue(b.toValue()), b);
  }

  @Test
  void numeric2Value2NumericJson() throws JsonLfDecoder.Error {
    Box b =
        new Box(
            new BigDecimal(0),
            new BigDecimal(10),
            new BigDecimal(17),
            new BigDecimal("0.37"),
            "alice");
    assertEquals(Box.fromJson(b.toJson()), b);
  }

  @Test
  void value2Decimal2value() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field("x0", new Numeric(new BigDecimal(0))),
            new DamlRecord.Field("x10", new Numeric(new BigDecimal(10))),
            new DamlRecord.Field("x17", new Numeric(new BigDecimal(17))),
            new DamlRecord.Field("x37", new Numeric(new BigDecimal("0.37"))),
            new DamlRecord.Field("party", new Party("alice")));
    assertEquals(Box.fromValue(record).toValue(), record);
  }

  @Test
  void testFromJson() throws java.io.IOException {
    Box expected =
        new Box(
            new BigDecimal(0),
            new BigDecimal(10),
            new BigDecimal(17),
            new BigDecimal("0.37"),
            "alice");
    assertEquals(
        expected,
        Box.fromJson("{\"x0\":0, \"x10\":\"10\", \"x17\":17, \"x37\":0.37, \"party\":\"alice\"}"));
  }
}
