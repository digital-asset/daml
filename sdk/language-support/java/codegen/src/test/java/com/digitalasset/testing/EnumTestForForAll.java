// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;

import static org.junit.jupiter.api.Assertions.*;

import com.daml.ledger.javaapi.data.DamlEnum;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.Variant;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.enummod.*;
import test.enummod.coloredtree.*;
import test.enummod.optionalcolor.*;

@RunWith(JUnitPlatform.class)
public class EnumTestForForAll {

  @Test
  void enum2Value2Enum() {
    for (Color c : new Color[] {Color.RED, Color.GREEN, Color.BLUE}) {
      Box record = new Box(c, "party");
      OptionalColor variant = new SomeColor(c);
      ColoredTree variantRecord =
          new Node(Color.RED, new Leaf(Unit.getInstance()), new Leaf(Unit.getInstance()));
      assertEquals(Color.fromValue(c.toValue()), c);
      assertEquals(Box.fromValue(record.toValue()), record);
      assertEquals(OptionalColor.fromValue(variant.toValue()), variant);
      assertEquals(ColoredTree.fromValue(variantRecord.toValue()), variantRecord);
    }
  }

  @Test
  void value2Enum2value() {
    for (String s : new String[] {"Red", "Green", "Blue"}) {
      DamlEnum damlEnum = new DamlEnum(s);
      DamlRecord record =
          new DamlRecord(
              new DamlRecord.Field("x", damlEnum),
              new DamlRecord.Field("party", new Party("party")));
      Variant variant = new Variant("SomeColor", damlEnum);
      Variant leaf = new Variant("Leaf", Unit.getInstance());
      DamlRecord node =
          new DamlRecord(
              new DamlRecord.Field("color", damlEnum),
              new DamlRecord.Field("left", leaf),
              new DamlRecord.Field("right", leaf));
      Variant tree = new Variant("Node", node);
      assertEquals(Color.fromValue(damlEnum).toValue(), damlEnum);
      assertEquals(Box.fromValue(record).toValue(), record);
      assertEquals(OptionalColor.fromValue(variant).toValue(), variant);
      assertEquals(ColoredTree.fromValue(tree).toValue(), tree);
    }
  }

  @Test
  void badValue2Enum() {
    DamlEnum value = new DamlEnum("Yellow");
    assertThrows(IllegalArgumentException.class, () -> Color.fromValue(value));
  }
}
