// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;

import static org.junit.jupiter.api.Assertions.*;

import com.daml.ledger.javaapi.data.DamlEnum;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.Variant;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.enummod.*;
import test.enummod.coloredtree.*;
import test.enummod.optionalcolor.*;

@RunWith(JUnitPlatform.class)
public class EnumTestForAll {

  @Test
  void enum2Value2Enum() {
    for (Color c : new Color[] {Color.RED, Color.GREEN, Color.BLUE}) {
      Box record = new Box(c, "party");
      OptionalColor variant = new SomeColor(c);
      ColoredTree variantRecord =
          new Node(Color.RED, new Leaf(Unit.getInstance()), new Leaf(Unit.getInstance()));
      assertEquals(Color.valueDecoder().decode(c.toValue()), c);
      assertEquals(Box.valueDecoder().decode(record.toValue()), record);
      assertEquals(OptionalColor.valueDecoder().decode(variant.toValue()), variant);
      assertEquals(ColoredTree.valueDecoder().decode(variantRecord.toValue()), variantRecord);
    }
  }

  @Test
  void enum2Value2EnumJson() throws JsonLfDecoder.Error {
    for (Color c : new Color[] {Color.RED, Color.GREEN, Color.BLUE}) {
      Box record = new Box(c, "party");
      OptionalColor variant = new SomeColor(c);
      ColoredTree variantRecord =
          new Node(Color.RED, new Leaf(Unit.getInstance()), new Leaf(Unit.getInstance()));
      assertEquals(Color.fromJson(c.toJson()), c);
      assertEquals(Box.fromJson(record.toJson()), record);
      assertEquals(OptionalColor.fromJson(variant.toJson()), variant);
      assertEquals(ColoredTree.fromJson(variantRecord.toJson()), variantRecord);
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
      assertEquals(Color.valueDecoder().decode(damlEnum).toValue(), damlEnum);
      assertEquals(Box.valueDecoder().decode(record).toValue(), record);
      assertEquals(OptionalColor.valueDecoder().decode(variant).toValue(), variant);
      assertEquals(ColoredTree.valueDecoder().decode(tree).toValue(), tree);
    }
  }

  @Test
  void fromJson() throws IOException {
    for (String s : new String[] {"Red", "Green", "Blue"}) {
      String damlEnum = String.format("\"%s\"", s);
      String record = String.format("{\"x\": %s, \"party\":\"party\"}", damlEnum);
      String variant = String.format("{\"tag\": \"SomeColor\", \"value\": %s}", damlEnum);
      String leaf = "{\"tag\": \"Leaf\", \"value\": {}}";
      String node =
          String.format("{\"color\": %s, \"left\": %s, \"right\": %s}", damlEnum, leaf, leaf);
      String tree = String.format("{\"tag\": \"Node\", \"value\": %s}", node);

      assertEquals(Color.valueOf(s.toUpperCase()), Color.fromJson(damlEnum));
      assertEquals(new Box(Color.valueOf(s.toUpperCase()), "party"), Box.fromJson(record));
      assertEquals(new SomeColor(Color.valueOf(s.toUpperCase())), OptionalColor.fromJson(variant));
      assertEquals(
          new Node(
              Color.valueOf(s.toUpperCase()),
              new Leaf(Unit.getInstance()),
              new Leaf(Unit.getInstance())),
          ColoredTree.fromJson(tree));
    }
  }

  @Test
  void badValue2Enum() {
    DamlEnum value = new DamlEnum("Yellow");
    assertThrows(IllegalArgumentException.class, () -> Color.valueDecoder().decode(value));
  }
}
