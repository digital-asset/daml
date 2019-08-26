// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing;


import com.daml.ledger.javaapi.data.DamlEnum;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Record;
import com.daml.ledger.javaapi.data.Variant;
import com.daml.ledger.javaapi.data.Unit;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import test.enum$.*;
import test.enum$.optionalcolor.*;
import test.enum$.coloredtree.*;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class EnumTestFor$1_6$1_dev$ {

    @Test
    void enum2Value2Enum() {
        for(Color c: new Color[]{Color.RED, Color.GREEN, Color.BLUE}) {
            Box record = new Box(c, "party");
            OptionalColor variant = new SomeColor(c);
            ColoredTree variantRecord = new Node(Color.RED, new Leaf(Unit.getInstance()), new Leaf(Unit.getInstance()));
            assertEquals(Color.fromValue(c.toValue()), c);
            assertEquals(Box.fromValue(record.toValue()), record);
            assertEquals(OptionalColor.fromValue(variant.toValue()), variant);
            assertEquals(ColoredTree.fromValue(variantRecord.toValue()), variantRecord);
        }
    }

    @Test
    void value2Enum2value() {
        for(String s: new String[]{"Red", "Green", "Blue"}) {
            DamlEnum damlEnum = new DamlEnum(s);
            Record record = new Record(
                    new Record.Field("x", damlEnum),
                    new Record.Field("party", new Party("party"))
            );
            Variant variant = new Variant("SomeColor", damlEnum);
            Variant leaf = new Variant("Leaf", Unit.getInstance());
            Record node = new Record(
                    new Record.Field("color", damlEnum),
                    new Record.Field("left", leaf),
                    new Record.Field("right", leaf)
            );
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
