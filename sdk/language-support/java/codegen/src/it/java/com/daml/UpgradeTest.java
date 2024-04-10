// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.*;

import com.daml.ledger.javaapi.data.*;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.upgradetest.*;
import tests.upgradetest.myvariant.*;

@RunWith(JUnitPlatform.class)
public class UpgradeTest {

  @Test
  void exactMatch() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")), new DamlRecord.Field(new Text("def")));
    NoOptional actual = NoOptional.valueDecoder().decode(record);

    NoOptional expected = new NoOptional("abc", "def");

    assertEquals(actual, expected);
  }

  @Test
  void downgradeEmptyOptional() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")),
            new DamlRecord.Field(new Text("def")),
            new DamlRecord.Field(DamlOptional.EMPTY),
            new DamlRecord.Field(DamlOptional.EMPTY));
    NoOptional actual = NoOptional.valueDecoder().decode(record);

    NoOptional expected = new NoOptional("abc", "def");

    assertEquals(actual, expected);
  }

  @Test
  void downgradeNonEmptyOptionalFails() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")),
            new DamlRecord.Field(new Text("def")),
            new DamlRecord.Field(DamlOptional.of(Unit.getInstance())));
    assertThrows(IllegalArgumentException.class, () -> NoOptional.valueDecoder().decode(record));
  }

  @Test
  void downgradeNonOptionalFails() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")),
            new DamlRecord.Field(new Text("def")),
            new DamlRecord.Field(Unit.getInstance()));
    assertThrows(IllegalArgumentException.class, () -> NoOptional.valueDecoder().decode(record));
  }

  @Test
  void upgradeOptionalFieldsTwoMissingOptionals() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")), new DamlRecord.Field(new Text("def")));
    OptionalAtEnd actual = OptionalAtEnd.valueDecoder().decode(record);
    OptionalAtEnd expected = new OptionalAtEnd("abc", "def", Optional.empty(), Optional.empty());
    assertEquals(actual, expected);
  }

  @Test
  void upgradeOptionalFieldsOneMissingOptional() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")),
            new DamlRecord.Field(new Text("def")),
            new DamlRecord.Field(DamlOptional.of(new Text("ghi"))));
    OptionalAtEnd actual = OptionalAtEnd.valueDecoder().decode(record);
    OptionalAtEnd expected = new OptionalAtEnd("abc", "def", Optional.of("ghi"), Optional.empty());
    assertEquals(actual, expected);
  }

  @Test
  void upgradeNonOptionalFields() {
    DamlRecord record = new DamlRecord(new DamlRecord.Field(new Text("abc")));
    assertThrows(IllegalArgumentException.class, () -> NoOptional.valueDecoder().decode(record));
  }

  @Test
  void exactMatchVariant() {
    Variant variant = new Variant("MyVariant1", new Text("abc"));
    MyVariant actual = MyVariant.valueDecoder().decode(variant);

    MyVariant expected = new MyVariant1("abc");

    assertEquals(actual, expected);
  }

  @Test
  void newMatchVariant() {
    Variant variant = new Variant("MyVariant3", new Text("abc"));
    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> MyVariant.valueDecoder().decode(variant));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "Found unknown constructor MyVariant3 for variant tests.upgradetest.MyVariant,"
                    + " expected one of [MyVariant1, MyVariant2]. This could be a failed variant"
                    + " downgrade."));
  }

  @Test
  void exactMatchEnum() {
    DamlEnum damlenum = new DamlEnum("MyEnum1");
    MyEnum actual = MyEnum.valueDecoder().decode(damlenum);

    MyEnum expected = MyEnum.MYENUM1;

    assertEquals(actual, expected);
  }

  @Test
  void newMatchEnum() {
    DamlEnum damlenum = new DamlEnum("MyEnum3");
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> MyEnum.valueDecoder().decode(damlenum));
    System.out.println(exception);
    assertTrue(
        exception
            .getMessage()
            .contains(
                "Found unknown constructor MyEnum3 for enum tests.upgradetest.MyEnum, expected one"
                    + " of [MyEnum1, MyEnum2]. This could be a failed enum downgrade."));
  }
}
