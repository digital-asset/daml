// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.*;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;
import java.util.ArrayList;
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

    NoOptional roundTripped =
        NoOptional.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.STRICT);
    NoOptional roundTrippedWithIgnore =
        NoOptional.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(actual, expected);
    assertEquals(expected, roundTripped);
    assertEquals(expected, roundTrippedWithIgnore);
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

    NoOptional roundTripped =
        NoOptional.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.STRICT);
    NoOptional roundTrippedWithIgnore =
        NoOptional.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(actual, expected);
    assertEquals(expected, roundTripped);
    assertEquals(expected, roundTrippedWithIgnore);
  }

  @Test
  void downgradeNonEmptyOptionalFails() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")),
            new DamlRecord.Field(new Text("def")),
            new DamlRecord.Field(DamlOptional.of(Unit.getInstance())));
    assertThrows(IllegalArgumentException.class, () -> NoOptional.valueDecoder().decode(record));
    assertThrows(
        IllegalArgumentException.class,
        () -> NoOptional.valueDecoder().decode(record, UnknownTrailingFieldPolicy.STRICT));
  }

  @Test
  void downgradeNonOptionalFails() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")),
            new DamlRecord.Field(new Text("def")),
            new DamlRecord.Field(Unit.getInstance()));
    assertThrows(IllegalArgumentException.class, () -> NoOptional.valueDecoder().decode(record));
    assertThrows(
        IllegalArgumentException.class,
        () -> NoOptional.valueDecoder().decode(record, UnknownTrailingFieldPolicy.STRICT));
  }

  @Test
  void upgradeOptionalFieldsTwoMissingOptionals() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")), new DamlRecord.Field(new Text("def")));
    OptionalAtEnd actual = OptionalAtEnd.valueDecoder().decode(record);
    OptionalAtEnd expected = new OptionalAtEnd("abc", "def", Optional.empty(), Optional.empty());

    OptionalAtEnd roundTripped =
        OptionalAtEnd.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.STRICT);
    OptionalAtEnd roundTrippedWithIgnore =
        OptionalAtEnd.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(actual, expected);
    assertEquals(expected, roundTripped);
    assertEquals(expected, roundTrippedWithIgnore);
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

    OptionalAtEnd roundTripped =
        OptionalAtEnd.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.STRICT);
    OptionalAtEnd roundTrippedWithIgnore =
        OptionalAtEnd.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(actual, expected);
    assertEquals(expected, roundTripped);
    assertEquals(expected, roundTrippedWithIgnore);
  }

  @Test
  void decodeOptionalAtEndWithTrailingOptionalFields() {
    OptionalAtEnd expected = new OptionalAtEnd("abc", "def", Optional.of("ghi"), Optional.empty());

    ArrayList<DamlRecord.Field> fieldsWithTrailing = new ArrayList<>(expected.toValue().getFields());
    fieldsWithTrailing.add(new DamlRecord.Field("extraField", DamlOptional.of(new Text("extra"))));
    DamlRecord recordWithTrailing = new DamlRecord(fieldsWithTrailing);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OptionalAtEnd.valueDecoder()
                .decode(recordWithTrailing, UnknownTrailingFieldPolicy.STRICT));

    OptionalAtEnd fromIgnore =
        OptionalAtEnd.valueDecoder()
            .decode(recordWithTrailing, UnknownTrailingFieldPolicy.IGNORE);
    assertEquals(expected, fromIgnore);
  }

  @Test
  void decodeNoOptionalWithTrailingOptionalFields() {
    NoOptional expected = new NoOptional("abc", "def");

    ArrayList<DamlRecord.Field> fieldsWithTrailing = new ArrayList<>(expected.toValue().getFields());
    fieldsWithTrailing.add(new DamlRecord.Field("extraField", DamlOptional.of(new Text("extra"))));
    DamlRecord recordWithTrailing = new DamlRecord(fieldsWithTrailing);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            NoOptional.valueDecoder()
                .decode(recordWithTrailing, UnknownTrailingFieldPolicy.STRICT));

    NoOptional fromIgnore =
        NoOptional.valueDecoder()
            .decode(recordWithTrailing, UnknownTrailingFieldPolicy.IGNORE);
    assertEquals(expected, fromIgnore);
  }

  @Test
  void upgradeNonOptionalFields() {
    DamlRecord record = new DamlRecord(new DamlRecord.Field(new Text("abc")));
    assertThrows(IllegalArgumentException.class, () -> NoOptional.valueDecoder().decode(record));
    assertThrows(
        IllegalArgumentException.class,
        () -> NoOptional.valueDecoder().decode(record, UnknownTrailingFieldPolicy.STRICT));
  }

  @Test
  void exactMatchVariant() {
    Variant variant = new Variant("MyVariant1", new Text("abc"));
    MyVariant actual = MyVariant.valueDecoder().decode(variant);

    MyVariant expected = new MyVariant1("abc");

    MyVariant roundTripped =
        MyVariant.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.STRICT);
    MyVariant roundTrippedWithIgnore =
        MyVariant.valueDecoder()
            .decode(expected.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(actual, expected);
    assertEquals(expected, roundTripped);
    assertEquals(expected, roundTrippedWithIgnore);
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
    Exception exceptionStrict =
        assertThrows(
            IllegalArgumentException.class,
            () -> MyVariant.valueDecoder().decode(variant, UnknownTrailingFieldPolicy.STRICT));
    assertTrue(
        exceptionStrict
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

    MyEnum roundTripped =
        MyEnum.valueDecoder().decode(expected.toValue(), UnknownTrailingFieldPolicy.STRICT);
    MyEnum roundTrippedWithIgnore =
        MyEnum.valueDecoder().decode(expected.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(actual, expected);
    assertEquals(expected, roundTripped);
    assertEquals(expected, roundTrippedWithIgnore);
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
    Exception exceptionStrict =
        assertThrows(
            IllegalArgumentException.class,
            () -> MyEnum.valueDecoder().decode(damlenum, UnknownTrailingFieldPolicy.STRICT));
    assertTrue(
        exceptionStrict
            .getMessage()
            .contains(
                "Found unknown constructor MyEnum3 for enum tests.upgradetest.MyEnum, expected one"
                    + " of [MyEnum1, MyEnum2]. This could be a failed enum downgrade."));
  }
}
