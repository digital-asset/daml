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

@RunWith(JUnitPlatform.class)
public class UpgradeTest {

  @Test
  void exactMatch() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")), new DamlRecord.Field(new Text("def")));
    NoOptional actual = NoOptional.fromValue(record);

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
    NoOptional actual = NoOptional.fromValue(record);

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
    assertThrows(IllegalArgumentException.class, () -> NoOptional.fromValue(record));
  }

  @Test
  void downgradeNonOptionalFails() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")),
            new DamlRecord.Field(new Text("def")),
            new DamlRecord.Field(Unit.getInstance()));
    assertThrows(IllegalArgumentException.class, () -> NoOptional.fromValue(record));
  }

  @Test
  void upgradeOptionalFieldsTwoMissingOptionals() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(new Text("abc")), new DamlRecord.Field(new Text("def")));
    OptionalAtEnd actual = OptionalAtEnd.fromValue(record);
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
    OptionalAtEnd actual = OptionalAtEnd.fromValue(record);
    OptionalAtEnd expected = new OptionalAtEnd("abc", "def", Optional.of("ghi"), Optional.empty());
    assertEquals(actual, expected);
  }

  @Test
  void upgradeNonOptionalFields() {
    DamlRecord record = new DamlRecord(new DamlRecord.Field(new Text("abc")));
    assertThrows(IllegalArgumentException.class, () -> NoOptional.fromValue(record));
  }
}
