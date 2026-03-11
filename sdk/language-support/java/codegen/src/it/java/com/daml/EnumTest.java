// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.enumtest.ALLUPPERCASE;
import tests.enumtest.FooBar;

@RunWith(JUnitPlatform.class)
public class EnumTest {

  @Test
  void roundtripFooValue() {
    FooBar expected = FooBar.FOO;
    FooBar actual = FooBar.valueDecoder().decode(expected.toValue());
    FooBar roundTripped =
        FooBar.valueDecoder().decode(expected.toValue(), UnknownTrailingFieldPolicy.STRICT);
    FooBar roundTrippedWithIgnore =
        FooBar.valueDecoder().decode(expected.toValue(), UnknownTrailingFieldPolicy.IGNORE);
    assertEquals(expected, actual);
    assertEquals(expected, roundTripped);
    assertEquals(expected, roundTrippedWithIgnore);
  }

  @Test
  void roundtripFoo() throws JsonLfDecoder.Error {
    FooBar expected = FooBar.FOO;
    FooBar actual = FooBar.fromJson(expected.toJson());
    assertEquals(expected, actual);
  }

  @Test
  void roundtripAllUpperCaseValue() {
    ALLUPPERCASE expected = ALLUPPERCASE.ALLUPPERCASE;
    ALLUPPERCASE actual = ALLUPPERCASE.valueDecoder().decode(expected.toValue());
    ALLUPPERCASE roundTripped =
        ALLUPPERCASE.valueDecoder().decode(expected.toValue(), UnknownTrailingFieldPolicy.STRICT);
    ALLUPPERCASE roundTrippedWithIgnore =
        ALLUPPERCASE.valueDecoder().decode(expected.toValue(), UnknownTrailingFieldPolicy.IGNORE);
    assertEquals(expected, actual);
    assertEquals(expected, roundTripped);
    assertEquals(expected, roundTrippedWithIgnore);
  }

  @Test
  void roundtripAllUpperCaseJson() throws JsonLfDecoder.Error {
    ALLUPPERCASE expected = ALLUPPERCASE.ALLUPPERCASE;
    ALLUPPERCASE actual = ALLUPPERCASE.fromJson(expected.toJson());
    assertEquals(expected, actual);
  }
}
