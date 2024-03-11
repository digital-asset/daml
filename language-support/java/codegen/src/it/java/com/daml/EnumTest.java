// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    assertEquals(expected, actual);
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
    assertEquals(expected, actual);
  }

  @Test
  void roundtripAllUpperCaseJson() throws JsonLfDecoder.Error {
    ALLUPPERCASE expected = ALLUPPERCASE.ALLUPPERCASE;
    ALLUPPERCASE actual = ALLUPPERCASE.fromJson(expected.toJson());
    assertEquals(expected, actual);
  }
}
