// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This test suite encompasses all tests that should be run for all Daml-LF target versions, and
 * should be included in the AllTests class for specific Daml-LF target versions.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  ContractKeysTest.class,
  DecoderTest.class,
  ListTest.class,
  OptionalTest.class,
  ParametrizedContractIdTest.class,
  RecordTest.class,
  TemplateMethodTest.class,
  TextMapTest.class,
  VariantTest.class,
})
public class AllGenericTests {}
