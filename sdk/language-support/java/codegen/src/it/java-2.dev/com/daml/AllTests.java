// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  AllGenericTests.class,
  ContractKeysTest.class,
  TextMapTest.class,
  InterfacePackageNameAndVersionTest.class
})
public class AllTests {}
