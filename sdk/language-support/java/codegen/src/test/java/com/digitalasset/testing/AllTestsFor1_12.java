// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  DecimalTestForAll.class,
  EnumTestForForAll.class,
  NumericTestForAll.class,
  GenMapTestFor1_11AndFor1_12ndFor1_13AndFor1_14AndFor1_15AndFor1_devAndFor2_dev.class,
})
public class AllTestsFor1_12 {}
