// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  DecimalTestForAll.class,
  EnumTestForForAll.class,
  NumericTestFor1_7AndFor1_8AndFor1_11AndFor1_12ndFor1_13AndFor1_14AndFor1_dev.class,
  GenMapTestFor1_11AndFor1_12ndFor1_13AndFor1_14AndFor1_dev.class,
})
public class AllTestsFor1_14 {}
