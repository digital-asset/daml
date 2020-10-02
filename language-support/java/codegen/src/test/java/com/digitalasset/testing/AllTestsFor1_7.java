// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DecimalTestForAll.class,
        EnumTestFor1_6AndFor1_7AndFor1_8.class,
        NumericTestFor1_7AndFor1_8AndFor1_dev.class,
})
public class AllTestsFor1_7{ }
