// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.math.BigDecimal;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DecimalTestForAll.class,
        NumericTestFor1_7AndFor1_8AndFor1_dev.class,
        GenMapTestFor1_dev.class,
})
public class AllTestsFor1_dev { }
