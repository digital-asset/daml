// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DecimalTestForAll.class,
        EnumTestFor1_6AndFor1_dev.class,
})
public class AllTestsFor1_6 { }
