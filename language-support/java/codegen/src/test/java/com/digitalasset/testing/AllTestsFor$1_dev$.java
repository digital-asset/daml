// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DecimalTestFor$All$.class,
        EnumTestFor$1_6$1_dev$.class
})
public class AllTestsFor$1_dev$ { }
