// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.lf_1_0;

import com.digitalasset.AllGenericTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        OptionalTest.class,
        ListTest.class,
        AllGenericTests.class
})
public class AllTests {
}
