// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.lf_1_5;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import com.digitalasset.AllGenericTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AllGenericTests.class,
        ListTest.class,
        OptionalTest.class,
        MapTest.class,
        ContractKeysTest.class,
})
public class AllTests {
}
