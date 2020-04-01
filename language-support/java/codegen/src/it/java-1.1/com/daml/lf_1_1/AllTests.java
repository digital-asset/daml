// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf_1_1;

import com.daml.AllGenericTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.List;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        OptionalTest.class,
        ListTest.class,
        AllGenericTests.class
})
public class AllTests {
}
