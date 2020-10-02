// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf_latest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import com.daml.AllGenericTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        AllGenericTests.class,
        ListTest.class,
        OptionalTest.class,
        MapTest.class,
        ContractKeysTest.class,
        ParametrizedContractIdTest.class,
        NumericTest.class
})
public class AllTests {
}
