// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This test suite encompasses all tests that should be run for all DAML-LF target versions,
 * and should be included in the AllTests class for specific DAML-LF target versions.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        DecoderTest.class,
        RecordTest.class,
        VariantTest.class,
        SerializableTest.class,
        TemplateMethodTest.class
})
public class AllGenericTests {
}
