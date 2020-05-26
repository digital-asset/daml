// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import com.daml.ledger.javaapi.TestDecoder;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(JUnitPlatform.class)
public class DecoderTest {

    @Test
    void containsAllKnownTemplates() {
        assertTrue(TestDecoder.getDecoder(tests.template1.TestTemplate.TEMPLATE_ID).isPresent());
        assertTrue(TestDecoder.getDecoder(tests.template2.TestTemplate.TEMPLATE_ID).isPresent());
    }
}
