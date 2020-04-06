// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import com.daml.ledger.javaapi.data.Int64;
import com.daml.ledger.javaapi.data.Record;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.serializable.serializability.Serializable;

@RunWith(JUnitPlatform.class)
public class SerializableTest {

    @Test
    void synthesizedRecordForVariantIsGenerated() {
        // we only need to access the `Serializability.Serializable` record type
        // if it's not being generated, it would be a compile error
        Serializable fromConstructor = new Serializable(42L);
        Record record = new Record(new Record.Field("field", new Int64(42L)));
        Assertions.assertEquals(record, fromConstructor.toValue());
    }
}
