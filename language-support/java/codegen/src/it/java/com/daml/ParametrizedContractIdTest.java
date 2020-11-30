// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import com.daml.ledger.javaapi.data.Record;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.daml.ledger.api.v1.ValueOuterClass;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.recordtest.FixedContractId;
import tests.recordtest.Foo;
import tests.recordtest.ParametrizedContractId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnitPlatform.class)
public class ParametrizedContractIdTest {

    @Test
    void contractIdsCanBeParameterized() {
        ValueOuterClass.Record protoRecord = ValueOuterClass.Record.newBuilder()
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("fixedContractId")
                        .setValue(ValueOuterClass.Value.newBuilder().setRecord(ValueOuterClass.Record.newBuilder()
                                        .addFields(ValueOuterClass.RecordField.newBuilder()
                                                .setLabel("parametrizedContractId")
                                                .setValue(ValueOuterClass.Value.newBuilder().setContractId("SomeID"))
                                        )
                                )
                        )
                ).build();
        Record dataRecord = Record.fromProto(protoRecord);
        FixedContractId fromValue = FixedContractId.fromValue(dataRecord);
        FixedContractId fromConstructor = new FixedContractId(new ParametrizedContractId<>(new Foo.ContractId("SomeID")));
        FixedContractId fromRoundTrip = FixedContractId.fromValue(fromConstructor.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataRecord);
        assertEquals(fromConstructor.toValue().toProtoRecord(), protoRecord);
        assertEquals(fromRoundTrip, fromConstructor);
    }

    @Test
    void fixedContractIdIsEqualToParametrizedContractId() {

        Foo.ContractId fixed = new Foo.ContractId("test");
        ContractId<Foo> parametrized = new ContractId<>("test");

        tests.template1.TestTemplate.ContractId test = new tests.template1.TestTemplate.ContractId("test");

        assertEquals(parametrized, fixed);
        assertEquals(fixed, parametrized);
        assertNotEquals(test, fixed);
        assertNotEquals(fixed, test);
    }
}
