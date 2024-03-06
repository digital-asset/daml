// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.daml.ledger.api.v2.ValueOuterClass;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.recordtest.FixedContractId;
import tests.recordtest.Foo;
import tests.recordtest.ParametrizedContractId;

@RunWith(JUnitPlatform.class)
public class ParametrizedContractIdTest {

  @Test
  void contractIdsCanBeParameterized() {
    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("fixedContractId")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setRecord(
                                ValueOuterClass.Record.newBuilder()
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("parametrizedContractId")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setContractId("SomeID"))))))
            .build();
    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    FixedContractId fromValue = FixedContractId.fromValue(dataRecord);
    FixedContractId fromConstructor =
        new FixedContractId(new ParametrizedContractId<>(new Foo.ContractId("SomeID")));
    FixedContractId fromRoundTrip = FixedContractId.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataRecord);
    assertEquals(fromConstructor.toValue().toProtoRecord(), protoRecord);
    assertEquals(fromRoundTrip, fromConstructor);
  }

  @Test
  void roundTripJson() throws JsonLfDecoder.Error {
    FixedContractId fixed =
        new FixedContractId(new ParametrizedContractId<>(new Foo.ContractId("SomeID")));
    FixedContractId parameterized =
        new FixedContractId(new ParametrizedContractId<>(new ContractId<Foo>("SomeID")));

    assertEquals(fixed, FixedContractId.fromJson(fixed.toJson()));
    assertEquals(parameterized, FixedContractId.fromJson(parameterized.toJson()));
  }

  @Test
  void fixedContractIdIsEqualToParametrizedContractId() {

    Foo.ContractId fixed = new Foo.ContractId("test");
    ContractId<Foo> parametrized = new ContractId<>("test");

    tests.template1.TestTemplate.ContractId test =
        new tests.template1.TestTemplate.ContractId("test");

    assertEquals(parametrized, fixed);
    assertEquals(fixed, parametrized);
    assertNotEquals(test, fixed);
    assertNotEquals(fixed, test);
  }
}
