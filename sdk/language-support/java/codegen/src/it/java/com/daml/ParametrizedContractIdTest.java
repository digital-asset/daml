// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import com.daml.ledger.api.v2.ValueOuterClass;
import com.daml.ledger.javaapi.data.DamlOptional;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Text;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import java.util.ArrayList;
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
    FixedContractId fromValue = FixedContractId.valueDecoder().decode(dataRecord);
    FixedContractId fromConstructor =
        new FixedContractId(new ParametrizedContractId<>(new Foo.ContractId("SomeID")));
    FixedContractId fromRoundTrip =
        FixedContractId.valueDecoder().decode(fromConstructor.toValue());
    FixedContractId roundTripped =
        FixedContractId.valueDecoder()
            .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.STRICT);
    FixedContractId roundTrippedWithIgnore =
        FixedContractId.valueDecoder()
            .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataRecord);
    assertEquals(fromConstructor.toValue().toProtoRecord(), protoRecord);
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void decodeFixedContractIdWithTrailingOptionalFields() {
    FixedContractId expected =
        new FixedContractId(new ParametrizedContractId<>(new Foo.ContractId("SomeID")));

    ArrayList<DamlRecord.Field> fieldsWithTrailing = new ArrayList<>(expected.toValue().getFields());
    fieldsWithTrailing.add(
        new DamlRecord.Field("extraField", DamlOptional.of(new Text("extra"))));
    DamlRecord recordWithTrailing = new DamlRecord(fieldsWithTrailing);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            FixedContractId.valueDecoder()
                .decode(recordWithTrailing, UnknownTrailingFieldPolicy.STRICT));

    FixedContractId fromIgnore =
        FixedContractId.valueDecoder()
            .decode(recordWithTrailing, UnknownTrailingFieldPolicy.IGNORE);
    assertEquals(expected, fromIgnore);
  }

  @Test
  void roundTripJson() throws JsonLfDecoder.Error {
    FixedContractId fixed =
        new FixedContractId(new ParametrizedContractId<>(new Foo.ContractId("SomeID")));
    FixedContractId parameterized =
        new FixedContractId(new ParametrizedContractId<>(new ContractId<Foo>("SomeID")));

    assertEquals(fixed, FixedContractId.fromJson(fixed.toJson()));
    assertEquals(parameterized, FixedContractId.fromJson(parameterized.toJson()));
    assertEquals(
        fixed, FixedContractId.fromJson(fixed.toJson(), UnknownTrailingFieldPolicy.STRICT));
    assertEquals(
        fixed, FixedContractId.fromJson(fixed.toJson(), UnknownTrailingFieldPolicy.IGNORE));
    assertEquals(
        parameterized,
        FixedContractId.fromJson(parameterized.toJson(), UnknownTrailingFieldPolicy.STRICT));
    assertEquals(
        parameterized,
        FixedContractId.fromJson(parameterized.toJson(), UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void fromJsonFixedContractIdWithExtraFieldStrict() throws JsonLfDecoder.Error {
    FixedContractId expected =
        new FixedContractId(new ParametrizedContractId<>(new Foo.ContractId("SomeID")));

    String json = expected.toJson();
    String jsonWithExtra = json.substring(0, json.length() - 1) + ",\"_extraField\":42}";

    assertThrows(
        JsonLfDecoder.Error.class,
        () -> FixedContractId.fromJson(jsonWithExtra, UnknownTrailingFieldPolicy.STRICT));

    assertEquals(
        expected, FixedContractId.fromJson(jsonWithExtra, UnknownTrailingFieldPolicy.IGNORE));
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
