// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.api.v1.ValueOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoders;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders;
import com.google.protobuf.Empty;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.optionaltest.*;
import tests.optionaltest.optionalvariant.OptionalParametricVariant;
import tests.optionaltest.optionalvariant.OptionalPrimVariant;

@RunWith(JUnitPlatform.class)
public class OptionalTest {

  @Test
  void constructRecordWithOptionalFields() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field("intOpt", DamlOptional.of(new Int64(42))),
            new DamlRecord.Field("unitOpt", DamlOptional.of(Unit.getInstance())));
    MyOptionalRecord fromValue = MyOptionalRecord.fromValue(record);

    MyOptionalRecord fromUnboxed =
        new MyOptionalRecord(Optional.of(42L), Optional.of(Unit.getInstance()));

    assertEquals(fromValue, fromUnboxed);
  }

  @Test
  void roundtripJsonRecordWithOptionalFields() throws JsonLfDecoder.Error {
    MyOptionalRecord expected =
        new MyOptionalRecord(Optional.of(42L), Optional.of(Unit.getInstance()));

    assertEquals(expected, MyOptionalRecord.fromJson(expected.toJson()));
  }

  @Test
  void optionalFieldRoundTrip() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field("intOpt", DamlOptional.of(new Int64(42))),
            new DamlRecord.Field("unitOpt", DamlOptional.of(Unit.getInstance())));

    MyOptionalRecord fromValue = MyOptionalRecord.fromValue(record);

    assertEquals(record.toProto(), fromValue.toValue().toProto());
  }

  @Test
  void optionalFieldRoundTripFromProtobuf() {
    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("intOpt")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setOptional(
                                ValueOuterClass.Optional.newBuilder()
                                    .setValue(ValueOuterClass.Value.newBuilder().setInt64(42)))))
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("unitOpt")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setOptional(
                                ValueOuterClass.Optional.newBuilder()
                                    .setValue(
                                        ValueOuterClass.Value.newBuilder()
                                            .setUnit(Empty.getDefaultInstance())))))
            .build();

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);

    assertEquals(dataRecord.toProtoRecord(), protoRecord);
  }

  @Test
  void constructNestedOptional() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field(
                DamlOptional.of(DamlOptional.of(DamlOptional.of(new Int64(42L))))));
    NestedOptionalRecord fromValue = NestedOptionalRecord.fromValue(record);

    NestedOptionalRecord fromConstructor =
        new NestedOptionalRecord(Optional.of(Optional.of(Optional.of(42L))));

    assertEquals(fromValue, fromConstructor);
  }

  @Test
  void roundtripJsonNestedOptional() throws JsonLfDecoder.Error {
    NestedOptionalRecord expected =
        new NestedOptionalRecord(Optional.of(Optional.of(Optional.of(42L))));

    assertEquals(expected, NestedOptionalRecord.fromJson(expected.toJson()));
  }

  @Test
  void optionalListRoundTripFromProtobuf() {
    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("list")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setOptional(
                                ValueOuterClass.Optional.newBuilder()
                                    .setValue(
                                        ValueOuterClass.Value.newBuilder()
                                            .setList(
                                                ValueOuterClass.List.newBuilder()
                                                    .addAllElements(
                                                        Arrays.asList(
                                                            ValueOuterClass.Value.newBuilder()
                                                                .setInt64(42)
                                                                .build())))))))
            .build();

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    MyOptionalListRecord fromCodegen = new MyOptionalListRecord(Optional.of(Arrays.asList(42L)));

    assertEquals(protoRecord, fromCodegen.toValue().toProtoRecord());
    assertEquals(dataRecord.toProtoRecord(), protoRecord);
  }

  @Test
  void roundtripJsonOptionalListRecord() throws JsonLfDecoder.Error {
    MyOptionalListRecord expected = new MyOptionalListRecord(Optional.of(Arrays.asList(42L)));

    assertEquals(expected, MyOptionalListRecord.fromJson(expected.toJson()));
  }

  @Test
  void listOfOptionals() {
    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("list")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setList(
                                ValueOuterClass.List.newBuilder()
                                    .addAllElements(
                                        Arrays.asList(
                                            ValueOuterClass.Value.newBuilder()
                                                .setOptional(
                                                    ValueOuterClass.Optional.newBuilder()
                                                        .setValue(
                                                            ValueOuterClass.Value.newBuilder()
                                                                .setInt64(42)))
                                                .build())))))
            .build();

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    MyListOfOptionalsRecord fromCodegen =
        new MyListOfOptionalsRecord(Arrays.asList(Optional.of(42L)));

    assertEquals(fromCodegen.toValue().toProtoRecord(), protoRecord);
    assertEquals(dataRecord.toProtoRecord(), protoRecord);
  }

  @Test
  void roundtripJsonListOfOptionalsRecord() throws JsonLfDecoder.Error {
    MyListOfOptionalsRecord expected = new MyListOfOptionalsRecord(Arrays.asList(Optional.of(42L)));

    assertEquals(expected, MyListOfOptionalsRecord.fromJson(expected.toJson()));
  }

  @Test
  void parametricOptionalVariant() {
    Variant variant = new Variant("OptionalParametricVariant", DamlOptional.of(new Int64(42)));

    OptionalParametricVariant<Long> fromValue =
        OptionalParametricVariant.fromValue(variant, f -> f.asInt64().get().getValue());
    OptionalParametricVariant<Long> fromConstructor =
        new OptionalParametricVariant<Long>(Optional.of(42L));

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(Int64::new), variant);
  }

  @Test
  void roundtripJsonOptionalParametricVariant() throws JsonLfDecoder.Error {
    OptionalVariant<Long> expected = new OptionalParametricVariant<Long>(Optional.of(42L));

    String json = expected.toJson(JsonLfEncoders::int64);
    var actual = OptionalVariant.fromJson(json, JsonLfDecoders.int64);

    assertEquals(expected, actual);
  }

  @Test
  void primOptionalVariant() {
    Variant variant = new Variant("OptionalPrimVariant", DamlOptional.of(new Int64(42)));

    OptionalPrimVariant<?> fromValue = OptionalPrimVariant.fromValue(variant);
    OptionalPrimVariant<?> fromConstructor = new OptionalPrimVariant(Optional.of(42L));

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), variant);
  }

  @Test
  void roundtripJsonOptionalPrimVariant() throws JsonLfDecoder.Error {
    OptionalVariant<Long> expected = new OptionalPrimVariant(Optional.of(42L));

    String json = expected.toJson(JsonLfEncoders::int64);
    var actual = OptionalVariant.fromJson(json, JsonLfDecoders.int64);

    assertEquals(expected, actual);
  }
}
