// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders.fromInt64;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.daml.ledger.api.v2.ValueOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoders;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders;
import com.google.protobuf.Empty;
import java.util.ArrayList;
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
    MyOptionalRecord fromValue = MyOptionalRecord.valueDecoder().decode(record);

    MyOptionalRecord fromUnboxed =
        new MyOptionalRecord(Optional.of(42L), Optional.of(Unit.getInstance()));

    MyOptionalRecord roundTripped =
        MyOptionalRecord.valueDecoder()
            .decode(fromUnboxed.toValue(), UnknownTrailingFieldPolicy.STRICT);
    MyOptionalRecord roundTrippedWithIgnore =
        MyOptionalRecord.valueDecoder()
            .decode(fromUnboxed.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromUnboxed);
    assertEquals(fromUnboxed, roundTripped);
    assertEquals(fromUnboxed, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonRecordWithOptionalFields() throws JsonLfDecoder.Error {
    MyOptionalRecord expected =
        new MyOptionalRecord(Optional.of(42L), Optional.of(Unit.getInstance()));

    assertEquals(expected, MyOptionalRecord.fromJson(expected.toJson()));
    assertEquals(
        expected, MyOptionalRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.STRICT));
    assertEquals(
        expected, MyOptionalRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void optionalFieldRoundTrip() {
    DamlRecord record =
        new DamlRecord(
            new DamlRecord.Field("intOpt", DamlOptional.of(new Int64(42))),
            new DamlRecord.Field("unitOpt", DamlOptional.of(Unit.getInstance())));

    MyOptionalRecord fromValue = MyOptionalRecord.valueDecoder().decode(record);

    assertEquals(record.toProto(), fromValue.toValue().toProto());

    MyOptionalRecord roundTripped =
        MyOptionalRecord.valueDecoder()
            .decode(fromValue.toValue(), UnknownTrailingFieldPolicy.STRICT);
    MyOptionalRecord roundTrippedWithIgnore =
        MyOptionalRecord.valueDecoder()
            .decode(fromValue.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, roundTripped);
    assertEquals(fromValue, roundTrippedWithIgnore);
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
    NestedOptionalRecord fromValue = NestedOptionalRecord.valueDecoder().decode(record);

    NestedOptionalRecord fromConstructor =
        new NestedOptionalRecord(Optional.of(Optional.of(Optional.of(42L))));

    NestedOptionalRecord roundTripped =
        NestedOptionalRecord.valueDecoder()
            .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.STRICT);
    NestedOptionalRecord roundTrippedWithIgnore =
        NestedOptionalRecord.valueDecoder()
            .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonNestedOptional() throws JsonLfDecoder.Error {
    NestedOptionalRecord expected =
        new NestedOptionalRecord(Optional.of(Optional.of(Optional.of(42L))));

    assertEquals(expected, NestedOptionalRecord.fromJson(expected.toJson()));
    assertEquals(
        expected,
        NestedOptionalRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.STRICT));
    assertEquals(
        expected,
        NestedOptionalRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.IGNORE));
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

    MyOptionalListRecord roundTripped =
        MyOptionalListRecord.valueDecoder()
            .decode(fromCodegen.toValue(), UnknownTrailingFieldPolicy.STRICT);
    MyOptionalListRecord roundTrippedWithIgnore =
        MyOptionalListRecord.valueDecoder()
            .decode(fromCodegen.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(protoRecord, fromCodegen.toValue().toProtoRecord());
    assertEquals(dataRecord.toProtoRecord(), protoRecord);
    assertEquals(fromCodegen, roundTripped);
    assertEquals(fromCodegen, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonOptionalListRecord() throws JsonLfDecoder.Error {
    MyOptionalListRecord expected = new MyOptionalListRecord(Optional.of(Arrays.asList(42L)));

    assertEquals(expected, MyOptionalListRecord.fromJson(expected.toJson()));
    assertEquals(
        expected,
        MyOptionalListRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.STRICT));
    assertEquals(
        expected,
        MyOptionalListRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.IGNORE));
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

    MyListOfOptionalsRecord roundTripped =
        MyListOfOptionalsRecord.valueDecoder()
            .decode(fromCodegen.toValue(), UnknownTrailingFieldPolicy.STRICT);
    MyListOfOptionalsRecord roundTrippedWithIgnore =
        MyListOfOptionalsRecord.valueDecoder()
            .decode(fromCodegen.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromCodegen.toValue().toProtoRecord(), protoRecord);
    assertEquals(dataRecord.toProtoRecord(), protoRecord);
    assertEquals(fromCodegen, roundTripped);
    assertEquals(fromCodegen, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonListOfOptionalsRecord() throws JsonLfDecoder.Error {
    MyListOfOptionalsRecord expected = new MyListOfOptionalsRecord(Arrays.asList(Optional.of(42L)));

    assertEquals(expected, MyListOfOptionalsRecord.fromJson(expected.toJson()));
    assertEquals(
        expected,
        MyListOfOptionalsRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.STRICT));
    assertEquals(
        expected,
        MyListOfOptionalsRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void fromJsonMyListOfOptionalsRecordWithExtraFieldStrict() throws JsonLfDecoder.Error {
    MyListOfOptionalsRecord expected = new MyListOfOptionalsRecord(Arrays.asList(Optional.of(42L)));

    String json = expected.toJson();
    String jsonWithExtra = json.substring(0, json.length() - 1) + ",\"_extraField\":42}";

    assertThrows(
        JsonLfDecoder.Error.class,
        () -> MyListOfOptionalsRecord.fromJson(jsonWithExtra, UnknownTrailingFieldPolicy.STRICT));

    assertEquals(
        expected,
        MyListOfOptionalsRecord.fromJson(jsonWithExtra, UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void decodeMyListOfOptionalsRecordWithTrailingOptionalFields() {
    MyListOfOptionalsRecord expected = new MyListOfOptionalsRecord(Arrays.asList(Optional.of(42L)));

    ArrayList<DamlRecord.Field> fieldsWithTrailing =
        new ArrayList<>(expected.toValue().getFields());
    fieldsWithTrailing.add(new DamlRecord.Field("extraField", DamlOptional.of(new Int64(99L))));
    DamlRecord recordWithTrailing = new DamlRecord(fieldsWithTrailing);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            MyListOfOptionalsRecord.valueDecoder()
                .decode(recordWithTrailing, UnknownTrailingFieldPolicy.STRICT));

    MyListOfOptionalsRecord fromIgnore =
        MyListOfOptionalsRecord.valueDecoder()
            .decode(recordWithTrailing, UnknownTrailingFieldPolicy.IGNORE);
    assertEquals(expected, fromIgnore);
  }

  {
    Variant variant = new Variant("OptionalParametricVariant", DamlOptional.of(new Int64(42)));

    OptionalParametricVariant<Long> fromValue =
        (OptionalParametricVariant<Long>)
            OptionalParametricVariant.valueDecoder(fromInt64).decode(variant);
    OptionalParametricVariant<Long> fromConstructor =
        new OptionalParametricVariant<Long>(Optional.of(42L));

    OptionalParametricVariant<Long> fromRoundTrip =
        (OptionalParametricVariant<Long>)
            OptionalParametricVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(Int64::new));

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(Int64::new), variant);
    assertEquals(fromConstructor, fromRoundTrip);
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

    OptionalPrimVariant<?> fromValue =
        (OptionalPrimVariant<?>) OptionalPrimVariant.valueDecoder(fromInt64).decode(variant);
    OptionalPrimVariant<?> fromConstructor = new OptionalPrimVariant(Optional.of(42L));

    OptionalPrimVariant<?> fromRoundTrip =
        (OptionalPrimVariant<?>)
            OptionalPrimVariant.valueDecoder(fromInt64).decode(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), variant);
    assertEquals(fromConstructor, fromRoundTrip);
  }

  @Test
  void roundtripJsonOptionalPrimVariant() throws JsonLfDecoder.Error {
    OptionalVariant<Long> expected = new OptionalPrimVariant(Optional.of(42L));

    String json = expected.toJson(JsonLfEncoders::int64);
    var actual = OptionalVariant.fromJson(json, JsonLfDecoders.int64);

    assertEquals(expected, actual);
  }
}
