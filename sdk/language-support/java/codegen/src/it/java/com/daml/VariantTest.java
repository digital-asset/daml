// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders.fromInt64;
import static com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders.fromUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.daml.ledger.api.v2.ValueOuterClass;
import com.daml.ledger.javaapi.data.DamlOptional;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Int64;
import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.Variant;
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoders;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders;
import com.google.protobuf.Empty;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.varianttest.Custom;
import tests.varianttest.VariantItem;
import tests.varianttest.customparametric.CustomParametricCons;
import tests.varianttest.variantitem.*;

@RunWith(JUnitPlatform.class)
public class VariantTest {

  @Test
  void emptyVariant() {
    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("EmptyVariant")
            .setValue(ValueOuterClass.Value.newBuilder().setUnit(Empty.getDefaultInstance()))
            .build();
    Variant dataVariant = Variant.fromProto(protoVariant);
    EmptyVariant fromValue = (EmptyVariant) EmptyVariant.valueDecoder(fromUnit).decode(dataVariant);

    EmptyVariant<?> fromConstructor = new EmptyVariant<>(Unit.getInstance());

    EmptyVariant fromRoundTrip =
        (EmptyVariant) EmptyVariant.valueDecoder(fromUnit).decode(fromValue.toValue());
    EmptyVariant roundTripped =
        (EmptyVariant)
            EmptyVariant.valueDecoder(fromUnit)
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.STRICT);
    EmptyVariant roundTrippedWithIgnore =
        (EmptyVariant)
            EmptyVariant.valueDecoder(fromUnit)
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataVariant);
    assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonEmptyVariant() throws JsonLfDecoder.Error {
    VariantItem<Unit> expected = new EmptyVariant<>(Unit.getInstance());

    String json = expected.toJson(JsonLfEncoders::unit);
    var actual = VariantItem.fromJson(json, JsonLfDecoders.unit);

    assertEquals(expected, actual);
  }

  @Test
  void primVariant() {
    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("PrimVariant")
            .setValue(ValueOuterClass.Value.newBuilder().setInt64(42))
            .build();
    Variant dataVariant = Variant.fromProto(protoVariant);
    PrimVariant fromValue = (PrimVariant) PrimVariant.valueDecoder(fromInt64).decode(dataVariant);

    PrimVariant<?> fromConstructor = new PrimVariant<>(42L);

    PrimVariant<?> fromRoundTrip =
        (PrimVariant<?>) PrimVariant.valueDecoder(fromInt64).decode(fromValue.toValue());
    PrimVariant<?> roundTripped =
        (PrimVariant<?>)
            PrimVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.STRICT);
    PrimVariant<?> roundTrippedWithIgnore =
        (PrimVariant<?>)
            PrimVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataVariant);
    assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonPrimVariant() throws JsonLfDecoder.Error {
    VariantItem<Long> expected = new PrimVariant<>(42L);

    String json = expected.toJson(JsonLfEncoders::int64);
    var actual = VariantItem.fromJson(json, JsonLfDecoders.int64);

    assertEquals(expected, actual);
  }

  @Test
  void recordVariant() {
    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("RecordVariant")
            .setValue(
                ValueOuterClass.Value.newBuilder()
                    .setRecord(
                        ValueOuterClass.Record.newBuilder()
                            .addFields(
                                ValueOuterClass.RecordField.newBuilder()
                                    .setLabel("x")
                                    .setValue(ValueOuterClass.Value.newBuilder().setInt64(42)))))
            .build();

    Variant dataVariant = Variant.fromProto(protoVariant);
    RecordVariant<?> fromValue =
        (RecordVariant<?>) RecordVariant.valueDecoder(fromInt64).decode(dataVariant);

    RecordVariant<?> fromConstructor = new RecordVariant<Long>(42L);

    RecordVariant<?> fromRoundTrip =
        (RecordVariant<?>) RecordVariant.valueDecoder(fromInt64).decode(fromValue.toValue());
    RecordVariant<?> roundTripped =
        (RecordVariant<?>)
            RecordVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.STRICT);
    RecordVariant<?> roundTrippedWithIgnore =
        (RecordVariant<?>)
            RecordVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataVariant);
    assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonRecordVariant() throws JsonLfDecoder.Error {
    VariantItem<Long> expected = new RecordVariant<Long>(42L);

    String json = expected.toJson(JsonLfEncoders::int64);
    var actual = VariantItem.fromJson(json, JsonLfDecoders.int64);

    assertEquals(expected, actual);
  }

  @Test
  void decodeRecordVariantWithTrailingOptionalFields() {
    RecordVariant<?> expected = new RecordVariant<Long>(42L);
    Variant value = expected.toValue();
    DamlRecord innerRecord = value.getValue().asRecord().get();
    ArrayList<DamlRecord.Field> fieldsWithTrailing = new ArrayList<>(innerRecord.getFields());
    fieldsWithTrailing.add(new DamlRecord.Field("extraField", DamlOptional.of(new Int64(99L))));
    DamlRecord recordWithTrailing = new DamlRecord(fieldsWithTrailing);
    Variant variantWithTrailing = new Variant(value.getConstructor(), recordWithTrailing);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RecordVariant.valueDecoder(fromInt64)
                .decode(variantWithTrailing, UnknownTrailingFieldPolicy.STRICT));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RecordVariant.valueDecoder(fromInt64)
                .decode(variantWithTrailing, UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void customVariant() {
    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("CustomVariant")
            .setValue(
                ValueOuterClass.Value.newBuilder()
                    .setRecord(ValueOuterClass.Record.getDefaultInstance()))
            .build();

    Variant dataVariant = Variant.fromProto(protoVariant);
    CustomVariant<?> fromValue =
        (CustomVariant<?>) CustomVariant.valueDecoder(Custom.valueDecoder()).decode(dataVariant);

    CustomVariant<?> fromConstructor = new CustomVariant<>(new Custom());

    CustomVariant<?> fromRoundTrip =
        (CustomVariant<?>)
            CustomVariant.valueDecoder(Custom.valueDecoder()).decode(fromConstructor.toValue());
    CustomVariant<?> roundTripped =
        (CustomVariant<?>)
            CustomVariant.valueDecoder(Custom.valueDecoder())
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.STRICT);
    CustomVariant<?> roundTrippedWithIgnore =
        (CustomVariant<?>)
            CustomVariant.valueDecoder(Custom.valueDecoder())
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataVariant);
    assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonCustomVariant() throws JsonLfDecoder.Error {
    VariantItem<Unit> expected = new CustomVariant<>(new Custom());

    String json = expected.toJson(JsonLfEncoders::unit);
    var actual = VariantItem.fromJson(json, JsonLfDecoders.unit);

    assertEquals(expected, actual);
  }

  @Test
  void customParametricVariant() {
    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("CustomParametricVariant")
            .setValue(
                ValueOuterClass.Value.newBuilder()
                    .setVariant(
                        ValueOuterClass.Variant.newBuilder()
                            .setConstructor("CustomParametricCons")
                            .setValue(ValueOuterClass.Value.newBuilder().setInt64(42))))
            .build();
    Variant dataVariant = Variant.fromProto(protoVariant);
    CustomParametricVariant<Long> fromValue =
        (CustomParametricVariant<Long>)
            CustomParametricVariant.valueDecoder(fromInt64).decode(dataVariant);

    CustomParametricVariant<Long> fromConstructor =
        new CustomParametricVariant<>(new CustomParametricCons<>(42L));

    CustomParametricVariant<Long> fromRoundTrip =
        (CustomParametricVariant<Long>)
            CustomParametricVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(Int64::new));
    CustomParametricVariant<Long> roundTripped =
        (CustomParametricVariant<Long>)
            CustomParametricVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(Int64::new), UnknownTrailingFieldPolicy.STRICT);
    CustomParametricVariant<Long> roundTrippedWithIgnore =
        (CustomParametricVariant<Long>)
            CustomParametricVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(Int64::new), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(Int64::new), dataVariant);
    assertEquals(
        fromConstructor.toValue(Int64::new).toProtoVariant(), dataVariant.toProtoVariant());
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonCustomParametricVariant() throws JsonLfDecoder.Error {
    VariantItem<Long> expected = new CustomParametricVariant<>(new CustomParametricCons<>(42L));

    String json = expected.toJson(JsonLfEncoders::int64);
    var actual = VariantItem.fromJson(json, JsonLfDecoders.int64);

    assertEquals(expected, actual);
  }

  @Test
  void recordVariantRecord() {
    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("RecordVariantRecord")
            .setValue(
                ValueOuterClass.Value.newBuilder()
                    .setRecord(
                        ValueOuterClass.Record.newBuilder()
                            .addFields(
                                ValueOuterClass.RecordField.newBuilder()
                                    .setLabel("y")
                                    .setValue(
                                        ValueOuterClass.Value.newBuilder()
                                            .setVariant(
                                                ValueOuterClass.Variant.newBuilder()
                                                    .setConstructor("EmptyVariant")
                                                    .setValue(
                                                        ValueOuterClass.Value.newBuilder()
                                                            .setUnit(
                                                                Empty.getDefaultInstance())))))))
            .build();

    Variant dataVariant = Variant.fromProto(protoVariant);
    RecordVariantRecord<?> fromValue =
        (RecordVariantRecord<?>)
            RecordVariantRecord.valueDecoder(VariantItem.valueDecoder(fromInt64))
                .decode(dataVariant);

    RecordVariantRecord<?> fromConstructor =
        new RecordVariantRecord<>(new EmptyVariant<>(Unit.getInstance()));

    RecordVariantRecord<?> fromRoundTrip =
        (RecordVariantRecord<?>)
            RecordVariantRecord.valueDecoder(VariantItem.valueDecoder(fromInt64))
                .decode(fromValue.toValue());
    RecordVariantRecord<?> roundTripped =
        (RecordVariantRecord<?>)
            RecordVariantRecord.valueDecoder(VariantItem.valueDecoder(fromInt64))
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.STRICT);
    RecordVariantRecord<?> roundTrippedWithIgnore =
        (RecordVariantRecord<?>)
            RecordVariantRecord.valueDecoder(VariantItem.valueDecoder(fromInt64))
                .decode(fromConstructor.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataVariant);
    assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonRecordVariantRecord() throws JsonLfDecoder.Error {
    VariantItem<Unit> expected = new RecordVariantRecord<>(new EmptyVariant<>(Unit.getInstance()));

    String json = expected.toJson(JsonLfEncoders::unit);
    var actual = VariantItem.fromJson(json, JsonLfDecoders.unit);

    assertEquals(expected, actual);
  }

  @Test
  void decodeRecordVariantRecordWithTrailingOptionalFields() {
    RecordVariantRecord<?> expected =
        new RecordVariantRecord<>(new EmptyVariant<>(Unit.getInstance()));
    Variant value = expected.toValue();
    DamlRecord innerRecord = value.getValue().asRecord().get();
    ArrayList<DamlRecord.Field> fieldsWithTrailing = new ArrayList<>(innerRecord.getFields());
    fieldsWithTrailing.add(new DamlRecord.Field("extraField", DamlOptional.of(new Int64(99L))));
    DamlRecord recordWithTrailing = new DamlRecord(fieldsWithTrailing);
    Variant variantWithTrailing = new Variant(value.getConstructor(), recordWithTrailing);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RecordVariantRecord.valueDecoder(VariantItem.valueDecoder(fromInt64))
                .decode(variantWithTrailing, UnknownTrailingFieldPolicy.STRICT));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RecordVariantRecord.valueDecoder(VariantItem.valueDecoder(fromInt64))
                .decode(variantWithTrailing, UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void parameterizedRecordVariant() {
    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("ParameterizedRecordVariant")
            .setValue(
                ValueOuterClass.Value.newBuilder()
                    .setRecord(
                        ValueOuterClass.Record.newBuilder()
                            .addFields(
                                ValueOuterClass.RecordField.newBuilder()
                                    .setLabel("x1")
                                    .setValue(ValueOuterClass.Value.newBuilder().setInt64(42L)))
                            .addFields(
                                ValueOuterClass.RecordField.newBuilder()
                                    .setLabel("x2")
                                    .setValue(ValueOuterClass.Value.newBuilder().setInt64(69L)))
                            .addFields(
                                ValueOuterClass.RecordField.newBuilder()
                                    .setLabel("x3")
                                    .setValue(
                                        ValueOuterClass.Value.newBuilder()
                                            .setList(
                                                ValueOuterClass.List.newBuilder()
                                                    .addElements(
                                                        ValueOuterClass.Value.newBuilder()
                                                            .setInt64(65536L)))))))
            .build();

    Variant dataVariant = Variant.fromProto(protoVariant);
    ParameterizedRecordVariant<Long> fromValue =
        (ParameterizedRecordVariant<Long>)
            ParameterizedRecordVariant.valueDecoder(fromInt64).decode(dataVariant);
    ParameterizedRecordVariant<Long> fromConstructor =
        new ParameterizedRecordVariant<>(42L, 69L, Collections.singletonList(65536L));
    ParameterizedRecordVariant<Long> fromRoundTrip =
        (ParameterizedRecordVariant<Long>)
            ParameterizedRecordVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(Int64::new));
    ParameterizedRecordVariant<Long> roundTripped =
        (ParameterizedRecordVariant<Long>)
            ParameterizedRecordVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(Int64::new), UnknownTrailingFieldPolicy.STRICT);
    ParameterizedRecordVariant<Long> roundTrippedWithIgnore =
        (ParameterizedRecordVariant<Long>)
            ParameterizedRecordVariant.valueDecoder(fromInt64)
                .decode(fromConstructor.toValue(Int64::new), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(Int64::new), dataVariant);
    assertEquals(
        fromConstructor.toValue(Int64::new).toProtoVariant(), dataVariant.toProtoVariant());
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonParameterizedRecordVariant() throws JsonLfDecoder.Error {
    VariantItem<Long> expected =
        new ParameterizedRecordVariant<>(42L, 69L, Collections.singletonList(65536L));

    String json = expected.toJson(JsonLfEncoders::int64);
    var actual = VariantItem.fromJson(json, JsonLfDecoders.int64);

    assertEquals(expected, actual);
  }

  @Test
  void decodeParameterizedRecordVariantWithTrailingOptionalFields() {
    ParameterizedRecordVariant<Long> expected =
        new ParameterizedRecordVariant<>(42L, 69L, Collections.singletonList(65536L));
    Variant value = expected.toValue(Int64::new);
    DamlRecord innerRecord = value.getValue().asRecord().get();
    ArrayList<DamlRecord.Field> fieldsWithTrailing = new ArrayList<>(innerRecord.getFields());
    fieldsWithTrailing.add(new DamlRecord.Field("extraField", DamlOptional.of(new Int64(99L))));
    DamlRecord recordWithTrailing = new DamlRecord(fieldsWithTrailing);
    Variant variantWithTrailing = new Variant(value.getConstructor(), recordWithTrailing);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ParameterizedRecordVariant.valueDecoder(fromInt64)
                .decode(variantWithTrailing, UnknownTrailingFieldPolicy.STRICT));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ParameterizedRecordVariant.valueDecoder(fromInt64)
                .decode(variantWithTrailing, UnknownTrailingFieldPolicy.IGNORE));
  }
}
