// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders.fromBool;
import static com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders.fromText;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.daml.ledger.api.v2.ValueOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoders;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.recordtest.MyRecord;
import tests.recordtest.NestedRecord;
import tests.recordtest.NestedVariant;
import tests.recordtest.OuterRecord;
import tests.recordtest.ParametricRecord;
import tests.recordtest.nestedvariant.Nested;

@RunWith(JUnitPlatform.class)
public class RecordTest {

  void checkRecord(MyRecord myRecord) {
    assertEquals(myRecord.int_, int64Value);
    assertEquals(myRecord.decimal, decimalValue);
    assertEquals(myRecord.text, textValue);
    assertEquals(myRecord.bool, boolValue);
    assertEquals(myRecord.party, partyValue);
    assertEquals(myRecord.date.toEpochDay(), dateValue);
    assertEquals(myRecord.time, timestampValue);
    assertEquals(myRecord.void$, boolValue);
    assertEquals(myRecord.list, listValue);
    assertEquals(myRecord.nestedList, nestedListValue);
    assertEquals(myRecord.unit, Unit.getInstance());
    assertEquals(myRecord.nestedRecord, nestedRecordValue);
    assertEquals(myRecord.nestedVariant, nestedVariantValue);
  }

  @Test
  void deserializableFromRecord() {
    Int64 int_ = new Int64(int64Value);
    Numeric decimal = new Numeric(decimalValue);
    Text text = new Text(textValue);
    Bool bool = Bool.of(boolValue);
    Party party = new Party(partyValue);
    Date date = new Date(dateValue);
    Timestamp timestamp = new Timestamp(timestampMicrosValue);
    DamlList list = DamlList.of(Unit.getInstance(), Unit.getInstance());
    DamlList nestedList =
        nestedListValue.stream()
            .collect(
                DamlCollectors.toDamlList(
                    ns -> ns.stream().collect(DamlCollectors.toDamlList(Int64::new))));
    DamlRecord nestedRecord = new DamlRecord(new DamlRecord.Field("value", new Int64(42)));
    Variant nestedVariant = new Variant("Nested", new Int64(42));
    ArrayList<DamlRecord.Field> fieldsList = new ArrayList<>(10);
    fieldsList.add(new DamlRecord.Field("int_", int_));
    fieldsList.add(new DamlRecord.Field("decimal", decimal));
    fieldsList.add(new DamlRecord.Field("text", text));
    fieldsList.add(new DamlRecord.Field("bool", bool));
    fieldsList.add(new DamlRecord.Field("party", party));
    fieldsList.add(new DamlRecord.Field("date", date));
    fieldsList.add(new DamlRecord.Field("timestamp", timestamp));
    fieldsList.add(new DamlRecord.Field("void", bool));
    fieldsList.add(new DamlRecord.Field("list", list));
    fieldsList.add(new DamlRecord.Field("nestedList", nestedList));
    fieldsList.add(new DamlRecord.Field("unit", Unit.getInstance()));
    fieldsList.add(new DamlRecord.Field("nestedRecord", nestedRecord));
    fieldsList.add(new DamlRecord.Field("nestedVariant", nestedVariant));
    DamlRecord myDataRecord = new DamlRecord(fieldsList);
    MyRecord myRecord = MyRecord.valueDecoder().decode(myDataRecord);
    checkRecord(myRecord);
    assertTrue(
        "to value uses original Daml-LF names for fields",
        myRecord.toValue().getFieldsMap().get("void").asBool().isPresent());

    MyRecord roundTripped =
        MyRecord.valueDecoder().decode(myRecord.toValue(), UnknownTrailingFieldPolicy.STRICT);
    MyRecord roundTrippedWithIgnore =
        MyRecord.valueDecoder().decode(myRecord.toValue(), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(myRecord, roundTripped);
    assertEquals(myRecord, roundTrippedWithIgnore);
  }

  @Test
  void decodeRecordWithTrailingOptionalFields() {
    LocalDate localDate = LocalDate.ofEpochDay(dateValue);
    Instant instant = Instant.ofEpochMilli(timestampMicrosValue);
    MyRecord expected =
        new MyRecord(
            int64Value,
            decimalValue,
            textValue,
            boolValue,
            partyValue,
            localDate,
            instant,
            boolValue,
            listValue,
            nestedListValue,
            unitValue,
            nestedRecordValue,
            nestedVariantValue);

    ArrayList<DamlRecord.Field> nestedRecordTrailingFields =
        new ArrayList<>(
            List.of(
                new DamlRecord.Field("long", new Int64(nestedRecordValue.value)),
                new DamlRecord.Field("text", DamlOptional.of(new Text("extra")))));
    DamlRecord nestedRecordWithTrailing = new DamlRecord(nestedRecordTrailingFields);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            NestedRecord.valueDecoder()
                .decode(nestedRecordWithTrailing, UnknownTrailingFieldPolicy.STRICT));
    NestedRecord decodedNestedRecord =
        NestedRecord.valueDecoder()
            .decode(nestedRecordWithTrailing, UnknownTrailingFieldPolicy.IGNORE);
    assertEquals(nestedRecordValue, decodedNestedRecord);

    DamlRecord valueWithTrailing = expected.toValue();
    List<DamlRecord.Field> transformedFields =
        valueWithTrailing.getFields().stream()
            .map(
                field -> {
                  String label = field.getLabel().orElse("");
                  if (!label.isBlank() && Objects.equals(label, "nestedRecord")) {
                    return new DamlRecord.Field("nestedRecord", nestedRecordWithTrailing);
                  } else {
                    return field;
                  }
                })
            .toList();
    ArrayList<DamlRecord.Field> fieldsWithTrailing = new ArrayList<>(transformedFields);
    fieldsWithTrailing.add(new DamlRecord.Field("extraField1", DamlOptional.of(new Int64(99L))));
    fieldsWithTrailing.add(new DamlRecord.Field("extraField2", DamlOptional.of(new Text("extra"))));
    DamlRecord recordWithTrailing = new DamlRecord(fieldsWithTrailing);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            MyRecord.valueDecoder().decode(recordWithTrailing, UnknownTrailingFieldPolicy.STRICT));

    MyRecord decodedRecord =
        MyRecord.valueDecoder().decode(recordWithTrailing, UnknownTrailingFieldPolicy.IGNORE);
    assertEquals(expected, decodedRecord);

    List<DamlRecord.Field> fieldsWithNotOptionalTrailing =
        new ArrayList<>(valueWithTrailing.getFields());
    fieldsWithNotOptionalTrailing.add(new DamlRecord.Field("extraField3", new Int64(99L)));
    DamlRecord recordWithNotOptionalTrailing = new DamlRecord(fieldsWithNotOptionalTrailing);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MyRecord.valueDecoder()
                .decode(recordWithNotOptionalTrailing, UnknownTrailingFieldPolicy.STRICT));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MyRecord.valueDecoder()
                .decode(recordWithNotOptionalTrailing, UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void objectMethodsWork() {
    LocalDate localDate = LocalDate.ofEpochDay(dateValue);
    Instant instant = Instant.ofEpochMilli(timestampMicrosValue);
    MyRecord myRecord1 =
        new MyRecord(
            int64Value,
            decimalValue,
            textValue,
            boolValue,
            partyValue,
            localDate,
            instant,
            boolValue,
            listValue,
            nestedListValue,
            unitValue,
            nestedRecordValue,
            nestedVariantValue);
    MyRecord myRecord2 =
        new MyRecord(
            int64Value,
            decimalValue,
            textValue,
            boolValue,
            partyValue,
            localDate,
            instant,
            boolValue,
            listValue,
            nestedListValue,
            unitValue,
            nestedRecordValue,
            nestedVariantValue);
    assertEquals(myRecord1, myRecord2);
    assertEquals(myRecord1.hashCode(), myRecord2.hashCode());
  }

  @Test
  void roundtripJsonMyRecord() throws JsonLfDecoder.Error {
    MyRecord expected =
        new MyRecord(
            int64Value,
            decimalValue,
            textValue,
            boolValue,
            partyValue,
            LocalDate.ofEpochDay(dateValue),
            Instant.ofEpochMilli(timestampMicrosValue),
            boolValue,
            listValue,
            nestedListValue,
            unitValue,
            nestedRecordValue,
            nestedVariantValue);

    assertEquals(expected, MyRecord.fromJson(expected.toJson()));
    assertEquals(expected, MyRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.STRICT));
    assertEquals(expected, MyRecord.fromJson(expected.toJson(), UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void fromJsonMyRecordWithExtraFieldStrict() throws JsonLfDecoder.Error {
    MyRecord expected =
        new MyRecord(
            int64Value,
            decimalValue,
            textValue,
            boolValue,
            partyValue,
            LocalDate.ofEpochDay(dateValue),
            Instant.ofEpochMilli(timestampMicrosValue),
            boolValue,
            listValue,
            nestedListValue,
            unitValue,
            nestedRecordValue,
            nestedVariantValue);

    String json = expected.toJson();
    String jsonWithExtra = json.substring(0, json.length() - 1) + ",\"_extraField\":42}";

    assertThrows(
        JsonLfDecoder.Error.class,
        () -> MyRecord.fromJson(jsonWithExtra, UnknownTrailingFieldPolicy.STRICT));

    assertEquals(expected, MyRecord.fromJson(jsonWithExtra, UnknownTrailingFieldPolicy.IGNORE));

    // Test with extra non-empty optional field inside nestedRecord
    String nestedRecordJson = nestedRecordValue.toJson();
    String nestedRecordWithExtra =
        nestedRecordJson.substring(0, nestedRecordJson.length() - 1)
            + ",\"_extraOptField\":\"someValue\"}";
    String jsonWithNestedExtra = json.replace(nestedRecordJson, nestedRecordWithExtra);

    assertThrows(
        JsonLfDecoder.Error.class,
        () -> MyRecord.fromJson(jsonWithNestedExtra, UnknownTrailingFieldPolicy.STRICT));

    assertEquals(
        expected, MyRecord.fromJson(jsonWithNestedExtra, UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void outerRecordRoundtrip() {
    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("inner")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setRecord(
                                ValueOuterClass.Record.newBuilder()
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("fieldX1")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setText("Text1")))
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("fieldX2")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setText("Text2")))
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("fieldY")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder().setBool(true)))
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("fieldInt")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder().setInt64(42L)))
                                    .build()))
                    .build())
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("innerFixed")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setRecord(
                                ValueOuterClass.Record.newBuilder()
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("fieldX1")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder().setInt64(42L)))
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("fieldX2")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder().setInt64(69L)))
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("fieldY")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setText("Text2")))
                                    .addFields(
                                        ValueOuterClass.RecordField.newBuilder()
                                            .setLabel("fieldInt")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder().setInt64(69L)))
                                    .build()))
                    .build())
            .build();

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    OuterRecord<String, Boolean> fromValue =
        OuterRecord.valueDecoder(fromText, fromBool).decode(dataRecord);
    OuterRecord<String, Boolean> fromConstructor =
        new OuterRecord<>(
            new ParametricRecord<String, Boolean>("Text1", "Text2", true, 42L),
            new ParametricRecord<Long, String>(42L, 69L, "Text2", 69L));
    OuterRecord<String, Boolean> fromRoundTrip =
        OuterRecord.valueDecoder(fromText, fromBool)
            .decode(fromConstructor.toValue(Text::new, Bool::of));
    OuterRecord<String, Boolean> roundTripped =
        OuterRecord.valueDecoder(fromText, fromBool)
            .decode(
                fromConstructor.toValue(Text::new, Bool::of), UnknownTrailingFieldPolicy.STRICT);
    OuterRecord<String, Boolean> roundTrippedWithIgnore =
        OuterRecord.valueDecoder(fromText, fromBool)
            .decode(
                fromConstructor.toValue(Text::new, Bool::of), UnknownTrailingFieldPolicy.IGNORE);

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(Text::new, Bool::of), dataRecord);
    assertEquals(fromConstructor.toValue(Text::new, Bool::of).toProtoRecord(), protoRecord);
    assertEquals(fromRoundTrip, fromConstructor);
    assertEquals(fromConstructor, roundTripped);
    assertEquals(fromConstructor, roundTrippedWithIgnore);
  }

  @Test
  void roundtripJsonOuterRecord() throws JsonLfDecoder.Error {
    OuterRecord<String, Boolean> expected =
        new OuterRecord<>(
            new ParametricRecord<String, Boolean>("Text1", "Text2", true, 42L),
            new ParametricRecord<Long, String>(42L, 69L, "Text2", 69L));

    String json = expected.toJson(JsonLfEncoders::text, JsonLfEncoders::bool);
    var actual = OuterRecord.fromJson(json, JsonLfDecoders.text, JsonLfDecoders.bool);

    assertEquals(expected, actual);
    assertEquals(
        expected,
        OuterRecord.fromJson(
            json, JsonLfDecoders.text, JsonLfDecoders.bool, UnknownTrailingFieldPolicy.STRICT));
    assertEquals(
        expected,
        OuterRecord.fromJson(
            json, JsonLfDecoders.text, JsonLfDecoders.bool, UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void fromJsonOuterRecordWithExtraFieldStrict() throws JsonLfDecoder.Error {
    OuterRecord<String, Boolean> expected =
        new OuterRecord<>(
            new ParametricRecord<String, Boolean>("Text1", "Text2", true, 42L),
            new ParametricRecord<Long, String>(42L, 69L, "Text2", 69L));

    String json = expected.toJson(JsonLfEncoders::text, JsonLfEncoders::bool);
    String jsonWithExtra = json.substring(0, json.length() - 1) + ",\"_extraField\":42}";

    assertThrows(
        JsonLfDecoder.Error.class,
        () ->
            OuterRecord.fromJson(
                jsonWithExtra,
                JsonLfDecoders.text,
                JsonLfDecoders.bool,
                UnknownTrailingFieldPolicy.STRICT));

    assertEquals(
        expected,
        OuterRecord.fromJson(
            jsonWithExtra,
            JsonLfDecoders.text,
            JsonLfDecoders.bool,
            UnknownTrailingFieldPolicy.IGNORE));

    String innerJson = expected.inner.toJson(JsonLfEncoders::text, JsonLfEncoders::bool);
    String innerJsonWithExtra =
        innerJson.substring(0, innerJson.length() - 1) + ",\"_extraInnerField\":\"hello\"}";
    String innerFixedJson = expected.innerFixed.toJson(JsonLfEncoders::int64, JsonLfEncoders::text);
    String innerFixedJsonWithExtra =
        innerFixedJson.substring(0, innerFixedJson.length() - 1)
            + ",\"_extraFixedField\":\"world\"}";
    String jsonWithInnerExtras =
        json.replace(innerJson, innerJsonWithExtra)
            .replace(innerFixedJson, innerFixedJsonWithExtra);

    assertThrows(
        JsonLfDecoder.Error.class,
        () ->
            OuterRecord.fromJson(
                jsonWithInnerExtras,
                JsonLfDecoders.text,
                JsonLfDecoders.bool,
                UnknownTrailingFieldPolicy.STRICT));

    assertEquals(
        expected,
        OuterRecord.fromJson(
            jsonWithInnerExtras,
            JsonLfDecoders.text,
            JsonLfDecoders.bool,
            UnknownTrailingFieldPolicy.IGNORE));
  }

  @Test
  void decodeOuterRecordWithExtraFieldStrict() {
    OuterRecord<String, Boolean> expected =
        new OuterRecord<>(
            new ParametricRecord<String, Boolean>("Text1", "Text2", true, 42L),
            new ParametricRecord<Long, String>(42L, 69L, "Text2", 69L));

    DamlRecord outerValue = expected.toValue(Text::new, Bool::of);

    // Add extra optional fields to the outer record
    ArrayList<DamlRecord.Field> outerFieldsWithExtra = new ArrayList<>(outerValue.getFields());
    outerFieldsWithExtra.add(new DamlRecord.Field("_extraField", DamlOptional.of(new Int64(42L))));
    DamlRecord outerWithExtra = new DamlRecord(outerFieldsWithExtra);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OuterRecord.valueDecoder(fromText, fromBool)
                .decode(outerWithExtra, UnknownTrailingFieldPolicy.STRICT));

    assertEquals(
        expected,
        OuterRecord.valueDecoder(fromText, fromBool)
            .decode(outerWithExtra, UnknownTrailingFieldPolicy.IGNORE));

    // Add extra optional fields inside inner ParametricRecords
    DamlRecord innerValue = expected.inner.toValue(Text::new, Bool::of);
    ArrayList<DamlRecord.Field> innerFieldsWithExtra = new ArrayList<>(innerValue.getFields());
    innerFieldsWithExtra.add(
        new DamlRecord.Field("_extraInnerField", DamlOptional.of(new Text("hello"))));
    DamlRecord innerWithExtra = new DamlRecord(innerFieldsWithExtra);

    DamlRecord innerFixedValue = expected.innerFixed.toValue(Int64::new, Text::new);
    ArrayList<DamlRecord.Field> innerFixedFieldsWithExtra =
        new ArrayList<>(innerFixedValue.getFields());
    innerFixedFieldsWithExtra.add(
        new DamlRecord.Field("_extraFixedField", DamlOptional.of(new Text("world"))));
    DamlRecord innerFixedWithExtra = new DamlRecord(innerFixedFieldsWithExtra);

    ArrayList<DamlRecord.Field> outerFieldsWithInnerExtras = new ArrayList<>();
    outerFieldsWithInnerExtras.add(new DamlRecord.Field("inner", innerWithExtra));
    outerFieldsWithInnerExtras.add(new DamlRecord.Field("innerFixed", innerFixedWithExtra));
    DamlRecord outerWithInnerExtras = new DamlRecord(outerFieldsWithInnerExtras);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OuterRecord.valueDecoder(fromText, fromBool)
                .decode(outerWithInnerExtras, UnknownTrailingFieldPolicy.STRICT));

    assertEquals(
        expected,
        OuterRecord.valueDecoder(fromText, fromBool)
            .decode(outerWithInnerExtras, UnknownTrailingFieldPolicy.IGNORE));
  }

  Long int64Value = 1L;
  java.math.BigDecimal decimalValue = new java.math.BigDecimal(2L);
  String textValue = "text";
  Boolean boolValue = false;
  String partyValue = "myparty";
  int dateValue = 3; // seconds from epoch
  Long timestampMicrosValue = 4L;
  Instant timestampValue = Instant.ofEpochSecond(0, timestampMicrosValue * 1000);
  Unit unitValue = Unit.getInstance();
  List<Unit> listValue = Arrays.asList(Unit.getInstance(), Unit.getInstance());
  List<List<Long>> nestedListValue =
      Arrays.asList(Arrays.asList(1L, 2L, 3L), Arrays.asList(1L, 2L, 3L));
  NestedRecord nestedRecordValue = new NestedRecord(42L);
  NestedVariant nestedVariantValue = new Nested(42L);
}
