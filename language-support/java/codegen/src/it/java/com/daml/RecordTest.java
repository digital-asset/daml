// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.api.v1.ValueOuterClass;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.recordtest.MyRecord;
import tests.recordtest.NestedRecord;
import tests.recordtest.NestedVariant;
import tests.recordtest.OuterRecord;
import tests.recordtest.ParametricRecord;
import tests.recordtest.nestedvariant.Nested;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        Bool bool = new Bool(boolValue);
        Party party = new Party(partyValue);
        Date date = new Date(dateValue);
        Timestamp timestamp = new Timestamp(timestampMicrosValue);
        DamlList list = DamlList.of(Unit.getInstance(), Unit.getInstance());
        DamlList nestedList =
                nestedListValue.stream().collect(DamlCollectors.toDamlList(ns -> ns.stream().collect(DamlCollectors.toDamlList(Int64::new))));
        Record nestedRecord = new Record(new Record.Field("value", new Int64(42)));
        Variant nestedVariant = new Variant("Nested", new Int64(42));
        ArrayList<Record.Field> fieldsList = new ArrayList<>(10);
        fieldsList.add(new Record.Field("int_", int_));
        fieldsList.add(new Record.Field("decimal", decimal));
        fieldsList.add(new Record.Field("text", text));
        fieldsList.add(new Record.Field("bool", bool));
        fieldsList.add(new Record.Field("party", party));
        fieldsList.add(new Record.Field("date", date));
        fieldsList.add(new Record.Field("timestamp", timestamp));
        fieldsList.add(new Record.Field("void", bool));
        fieldsList.add(new Record.Field("list", list));
        fieldsList.add(new Record.Field("nestedList", nestedList));
        fieldsList.add(new Record.Field("unit", Unit.getInstance()));
        fieldsList.add(new Record.Field("nestedRecord", nestedRecord));
        fieldsList.add(new Record.Field("nestedVariant", nestedVariant));
        Record myDataRecord = new Record(fieldsList);
        MyRecord myRecord = MyRecord.fromValue(myDataRecord);
        checkRecord(myRecord);
        assertTrue("to value uses original daml lf names for fields", myRecord.toValue().getFieldsMap().get("void").asBool().isPresent());
    }

    @Test
    void objectMethodsWork() {
        LocalDate localDate = LocalDate.ofEpochDay((long) dateValue);
        Instant instant = Instant.ofEpochMilli(timestampMicrosValue);
        MyRecord myRecord1 = new MyRecord(
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
                nestedVariantValue
        );
        MyRecord myRecord2 = new MyRecord(
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
                nestedVariantValue
        );
        assertEquals(myRecord1, myRecord2);
        assertEquals(myRecord1.hashCode(), myRecord2.hashCode());

    }

    @Test
    void outerRecordRoundtrip() {
        ValueOuterClass.Record protoRecord = ValueOuterClass.Record.newBuilder()
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("inner")
                        .setValue(ValueOuterClass.Value.newBuilder().setRecord(ValueOuterClass.Record.newBuilder()
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("fieldX1")
                                        .setValue(ValueOuterClass.Value.newBuilder().setText("Text1"))
                                )
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("fieldX2")
                                        .setValue(ValueOuterClass.Value.newBuilder().setText("Text2"))
                                )
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("fieldY")
                                        .setValue(ValueOuterClass.Value.newBuilder().setBool(true))
                                )
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("fieldInt")
                                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(42L))
                                )
                            .build())
                        )
                        .build())
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("innerFixed")
                        .setValue(ValueOuterClass.Value.newBuilder().setRecord(ValueOuterClass.Record.newBuilder()
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("fieldX1")
                                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(42L))
                                )
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("fieldX2")
                                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(69L))
                                )
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("fieldY")
                                        .setValue(ValueOuterClass.Value.newBuilder().setText("Text2"))
                                )
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("fieldInt")
                                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(69L))
                                )
                                .build())
                        )
                        .build())
                .build();

        Record dataRecord = Record.fromProto(protoRecord);
        OuterRecord<String, Boolean> fromValue = OuterRecord.fromValue(dataRecord, f -> f.asText().get().getValue(),
                f -> f.asBool().get().getValue());
        OuterRecord<String, Boolean> fromConstructor = new OuterRecord<>(
                new ParametricRecord<String, Boolean>("Text1", "Text2", true, 42L),
            new ParametricRecord<Long, String>(42L, 69L, "Text2", 69L));
        OuterRecord<String, Boolean> fromRoundTrip = OuterRecord.<String, Boolean>fromValue(fromConstructor.toValue(Text::new, Bool::new),
                f -> f.asText().get().getValue(), f -> f.asBool().get().getValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(Text::new, Bool::new), dataRecord);
        assertEquals(fromConstructor.toValue(Text::new, Bool::new).toProtoRecord(), protoRecord);
        assertEquals(fromRoundTrip, fromConstructor);
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
    List<List<Long>> nestedListValue = Arrays.asList(Arrays.asList(1L, 2L, 3L), Arrays.asList(1L, 2L, 3L));
    NestedRecord nestedRecordValue = new NestedRecord(42L);
    NestedVariant nestedVariantValue = new Nested(42L);
}
