// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.lf1_3;

import com.daml.ledger.javaapi.data.Int64;
import com.daml.ledger.javaapi.data.Record;
import com.daml.ledger.javaapi.data.Text;
import com.daml.ledger.javaapi.data.Variant;
import com.digitalasset.ledger.api.v1.ValueOuterClass;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.maptest.MapItem;
import tests.maptest.MapItemMapRecord;
import tests.maptest.MapRecord;
import tests.maptest.TemplateWithMap;
import tests.maptest.mapvariant.ParameterizedVariant;
import tests.maptest.mapvariant.RecordVariant;
import tests.maptest.mapvariant.TextVariant;
import tests.varianttest.variantitem.ParameterizedRecordVariant;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnitPlatform.class)
public class MapTest {

    @Test
    public void mapRecordRoundTrip() {

        ValueOuterClass.Record protoRecord = ValueOuterClass.Record.newBuilder()
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("mapField")
                        .setValue(ValueOuterClass.Value.newBuilder()
                                .setMap(ValueOuterClass.Map.newBuilder()
                                        .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                .setKey("key1")
                                                .setValue(ValueOuterClass.Value.newBuilder().setText("value1"))
                                                .build())
                                        .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                .setKey("key2")
                                                .setValue(ValueOuterClass.Value.newBuilder().setText("value2"))
                                                .build()
                                        )
                                        .build())
                                .build())
                        .build())
                .build();

        Map<String, String> javaMap = new HashMap<>();
        javaMap.put("key1", "value1");
        javaMap.put("key2", "value2");

        Record dataRecord = Record.fromProto(protoRecord);
        MapRecord fromValue = MapRecord.fromValue(dataRecord);
        MapRecord fromConstructor = new MapRecord(javaMap);
        MapRecord fromRoundtrip = MapRecord.fromValue(fromConstructor.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataRecord);
        assertEquals(fromConstructor, fromRoundtrip);
    }

    @Test
    public void mapItemMapRecordRoundTrip() {

        ValueOuterClass.Record protoRecord = ValueOuterClass.Record.newBuilder()
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("field")
                        .setValue(ValueOuterClass.Value.newBuilder()
                                .setMap(ValueOuterClass.Map.newBuilder()
                                        .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                .setKey("outerkey1")
                                                .setValue(ValueOuterClass.Value.newBuilder()
                                                        .setMap(ValueOuterClass.Map.newBuilder()
                                                                .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                                        .setKey("key1")
                                                                        .setValue(ValueOuterClass.Value.newBuilder()
                                                                                .setRecord(ValueOuterClass.Record.newBuilder()
                                                                                        .addFields(ValueOuterClass.RecordField.newBuilder()
                                                                                                .setLabel("value")
                                                                                                .setValue(ValueOuterClass.Value.newBuilder()
                                                                                                        .setInt64(1L)
                                                                                                        .build())
                                                                                                .build())
                                                                                        .build())
                                                                                .build()
                                                                        ).build())
                                                                .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                                        .setKey("key2")
                                                                        .setValue(ValueOuterClass.Value.newBuilder()
                                                                                .setRecord(ValueOuterClass.Record.newBuilder()
                                                                                        .addFields(ValueOuterClass.RecordField.newBuilder()
                                                                                                .setLabel("value")
                                                                                                .setValue(ValueOuterClass.Value.newBuilder()
                                                                                                        .setInt64(2L)
                                                                                                        .build())
                                                                                                .build())
                                                                                        .build())
                                                                                .build()
                                                                        ).build())
                                                        )
                                                        .build())
                                                .build())
                                        .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                .setKey("outerkey2")
                                                .setValue(ValueOuterClass.Value.newBuilder()
                                                        .setMap(ValueOuterClass.Map.newBuilder()
                                                                .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                                        .setKey("key1")
                                                                        .setValue(ValueOuterClass.Value.newBuilder()
                                                                                .setRecord(ValueOuterClass.Record.newBuilder()
                                                                                        .addFields(ValueOuterClass.RecordField.newBuilder()
                                                                                                .setLabel("value")
                                                                                                .setValue(ValueOuterClass.Value.newBuilder()
                                                                                                        .setInt64(3L)
                                                                                                        .build())
                                                                                                .build())
                                                                                        .build())
                                                                                .build()
                                                                        ).build())
                                                                .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                                        .setKey("key2")
                                                                        .setValue(ValueOuterClass.Value.newBuilder()
                                                                                .setRecord(ValueOuterClass.Record.newBuilder()
                                                                                        .addFields(ValueOuterClass.RecordField.newBuilder()
                                                                                                .setLabel("value")
                                                                                                .setValue(ValueOuterClass.Value.newBuilder()
                                                                                                        .setInt64(4L)
                                                                                                        .build())
                                                                                                .build())
                                                                                        .build())
                                                                                .build()
                                                                        ).build())
                                                        )
                                                        .build())
                                                .build())
                                        .build()
                                )
                                .build())
                        .build())
                .build();


        Map<String, MapItem<Long>> inner1Map = new HashMap<>();
        inner1Map.put("key1", new MapItem<Long>(1L));
        inner1Map.put("key2", new MapItem<Long>(2L));

        Map<String, MapItem<Long>> inner2Map = new HashMap<>();
        inner2Map.put("key1", new MapItem<Long>(3L));
        inner2Map.put("key2", new MapItem<Long>(4L));

        Map<String, Map<String, MapItem<Long>>> javaMap = new HashMap<>();
        javaMap.put("outerkey1", inner1Map);
        javaMap.put("outerkey2", inner2Map);

        Record dataRecord = Record.fromProto(protoRecord);
        MapItemMapRecord fromValue = MapItemMapRecord.fromValue(dataRecord);
        MapItemMapRecord fromConstructor = new MapItemMapRecord(javaMap);
        MapItemMapRecord fromRoundtrip = MapItemMapRecord.fromValue(fromConstructor.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataRecord);
        assertEquals(fromConstructor, fromRoundtrip);

    }

    @Test
    public void textMapVariantRoundtripTest() {

        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("TextVariant")
                .setValue(ValueOuterClass.Value.newBuilder()
                        .setMap(ValueOuterClass.Map.newBuilder()
                                .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                        .setKey("key")
                                        .setValue(ValueOuterClass.Value.newBuilder().setText("value"))
                                        .build())
                                .build())
                        .build())
                .build();

        Variant dataVariant = Variant.fromProto(protoVariant);
        TextVariant<?> fromValue = TextVariant.fromValue(dataVariant, f -> f.asText().orElseThrow(() -> new IllegalArgumentException("Expecting Text")));
        TextVariant<?> fromConstructor = new TextVariant<>(Collections.singletonMap("key", "value"));
        TextVariant<?> fromRoundtrip = TextVariant.fromValue(fromConstructor.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataVariant);
        assertEquals(fromConstructor, fromRoundtrip);

    }

    @Test
    public void mapRecordVariantRoundtripTest() {

        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("RecordVariant")
                .setValue(ValueOuterClass.Value.newBuilder()
                        .setRecord(ValueOuterClass.Record.newBuilder()
                                .addFields(ValueOuterClass.RecordField.newBuilder()
                                        .setLabel("x")
                                        .setValue(ValueOuterClass.Value.newBuilder()
                                                .setMap(ValueOuterClass.Map.newBuilder()
                                                        .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                                                .setKey("key")
                                                                .setValue(ValueOuterClass.Value.newBuilder().setInt64(42L))
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        Variant dataVariant = Variant.fromProto(protoVariant);
        RecordVariant<?> fromValue = RecordVariant.fromValue(dataVariant);
        RecordVariant<?> fromConstructor = new RecordVariant<>(Collections.singletonMap("key", 42L));
        RecordVariant<?> fromRoundtrip = RecordVariant.fromValue(fromConstructor.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataVariant);
        assertEquals(fromConstructor, fromRoundtrip);
    }

    @Test
    public void mapParameterizedVariantRoundtripTest() {

        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("ParameterizedVariant")
                .setValue(ValueOuterClass.Value.newBuilder()
                        .setMap(ValueOuterClass.Map.newBuilder()
                                .addEntries(ValueOuterClass.Map.Entry.newBuilder()
                                        .setKey("key")
                                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(42L))
                                        .build())
                                .build())
                        .build())
                .build();

        Variant dataVariant = Variant.fromProto(protoVariant);
        ParameterizedVariant<Long> fromValue = ParameterizedVariant.fromValue(dataVariant,
                f -> f.asInt64().orElseThrow(() -> new IllegalArgumentException("Expected Long value")).getValue());
        ParameterizedVariant<Long> fromConstructor = new ParameterizedVariant<>(Collections.singletonMap("key", 42L));

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(f -> new Int64(f)), dataVariant);
        assertEquals(fromValue.toValue(f -> new Int64(f)), dataVariant);

    }

    @Test
    public void mapTemplateRoundtripTest() {

        ValueOuterClass.Record protoRecord = ValueOuterClass.Record.newBuilder()
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("owner")
                        .setValue(ValueOuterClass.Value.newBuilder().setParty("party1").build())
                        .build())
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("valueMap")
                        .setValue(ValueOuterClass.Value.newBuilder()
                                .setMap(ValueOuterClass.Map.newBuilder().addEntries(
                                        ValueOuterClass.Map.Entry.newBuilder()
                                                .setKey("key")
                                                .setValue(ValueOuterClass.Value.newBuilder().setInt64(42L).build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        Record dataRecord = Record.fromProto(protoRecord);
        TemplateWithMap fromValue = TemplateWithMap.fromValue(dataRecord);
        TemplateWithMap fromConstructor = new TemplateWithMap("party1", Collections.singletonMap("key", 42L));
        TemplateWithMap fromRoundtrip = TemplateWithMap.fromValue(fromConstructor.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataRecord);
        assertEquals(fromValue.toValue(), dataRecord);
        assertEquals(fromConstructor, fromRoundtrip);

    }
}