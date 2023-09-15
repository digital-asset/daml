// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.api.v1.ValueOuterClass;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Int64;
import com.daml.ledger.javaapi.data.Variant;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.textmaptest.MapItem;
import tests.textmaptest.MapItemMapRecord;
import tests.textmaptest.MapRecord;
import tests.textmaptest.MapVariant;
import tests.textmaptest.TemplateWithMap;
import tests.textmaptest.mapvariant.ParameterizedVariant;
import tests.textmaptest.mapvariant.RecordVariant;
import tests.textmaptest.mapvariant.TextVariant;

@RunWith(JUnitPlatform.class)
public class TextMapTest {

  @Test
  public void mapRecordRoundTrip() {

    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("mapField")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setMap(
                                ValueOuterClass.Map.newBuilder()
                                    .addEntries(
                                        ValueOuterClass.Map.Entry.newBuilder()
                                            .setKey("key1")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setText("value1"))
                                            .build())
                                    .addEntries(
                                        ValueOuterClass.Map.Entry.newBuilder()
                                            .setKey("key2")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setText("value2"))
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    Map<String, String> javaMap = new HashMap<>();
    javaMap.put("key1", "value1");
    javaMap.put("key2", "value2");

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    MapRecord fromValue = MapRecord.fromValue(dataRecord);
    MapRecord fromConstructor = new MapRecord(javaMap);
    MapRecord fromRoundtrip = MapRecord.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataRecord);
    assertEquals(fromConstructor, fromRoundtrip);
  }

  @Test
  void fromJsonMapRecord() throws JsonLfDecoder.Error {
    MapRecord expected =
        new MapRecord(
            new HashMap<>() {
              {
                put("key1", "value1");
                put("key2", "value2");
              }
            });

    assertEquals(
        expected,
        MapRecord.fromJson("{\"mapField\": {\"key1\": \"value1\", \"key2\": \"value2\"}}"));
  }

  @Test
  public void mapItemMapRecordRoundTrip() {

    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("field")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setMap(
                                ValueOuterClass.Map.newBuilder()
                                    .addEntries(
                                        ValueOuterClass.Map.Entry.newBuilder()
                                            .setKey("outerkey1")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setMap(
                                                        ValueOuterClass.Map.newBuilder()
                                                            .addEntries(
                                                                ValueOuterClass.Map.Entry
                                                                    .newBuilder()
                                                                    .setKey("key1")
                                                                    .setValue(
                                                                        ValueOuterClass.Value
                                                                            .newBuilder()
                                                                            .setRecord(
                                                                                ValueOuterClass
                                                                                    .Record
                                                                                    .newBuilder()
                                                                                    .addFields(
                                                                                        ValueOuterClass
                                                                                            .RecordField
                                                                                            .newBuilder()
                                                                                            .setLabel(
                                                                                                "value")
                                                                                            .setValue(
                                                                                                ValueOuterClass
                                                                                                    .Value
                                                                                                    .newBuilder()
                                                                                                    .setInt64(
                                                                                                        1L)
                                                                                                    .build())
                                                                                            .build())
                                                                                    .build())
                                                                            .build())
                                                                    .build())
                                                            .addEntries(
                                                                ValueOuterClass.Map.Entry
                                                                    .newBuilder()
                                                                    .setKey("key2")
                                                                    .setValue(
                                                                        ValueOuterClass.Value
                                                                            .newBuilder()
                                                                            .setRecord(
                                                                                ValueOuterClass
                                                                                    .Record
                                                                                    .newBuilder()
                                                                                    .addFields(
                                                                                        ValueOuterClass
                                                                                            .RecordField
                                                                                            .newBuilder()
                                                                                            .setLabel(
                                                                                                "value")
                                                                                            .setValue(
                                                                                                ValueOuterClass
                                                                                                    .Value
                                                                                                    .newBuilder()
                                                                                                    .setInt64(
                                                                                                        2L)
                                                                                                    .build())
                                                                                            .build())
                                                                                    .build())
                                                                            .build())
                                                                    .build()))
                                                    .build())
                                            .build())
                                    .addEntries(
                                        ValueOuterClass.Map.Entry.newBuilder()
                                            .setKey("outerkey2")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setMap(
                                                        ValueOuterClass.Map.newBuilder()
                                                            .addEntries(
                                                                ValueOuterClass.Map.Entry
                                                                    .newBuilder()
                                                                    .setKey("key1")
                                                                    .setValue(
                                                                        ValueOuterClass.Value
                                                                            .newBuilder()
                                                                            .setRecord(
                                                                                ValueOuterClass
                                                                                    .Record
                                                                                    .newBuilder()
                                                                                    .addFields(
                                                                                        ValueOuterClass
                                                                                            .RecordField
                                                                                            .newBuilder()
                                                                                            .setLabel(
                                                                                                "value")
                                                                                            .setValue(
                                                                                                ValueOuterClass
                                                                                                    .Value
                                                                                                    .newBuilder()
                                                                                                    .setInt64(
                                                                                                        3L)
                                                                                                    .build())
                                                                                            .build())
                                                                                    .build())
                                                                            .build())
                                                                    .build())
                                                            .addEntries(
                                                                ValueOuterClass.Map.Entry
                                                                    .newBuilder()
                                                                    .setKey("key2")
                                                                    .setValue(
                                                                        ValueOuterClass.Value
                                                                            .newBuilder()
                                                                            .setRecord(
                                                                                ValueOuterClass
                                                                                    .Record
                                                                                    .newBuilder()
                                                                                    .addFields(
                                                                                        ValueOuterClass
                                                                                            .RecordField
                                                                                            .newBuilder()
                                                                                            .setLabel(
                                                                                                "value")
                                                                                            .setValue(
                                                                                                ValueOuterClass
                                                                                                    .Value
                                                                                                    .newBuilder()
                                                                                                    .setInt64(
                                                                                                        4L)
                                                                                                    .build())
                                                                                            .build())
                                                                                    .build())
                                                                            .build())
                                                                    .build()))
                                                    .build())
                                            .build())
                                    .build())
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

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    MapItemMapRecord fromValue = MapItemMapRecord.fromValue(dataRecord);
    MapItemMapRecord fromConstructor = new MapItemMapRecord(javaMap);
    MapItemMapRecord fromRoundtrip = MapItemMapRecord.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataRecord);
    assertEquals(fromConstructor, fromRoundtrip);
  }

  @Test
  void fromJsonMapItemMapRecord() throws JsonLfDecoder.Error {
    MapItemMapRecord expected =
        new MapItemMapRecord(
            new HashMap<>() {
              {
                put(
                    "outerkey1",
                    new HashMap<>() {
                      {
                        put("key1", new MapItem<Long>(1L));
                        put("key2", new MapItem<Long>(2L));
                      }
                    });
                put(
                    "outerkey2",
                    new HashMap<>() {
                      {
                        put("key1", new MapItem<Long>(3L));
                        put("key2", new MapItem<Long>(4L));
                      }
                    });
              }
            });

    assertEquals(
        expected,
        MapItemMapRecord.fromJson(
            "{\"field\": {"
                + "\"outerkey1\": {\"key1\": {\"value\": 1}, \"key2\": {\"value\": 2} }, "
                + "\"outerkey2\": {\"key1\": {\"value\": 3}, \"key2\": {\"value\": 4} } "
                + "}}"));
  }

  @Test
  public void textMapVariantRoundtripTest() {

    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("TextVariant")
            .setValue(
                ValueOuterClass.Value.newBuilder()
                    .setMap(
                        ValueOuterClass.Map.newBuilder()
                            .addEntries(
                                ValueOuterClass.Map.Entry.newBuilder()
                                    .setKey("key")
                                    .setValue(ValueOuterClass.Value.newBuilder().setText("value"))
                                    .build())
                            .build())
                    .build())
            .build();

    Variant dataVariant = Variant.fromProto(protoVariant);
    TextVariant<?> fromValue =
        TextVariant.fromValue(
            dataVariant,
            f -> f.asText().orElseThrow(() -> new IllegalArgumentException("Expecting Text")));
    TextVariant<?> fromConstructor = new TextVariant<>(Collections.singletonMap("key", "value"));
    TextVariant<?> fromRoundtrip = TextVariant.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataVariant);
    assertEquals(fromConstructor, fromRoundtrip);
  }

  @Test
  public void fromJsonTextVariant() throws JsonLfDecoder.Error {
    MapVariant<?> expected = new TextVariant<>(Collections.singletonMap("key", "value"));

    assertEquals(
        expected,
        MapVariant.fromJson(
            "{\"tag\": \"TextVariant\", \"value\": {\"key\": \"value\"}}",
            JsonLfReader.Decoders.unit));
  }

  @Test
  public void mapRecordVariantRoundtripTest() {

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
                                    .setValue(
                                        ValueOuterClass.Value.newBuilder()
                                            .setMap(
                                                ValueOuterClass.Map.newBuilder()
                                                    .addEntries(
                                                        ValueOuterClass.Map.Entry.newBuilder()
                                                            .setKey("key")
                                                            .setValue(
                                                                ValueOuterClass.Value.newBuilder()
                                                                    .setInt64(42L))
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
  void fromJsonRecordVariant() throws JsonLfDecoder.Error {
    MapVariant<?> expected = new RecordVariant<>(Collections.singletonMap("key", 42L));

    assertEquals(
        expected,
        MapVariant.fromJson(
            "{\"tag\": \"RecordVariant\", \"value\": {\"x\": {\"key\": 42}}}",
            JsonLfReader.Decoders.unit));
  }

  @Test
  public void mapParameterizedVariantRoundtripTest() {

    ValueOuterClass.Variant protoVariant =
        ValueOuterClass.Variant.newBuilder()
            .setConstructor("ParameterizedVariant")
            .setValue(
                ValueOuterClass.Value.newBuilder()
                    .setMap(
                        ValueOuterClass.Map.newBuilder()
                            .addEntries(
                                ValueOuterClass.Map.Entry.newBuilder()
                                    .setKey("key")
                                    .setValue(ValueOuterClass.Value.newBuilder().setInt64(42L))
                                    .build())
                            .build())
                    .build())
            .build();

    Variant dataVariant = Variant.fromProto(protoVariant);
    ParameterizedVariant<Long> fromValue =
        ParameterizedVariant.fromValue(
            dataVariant,
            f ->
                f.asInt64()
                    .orElseThrow(() -> new IllegalArgumentException("Expected Long value"))
                    .getValue());
    ParameterizedVariant<Long> fromConstructor =
        new ParameterizedVariant<>(Collections.singletonMap("key", 42L));

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(f -> new Int64(f)), dataVariant);
    assertEquals(fromValue.toValue(f -> new Int64(f)), dataVariant);
  }

  @Test
  void fromJsonParameterizedVariant() throws JsonLfDecoder.Error {
    MapVariant<Long> expected = new ParameterizedVariant<>(Collections.singletonMap("key", 42L));

    assertEquals(
        expected,
        MapVariant.fromJson(
            "{\"tag\": \"ParameterizedVariant\", \"value\": {\"key\": 42}}",
            JsonLfReader.Decoders.int64));
  }

  @Test
  public void mapTemplateRoundtripTest() {

    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("owner")
                    .setValue(ValueOuterClass.Value.newBuilder().setParty("party1").build())
                    .build())
            .addFields(
                ValueOuterClass.RecordField.newBuilder()
                    .setLabel("valueMap")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setMap(
                                ValueOuterClass.Map.newBuilder()
                                    .addEntries(
                                        ValueOuterClass.Map.Entry.newBuilder()
                                            .setKey("key")
                                            .setValue(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setInt64(42L)
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    TemplateWithMap fromValue = TemplateWithMap.fromValue(dataRecord);
    TemplateWithMap fromConstructor =
        new TemplateWithMap("party1", Collections.singletonMap("key", 42L));
    TemplateWithMap fromRoundtrip = TemplateWithMap.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataRecord);
    assertEquals(fromValue.toValue(), dataRecord);
    assertEquals(fromConstructor, fromRoundtrip);
  }

  @Test
  void fromJsonTemplateWithMap() throws JsonLfDecoder.Error {
    TemplateWithMap expected = new TemplateWithMap("party1", Collections.singletonMap("key", 42L));

    assertEquals(
        expected, TemplateWithMap.fromJson("{\"owner\": \"party1\", \"valueMap\": {\"key\": 42}}"));
  }
}
