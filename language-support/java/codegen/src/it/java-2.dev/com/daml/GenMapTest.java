// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.api.v1.ValueOuterClass;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Int64;
import com.daml.ledger.javaapi.data.Text;
import com.daml.ledger.javaapi.data.Variant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.genmaptest.MapMapRecord;
import tests.genmaptest.MapRecord;
import tests.genmaptest.TemplateWithMap;
import tests.genmaptest.mapvariant.ParameterizedVariant;
import tests.genmaptest.mapvariant.RecordVariant;
import tests.genmaptest.mapvariant.TextVariant;

@RunWith(JUnitPlatform.class)
public class GenMapTest {

  @Test
  public void mapRecordRoundTrip() {

    ValueOuterClass.Record protoRecord =
        buildRecord(
                buildRecordField(
                    "field",
                    buildMap(
                        buildEntryMap(buildNone, buildText("None")),
                        buildEntryMap(buildSome(buildInt(1)), buildText("Some(1)")),
                        buildEntryMap(buildSome(buildInt(42)), buildText("Some(42)")))))
            .getRecord();

    Map<Optional<Long>, String> javaMap = new HashMap<>();
    javaMap.put(Optional.empty(), "None");
    javaMap.put(Optional.of(1L), "Some(1)");
    javaMap.put(Optional.of(42L), "Some(42)");

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    MapRecord fromValue = MapRecord.fromValue(dataRecord);
    MapRecord fromConstructor = new MapRecord(javaMap);
    MapRecord fromRoundTrip = MapRecord.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataRecord);
    assertEquals(fromConstructor, fromRoundTrip);
  }

  private <K, V> Map<V, K> reverseMap(Map<K, V> m) {
    Map<V, K> reversedMap = new HashMap<>();
    for (Map.Entry<K, V> entry : m.entrySet()) reversedMap.put(entry.getValue(), entry.getKey());
    return reversedMap;
  }

  @Test
  public void mapMapRecordRoundTrip() {

    ValueOuterClass.Record protoRecord =
        buildRecord(
                buildRecordField(
                    "field",
                    buildMap(
                        buildEntryMap(buildMap(), buildMap()),
                        buildEntryMap(
                            buildMap(buildEntryMap(buildInt(1), buildText("1L"))),
                            buildMap(buildEntryMap(buildText("1L"), buildInt(1)))),
                        buildEntryMap(
                            buildMap(
                                buildEntryMap(buildInt(1), buildText("1L")),
                                buildEntryMap(buildInt(42), buildText("42L"))),
                            buildMap(
                                buildEntryMap(buildText("1L"), buildInt(1)),
                                buildEntryMap(buildText("42L"), buildInt(42)))))))
            .getRecord();

    Map<Long, String> inner1Map = new HashMap<>();
    Map<Long, String> inner2Map = new HashMap<>();
    inner2Map.put(1L, "1L");
    Map<Long, String> inner3Map = new HashMap<>();
    inner3Map.put(1L, "1L");
    inner3Map.put(42L, "42L");

    Map<Map<Long, String>, Map<String, Long>> javaMap = new HashMap<>();
    javaMap.put(inner1Map, reverseMap(inner1Map));
    javaMap.put(inner2Map, reverseMap(inner2Map));
    javaMap.put(inner3Map, reverseMap(inner3Map));

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    MapMapRecord fromValue = MapMapRecord.fromValue(dataRecord);
    MapMapRecord fromConstructor = new MapMapRecord(javaMap);
    MapMapRecord fromRoundTrip = MapMapRecord.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataRecord);
    assertEquals(fromConstructor, fromRoundTrip);
  }

  @Test
  public void textMapVariantRoundTripTest() {

    ValueOuterClass.Variant protoVariant =
        buildVariant("TextVariant", buildMap(buildEntryMap(buildText("key"), buildText("value"))))
            .getVariant();

    Variant dataVariant = Variant.fromProto(protoVariant);
    TextVariant<?, ?> fromValue = TextVariant.fromValue(dataVariant);
    TextVariant<?, ?> fromConstructor = new TextVariant<>(Collections.singletonMap("key", "value"));
    TextVariant<?, ?> fromRoundTrip = TextVariant.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataVariant);
    assertEquals(fromConstructor, fromRoundTrip);
  }

  @Test
  public void mapRecordVariantRoundTripTest() {

    ValueOuterClass.Variant protoVariant =
        buildVariant(
                "RecordVariant",
                buildRecord(
                    buildRecordField("x", buildMap(buildEntryMap(buildText("key"), buildInt(42))))))
            .getVariant();

    Variant dataVariant = Variant.fromProto(protoVariant);
    RecordVariant<?, ?> fromValue = RecordVariant.fromValue(dataVariant);
    RecordVariant<?, ?> fromConstructor = new RecordVariant<>(Collections.singletonMap("key", 42L));
    RecordVariant<?, ?> fromRoundTrip = RecordVariant.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataVariant);
    assertEquals(fromConstructor, fromRoundTrip);
  }

  @Test
  public void mapParameterizedVariantRoundTripTest() {

    ValueOuterClass.Variant protoVariant =
        buildVariant(
                "ParameterizedVariant", buildMap(buildEntryMap(buildText("key"), buildInt(42))))
            .getVariant();

    Variant dataVariant = Variant.fromProto(protoVariant);
    ParameterizedVariant<String, Long> fromValue =
        ParameterizedVariant.fromValue(
            dataVariant, x -> x.asText().get().getValue(), x -> x.asInt64().get().getValue());
    ParameterizedVariant<String, Long> fromConstructor =
        new ParameterizedVariant<>(Collections.singletonMap("key", 42L));

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(x -> new Text(x), x -> new Int64(x)), dataVariant);
    assertEquals(fromValue.toValue(x -> new Text(x), x -> new Int64(x)), dataVariant);
  }

  @Test
  public void mapTemplateRoundTripTest() {

    ValueOuterClass.Record protoRecord =
        buildRecord(
                buildRecordField("owner", buildParty("party1")),
                buildRecordField(
                    "valueMap", buildMap(buildEntryMap(buildInt(42), buildText("42")))))
            .getRecord();

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    TemplateWithMap fromValue = TemplateWithMap.fromValue(dataRecord);
    TemplateWithMap fromConstructor =
        new TemplateWithMap("party1", Collections.singletonMap(42L, "42"));

    TemplateWithMap fromRoundTrip = TemplateWithMap.fromValue(fromConstructor.toValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), dataRecord);
    assertEquals(fromValue.toValue(), dataRecord);
    assertEquals(fromConstructor, fromRoundTrip);
  }

  private static ValueOuterClass.Value buildInt(int i) {
    return ValueOuterClass.Value.newBuilder().setInt64(i).build();
  }

  private static ValueOuterClass.Value buildText(String text) {
    return ValueOuterClass.Value.newBuilder().setText(text).build();
  }

  private static ValueOuterClass.Value buildParty(String party) {
    return ValueOuterClass.Value.newBuilder().setParty(party).build();
  }

  private static final ValueOuterClass.Value buildNone =
      ValueOuterClass.Value.newBuilder().setOptional(ValueOuterClass.Optional.newBuilder()).build();

  private static ValueOuterClass.Value buildSome(ValueOuterClass.Value value) {
    return ValueOuterClass.Value.newBuilder()
        .setOptional(ValueOuterClass.Optional.newBuilder().setValue(value))
        .build();
  }

  private static ValueOuterClass.Value buildMap(ValueOuterClass.GenMap.Entry... entries) {
    ValueOuterClass.GenMap.Builder builder = ValueOuterClass.GenMap.newBuilder();
    for (ValueOuterClass.GenMap.Entry entry : entries) builder.addEntries(entry);
    return ValueOuterClass.Value.newBuilder().setGenMap(builder).build();
  }

  private static ValueOuterClass.GenMap.Entry buildEntryMap(
      ValueOuterClass.Value key, ValueOuterClass.Value value) {
    return ValueOuterClass.GenMap.Entry.newBuilder().setKey(key).setValue(value).build();
  }

  private static ValueOuterClass.RecordField buildRecordField(
      String field, ValueOuterClass.Value value) {
    return ValueOuterClass.RecordField.newBuilder().setLabel(field).setValue(value).build();
  }

  private static ValueOuterClass.Value buildRecord(ValueOuterClass.RecordField... fields) {
    ValueOuterClass.Record.Builder builder = ValueOuterClass.Record.newBuilder();
    for (ValueOuterClass.RecordField field : fields) builder.addFields(field);
    return ValueOuterClass.Value.newBuilder().setRecord(builder).build();
  }

  private static ValueOuterClass.Value buildVariant(
      String constructor, ValueOuterClass.Value value) {
    return ValueOuterClass.Value.newBuilder()
        .setVariant(
            ValueOuterClass.Variant.newBuilder().setConstructor(constructor).setValue(value))
        .build();
  }
}
