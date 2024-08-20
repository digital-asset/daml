// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders.fromList;
import static com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders.fromText;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.api.v2.ValueOuterClass;
import com.daml.ledger.javaapi.data.DamlCollectors;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Text;
import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoders;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders;
import com.google.protobuf.Empty;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.listtest.*;
import tests.listtest.listitem.Node;

@RunWith(JUnitPlatform.class)
public class ListTest {

  @Test
  void fromProtobufValue() {
    ValueOuterClass.Record protoListRecord =
        ValueOuterClass.Record.newBuilder()
            .addAllFields(
                Arrays.asList(
                    ValueOuterClass.Record.Field.newBuilder()
                        .setLabel("intList")
                        .setValue(
                            ValueOuterClass.Value.newBuilder()
                                .setList(
                                    ValueOuterClass.List.newBuilder()
                                        .addAllElements(
                                            Arrays.asList(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setInt64(1)
                                                    .build(),
                                                ValueOuterClass.Value.newBuilder()
                                                    .setInt64(2)
                                                    .build()))))
                        .build(),
                    ValueOuterClass.Record.Field.newBuilder()
                        .setLabel("unitList")
                        .setValue(
                            ValueOuterClass.Value.newBuilder()
                                .setList(
                                    ValueOuterClass.List.newBuilder()
                                        .addElements(
                                            ValueOuterClass.Value.newBuilder()
                                                .setUnit(Empty.getDefaultInstance()))))
                        .build(),
                    ValueOuterClass.Record.Field.newBuilder()
                        .setLabel("itemList")
                        .setValue(
                            ValueOuterClass.Value.newBuilder()
                                .setList(
                                    ValueOuterClass.List.newBuilder()
                                        .addAllElements(
                                            Arrays.asList(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setVariant(
                                                        ValueOuterClass.Variant.newBuilder()
                                                            .setConstructor("Node")
                                                            .setValue(
                                                                ValueOuterClass.Value.newBuilder()
                                                                    .setInt64(17)))
                                                    .build(),
                                                ValueOuterClass.Value.newBuilder()
                                                    .setVariant(
                                                        ValueOuterClass.Variant.newBuilder()
                                                            .setConstructor("Node")
                                                            .setValue(
                                                                ValueOuterClass.Value.newBuilder()
                                                                    .setInt64(42)))
                                                    .build()))))
                        .build()))
            .build();

    DamlRecord record = DamlRecord.fromProto(protoListRecord);
    MyListRecord fromRecord = MyListRecord.valueDecoder().decode(record);

    MyListRecord fromCodegen =
        new MyListRecord(
            Arrays.asList(1L, 2L),
            Collections.singletonList(Unit.getInstance()),
            Arrays.asList(new Node<Long>(17L), new Node<Long>(42L)));

    MyListRecord roundTripped = MyListRecord.valueDecoder().decode(fromCodegen.toValue());

    assertEquals(fromRecord, fromCodegen);
    assertEquals(fromCodegen.toValue().toProtoRecord(), protoListRecord);
    assertEquals(fromCodegen, roundTripped);
  }

  @Test
  void roundtripJsonMyListRecord() throws JsonLfDecoder.Error {
    MyListRecord expected =
        new MyListRecord(
            Arrays.asList(1L, 2L),
            Collections.singletonList(Unit.getInstance()),
            Arrays.asList(new Node<Long>(17L), new Node<Long>(42L)));

    assertEquals(expected, MyListRecord.fromJson(expected.toJson()));
  }

  @Test
  void listOfListsFromProtobufValue() {

    ValueOuterClass.Record protoListRecord =
        ValueOuterClass.Record.newBuilder()
            .addAllFields(
                Arrays.asList(
                    ValueOuterClass.Record.Field.newBuilder()
                        .setLabel("itemList")
                        .setValue(
                            ValueOuterClass.Value.newBuilder()
                                .setList(
                                    ValueOuterClass.List.newBuilder()
                                        .addAllElements(
                                            Arrays.asList(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setList(
                                                        ValueOuterClass.List.newBuilder()
                                                            .addAllElements(
                                                                Arrays.asList(
                                                                    ValueOuterClass.Value
                                                                        .newBuilder()
                                                                        .setVariant(
                                                                            ValueOuterClass.Variant
                                                                                .newBuilder()
                                                                                .setConstructor(
                                                                                    "Node")
                                                                                .setValue(
                                                                                    ValueOuterClass
                                                                                        .Value
                                                                                        .newBuilder()
                                                                                        .setInt64(
                                                                                            17)))
                                                                        .build(),
                                                                    ValueOuterClass.Value
                                                                        .newBuilder()
                                                                        .setVariant(
                                                                            ValueOuterClass.Variant
                                                                                .newBuilder()
                                                                                .setConstructor(
                                                                                    "Node")
                                                                                .setValue(
                                                                                    ValueOuterClass
                                                                                        .Value
                                                                                        .newBuilder()
                                                                                        .setInt64(
                                                                                            42)))
                                                                        .build())))
                                                    .build(),
                                                ValueOuterClass.Value.newBuilder()
                                                    .setList(
                                                        ValueOuterClass.List.newBuilder()
                                                            .addAllElements(
                                                                Arrays.asList(
                                                                    ValueOuterClass.Value
                                                                        .newBuilder()
                                                                        .setVariant(
                                                                            ValueOuterClass.Variant
                                                                                .newBuilder()
                                                                                .setConstructor(
                                                                                    "Node")
                                                                                .setValue(
                                                                                    ValueOuterClass
                                                                                        .Value
                                                                                        .newBuilder()
                                                                                        .setInt64(
                                                                                            1337)))
                                                                        .build())))
                                                    .build()))))
                        .build()))
            .build();

    DamlRecord record = DamlRecord.fromProto(protoListRecord);
    MyListOfListRecord fromRecord = MyListOfListRecord.valueDecoder().decode(record);

    MyListOfListRecord fromCodegen =
        new MyListOfListRecord(
            Arrays.asList(
                Arrays.asList(new Node<>(17L), new Node<>(42L)), Arrays.asList(new Node<>(1337L))));

    assertEquals(fromRecord, fromCodegen);
    assertEquals(fromCodegen.toValue().toProtoRecord(), protoListRecord);
  }

  @Test
  void roundtripJsonMyListOfListRecord() throws JsonLfDecoder.Error {
    MyListOfListRecord expected =
        new MyListOfListRecord(
            Arrays.asList(
                Arrays.asList(new Node<>(17L), new Node<>(42L)), Arrays.asList(new Node<>(1337L))));

    assertEquals(expected, MyListOfListRecord.fromJson(expected.toJson()));
  }

  @Test
  void listOfColorsFromProtobufValue() {

    ValueOuterClass.Record protoColorListRecord =
        ValueOuterClass.Record.newBuilder()
            .addAllFields(
                Arrays.asList(
                    ValueOuterClass.Record.Field.newBuilder()
                        .setLabel("colors")
                        .setValue(
                            ValueOuterClass.Value.newBuilder()
                                .setList(
                                    ValueOuterClass.List.newBuilder()
                                        .addAllElements(
                                            Arrays.asList(
                                                ValueOuterClass.Value.newBuilder()
                                                    .setEnum(
                                                        ValueOuterClass.Enum.newBuilder()
                                                            .setConstructor("Green"))
                                                    .build(),
                                                ValueOuterClass.Value.newBuilder()
                                                    .setEnum(
                                                        ValueOuterClass.Enum.newBuilder()
                                                            .setConstructor("Red"))
                                                    .build()))
                                        .build()))
                        .build()))
            .build();

    DamlRecord record = DamlRecord.fromProto(protoColorListRecord);
    ColorListRecord fromRecord = ColorListRecord.valueDecoder().decode(record);

    ColorListRecord fromCodegen = new ColorListRecord(Arrays.asList(Color.GREEN, Color.RED));

    assertEquals(fromRecord, fromCodegen);
    assertEquals(fromCodegen.toValue().toProtoRecord(), protoColorListRecord);
  }

  @Test
  void roundtripJsonColorListRecord() throws JsonLfDecoder.Error {
    ColorListRecord expected = new ColorListRecord(Arrays.asList(Color.GREEN, Color.RED));

    assertEquals(expected, ColorListRecord.fromJson(expected.toJson()));
  }

  @Test
  void parameterizedListTestRoundTrip() {

    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.Record.Field.newBuilder()
                    .setLabel("list")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setList(
                                ValueOuterClass.List.newBuilder()
                                    .addAllElements(
                                        Arrays.asList(
                                            ValueOuterClass.Value.newBuilder()
                                                .setText("Element1")
                                                .build(),
                                            ValueOuterClass.Value.newBuilder()
                                                .setText("Element2")
                                                .build())))
                            .build())
                    .build())
            .build();

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    ParameterizedListRecord<String> fromValue =
        ParameterizedListRecord.valueDecoder(fromText).decode(dataRecord);
    ParameterizedListRecord<String> fromConstructor =
        new ParameterizedListRecord<>(Arrays.asList("Element1", "Element2"));
    ParameterizedListRecord<String> fromRoundTrip =
        ParameterizedListRecord.valueDecoder(fromText).decode(fromConstructor.toValue(Text::new));

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(Text::new), dataRecord);
    assertEquals(fromConstructor.toValue(Text::new).toProtoRecord(), protoRecord);
    assertEquals(fromRoundTrip, fromConstructor);
  }

  @Test
  void roundtripJsonParameterizedListRecordWithString() throws JsonLfDecoder.Error {
    ParameterizedListRecord<String> expected =
        new ParameterizedListRecord<>(Arrays.asList("Element1", "Element2"));

    String json = expected.toJson(JsonLfEncoders::text);
    var actual = ParameterizedListRecord.fromJson(json, JsonLfDecoders.text);

    assertEquals(expected, actual);
  }

  @Test
  void parameterizedListOfListTestRoundTrip() {

    ValueOuterClass.Record protoRecord =
        ValueOuterClass.Record.newBuilder()
            .addFields(
                ValueOuterClass.Record.Field.newBuilder()
                    .setLabel("list")
                    .setValue(
                        ValueOuterClass.Value.newBuilder()
                            .setList(
                                ValueOuterClass.List.newBuilder()
                                    .addAllElements(
                                        Arrays.asList(
                                            ValueOuterClass.Value.newBuilder()
                                                .setList(
                                                    ValueOuterClass.List.newBuilder()
                                                        .addAllElements(
                                                            Arrays.asList(
                                                                ValueOuterClass.Value.newBuilder()
                                                                    .setText("Element1")
                                                                    .build(),
                                                                ValueOuterClass.Value.newBuilder()
                                                                    .setText("Element2")
                                                                    .build()))
                                                        .build())
                                                .build(),
                                            ValueOuterClass.Value.newBuilder()
                                                .setList(
                                                    ValueOuterClass.List.newBuilder()
                                                        .addAllElements(
                                                            Arrays.asList(
                                                                ValueOuterClass.Value.newBuilder()
                                                                    .setText("Element3")
                                                                    .build(),
                                                                ValueOuterClass.Value.newBuilder()
                                                                    .setText("Element4")
                                                                    .build()))
                                                        .build())
                                                .build()))
                                    .build()))
                    .build())
            .build();

    DamlRecord dataRecord = DamlRecord.fromProto(protoRecord);
    ParameterizedListRecord<List<String>> fromValue =
        ParameterizedListRecord.valueDecoder(fromList(fromText)).decode(dataRecord);
    ParameterizedListRecord<List<String>> fromConstructor =
        new ParameterizedListRecord<List<String>>(
            Arrays.asList(
                Arrays.asList("Element1", "Element2"), Arrays.asList("Element3", "Element4")));

    assertEquals(fromValue, fromConstructor);
    assertEquals(
        fromConstructor.toValue(f -> f.stream().collect(DamlCollectors.toDamlList(Text::new))),
        dataRecord);
  }

  @Test
  void roundtripJsonParameterizedListRecordWithListOfString() throws JsonLfDecoder.Error {
    ParameterizedListRecord<List<String>> expected =
        new ParameterizedListRecord<>(
            Arrays.asList(
                Arrays.asList("Element1", "Element2"), Arrays.asList("Element3", "Element4")));

    String json = expected.toJson(JsonLfEncoders.list(JsonLfEncoders::text));
    var actual = ParameterizedListRecord.fromJson(json, JsonLfDecoders.list(JsonLfDecoders.text));

    assertEquals(expected, actual);
  }
}
