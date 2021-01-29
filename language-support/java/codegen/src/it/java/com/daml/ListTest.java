// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.daml.ledger.api.v1.ValueOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.google.protobuf.Empty;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.listtest.*;
import tests.listtest.Color;
import tests.listtest.listitem.Node;

@RunWith(JUnitPlatform.class)
public class ListTest {

  @Test
  void fromProtobufValue() {
    ValueOuterClass.Record protoListRecord =
        ValueOuterClass.Record.newBuilder()
            .addAllFields(
                Arrays.asList(
                    ValueOuterClass.RecordField.newBuilder()
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
                    ValueOuterClass.RecordField.newBuilder()
                        .setLabel("unitList")
                        .setValue(
                            ValueOuterClass.Value.newBuilder()
                                .setList(
                                    ValueOuterClass.List.newBuilder()
                                        .addElements(
                                            ValueOuterClass.Value.newBuilder()
                                                .setUnit(Empty.getDefaultInstance()))))
                        .build(),
                    ValueOuterClass.RecordField.newBuilder()
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

    Record record = Record.fromProto(protoListRecord);
    MyListRecord fromRecord = MyListRecord.fromValue(record);

    MyListRecord fromCodegen =
        new MyListRecord(
            Arrays.asList(1L, 2L),
            Collections.singletonList(Unit.getInstance()),
            Arrays.asList(new Node<Long>(17L), new Node<Long>(42L)));

    MyListRecord roundTripped = MyListRecord.fromValue(fromCodegen.toValue());

    assertEquals(fromRecord, fromCodegen);
    assertEquals(fromCodegen.toValue().toProtoRecord(), protoListRecord);
    assertEquals(fromCodegen, roundTripped);
  }

  @Test
  void listOfListsFromProtobufValue() {

    ValueOuterClass.Record protoListRecord =
        ValueOuterClass.Record.newBuilder()
            .addAllFields(
                Arrays.asList(
                    ValueOuterClass.RecordField.newBuilder()
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

    Record record = Record.fromProto(protoListRecord);
    MyListOfListRecord fromRecord = MyListOfListRecord.fromValue(record);

    MyListOfListRecord fromCodegen =
        new MyListOfListRecord(
            Arrays.asList(
                Arrays.asList(new Node<>(17L), new Node<>(42L)), Arrays.asList(new Node<>(1337L))));

    assertEquals(fromRecord, fromCodegen);
    assertEquals(fromCodegen.toValue().toProtoRecord(), protoListRecord);
  }

  @Test
  void listOfColorsFromProtobufValue() {

    ValueOuterClass.Record protoColorListRecord =
        ValueOuterClass.Record.newBuilder()
            .addAllFields(
                Arrays.asList(
                    ValueOuterClass.RecordField.newBuilder()
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

    Record record = Record.fromProto(protoColorListRecord);
    ColorListRecord fromRecord = ColorListRecord.fromValue(record);

    ColorListRecord fromCodegen = new ColorListRecord(Arrays.asList(Color.GREEN, Color.RED));

    assertEquals(fromRecord, fromCodegen);
    assertEquals(fromCodegen.toValue().toProtoRecord(), protoColorListRecord);
  }

  @Test
  void parameterizedListTestRoundTrip() {

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
                                                .setText("Element1")
                                                .build(),
                                            ValueOuterClass.Value.newBuilder()
                                                .setText("Element2")
                                                .build())))
                            .build())
                    .build())
            .build();

    Record dataRecord = Record.fromProto(protoRecord);
    ParameterizedListRecord<String> fromValue =
        ParameterizedListRecord.fromValue(dataRecord, f -> f.asText().get().getValue());
    ParameterizedListRecord<String> fromConstructor =
        new ParameterizedListRecord<>(Arrays.asList("Element1", "Element2"));
    ParameterizedListRecord<String> fromRoundTrip =
        ParameterizedListRecord.fromValue(
            fromConstructor.toValue(Text::new), f -> f.asText().get().getValue());

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(Text::new), dataRecord);
    assertEquals(fromConstructor.toValue(Text::new).toProtoRecord(), protoRecord);
    assertEquals(fromRoundTrip, fromConstructor);
  }

  @Test
  void parameterizedListOfListTestRoundTrip() {

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

    Record dataRecord = Record.fromProto(protoRecord);
    ParameterizedListRecord<List<String>> fromValue =
        ParameterizedListRecord.fromValue(
            dataRecord,
            f ->
                f
                    .asList()
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                "Expected list to be of type"
                                    + " com.daml.ledger.javaapi.data.DamlList"))
                    .getValues()
                    .stream()
                    .map(
                        f1 ->
                            f1.asText()
                                .orElseThrow(
                                    () ->
                                        new IllegalArgumentException(
                                            "Expected list to be of type"
                                                + " com.daml.ledger.javaapi.data.Text"))
                                .getValue())
                    .collect(Collectors.toList()));
    ParameterizedListRecord<List<String>> fromConstructor =
        new ParameterizedListRecord<List<String>>(
            Arrays.asList(
                Arrays.asList("Element1", "Element2"), Arrays.asList("Element3", "Element4")));

    assertEquals(fromValue, fromConstructor);
    assertEquals(
        fromConstructor.toValue(f -> f.stream().collect(DamlCollectors.toDamlList(Text::new))),
        dataRecord);
  }
}
