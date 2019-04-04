// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset;

import com.daml.ledger.javaapi.data.*;
import com.digitalasset.ledger.api.v1.ValueOuterClass;
import com.google.protobuf.Empty;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.listtest.*;
import tests.listtest.color.Green;
import tests.listtest.color.Red;
import tests.listtest.listitem.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;


@RunWith(JUnitPlatform.class)
public class ListTest {


    @Test
    void fromProtobufValue() {
        ValueOuterClass.Record protoListRecord =
                ValueOuterClass.Record.newBuilder().addAllFields(Arrays.asList(
                        ValueOuterClass.RecordField.newBuilder()
                                .setLabel("intList")
                                .setValue(ValueOuterClass.Value.newBuilder().setList(
                                        ValueOuterClass.List.newBuilder().addAllElements(Arrays.asList(
                                                ValueOuterClass.Value.newBuilder().setInt64(1).build(),
                                                ValueOuterClass.Value.newBuilder().setInt64(2).build()
                                        ))
                                ))
                                .build(),
                        ValueOuterClass.RecordField.newBuilder()
                                .setLabel("unitList")
                                .setValue(ValueOuterClass.Value.newBuilder().setList(
                                        ValueOuterClass.List.newBuilder().addElements(
                                                ValueOuterClass.Value.newBuilder().setUnit(Empty.getDefaultInstance()))
                                )).build(),
                        ValueOuterClass.RecordField.newBuilder()
                                .setLabel("itemList")
                                .setValue(ValueOuterClass.Value.newBuilder().setList(ValueOuterClass.List.newBuilder().addAllElements(Arrays.asList(
                                        ValueOuterClass.Value.newBuilder().setVariant(
                                                ValueOuterClass.Variant.newBuilder()
                                                        .setConstructor("Node")
                                                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(17))).build(),
                                        ValueOuterClass.Value.newBuilder().setVariant(
                                                ValueOuterClass.Variant.newBuilder()
                                                        .setConstructor("Node")
                                                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(42))).build()))
                                )).build()
                        )
                ).build();

        Record record = Record.fromProto(protoListRecord);
        MyListRecord fromRecord = MyListRecord.fromValue(record);

        MyListRecord fromCodegen = new MyListRecord(
                Arrays.<Long>asList(1L, 2L),
                Collections.<Unit>singletonList(Unit.getInstance()),
                Arrays.<ListItem<Long>>asList(new Node<Long>(17L), new Node<Long>(42L))
        );

        MyListRecord roundTripped = MyListRecord.fromValue(fromCodegen.toValue());

        assertEquals(fromRecord, fromCodegen);
        assertEquals(fromCodegen.toValue().toProtoRecord(), protoListRecord);
        assertEquals(fromCodegen, roundTripped);
    }

    @Test
    void listOfListsFromProtobufValue() {

        ValueOuterClass.Record protoListRecord =
                ValueOuterClass.Record.newBuilder().addAllFields(Arrays.asList(
                        ValueOuterClass.RecordField.newBuilder()
                                .setLabel("itemList")
                                .setValue(ValueOuterClass.Value.newBuilder().setList(ValueOuterClass.List.newBuilder().addAllElements(Arrays.asList(
                                        ValueOuterClass.Value.newBuilder().setList(ValueOuterClass.List.newBuilder().addAllElements(Arrays.asList(
                                                ValueOuterClass.Value.newBuilder().setVariant(
                                                        ValueOuterClass.Variant.newBuilder()
                                                                .setConstructor("Node")
                                                                .setValue(ValueOuterClass.Value.newBuilder().setInt64(17))).build(),
                                                ValueOuterClass.Value.newBuilder().setVariant(
                                                        ValueOuterClass.Variant.newBuilder()
                                                                .setConstructor("Node")
                                                                .setValue(ValueOuterClass.Value.newBuilder().setInt64(42))).build()
                                        ))).build(),
                                        ValueOuterClass.Value.newBuilder().setList(ValueOuterClass.List.newBuilder().addAllElements(Arrays.asList(
                                                ValueOuterClass.Value.newBuilder().setVariant(
                                                        ValueOuterClass.Variant.newBuilder()
                                                                .setConstructor("Node")
                                                                .setValue(ValueOuterClass.Value.newBuilder().setInt64(1337))).build()
                                        ))).build()
                                ))))
                                .build()
                )).build();

        Record record = Record.fromProto(protoListRecord);
        MyListOfListRecord fromRecord = MyListOfListRecord.fromValue(record);

        MyListOfListRecord fromCodegen = new MyListOfListRecord(
                Arrays.asList(Arrays.<ListItem<Long>>asList(new Node<>(17L), new Node<>(42L)),
                        Arrays.<ListItem<Long>>asList(new Node<>(1337L)))
        );

        assertEquals(fromRecord, fromCodegen);
        assertEquals(fromCodegen.toValue().toProtoRecord(), protoListRecord);

    }

    @Test
    void listOfColorsFromProtobufValue() {

        ValueOuterClass.Record protoColorListRecord = ValueOuterClass.Record.newBuilder().addAllFields(Arrays.asList(
                ValueOuterClass.RecordField.newBuilder()
                        .setLabel("colors")
                        .setValue(ValueOuterClass.Value.newBuilder().setList(ValueOuterClass.List.newBuilder().addAllElements(Arrays.asList(
                                ValueOuterClass.Value.newBuilder().setVariant(
                                        ValueOuterClass.Variant.newBuilder()
                                                .setConstructor("Green")
                                                .setValue(ValueOuterClass.Value.newBuilder().setUnit(Empty.newBuilder().build()))).build(),
                                ValueOuterClass.Value.newBuilder().setVariant(
                                        ValueOuterClass.Variant.newBuilder()
                                                .setConstructor("Red")
                                                .setValue(ValueOuterClass.Value.newBuilder().setUnit(Empty.newBuilder().build()))).build()

                        )).build()))
                .build()
        )).build();

        Record record = Record.fromProto(protoColorListRecord);
        ColorListRecord fromRecord = ColorListRecord.fromValue(record);

        ColorListRecord fromCodegen = new ColorListRecord(Arrays.asList(
                new Green(Unit.getInstance()), new Red(Unit.getInstance())));

        assertEquals(fromRecord, fromCodegen);
        assertEquals(fromCodegen.toValue().toProtoRecord(), protoColorListRecord);

    }

}