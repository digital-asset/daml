// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.lf1_1;

import com.daml.ledger.javaapi.data.*;
import com.digitalasset.ledger.api.v1.ValueOuterClass;
import com.google.protobuf.Empty;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import lf1_1.tests.optionaltest.*;
import lf1_1.tests.optionaltest.optionalvariant.OptionalParametricVariant;
import lf1_1.tests.optionaltest.optionalvariant.OptionalPrimVariant;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;


@RunWith(JUnitPlatform.class)
public class OptionalTest {

    @Test
    void constructRecordWithOptionalFields() {
        Record record = new Record(
                new Record.Field("intOpt", DamlOptional.of(new Int64(42))),
                new Record.Field("unitOpt", DamlOptional.of(Unit.getInstance()))
        );
        MyOptionalRecord fromValue = MyOptionalRecord.fromValue(record);

        MyOptionalRecord fromUnboxed = new MyOptionalRecord(
                Optional.of(42L),
                Optional.of(Unit.getInstance())
        );

        assertEquals(fromValue, fromUnboxed);
    }

    @Test
    void optionalFieldRoundTrip() {
        Record record = new Record(
                new Record.Field("intOpt", DamlOptional.of(new Int64(42))),
                new Record.Field("unitOpt", DamlOptional.of(Unit.getInstance()))
        );

        MyOptionalRecord fromValue = MyOptionalRecord.fromValue(record);

        assertEquals(record.toProto(), fromValue.toValue().toProto());
    }

    @Test
    void optionalFieldRoundTripFromProtobuf() {
        ValueOuterClass.Record protoRecord = ValueOuterClass.Record.newBuilder()
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("intOpt").setValue(ValueOuterClass.Value.newBuilder()
                                .setOptional(ValueOuterClass.Optional.newBuilder()
                                        .setValue(ValueOuterClass.Value.newBuilder()
                                                .setInt64(42)
                                        )
                                )
                        )
                )
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("unitOpt").setValue(ValueOuterClass.Value.newBuilder()
                                .setOptional(ValueOuterClass.Optional.newBuilder()
                                        .setValue(ValueOuterClass.Value.newBuilder().setUnit(Empty.getDefaultInstance()))
                                )
                        )
                ).build();

        Record dataRecord = Record.fromProto(protoRecord);

        assertEquals(dataRecord.toProtoRecord(), protoRecord);
    }

    @Test
    void constructNestedOptional() {
        Record record = new Record(
                new Record.Field(DamlOptional.of(DamlOptional.of(new Int64(42L))))
        );
        NestedOptionalRecord fromValue = NestedOptionalRecord.fromValue(record);

        NestedOptionalRecord fromConstructor = new NestedOptionalRecord(Optional.of(Optional.of(42L)));

        assertEquals(fromValue, fromConstructor);
    }

    @Test
    void optionalListRoundTripFromProtobuf() {
        ValueOuterClass.Record protoRecord = ValueOuterClass.Record.newBuilder()
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("list")
                        .setValue(ValueOuterClass.Value.newBuilder()
                                .setOptional(ValueOuterClass.Optional.newBuilder().setValue(
                                        ValueOuterClass.Value.newBuilder()
                                                .setList(ValueOuterClass.List.newBuilder().addAllElements(Arrays.asList(ValueOuterClass.Value.newBuilder().setInt64(42).build()))))
                                )
                        )
                ).build();

        Record dataRecord = Record.fromProto(protoRecord);
        MyOptionalListRecord fromCodegen = new MyOptionalListRecord(Optional.of(Arrays.asList(42L)));

        assertEquals(protoRecord, fromCodegen.toValue().toProtoRecord());
        assertEquals(dataRecord.toProtoRecord(), protoRecord);

    }

    @Test
    void listOfOptionals() {
        ValueOuterClass.Record protoRecord = ValueOuterClass.Record.newBuilder()
                .addFields(ValueOuterClass.RecordField.newBuilder()
                        .setLabel("list")
                        .setValue(ValueOuterClass.Value.newBuilder()
                                .setList(ValueOuterClass.List.newBuilder().addAllElements(Arrays.asList(
                                        ValueOuterClass.Value.newBuilder()
                                                .setOptional(ValueOuterClass.Optional.newBuilder()
                                                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(42))
                                                )
                                                .build()
                                ))))
                        )
                .build();

        Record dataRecord = Record.fromProto(protoRecord);
        MyListOfOptionalsRecord fromCodegen = new MyListOfOptionalsRecord(Arrays.asList(Optional.of(42L)));

        assertEquals(fromCodegen.toValue().toProtoRecord(), protoRecord);
        assertEquals(dataRecord.toProtoRecord(), protoRecord);

    }

    @Test
    void parametricOptionalVariant() {
        Variant variant = new Variant("OptionalParametricVariant", DamlOptional.of(new Int64(42)));

        OptionalParametricVariant<Long> fromValue = OptionalParametricVariant.<Long>fromValue(variant, f -> f.asInt64().get().getValue());
        OptionalParametricVariant<Long> fromConstructor = new OptionalParametricVariant<Long>(Optional.of(42L));


        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(Int64::new), variant);
    }

    @Test
    void primOptionalVariant() {
        Variant variant = new Variant("OptionalPrimVariant", DamlOptional.of(new Int64(42)));

        OptionalPrimVariant<?> fromValue = OptionalPrimVariant.fromValue(variant);
        OptionalPrimVariant<?> fromConstructor = new OptionalPrimVariant(Optional.of(42L));


        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), variant);
    }
}
