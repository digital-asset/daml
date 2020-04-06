// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import com.daml.ledger.javaapi.data.Int64;
import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.Variant;
import com.daml.ledger.api.v1.ValueOuterClass;
import com.google.protobuf.Empty;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.varianttest.Custom;
import tests.varianttest.customparametric.CustomParametricCons;
import tests.varianttest.variantitem.*;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnitPlatform.class)
public class VariantTest {

    @Test
    void emptyVariant() {
        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("EmptyVariant")
                .setValue(ValueOuterClass.Value.newBuilder().setUnit(Empty.getDefaultInstance()))
                .build();
        Variant dataVariant = Variant.fromProto(protoVariant);
        EmptyVariant fromValue = EmptyVariant.fromValue(dataVariant);

        EmptyVariant<?> fromConstructor = new EmptyVariant<>(Unit.getInstance());

        EmptyVariant fromRoundTrip = EmptyVariant.fromValue(fromValue.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataVariant);
        assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
        assertEquals(fromRoundTrip, fromConstructor);
    }

    @Test
    void primVariant() {
        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("PrimVariant")
                .setValue(ValueOuterClass.Value.newBuilder().setInt64(42))
                .build();
        Variant dataVariant = Variant.fromProto(protoVariant);
        PrimVariant fromValue = PrimVariant.fromValue(dataVariant);

        PrimVariant<?> fromConstructor = new PrimVariant<>(42L);

        PrimVariant<?> fromRoundTrip = PrimVariant.fromValue(fromValue.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataVariant);
        assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
        assertEquals(fromRoundTrip, fromConstructor);
    }

    @Test
    void recordVariant() {
        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("RecordVariant")
                .setValue(ValueOuterClass.Value.newBuilder().setRecord(ValueOuterClass.Record.newBuilder()
                        .addFields(ValueOuterClass.RecordField.newBuilder()
                                .setLabel("x").setValue(ValueOuterClass.Value.newBuilder().setInt64(42))))).build();

        Variant dataVariant = Variant.fromProto(protoVariant);
        RecordVariant<?> fromValue = RecordVariant.fromValue(dataVariant);


        RecordVariant<?> fromConstructor = new RecordVariant<Long>(42L);

        RecordVariant<?> fromRoundTrip = RecordVariant.fromValue(fromValue.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataVariant);
        assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
        assertEquals(fromRoundTrip, fromConstructor);
    }

    @Test
    void customVariant() {
        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("CustomVariant")
                .setValue(ValueOuterClass.Value.newBuilder().setRecord(
                        ValueOuterClass.Record.getDefaultInstance()
                ))
                .build();

        Variant dataVariant = Variant.fromProto(protoVariant);
        CustomVariant<?> fromValue = CustomVariant.fromValue(dataVariant);

        CustomVariant<?> fromConstructor = new CustomVariant<>(new Custom());

        CustomVariant<?> fromRoundTrip = CustomVariant.fromValue(fromConstructor.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataVariant);
        assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
        assertEquals(fromRoundTrip, fromConstructor);
    }


    @Test
    void customParametricVariant() {
        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("CustomParametricVariant")
                .setValue(ValueOuterClass.Value.newBuilder().setVariant(ValueOuterClass.Variant.newBuilder()
                        .setConstructor("CustomParametricCons")
                        .setValue(ValueOuterClass.Value.newBuilder().setInt64(42))

                ))
                .build();
        Variant dataVariant = Variant.fromProto(protoVariant);
        CustomParametricVariant<Long> fromValue = CustomParametricVariant.<Long>fromValue(dataVariant, f -> f.asInt64().get().getValue());

        CustomParametricVariant<Long> fromConstructor = new CustomParametricVariant<>(new CustomParametricCons<>(42L));

        CustomParametricVariant<Long> fromRoundTrip = CustomParametricVariant.<Long>fromValue(fromConstructor.toValue(Int64::new), f -> f.asInt64().get().getValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(Int64::new), dataVariant);
        assertEquals(fromConstructor.toValue(Int64::new).toProtoVariant(), dataVariant.toProtoVariant());
        assertEquals(fromRoundTrip, fromConstructor);
    }

    @Test
    void recordVariantRecord() {
        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
                .setConstructor("RecordVariantRecord")
                .setValue(ValueOuterClass.Value.newBuilder().setRecord(ValueOuterClass.Record.newBuilder()
                        .addFields(ValueOuterClass.RecordField.newBuilder()
                                .setLabel("y").setValue(ValueOuterClass.Value.newBuilder().setVariant(ValueOuterClass.Variant.newBuilder()
                                        .setConstructor("EmptyVariant")
                                        .setValue(ValueOuterClass.Value.newBuilder().setUnit(Empty.getDefaultInstance())))))
                )).build();

        Variant dataVariant = Variant.fromProto(protoVariant);
        RecordVariantRecord<?> fromValue = RecordVariantRecord.fromValue(dataVariant);


        RecordVariantRecord<?> fromConstructor = new RecordVariantRecord<>(new EmptyVariant<>(Unit.getInstance()));

        RecordVariantRecord<?> fromRoundTrip = RecordVariantRecord.fromValue(fromValue.toValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(), dataVariant);
        assertEquals(fromConstructor.toValue().toProtoVariant(), protoVariant);
        assertEquals(fromRoundTrip, fromConstructor);
    }

    @Test
    void parameterizedRecordVariant() {
        ValueOuterClass.Variant protoVariant = ValueOuterClass.Variant.newBuilder()
            .setConstructor("ParameterizedRecordVariant")
                .setValue(ValueOuterClass.Value.newBuilder().setRecord(ValueOuterClass.Record.newBuilder()
                        .addFields(ValueOuterClass.RecordField.newBuilder().setLabel("x1").setValue(ValueOuterClass.Value.newBuilder().setInt64(42L)))
                        .addFields(ValueOuterClass.RecordField.newBuilder().setLabel("x2").setValue(ValueOuterClass.Value.newBuilder().setInt64(69L)))
                        .addFields(ValueOuterClass.RecordField.newBuilder().setLabel("x3").setValue(ValueOuterClass.Value.newBuilder().setList(ValueOuterClass.List
                                .newBuilder().addElements(ValueOuterClass.Value.newBuilder().setInt64(65536L)))))
                ))
                .build();

        Variant dataVariant = Variant.fromProto(protoVariant);
        ParameterizedRecordVariant<Long> fromValue = ParameterizedRecordVariant.fromValue(dataVariant, f -> f.asInt64().get().getValue());
        ParameterizedRecordVariant<Long> fromConstructor = new ParameterizedRecordVariant<>(42L, 69L, Collections.singletonList(65536L));
        ParameterizedRecordVariant<Long> fromRoundTrip = ParameterizedRecordVariant.<Long>fromValue(fromConstructor.toValue(Int64::new), f -> f.asInt64().get().getValue());

        assertEquals(fromValue, fromConstructor);
        assertEquals(fromConstructor.toValue(Int64::new), dataVariant);
        assertEquals(fromConstructor.toValue(Int64::new).toProtoVariant(), dataVariant.toProtoVariant());
        assertEquals(fromRoundTrip, fromConstructor);

    }
}
