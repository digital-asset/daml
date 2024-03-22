// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.daml.ledger.api.v1.ValueOuterClass;
import com.daml.ledger.javaapi.data.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import tests.numerictest.NumericBox;
import tests.numerictest.nestednumericbox.Nested;
import tests.numerictest.nestednumericbox.NoMore;

@RunWith(JUnitPlatform.class)
public class NumericTest {

  void checkRecord(NumericBox myRecord) {
    assertEquals(myRecord.decimal, decimalValue());
    assertEquals(myRecord.numeric0, numeric0Value());
    assertEquals(myRecord.numeric37, numeric37Value());
    NumericBox myNestedRecord = ((Nested) myRecord.nestedBox).numericBoxValue;
    assertEquals(myNestedRecord.decimal, decimalValue().negate());
    assertEquals(myNestedRecord.numeric0, numeric0Value().negate());
    assertEquals(myNestedRecord.numeric37, numeric37Value().negate());
    assertTrue(myNestedRecord.nestedBox instanceof NoMore);
  }

  @Test
  void deserializableFromRecord() {
    ArrayList<DamlRecord.Field> fieldsList_ = new ArrayList<>(10);
    fieldsList_.add(new DamlRecord.Field("decimal", new Numeric(decimalValue().negate())));
    fieldsList_.add(new DamlRecord.Field("numeric0", new Numeric(numeric0Value().negate())));
    fieldsList_.add(new DamlRecord.Field("numeric37", new Numeric(numeric37Value().negate())));
    fieldsList_.add(new DamlRecord.Field("nested", new Variant("NoMore", Unit.getInstance())));
    DamlRecord nestedRecord = new DamlRecord(fieldsList_);
    ArrayList<DamlRecord.Field> fieldsList = new ArrayList<>(10);
    fieldsList.add(new DamlRecord.Field("decimal", new Numeric(decimalValue())));
    fieldsList.add(new DamlRecord.Field("numeric0", new Numeric(numeric0Value())));
    fieldsList.add(new DamlRecord.Field("numeric37", new Numeric(numeric37Value())));
    fieldsList.add(new DamlRecord.Field("nestedBox", new Variant("Nested", nestedRecord)));
    Value value = new DamlRecord(fieldsList);
    NumericBox record = NumericBox.fromValue(value);
    checkRecord(record);
    assertTrue(
        "to value uses original Daml-LF names for fields",
        record.toValue().getFieldsMap().get("numeric37").asNumeric().isPresent());
  }

  @Test
  void objectMethodsWork() {

    NumericBox record1 = numericBox();
    NumericBox record2 = numericBox();

    assertEquals(record1, record2);
    assertEquals(record1.hashCode(), record2.hashCode());
  }

  ValueOuterClass.Record.Builder recordBuilder() {
    return ValueOuterClass.Record.newBuilder();
  }

  ValueOuterClass.RecordField.Builder recordFieldBuilder() {
    return ValueOuterClass.RecordField.newBuilder();
  }

  ValueOuterClass.Value.Builder valueBuilder() {
    return ValueOuterClass.Value.newBuilder();
  }

  ValueOuterClass.Variant.Builder variantBuilder() {
    return ValueOuterClass.Variant.newBuilder();
  }

  com.google.protobuf.Empty.Builder emptyBuilder() {
    return com.google.protobuf.Empty.newBuilder();
  }

  @Test
  void outerRecordRoundtrip() {
    ValueOuterClass.Value protoValue =
        valueBuilder()
            .setRecord(
                recordBuilder()
                    .addFields(
                        recordFieldBuilder()
                            .setLabel("decimal")
                            .setValue(valueBuilder().setNumeric("10.0000000000")))
                    .addFields(
                        recordFieldBuilder()
                            .setLabel("numeric0")
                            .setValue(
                                valueBuilder()
                                    .setNumeric("99999999999999999999999999999999999999")))
                    .addFields(
                        recordFieldBuilder()
                            .setLabel("numeric37")
                            .setValue(
                                valueBuilder()
                                    .setNumeric("9.9999999999999999999999999999999999999")))
                    .addFields(
                        recordFieldBuilder()
                            .setLabel("nestedBox")
                            .setValue(
                                valueBuilder()
                                    .setVariant(
                                        variantBuilder()
                                            .setConstructor("Nested")
                                            .setValue(
                                                valueBuilder()
                                                    .setRecord(
                                                        recordBuilder()
                                                            .addFields(
                                                                recordFieldBuilder()
                                                                    .setLabel("decimal")
                                                                    .setValue(
                                                                        valueBuilder()
                                                                            .setNumeric(
                                                                                "-10.0000000000")))
                                                            .addFields(
                                                                recordFieldBuilder()
                                                                    .setLabel("numeric0")
                                                                    .setValue(
                                                                        valueBuilder()
                                                                            .setNumeric(
                                                                                "-99999999999999999999999999999999999999")))
                                                            .addFields(
                                                                recordFieldBuilder()
                                                                    .setLabel("numeric37")
                                                                    .setValue(
                                                                        valueBuilder()
                                                                            .setNumeric(
                                                                                "-9.9999999999999999999999999999999999999")))
                                                            .addFields(
                                                                recordFieldBuilder()
                                                                    .setLabel("nestedBox")
                                                                    .setValue(
                                                                        valueBuilder()
                                                                            .setVariant(
                                                                                variantBuilder()
                                                                                    .setConstructor(
                                                                                        "NoMore")
                                                                                    .setValue(
                                                                                        valueBuilder()
                                                                                            .setUnit(
                                                                                                emptyBuilder())))))))))))
            .build();

    Value value = Value.fromProto(protoValue);
    NumericBox fromValue = NumericBox.fromValue(value);
    NumericBox fromConstructor = numericBox();
    NumericBox fromRoundTrip =
        NumericBox.fromValue(Value.fromProto(fromConstructor.toValue().toProto()));

    assertEquals(fromValue, fromConstructor);
    assertEquals(fromConstructor.toValue(), value);
    assertEquals(fromConstructor.toValue().toProto(), protoValue);
    assertEquals(fromRoundTrip, fromConstructor);
  }

  BigDecimal maxValue(int s) {
    return new BigDecimal(BigInteger.TEN.pow(38).subtract(BigInteger.ONE), s);
  }

  java.math.BigDecimal decimalValue() {
    return BigDecimal.TEN.setScale(10);
  }

  java.math.BigDecimal numeric0Value() {
    return maxValue(0);
  }

  java.math.BigDecimal numeric37Value() {
    return maxValue(37);
  }

  NumericBox numericBox() {
    return new NumericBox(
        decimalValue(),
        numeric0Value(),
        numeric37Value(),
        new Nested(
            new NumericBox(
                decimalValue().negate(),
                numeric0Value().negate(),
                numeric37Value().negate(),
                new NoMore(Unit.getInstance()))));
  }
}
