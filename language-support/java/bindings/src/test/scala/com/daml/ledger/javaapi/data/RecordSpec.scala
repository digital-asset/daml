// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import java.util.Collections

import com.daml.ledger.api.v1.ValueOuterClass
import org.scalatest.{FlatSpec, Matchers}

import collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class RecordSpec extends FlatSpec with Matchers {

  behavior of "Record.fromProto"

  it should "build a record from a grpc.Record without fields" in {
    val fields = Collections.emptyList()
    val recordValue = ValueOuterClass.Record.newBuilder().addAllFields(fields).build()
    val record = Record.fromProto(recordValue)
    record.getFields shouldBe empty
    record.getFieldsMap shouldBe empty
  }

  it should "build a record with an empty field map if there are no labels" in {
    val fields = List(
      ValueOuterClass.RecordField
        .newBuilder()
        .setValue(ValueOuterClass.Value.newBuilder().setInt64(1l))
        .build(),
      ValueOuterClass.RecordField
        .newBuilder()
        .setValue(ValueOuterClass.Value.newBuilder().setInt64(2l))
        .build()
    ).asJava
    val recordValue = ValueOuterClass.Record.newBuilder().addAllFields(fields).build()
    val record = Record.fromProto(recordValue)
    record.getFields should contain theSameElementsInOrderAs List(
      new Record.Field(new Int64(1l)),
      new Record.Field(new Int64(2l)))
    record.getFieldsMap shouldBe empty
  }

  it should "build a record with a full field map if there are labels" in {
    val fields = List(
      ValueOuterClass.RecordField
        .newBuilder()
        .setLabel("label1")
        .setValue(ValueOuterClass.Value.newBuilder().setInt64(1l))
        .build(),
      ValueOuterClass.RecordField
        .newBuilder()
        .setLabel("label2")
        .setValue(ValueOuterClass.Value.newBuilder().setInt64(2l))
        .build()
    ).asJava
    val recordValue = ValueOuterClass.Record.newBuilder().addAllFields(fields).build()
    val record = Record.fromProto(recordValue)
    record.getFields should contain theSameElementsInOrderAs List(
      new Record.Field("label1", new Int64(1l)),
      new Record.Field("label2", new Int64(2l)))
    record.getFieldsMap.asScala should contain theSameElementsAs Map(
      "label1" -> new Int64(1l),
      "label2" -> new Int64(2l))
  }
}
