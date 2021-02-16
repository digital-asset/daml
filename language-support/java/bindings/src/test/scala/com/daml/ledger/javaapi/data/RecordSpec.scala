// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import java.util.Collections

import com.daml.ledger.api.v1.ValueOuterClass
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class RecordSpec extends AnyFlatSpec with Matchers {

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
        .setValue(ValueOuterClass.Value.newBuilder().setInt64(1L))
        .build(),
      ValueOuterClass.RecordField
        .newBuilder()
        .setValue(ValueOuterClass.Value.newBuilder().setInt64(2L))
        .build(),
    ).asJava
    val recordValue = ValueOuterClass.Record.newBuilder().addAllFields(fields).build()
    val record = Record.fromProto(recordValue)
    record.getFields should contain theSameElementsInOrderAs List(
      new Record.Field(new Int64(1L)),
      new Record.Field(new Int64(2L)),
    )
    record.getFieldsMap shouldBe empty
  }

  it should "build a record with a full field map if there are labels" in {
    val fields = List(
      ValueOuterClass.RecordField
        .newBuilder()
        .setLabel("label1")
        .setValue(ValueOuterClass.Value.newBuilder().setInt64(1L))
        .build(),
      ValueOuterClass.RecordField
        .newBuilder()
        .setLabel("label2")
        .setValue(ValueOuterClass.Value.newBuilder().setInt64(2L))
        .build(),
    ).asJava
    val recordValue = ValueOuterClass.Record.newBuilder().addAllFields(fields).build()
    val record = Record.fromProto(recordValue)
    record.getFields should contain theSameElementsInOrderAs List(
      new Record.Field("label1", new Int64(1L)),
      new Record.Field("label2", new Int64(2L)),
    )
    record.getFieldsMap.asScala should contain theSameElementsAs Map(
      "label1" -> new Int64(1L),
      "label2" -> new Int64(2L),
    )
  }
}
