// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.proto

import com.digitalasset.transcode.schema.*
import com.digitalasset.transcode.utils.codecspec.CodecCommonSpec
import com.google.protobuf.empty.Empty
import zio.*
import zio.test.*

import Value.Sum

trait ProtobufCodecSpecDefault extends CodecCommonSpec[Value]:
  def textMapSuite: Spec[Any, Nothing]

  override val schemaProcessor = ProtobufCodec

  override def spec =
    suite("ProtobufCodec (Scala)")(
      autoTests,
      suite("encode/decode tests")(
        testCodec(
          "unit",
          Value(Sum.Unit(Empty())),
          DynamicValue.Unit,
          schemaProcessor.unit,
        ),
        testCodec(
          "bool",
          Value(Sum.Bool(true)),
          DynamicValue.Bool(true),
          schemaProcessor.bool,
        ),
        testCodec(
          "text",
          Value(Sum.Text("Some text")),
          DynamicValue.Text("Some text"),
          schemaProcessor.text,
        ),
        testCodec(
          "int64",
          Value(Sum.Int64(1)),
          DynamicValue.Int64(1),
          schemaProcessor.int64,
        ),
        testCodec(
          "numeric",
          Value(Sum.Numeric("10")),
          DynamicValue.Numeric("10"),
          schemaProcessor.numeric(10),
        ),
        testCodec(
          "timestamp",
          Value(Sum.Timestamp(10)),
          DynamicValue.Timestamp(10),
          schemaProcessor.timestamp,
        ),
        testCodec(
          "date",
          Value(Sum.Date(10)),
          DynamicValue.Date(10),
          schemaProcessor.date,
        ),
        testCodec(
          "party",
          Value(Sum.Party("party")),
          DynamicValue.Party("party"),
          schemaProcessor.party,
        ),
        testCodec(
          "contractId",
          Value(Sum.ContractId("contractId")),
          DynamicValue.ContractId("contractId"),
          schemaProcessor.contractId(schemaProcessor.unit),
        ),
        testCodec(
          "record",
          Value(
            Sum.Record(
              Record(
                fields = Seq(
                  RecordField(
                    label = "field1",
                    value = Some(Value(Sum.Text("some text"))),
                  ),
                  RecordField(
                    label = "field2",
                    value = Some(Value(Sum.Int64(1))),
                  ),
                )
              )
            )
          ),
          DynamicValue.Record(
            Seq(
              DynamicValue.Text("some text"),
              DynamicValue.Int64(1),
            )
          ),
          schemaProcessor.constructor(
            Identifier.fromString("aa:aa:aa"),
            Seq(),
            schemaProcessor.record(
              Seq(
                FieldName("field1") -> schemaProcessor.text,
                FieldName("field2") -> schemaProcessor.int64,
              )
            ),
          ),
        ),
        testCodec(
          "variant",
          Value(
            Sum.Variant(
              Variant(constructor = "constructor1", value = Some(Value(Sum.Text("some text"))))
            )
          ),
          DynamicValue.Variant(0, DynamicValue.Text("some text")),
          schemaProcessor.constructor(
            Identifier.fromString("aa:aa:aa"),
            Seq(),
            schemaProcessor.variant(
              Seq(VariantConName("constructor1") -> schemaProcessor.text)
            ),
          ),
        ),
        testCodec(
          "enum",
          Value(
            Sum.Enum(
              Enum(constructor = "some text")
            )
          ),
          DynamicValue.Enumeration(0),
          schemaProcessor.constructor(
            Identifier.fromString("aa:aa:aa"),
            Seq(),
            schemaProcessor.enumeration(
              Seq(EnumConName("some text"))
            ),
          ),
        ),
        suite("traversables")(
          suite("list")(
            testCodec(
              "empty",
              Value(Sum.List(List())),
              DynamicValue.List(Seq.empty),
              schemaProcessor.list(schemaProcessor.unit),
            ),
            testCodec(
              "non-empty",
              Value(
                Sum.List(
                  List(Seq(Value(Sum.Text("some text"))))
                )
              ),
              DynamicValue.List(Seq(DynamicValue.Text("some text"))),
              schemaProcessor.list(schemaProcessor.text),
            ),
          ),
          suite("optional")(
            testCodec(
              "none",
              Value(Sum.Optional(Optional())),
              DynamicValue.Optional(None),
              schemaProcessor.optional(schemaProcessor.unit),
            ),
            testCodec(
              "some",
              Value(
                Sum.Optional(
                  Optional(value = Some(Value(Sum.Text("some text"))))
                )
              ),
              DynamicValue.Optional(Some(DynamicValue.Text("some text"))),
              schemaProcessor.optional(schemaProcessor.text),
            ),
          ),
          textMapSuite,
          suite("genMap")(
            testCodec(
              "empty",
              Value(Sum.GenMap(GenMap())),
              DynamicValue.GenMap(Seq.empty),
              schemaProcessor.genMap(schemaProcessor.unit, schemaProcessor.unit),
            ),
            testCodec(
              "non-empty",
              Value(
                Sum.GenMap(
                  GenMap(
                    Seq(
                      GenMap.Entry(Some(Value(Sum.Text("entry1"))), Some(Value(Sum.Int64(1)))),
                      GenMap.Entry(Some(Value(Sum.Text("entry2"))), Some(Value(Sum.Int64(2)))),
                    )
                  )
                )
              ),
              DynamicValue.GenMap(
                Seq(
                  DynamicValue.Text("entry1") -> DynamicValue.Int64(1),
                  DynamicValue.Text("entry2") -> DynamicValue.Int64(2),
                )
              ),
              schemaProcessor.genMap(schemaProcessor.text, schemaProcessor.int64),
            ),
          ),
        ),
      ),
    )
