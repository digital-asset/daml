// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.json

import com.digitalasset.transcode.schema.*
import com.digitalasset.transcode.utils.codecspec.CodecCommonSpec
import ujson.Value
import zio.*
import zio.test.*

object JsonCodecSpec extends CodecCommonSpec[Value]:
  override val schemaProcessor = JsonCodec()

  override def spec =
    suite("JsonCodecSpec")(
      autoTests,
      suite("encode/decode tests")(
        testCodec(
          "unit",
          ujson.Obj(),
          DynamicValue.Unit,
          schemaProcessor.unit,
        ),
        testCodec(
          "bool",
          ujson.Bool(true),
          DynamicValue.Bool(true),
          schemaProcessor.bool,
        ),
        testCodec(
          "text",
          ujson.Str("Some text"),
          DynamicValue.Text("Some text"),
          schemaProcessor.text,
        ),
        testCodec(
          "int64",
          ujson.Str("1"),
          DynamicValue.Int64(1),
          schemaProcessor.int64,
        ),
        testCodec(
          "numeric",
          ujson.Str("10"),
          DynamicValue.Numeric("10"),
          schemaProcessor.numeric(10),
        ),
        testCodec(
          "timestamp",
          ujson.Str("1970-01-01T00:00:00.00001Z"),
          DynamicValue.Timestamp(10),
          schemaProcessor.timestamp,
        ),
        testCodec(
          "date",
          ujson.Str("1970-01-11"),
          DynamicValue.Date(10),
          schemaProcessor.date,
        ),
        testCodec(
          "party",
          ujson.Str("party"),
          DynamicValue.Party("party"),
          schemaProcessor.party,
        ),
        testCodec(
          "contractId",
          ujson.Str("contractId"),
          DynamicValue.ContractId("contractId"),
          schemaProcessor.contractId(schemaProcessor.unit),
        ),
        testCodec(
          "interface",
          ujson.Obj(),
          DynamicValue.Unit,
          schemaProcessor
            .constructor(Identifier.fromString("aa:aa:aa"), Seq(), schemaProcessor.unit),
        ),
        testCodec(
          "record",
          ujson.Obj.from(
            Seq(
              "field1" -> ujson.Str("some text"),
              "field2" -> ujson.Str("1"),
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
          "enum",
          ujson.Str("caseEnum2"),
          DynamicValue.Enumeration(1),
          schemaProcessor.constructor(
            Identifier.fromString("aa:aa:aa"),
            Seq(),
            schemaProcessor.enumeration(
              Seq(
                EnumConName("caseEnum1"),
                EnumConName("caseEnum2"),
              )
            ),
          ),
        ),
        testCodec(
          "variant",
          ujson.Obj("tag" -> "case1", "value" -> ujson.Str("value1")),
          DynamicValue.Variant(0, DynamicValue.Text("value1")),
          schemaProcessor.constructor(
            Identifier.fromString("aa:aa:aa"),
            Seq(),
            schemaProcessor.variant(
              Seq(
                VariantConName("case1") -> schemaProcessor.text,
                VariantConName("case2") -> schemaProcessor.int64,
              )
            ),
          ),
        ),
        testCodec(
          "optional-some",
          ujson.Str("value1"),
          DynamicValue.Optional(Some(DynamicValue.Text("value1"))),
          schemaProcessor.optional(schemaProcessor.text),
        ),
        testCodec(
          "optional-none",
          ujson.Null,
          DynamicValue.Optional(None),
          schemaProcessor.optional(schemaProcessor.text),
        ),
        testCodec(
          "list-nonempty",
          ujson.Arr("value1", "value2"),
          DynamicValue.List(Seq(DynamicValue.Text("value1"), DynamicValue.Text("value2"))),
          schemaProcessor.list(schemaProcessor.text),
        ),
        testCodec(
          "list-empty",
          ujson.Arr(),
          DynamicValue.List(Seq.empty),
          schemaProcessor.list(schemaProcessor.text),
        ),
        testCodec(
          "textMap",
          ujson.Obj.from(Seq("key1" -> "value1", "key2" -> "value2")),
          DynamicValue.TextMap(
            Seq("key1" -> DynamicValue.Text("value1"), "key2" -> DynamicValue.Text("value2"))
          ),
          schemaProcessor.textMap(schemaProcessor.text),
        ),
        testCodec(
          "genMap",
          ujson.Arr(
            ujson.Arr(ujson.Str("1"), ujson.Str("value1")),
            ujson.Arr(ujson.Str("2"), ujson.Str("value2")),
          ),
          DynamicValue.GenMap(
            Seq(
              DynamicValue.Int64(1) -> DynamicValue.Text("value1"),
              DynamicValue.Int64(2) -> DynamicValue.Text("value2"),
            )
          ),
          schemaProcessor.genMap(schemaProcessor.int64, schemaProcessor.text),
        ),
      ),
    )
