// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.{Descriptor, DynamicValue}

trait Records extends TestCase:
  private val Record = ctor(record("int" -> int64, "text" -> text, "upgraded" -> optional(unit)))
  addCase(
    Record,
    DynamicValue.Record( // simulates upgraded codec/codegen with old version in ledger
      DynamicValue.Int64(0),
      DynamicValue.Text("Text"),
    ),
    DynamicValue.Record( // simulates current version
      DynamicValue.Int64(0),
      DynamicValue.Text("Text"),
      DynamicValue.Optional(Some(DynamicValue.Unit)),
    ),
    DynamicValue
      .Record( // simulates downgraded codec/codegen with newer not yet initialized version in ledger
        DynamicValue.Int64(0),
        DynamicValue.Text("Text"),
        DynamicValue.Optional(Some(DynamicValue.Unit)),
        DynamicValue.Optional(None), // new uninitialized field
      ),
  )
  addFailingCase(
    Record,
    "Unexpected field" -> DynamicValue
      .Record( // simulates downgraded codec/codegen with newer initialized field
        DynamicValue.Int64(0),
        DynamicValue.Text("Text"),
        DynamicValue.Optional(Some(DynamicValue.Unit)),
        DynamicValue.Optional(Some(DynamicValue.Int64(1))), // new initialized field
      ),
  )

  private val RecordWithParam = ctor((a, b) => record("aField" -> a, "bField" -> b))
  addCase(
    application(RecordWithParam, Seq(int64, text)),
    DynamicValue.Record(DynamicValue.Int64(0), DynamicValue.Text("Text")),
  )

  private val NestedRecordWithParam = ctor((a, b, c) =>
    record(
      "nested" -> Descriptor.application(RecordWithParam, Seq(a, b)),
      "cField" -> c,
    )
  )
  addCase(
    application(NestedRecordWithParam, Seq(int64, text, bool)),
    DynamicValue.Record(
      DynamicValue.Record(DynamicValue.Int64(0), DynamicValue.Text("Text")),
      DynamicValue.Bool(true),
    ),
  )
