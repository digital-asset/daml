// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.DynamicValue as DV

trait Variants extends TestCase:
  private val Record = ctor(
    record(
      "int" -> int64,
      "text" -> text,
    )
  )
  private val Foo =
    val Bar = ctor(record("int" -> int64, "text" -> text))
    ctor(
      variant(
        "Foo" -> Record,
        "Bar" -> Bar,
        "Baz" -> int64,
        "Qux" -> unit,
      )
    )
  addCase(
    Foo,
    DV.Variant(0, DV.Record(DV.Int64(0), DV.Text("Text"))),
    DV.Variant(1, DV.Record(DV.Int64(0), DV.Text("Text"))),
    DV.Variant(2, DV.Int64(0)),
    DV.Variant(3, DV.Unit),
  )
  addFailingCase(
    Foo,
    "Unexpected case" -> DV.Variant(4, DV.Unit),
  )

  private val VariantWithParam =
    val TupleCase = ctor((a, b) => record("aField" -> a, "bField" -> b))
    ctor((a, b) =>
      variant(
        "NoCase" -> unit,
        "ACase" -> a,
        "BCase" -> b,
        "TupleCase" -> application(TupleCase, a, b),
      )
    )
  addCase(
    application(VariantWithParam, Seq(int64, text)),
    DV.Variant(0, DV.Unit),
    DV.Variant(1, DV.Int64(0)),
    DV.Variant(2, DV.Text("Text")),
    DV.Variant(3, DV.Record(DV.Int64(0), DV.Text("Text"))),
  )
