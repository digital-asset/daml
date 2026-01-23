// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.DynamicValue as DV

trait Traversables extends TestCase:
  private val Record = ctor(record("int" -> int64, "text" -> text))

  addCase(
    list(int64),
    DV.List(Seq.empty),
    DV.List(Seq(DV.Int64(0))),
    DV.List(Seq(DV.Int64(1), DV.Int64(2), DV.Int64(3))),
  )

  private val RecordWithOptional = ctor(record("field" -> optional(int64)))
  addCase(
    RecordWithOptional,
    DV.Record(),
    DV.Record(DV.Optional(None)),
    DV.Record(DV.Optional(Some(DV.Int64(0)))),
  )

  addCase(
    list(optional(int64)),
    DV.List(Seq.empty),
    DV.List(Seq(DV.Optional(None))),
    DV.List(Seq(DV.Optional(None), DV.Optional(None))),
    DV.List(Seq(DV.Optional(Some(DV.Int64(0))))),
    DV.List(Seq(DV.Optional(Some(DV.Int64(0))), DV.Optional(Some(DV.Int64(1))))),
  )

  addCase(
    optional(optional(int64)),
    DV.Optional(None),
    DV.Optional(Some(DV.Optional(None))),
    DV.Optional(Some(DV.Optional(Some(DV.Int64(1))))),
  )

  addCase(
    list(optional(optional(int64))),
    DV.List(Seq.empty),
    DV.List(Seq(DV.Optional(None))),
    DV.List(Seq(DV.Optional(Some(DV.Optional(None))), DV.Optional(None))),
    DV.List(Seq(DV.Optional(Some(DV.Optional(Some(DV.Int64(1))))), DV.Optional(None))),
  )

  addCase(
    list(optional(list(int64))),
    DV.List(Seq.empty),
    DV.List(Seq(DV.Optional(None))),
    DV.List(Seq(DV.Optional(Some(DV.List(Seq.empty))))),
    DV.List(
      Seq(DV.Optional(None), DV.Optional(Some(DV.List(Seq(DV.Int64(0))))), DV.Optional(None))
    ),
    DV.List(Seq(DV.Optional(Some(DV.List(Seq(DV.Int64(0))))))),
  )

  addCase(
    list(Record),
    DV.List(Seq.empty),
    DV.List(Seq(DV.Record(DV.Int64(0), DV.Text("Text")))),
  )

  addCase(
    optional(int64),
    DV.Optional(None),
    DV.Optional(Some(DV.Int64(0))),
  )

  addCase(
    optional(list(int64)),
    DV.Optional(None),
    DV.Optional(Some(DV.List(Seq.empty))),
    DV.Optional(Some(DV.List(Seq(DV.Int64(0))))),
    DV.Optional(Some(DV.List(Seq(DV.Int64(1), DV.Int64(2), DV.Int64(3))))),
  )

  addCase(
    optional(Record),
    DV.Optional(None),
    DV.Optional(Some(DV.Record(DV.Int64(0), DV.Text("Text")))),
  )

  addCase(
    textMap(int64),
    DV.TextMap(Seq.empty),
    DV.TextMap(
      Seq(
        "one" -> DV.Int64(1),
        "two" -> DV.Int64(2),
      )
    ),
  )

  addCase(
    textMap(Record),
    DV.TextMap(Seq.empty),
    DV.TextMap(
      Seq(
        "one" -> DV.Record(DV.Int64(1), DV.Text("One")),
        "two" -> DV.Record(DV.Int64(2), DV.Text("Two")),
      )
    ),
  )

  addCase(
    genMap(Record, Record),
    DV.GenMap(Seq.empty),
    DV.GenMap(
      Seq(
        DV.Record(DV.Int64(0), DV.Text("Text")) -> DV.Record(DV.Int64(1), DV.Text("txeT"))
      )
    ),
  )
