// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.{Descriptor, DynamicValue as DV}

trait Recursive extends TestCase:
  private val NestingLevel = 18 // Deepest nesting level that doesn't cause error

  lazy val Recurs: Constructor = ctor(record("self" -> optional(Recurs)))

  addCase(
    Recurs,
    DV.Record(DV.Optional(None)),
    (1 to NestingLevel).foldLeft(DV.Record(DV.Optional(None)))((a, b) =>
      DV.Record(DV.Optional(Some(a)))
    ),
  )

  lazy val ConsList: Constructor =
    val Cons = ctor(a => record("head" -> a, "tail" -> application(ConsList, a)))
    ctor(a => variant("Nil" -> unit, "Cons" -> application(Cons, a)))

  addCase(
    application(ConsList, int64),
    DV.Variant(0, DV.Unit),
    (1 to NestingLevel).foldLeft(DV.Variant(0, DV.Unit))((a, b) =>
      DV.Variant(1, DV.Record(DV.Int64(b), a))
    ),
  )
