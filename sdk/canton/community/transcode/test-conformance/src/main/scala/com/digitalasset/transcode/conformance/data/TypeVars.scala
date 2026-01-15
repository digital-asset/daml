// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance.data

import com.digitalasset.transcode.conformance.*
import com.digitalasset.transcode.schema.Descriptor.*
import com.digitalasset.transcode.schema.{Descriptor, DynamicValue as DV}

trait TypeVars extends TestCase:
  private val Things = ctor(a =>
    record(
      "things" -> list(a),
      "contractIds" -> list(contractId(a)),
    )
  )

  private lazy val Thing: Constructor = ctor((a, b, c) =>
    variant(
      "SimpleThing" -> a,
      "ListThing" -> list(b),
      "ListOfThings" -> list(application(Thing, a, b, c)),
    )
  )
  private val simpleThing = DV.Variant(0, DV.List(Seq(DV.Text("one"), DV.Text("two"))))
  private val listThing = DV.Variant(1, DV.List(Seq(DV.Int64(1), DV.Int64(2))))
  private val listOfThings = DV.Variant(2, DV.List(Seq(simpleThing, listThing)))

  private lazy val SelfThing: Constructor = ctor(a =>
    record(
      "next" -> optional(application(SelfThing, optional(a)))
    )
  )

  private val DiyOptional: Constructor = ctor(a =>
    variant(
      "DiySome" -> a,
      "DiyNone" -> unit,
    )
  )
  private lazy val SelfThing2: Constructor = ctor(a =>
    record(
      "next" -> application(DiyOptional, application(SelfThing, application(DiyOptional, a)))
    )
  )

  addCase(
    application(Things, unit),
    DV.Record(
      DV.List(Seq(DV.Unit, DV.Unit)),
      DV.List(Seq.empty),
    ),
  )

  addCase(
    application(Things, application(Thing, list(text), int64, date)),
    DV.Record(
      DV.List(Seq(simpleThing, listThing, listOfThings)),
      DV.List(Seq.empty),
    ),
  )

  addCase(
    application(SelfThing, text),
    DV.Record(DV.Optional(None)),
    DV.Record(DV.Optional(Some(DV.Record(DV.Optional(None))))),
    DV.Record(DV.Optional(Some(DV.Record(DV.Optional(Some(DV.Record(DV.Optional(None)))))))),
  )

  addCase(
    application(SelfThing2, text),
    DV.Record(DV.Variant(1, DV.Unit)),
  )
