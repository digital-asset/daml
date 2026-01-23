// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec

import com.daml.ledger.api.v2.value
import com.daml.ledger.api.v2.value.Value.Sum

package object proto:
  type Value = value.Value

  val Value = value.Value
  val RecordField = value.RecordField
  val Record = value.Record
  val Variant = value.Variant
  val Enum = value.Enum
  val List = value.List
  val Optional = value.Optional
  val GenMap = value.GenMap

  def mkTextMap(values: Iterator[(String, Value)]) =
    val entries = values.map((k, v) => value.TextMap.Entry(k, Some(v)))
    Sum.TextMap(value.TextMap(entries = entries.toSeq))

  extension (vv: Value) def `textMap!` = vv.sum.textMap
