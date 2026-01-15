// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec

import com.daml.ledger.api.v2.ValueOuterClass

package object proto:
  type Value = ValueOuterClass.Value
  type ValueBuilder = ValueOuterClass.Value.Builder

  def newValue() = ValueOuterClass.Value.newBuilder()
  def newRecord() = ValueOuterClass.Record.newBuilder()
  def newRecordField() = ValueOuterClass.RecordField.newBuilder()
  def newVariant() = ValueOuterClass.Variant.newBuilder()
  def newEnum() = ValueOuterClass.Enum.newBuilder()
  def newList() = ValueOuterClass.List.newBuilder()
  def newOptional() = ValueOuterClass.Optional.newBuilder()
  def newGenMap() = ValueOuterClass.GenMap.newBuilder()
  def newGenMapEntry() = ValueOuterClass.GenMap.Entry.newBuilder()

  def mkTextMap(values: Iterator[(String, Value)]) =
    val textMap = ValueOuterClass.TextMap.newBuilder()
    values.foreach { (k, v) =>
      textMap.addEntries(ValueOuterClass.TextMap.Entry.newBuilder().setKey(k).setValue(v))
    }
    newValue().setTextMap(textMap).build()

  extension (vv: Value) def textMap = vv.getTextMap
