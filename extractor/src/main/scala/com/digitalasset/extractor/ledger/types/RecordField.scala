// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.ledger.types

import com.digitalasset.ledger.api.{v1 => api}
import LedgerValue._

import scalaz._

final case class RecordField(label: String, value: LedgerValue)

object RecordField {
  private val fieldLens = ReqFieldLens.create[api.value.RecordField, api.value.Value]('value)

  final implicit class ApiRecordFieldOps(val apiRecordField: api.value.RecordField) extends AnyVal {
    def convert: String \/ RecordField =
      for {
        apiValue <- fieldLens(apiRecordField)
        value <- apiValue.sum.convert
      } yield RecordField(apiRecordField.label, value)
  }
}
