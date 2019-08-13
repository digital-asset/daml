// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.ledger.types

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.{v1 => api}
import LedgerValue._

import scalaz.\/
import scalaz.std.option._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.either._
import scalaz.syntax.traverse._

private[types] object RecordField {
  private val fieldLens = ReqFieldLens.create[api.value.RecordField, api.value.Value]('value)

  final implicit class ApiRecordFieldOps(val apiRecordField: api.value.RecordField) extends AnyVal {
    def convert: String \/ (Option[Ref.Name], LedgerValue) =
      for {
        label <- {
          val rawLabel = apiRecordField.label
          (rawLabel.nonEmpty option Ref.Name.fromString(rawLabel).disjunction).sequenceU
        }
        apiValue <- fieldLens(apiRecordField)
        value <- apiValue.sum.convert
      } yield (label, value)
  }
}
