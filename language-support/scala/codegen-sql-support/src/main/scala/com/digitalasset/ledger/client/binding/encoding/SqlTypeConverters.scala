// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding
import com.digitalasset.ledger.client.binding.{Primitive => P}

object SqlTypeConverters {

  def primitiveDateToSqlDate(d: P.Date): java.sql.Date = java.sql.Date.valueOf(d)

  def sqlDateToPrimitiveDate(d: java.sql.Date): P.Date = {
    P.Date
      .fromLocalDate(d.toLocalDate)
      .getOrElse(throw SqlTypeConversionException(s"Cannot convert $d to Primitive.Date"))
  }

  def primitiveTimestampToSqlTimestamp(t: P.Timestamp): java.sql.Timestamp =
    java.sql.Timestamp.from(t)

  def sqlTimestampToPrimitiveTimestamp(t: java.sql.Timestamp): P.Timestamp = {
    P.Timestamp
      .discardNanos(t.toInstant)
      .getOrElse(throw SqlTypeConversionException(s"Cannot convert $t to Primitive.Timestamp"))
  }

  case class SqlTypeConversionException(msg: String)
      extends IllegalStateException(
        msg + ". This would only happen if someone manually updated SQL values in the database")
}
