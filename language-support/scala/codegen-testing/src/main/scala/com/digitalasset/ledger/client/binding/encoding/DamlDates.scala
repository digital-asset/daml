// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.encoding
import java.time.{LocalDate, ZoneOffset}

import com.daml.api.util.TimestampConversion
import com.daml.ledger.client.binding.{Primitive => P}
import scalaz.std.stream

object DamlDates {
  val Min: LocalDate = TimestampConversion.MIN.atZone(ZoneOffset.UTC).toLocalDate
  val Max: LocalDate = TimestampConversion.MAX.atZone(ZoneOffset.UTC).toLocalDate

  /**
    * The dates from `1582-10-05` to `1582-10-14` do not exist and cannot be represented as [java.sql.Date].
    * See [http://www.findingdulcinea.com/news/on-this-day/September-October-08/On-this-Day--In-1582--Oct--5-Did-Not-Exist-.html]
    * {{{
    * scala>  java.sql.Date.valueOf(java.time.LocalDate.parse("1582-10-05"))
    * res47: java.sql.Date = 1582-10-15
    *
    * scala>  java.sql.Date.valueOf(java.time.LocalDate.parse("1582-10-14"))
    * res48: java.sql.Date = 1582-10-24
    * }}}
    * Here is an example of two [java.time.LocalDate] values mapped to the same [java.sql.Date]:
    * {{{
    * scala> java.sql.Date.valueOf(java.time.LocalDate.parse("1582-10-05"))
    * res0: java.sql.Date = 1582-10-15
    *
    * scala> java.sql.Date.valueOf(java.time.LocalDate.parse("1582-10-15"))
    * res1: java.sql.Date = 1582-10-15
    * }}}
    */
  val RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate: (LocalDate, LocalDate) =
    (LocalDate.parse("1582-10-05"), LocalDate.parse("1582-10-14"))

  def localDatesWithoutInjectiveFunctionToSqlDate: Stream[LocalDate] =
    stream
      .unfold(RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate._1) { a: LocalDate =>
        if (!a.isAfter(RangeOfLocalDatesWithoutInjectiveFunctionToSqlDate._2))
          Some((a, a.plusDays(1)))
        else None
      }

  def damlDatesWithoutInjectiveFunctionToSqlDate: Stream[P.Date] =
    localDatesWithoutInjectiveFunctionToSqlDate.map(pDate)

  private def pDate(d: LocalDate): P.Date =
    P.Date
      .fromLocalDate(d)
      .getOrElse(sys.error(s"expected `P.Date` friendly `LocalDate`, but got: $d"))
}
