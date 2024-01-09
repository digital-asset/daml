// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.daml.lf.value.json

import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

import com.daml.lf.data.Time
import com.daml.lf.value.{Value => V}

object ApiValueImplicits {

  implicit final class `ApiTimestamp additions`(private val it: V.ValueTimestamp) extends AnyVal {
    import it._
    def toInstant: Instant = value.toInstant
    def toIso8601: String = DateTimeFormatter.ISO_INSTANT.format(toInstant)
  }

  implicit final class `ApiDate additions`(private val it: V.ValueDate) extends AnyVal {
    import it._
    def toLocalDate: LocalDate = LocalDate.ofEpochDay((value.days: Int).toLong)
    def toInstant: Instant = Instant.from(toLocalDate)
    def toIso8601: String = DateTimeFormatter.ISO_LOCAL_DATE.format(toLocalDate)
  }

  // Timestamp has microsecond resolution
  implicit final class `ApiTimestamp.type additions`(private val it: V.ValueTimestamp.type)
      extends AnyVal {
    def fromIso8601(t: String): V.ValueTimestamp = fromInstant(Instant.parse(t))
    def fromInstant(t: Instant): V.ValueTimestamp =
      V.ValueTimestamp(Time.Timestamp.assertFromInstant(t))
    def fromMillis(t: Long): V.ValueTimestamp =
      V.ValueTimestamp(Time.Timestamp.assertFromLong(micros = t * 1000L))
  }

  implicit final class `ApiDate.type additions`(private val it: V.ValueDate.type) extends AnyVal {
    def fromIso8601(t: String): V.ValueDate =
      fromLocalDate(LocalDate.parse(t, DateTimeFormatter.ISO_LOCAL_DATE))
    def fromLocalDate(t: LocalDate): V.ValueDate =
      V.ValueDate(Time.Date.assertFromDaysSinceEpoch(t.toEpochDay.toInt))
  }
}
