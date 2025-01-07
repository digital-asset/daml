// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.daml.lf.value.json

import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.value.Value as V

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

object ApiValueImplicits {

  implicit final class `ApiTimestamp additions`(private val it: V.ValueTimestamp) extends AnyVal {
    import it.*
    def toInstant: Instant = value.toInstant
    def toIso8601: String = DateTimeFormatter.ISO_INSTANT.format(toInstant)
  }

  implicit final class `ApiDate additions`(private val it: V.ValueDate) extends AnyVal {
    import it.*
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
  }

  implicit final class `ApiDate.type additions`(private val it: V.ValueDate.type) extends AnyVal {
    def fromIso8601(t: String): V.ValueDate =
      fromLocalDate(LocalDate.parse(t, DateTimeFormatter.ISO_LOCAL_DATE))
    def fromLocalDate(t: LocalDate): V.ValueDate =
      V.ValueDate(Time.Date.assertFromDaysSinceEpoch(t.toEpochDay.toInt))
  }
}
