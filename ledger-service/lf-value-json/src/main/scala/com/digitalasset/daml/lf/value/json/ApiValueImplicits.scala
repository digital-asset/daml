// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value.json

import java.time.{Instant, LocalDate}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.value.{Value => V}

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
  private val formatter: DateTimeFormatter =
    new DateTimeFormatterBuilder().appendInstant(6).toFormatter()
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

  object FullyNamedApiRecord {
    def unapply[Cid](
        r: V.ValueRecord[Cid]): Option[(Option[Ref.Identifier], ImmArray[(Ref.Name, V[Cid])])] = {
      val V.ValueRecord(tycon, fields) = r
      if (fields.toSeq.forall(_._1.isDefined))
        Some((tycon, fields.map {
          case (Some(n), v) => (n, v)
          case (None, _) => sys.error("impossible None")
        }))
      else None
    }
  }
}
