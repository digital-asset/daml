// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model

import java.time.{Instant, LocalDate}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import scala.annotation.tailrec

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.value.{Value => V}

object ApiValueImplicits {

  implicit final class `FrontStack additions`[A](private val it: FrontStack[A]) extends AnyVal {
    @throws[IndexOutOfBoundsException]
    def slowApply(ix: Int): A = {
      val i = it.iterator
      @tailrec def lp(ix: Int): A =
        if (!i.hasNext) throw new IndexOutOfBoundsException("it")
        else if (ix <= 0) i.next
        else lp(ix - 1)
      lp(ix)
    }

  }

  implicit final class `ApiTimestamp additions`(private val it: ApiTimestamp) extends AnyVal {
    import it._
    def toInstant: Instant = value.toInstant
    def toIso8601: String = DateTimeFormatter.ISO_INSTANT.format(toInstant)
  }

  implicit final class `ApiDate additions`(private val it: ApiDate) extends AnyVal {
    import it._
    def toLocalDate: LocalDate = LocalDate.ofEpochDay((value.days: Int).toLong)
    def toInstant: Instant = Instant.from(toLocalDate)
    def toIso8601: String = DateTimeFormatter.ISO_LOCAL_DATE.format(toLocalDate)
  }

  // Timestamp has microsecond resolution
  private val formatter: DateTimeFormatter =
    new DateTimeFormatterBuilder().appendInstant(6).toFormatter()
  implicit final class `ApiTimestamp.type additions`(private val it: ApiTimestamp.type)
      extends AnyVal {
    def fromIso8601(t: String): ApiTimestamp = fromInstant(Instant.parse(t))
    def fromInstant(t: Instant): ApiTimestamp =
      ApiTimestamp(Time.Timestamp.assertFromInstant(t))
    def fromMillis(t: Long): ApiTimestamp =
      ApiTimestamp(Time.Timestamp.assertFromLong(micros = t * 1000L))
  }

  implicit final class `ApiDate.type additions`(private val it: ApiDate.type) extends AnyVal {
    def fromIso8601(t: String): ApiDate =
      fromLocalDate(LocalDate.parse(t, DateTimeFormatter.ISO_LOCAL_DATE))
    def fromLocalDate(t: LocalDate): ApiDate =
      ApiDate(Time.Date.assertFromDaysSinceEpoch(t.toEpochDay.toInt))
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
