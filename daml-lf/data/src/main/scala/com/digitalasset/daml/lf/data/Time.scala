// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, ZoneId}
import java.util.concurrent.TimeUnit

import scala.util.Try

object Time {

  case class Date private (days: Int) extends Ordered[Date] {

    override def toString: String =
      Date.formatter.format(LocalDate.ofEpochDay(days.toLong))

    override def compare(that: Date): Int =
      days.compareTo(that.days)
  }

  object Date {

    type T = Date

    private def apply(days: Int): Date =
      new Date(days)

    private val formatter: DateTimeFormatter =
      DateTimeFormatter.ISO_DATE.withZone(ZoneId.of("Z"))

    private def assertDaysFromString(str: String) = {
      asInt(formatter.parse(str).getLong(ChronoField.EPOCH_DAY)).fold(sys.error, identity)
    }

    private[lf] def asInt(days: Long): Either[String, Int] = {
      val daysI = days.toInt
      if (daysI.toLong == days) Right(daysI)
      else Left(s"out of bound Date $days")
    }

    val MinValue: Date =
      Date(assertDaysFromString("0001-01-01"))

    val MaxValue: Date =
      Date(assertDaysFromString("9999-12-31"))

    val Epoch: Date =
      Date(0)

    def fromDaysSinceEpoch(days: Int): Either[String, Date] =
      if (MinValue.days <= days && days <= MaxValue.days)
        Right(Date(days))
      else
        Left(s"out of bound Date $days")

    def fromString(str: String): Either[String, Date] =
      Try(assertDaysFromString(str)).toEither.left
        .map(_ => s"cannot interpret $str as Date")
        .flatMap(fromDaysSinceEpoch)

    @throws[IllegalArgumentException]
    final def assertFromString(s: String): T =
      assertRight(fromString(s))

    def assertFromDaysSinceEpoch(days: Int): Date =
      assertRight(fromDaysSinceEpoch(days))

  }

  case class Timestamp private (micros: Long) extends Ordered[Timestamp] {

    override def toString: String =
      Timestamp.formatter.format(toInstant)

    def compare(that: Timestamp): Int =
      micros.compareTo(that.micros)

    def toInstant: Instant = {
      val seconds = TimeUnit.MICROSECONDS.toSeconds(micros)
      val microsOfSecond = micros - TimeUnit.SECONDS.toMicros(seconds)
      Instant.ofEpochSecond(seconds, TimeUnit.MICROSECONDS.toNanos(microsOfSecond))
    }

    @throws[IllegalArgumentException]
    def addMicros(x: Long): Timestamp =
      Timestamp.assertFromLong(micros + x)
  }

  object Timestamp {

    type T = Timestamp

    private def apply(micros: Long): Timestamp =
      new Timestamp(micros)

    private val formatter: DateTimeFormatter =
      DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("Z"))

    private def assertMicrosFromInstant(i: Instant): Long =
      TimeUnit.SECONDS.toMicros(i.getEpochSecond) + TimeUnit.NANOSECONDS.toMicros(i.getNano.toLong)

    private def assertMicrosFromString(str: String): Long =
      assertMicrosFromInstant(Instant.parse(str))

    val MinValue: Timestamp =
      Timestamp(assertMicrosFromString("0001-01-01T00:00:00.000000Z"))

    val MaxValue: Timestamp =
      Timestamp(assertMicrosFromString("9999-12-31T23:59:59.999999Z"))

    val Epoch: Timestamp =
      Timestamp(0)

    def now(): Timestamp =
      assertFromLong(assertMicrosFromInstant(Instant.now()))

    def fromLong(micros: Long): Either[String, Timestamp] =
      if (MinValue.micros <= micros && micros <= MaxValue.micros)
        Right(Timestamp(micros))
      else
        Left(s"out of bound Timestamp $micros")

    @throws[IllegalArgumentException]
    def assertFromLong(micros: Long): Timestamp =
      assertRight(fromLong(micros))

    def fromString(str: String): Either[String, Timestamp] =
      Try(assertMicrosFromString(str)).toEither.left
        .map(_ => s"cannot interpret $str as Timestamp")
        .flatMap(fromLong)

    @throws[IllegalArgumentException]
    final def assertFromString(s: String): T =
      assertRight(fromString(s))

    def fromInstant(i: Instant): Either[String, Timestamp] =
      Try(assertMicrosFromInstant(i)).toEither.left
        .map(_ => s"cannot interpret $i as Timestamp")
        .flatMap(fromLong)

    def assertFromInstant(i: Instant): Timestamp =
      assertFromLong(assertMicrosFromInstant(i))

  }

}
