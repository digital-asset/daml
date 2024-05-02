// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import java.math.{RoundingMode, BigDecimal}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit}
import java.time.{ZoneId, Instant, Duration, LocalDate}
import java.util.concurrent.TimeUnit

import scalaz.std.anyVal._
import scalaz.syntax.order._
import scalaz.{Order, Ordering}

object Time {

  private[this] def safely[X](x: => X, error: String): Either[String, X] =
    try {
      Right(x)
    } catch {
      case scala.util.control.NonFatal(e) => Left(error + ":" + e.getMessage)
    }

  case class Date private (days: Int) extends Ordered[Date] {

    override def toString: String =
      Date.formatter.format(LocalDate.ofEpochDay(days.toLong))

    override def compare(that: Date): Int =
      days.compareTo(that.days)
  }

  object Date {

    @deprecated("use com.daml.lf.data.Time.Data", since = "2.9.0")
    type T = Date

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
      safely(assertDaysFromString(str), s"""cannot interpret string "$str" as Date""")
        .flatMap(fromDaysSinceEpoch)

    @throws[IllegalArgumentException]
    final def assertFromString(s: String): Date =
      assertRight(fromString(s))

    def assertFromDaysSinceEpoch(days: Int): Date =
      assertRight(fromDaysSinceEpoch(days))

    implicit val `Time.Date Order`: Order[Date] = new Order[Date] {
      override def equalIsNatural = true

      override def equal(x: Date, y: Date): Boolean = x == y

      override def order(x: Date, y: Date): Ordering = x.days ?|? y.days
    }

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
    def add(duration: Duration): Timestamp = {
      val secondsInMicros = TimeUnit.SECONDS.toMicros(duration.getSeconds)
      val nanosecondsInMicros = TimeUnit.NANOSECONDS.toMicros(duration.getNano.toLong)
      addMicros(secondsInMicros + nanosecondsInMicros)
    }

    @throws[IllegalArgumentException]
    def subtract(duration: Duration): Timestamp =
      add(duration.negated())

    @throws[IllegalArgumentException]
    def addMicros(x: Long): Timestamp =
      Timestamp.assertFromLong(micros + x)
  }

  object Timestamp {

    // TODO: https://github.com/digital-asset/daml/issues/17965
    //  lets change that HalfUp for daml 3.
    val DefaultRounding = RoundingMode.FLOOR

    @deprecated("use com.daml.lf.data.Time.Data", since = "2.9.0")
    type T = Timestamp

    val Resolution: Duration = Duration.of(1L, ChronoUnit.MICROS)

    private val formatter: DateTimeFormatter =
      DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("Z"))

    private[this] val ResolutionInNanos = BigDecimal.valueOf(Resolution.toNanos)
    private[this] val OneSecondInNanos = BigDecimal.valueOf(TimeUnit.SECONDS.toNanos(1))

    private[this] def assertMicrosFromEpochSeconds(
        seconds: Long,
        nanos: Long,
        roundingMode: RoundingMode,
    ): Long = {
      val secondPart = BigDecimal.valueOf(seconds) multiply OneSecondInNanos
      val nanoPart = BigDecimal.valueOf(nanos)
      val totalInNanos = secondPart add nanoPart
      totalInNanos.divide(ResolutionInNanos, roundingMode).longValueExact()
    }

    private[this] def assertMicrosFromInstant(i: Instant, rounding: RoundingMode): Long =
      assertMicrosFromEpochSeconds(i.getEpochSecond, i.getNano.toLong, rounding)

    private[this] def assertMicrosFromString(str: String, rounding: RoundingMode): Long =
      assertMicrosFromInstant(Instant.parse(str), rounding)

    val MinValue: Timestamp =
      Timestamp(assertMicrosFromString("0001-01-01T00:00:00.000000Z", RoundingMode.UNNECESSARY))

    val MaxValue: Timestamp =
      Timestamp(assertMicrosFromString("9999-12-31T23:59:59.999999Z", RoundingMode.UNNECESSARY))

    val Epoch: Timestamp = Timestamp(0)

    def assertFromLong(micros: Long): Timestamp =
      if (MinValue.micros <= micros && micros <= MaxValue.micros)
        Timestamp(micros)
      else
        throw new IllegalArgumentException(s"out of bound Timestamp $micros")

    def fromLong(micros: Long): Either[String, Timestamp] =
      safely(assertFromLong(micros), s"cannot convert long $micros into Timestamp")

    def assertFromInstant(i: Instant, rounding: RoundingMode = DefaultRounding): Timestamp =
      assertFromLong(assertMicrosFromInstant(i, rounding))

    def fromInstant(
        i: Instant,
        rounding: RoundingMode = DefaultRounding,
    ): Either[String, Timestamp] =
      safely(assertFromInstant(i, rounding), s"cannot convert instant $i into Timestamp")

    def assertFromString(str: String, rounding: RoundingMode = DefaultRounding): Timestamp =
      assertFromLong(assertMicrosFromString(str, rounding))

    final def fromString(
        s: String,
        rounding: RoundingMode = DefaultRounding,
    ): Either[String, Timestamp] =
      safely(assertFromString(s, rounding), s"cannot convert string $s into Timestamp")

    def now(): Timestamp = assertFromInstant(Instant.now(), DefaultRounding)

    implicit val `Time.Timestamp Order`: Order[Timestamp] = new Order[Timestamp] {
      override def equalIsNatural = true

      override def equal(x: Timestamp, y: Timestamp): Boolean = x == y

      override def order(x: Timestamp, y: Timestamp): Ordering = x.micros ?|? y.micros
    }
  }

}
