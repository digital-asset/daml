// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import java.math

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.time.temporal.ChronoUnit.MICROS
import java.time.{Duration, Instant, LocalDate, ZoneId}
import java.util.concurrent.TimeUnit

import scalaz.std.anyVal._
import scalaz.syntax.order._
import scalaz.{Order, Ordering}

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

    type T = Timestamp

    private val formatter: DateTimeFormatter =
      DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("Z"))

    private[this] val OneMicroInNanos = math.BigDecimal.valueOf(TimeUnit.MICROSECONDS.toNanos(1))
    private[this] val OneSecondInNanos = math.BigDecimal.valueOf(TimeUnit.SECONDS.toMicros(1))

    private[this] def microsFromEpochSeconds(seconds: Long, nanos: Long, roundingMode: math.RoundingMode): Either[String, Long] =
      try {
        Right(
          math.BigDecimal.valueOf(seconds).multiply(OneSecondInNanos).add(math.BigDecimal.valueOf(nanos)).divide(OneMicroInNanos, roundingMode).longValueExact()
        )
      } catch {
        case _: ArithmeticException =>
          Left(s"($seconds s + $nanos ns) cannot be convert to Timestamp without lost of precision")
      }

    private[this] def microsFromInstant(i: Instant, rounding: math.RoundingMode ): Either[String, Long] =
      microsFromEpochSeconds(i.getEpochSecond, i.getNano.toLong, rounding)

    private[this] def microsFromString(str: String,  rounding: math.RoundingMode): Either[String, Long] =
      microsFromInstant(Instant.parse(str), rounding)

    private[this] def assertMicrosFromString(str: String): Long =
      assertRight(microsFromString(str, math.RoundingMode.UNNECESSARY))

    val MinValue: Timestamp =
      Timestamp(assertMicrosFromString("0001-01-01T00:00:00.000000Z"))

    val MaxValue: Timestamp =
      Timestamp(assertMicrosFromString("9999-12-31T23:59:59.999999Z"))

    val Resolution: Duration = Duration.of(1L, MICROS)

    val Epoch: Timestamp =
      Timestamp(0)

    def now(): Timestamp = assertFromInstant(Instant.now())

    def fromLong(micros: Long): Either[String, Timestamp] =
      if (MinValue.micros <= micros && micros <= MaxValue.micros)
        Right(Timestamp(micros))
      else
        Left(s"out of bound Timestamp $micros")

    @throws[IllegalArgumentException]
    def assertFromLong(micros: Long): Timestamp =
      assertRight(fromLong(micros))

    def fromString(str: String, rounding: math.RoundingMode = math.RoundingMode.FLOOR): Either[String, Timestamp] =
      microsFromString(str, rounding).flatMap(fromLong)

    @throws[IllegalArgumentException]
    final def assertFromString(s: String, rounding: math.RoundingMode = math.RoundingMode.FLOOR): T =
      assertRight(fromString(s, rounding))

    def fromInstant(i: Instant, rounding: math.RoundingMode = math.RoundingMode.FLOOR): Either[String, Timestamp] =
      microsFromInstant(i, rounding).flatMap(fromLong)

    def assertFromInstant(i: Instant, rounding: math.RoundingMode = math.RoundingMode.FLOOR): Timestamp =
      assertRight(fromInstant(i, rounding))

    implicit val `Time.Timestamp Order`: Order[Timestamp] = new Order[Timestamp] {
      override def equalIsNatural = true

      override def equal(x: Timestamp, y: Timestamp): Boolean = x == y

      override def order(x: Timestamp, y: Timestamp): Ordering = x.micros ?|? y.micros
    }
  }

}
