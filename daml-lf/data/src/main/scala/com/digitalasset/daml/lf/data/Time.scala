// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit}
import java.time.{Duration, Instant, LocalDate, ZoneId}
import java.util.concurrent.TimeUnit

import scalaz.std.anyVal._
import scalaz.syntax.order._
import scalaz.{Order, Ordering}

import scala.util.Try

object Time {

  class Date private (val days: Int) extends Ordered[Date] {

    override def toString: String =
      Date.formatter.format(LocalDate.ofEpochDay(days.toLong))

    override def equals(obj: Any): Boolean =
      obj match {
        case that: Date => this.days == that.days
        case _ => false
      }

    override def hashCode(): Int = days + Date.seed

    override def compare(that: Date): Int =
      days.compareTo(that.days)
  }

  object Date {

    private val seed: Int = getClass.getName.hashCode

    private val formatter: DateTimeFormatter =
      DateTimeFormatter.ISO_DATE.withZone(ZoneId.of("Z"))

    private def assertDaysFromString(str: String) =
      asInt(formatter.parse(str).getLong(ChronoField.EPOCH_DAY)).fold(sys.error, identity)

    private[lf] def asInt(days: Long): Either[String, Int] = {
      val daysI = days.toInt
      if (daysI.toLong == days) Right(daysI)
      else Left(s"out of bound Date $days")
    }

    val MinValue: Date =
      new Date(assertDaysFromString("0001-01-01"))

    val MaxValue: Date =
      new Date(assertDaysFromString("9999-12-31"))

    val Epoch: Date =
      assertFromDaysSinceEpoch(0)

    def fromDaysSinceEpoch(days: Int): Either[String, Date] =
      if (MinValue.days <= days && days <= MaxValue.days)
        Right(new Date(days))
      else
        Left(s"out of bound Date $days")

    def fromString(str: String): Either[String, Date] =
      Try(assertDaysFromString(str)).toEither.left
        .map(_ => s"cannot interpret $str as Date")
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

  class Timestamp private (val micros: Long) extends Ordered[Timestamp] {

    override def toString: String =
      Timestamp.formatter.format(toInstant)

    override def equals(obj: Any): Boolean =
      obj match {
        case that: Timestamp => this.micros == that.micros
        case _ => false
      }

    override def hashCode(): Int = micros.hashCode() + Timestamp.seed

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

    private val seed: Int = getClass.getName.hashCode

    private lazy val formatter: DateTimeFormatter =
      DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("Z"))

    private def microsOfInstant(i: Instant): Long =
      TimeUnit.SECONDS.toMicros(i.getEpochSecond) + TimeUnit.NANOSECONDS.toMicros(i.getNano.toLong)

    val MinValue: Timestamp =
      new Timestamp(microsOfInstant(Instant.parse("0001-01-01T00:00:00.000000Z")))

    val MaxValue: Timestamp =
      new Timestamp(microsOfInstant(Instant.parse("9999-12-31T23:59:59.999999Z")))

    val Resolution: Duration = Duration.of(1L, ChronoUnit.MICROS)

    val Epoch: Timestamp = assertFromLong(0)

    def now(): Timestamp =
      assertLenientFromInstant(Instant.now())

    def fromLong(micros: Long): Either[String, Timestamp] =
      if (MinValue.micros <= micros && micros <= MaxValue.micros)
        Right(new Timestamp(micros))
      else
        Left(s"out of bound Timestamp $micros")

    @throws[IllegalArgumentException]
    def assertFromLong(micros: Long): Timestamp =
      assertRight(fromLong(micros))

    // Drop nanoseconds
    def lenientFromInstant(i: Instant): Either[String, Timestamp] =
      fromLong(microsOfInstant(i))

    @throws[IllegalArgumentException]
    def assertLenientFromInstant(i: Instant): Timestamp =
      assertRight(lenientFromInstant(i))

    private val nanosInOneMicros: Long =
      TimeUnit.MICROSECONDS.toNanos(1)

    // reject if cannot be converted without loss of precision
    def strictFromInstant(i: Instant): Either[String, Timestamp] =
      if (i.getNano % nanosInOneMicros == 0)
        lenientFromInstant(i)
      else
        Left(s"cannot interpret $i as Timestamp")

    @throws[IllegalArgumentException]
    def assertStrictFromInstant(i: Instant): Timestamp =
      assertRight(strictFromInstant(i))

    @deprecated(
      "use lenientFromInstant or strictFromInstant, legacy behavior is lenient, preferred is strict",
      since = "2.7.0",
    )
    def fromInstant(i: Instant): Either[String, Timestamp] = lenientFromInstant(i)

    @deprecated(
      "use lenientFromInstant or strictFromInstant, legacy behavior is lenient, preferred is strict",
      since = "2.7.0",
    )
    def assertFromInstant(i: Instant): Timestamp = assertLenientFromInstant(i)

    private def instantFromString(str: String): Either[String, Instant] =
      try {
        Right(Instant.from(formatter.parse(str)))
      } catch {
        case scala.util.control.NonFatal(_) => Left(s"cannot interpret $str as Timestamp")
      }

    def lenientFromString(str: String): Either[String, Timestamp] =
      instantFromString(str).flatMap(lenientFromInstant)

    @throws[IllegalArgumentException]
    def assertLenientFromString(str: String): Timestamp =
      assertRight(lenientFromString(str))

    def strictFromString(str: String): Either[String, Timestamp] =
      instantFromString(str).flatMap(strictFromInstant)

    @throws[IllegalArgumentException]
    def assertStrictFromString(str: String): Timestamp =
      assertRight(strictFromString(str))

    @deprecated(
      "use lenientFromString or strictFromString, legacy behavior is lenient, preferred is strict",
      since = "2.7.0",
    )
    def fromString(str: String): Either[String, Timestamp] = lenientFromString(str)

    @deprecated(
      "use lenientFromString or strictFromString, legacy behavior is lenient, preferred is strict",
      since = "2.7.0",
    )
    def assertFromString(str: String): Timestamp = assertLenientFromString(str)

    implicit val `Time.Timestamp Order`: Order[Timestamp] = new Order[Timestamp] {
      override def equalIsNatural = true

      override def equal(x: Timestamp, y: Timestamp): Boolean = x.micros == y.micros

      override def order(x: Timestamp, y: Timestamp): Ordering = x.micros ?|? y.micros
    }
  }

}
