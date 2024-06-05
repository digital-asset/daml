// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.RefinedNonNegativeDuration.strToFiniteDuration
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{DurationConverter, ParsingResult}
import com.digitalasset.canton.time.{
  NonNegativeFiniteDuration as NonNegativeFiniteDurationInternal,
  PositiveFiniteDuration as PositiveFiniteDurationInternal,
  PositiveSeconds as PositiveSecondsInternal,
}
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.util.FutureUtil.defaultStackTraceFilter
import com.digitalasset.canton.{DiscardOps, checked}
import com.google.protobuf.duration.Duration as PbDuration
import io.circe.Encoder
import io.scalaland.chimney.Transformer
import org.slf4j.event.Level
import pureconfig.error.FailureReason
import pureconfig.{ConfigReader, ConfigWriter}

import java.time.Duration as JDuration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.concurrent.{Future, TimeoutException}

trait RefinedNonNegativeDuration[D <: RefinedNonNegativeDuration[D]] extends PrettyPrinting {
  this: {
    def update(newDuration: Duration): D
  } =>

  override def pretty: Pretty[RefinedNonNegativeDuration[D]] = prettyOfParam(_.duration)

  def duration: Duration

  def unwrap: Duration = duration

  def asFiniteApproximation: FiniteDuration

  def asJavaApproximation: JDuration = JDuration.ofMillis(asFiniteApproximation.toMillis)
  def minusSeconds(s: Int): D = update(duration.minus(s.seconds))

  def +(other: D): D = update(duration.plus(other.duration))
  def plusSeconds(s: Int): D = update(duration.plus(s.seconds))

  def *(d: Double): D = update(duration * d)

  def retries(interval: Duration): Int = {
    if (interval.isFinite && duration.isFinite)
      Math.max(0, duration.toMillis / Math.max(1, interval.toMillis)).toInt
    else Int.MaxValue
  }

  /** Same as Await.result, but with this timeout */
  def await[F](
      description: => String,
      logFailing: Option[Level] = None,
      stackTraceFilter: Thread => Boolean = defaultStackTraceFilter,
      onTimeout: TimeoutException => Unit = _ => (),
  )(fut: Future[F])(implicit loggingContext: ErrorLoggingContext): F = {
    FutureUtil.noisyAwaitResult(
      logFailing.fold(fut)(level => FutureUtil.logOnFailure(fut, description, level = level)),
      description,
      timeout = duration,
      stackTraceFilter = stackTraceFilter,
      onTimeout = onTimeout,
    )
  }

  /** Same as await, but not returning a value */
  def await_(
      description: => String,
      logFailing: Option[Level] = None,
  )(fut: Future[_])(implicit loggingContext: ErrorLoggingContext): Unit = {
    await(description, logFailing)(fut).discard
  }

  def toProtoPrimitive: com.google.protobuf.duration.Duration = {
    val d = asJavaApproximation
    com.google.protobuf.duration.Duration(d.getSeconds, d.getNano)
  }
}

trait RefinedNonNegativeDurationCompanion[D <: RefinedNonNegativeDuration[D]] {
  this: {
    def apply(newDuration: Duration): D
  } =>

  implicit val timeoutDurationEncoder: Encoder[D] =
    Encoder[String].contramap(_.unwrap.toString)

  implicit val orderingRefinedDuration: Ordering[D] = Ordering.by(_.duration)

  def fromDuration(duration: Duration): Either[String, D]

  def fromProtoPrimitive(
      field: String
  )(durationP: PbDuration): ParsingResult[D] =
    for {
      duration <- DurationConverter.fromProtoPrimitive(durationP)
      refinedDuration <- fromJavaDuration(duration).leftMap(err => ValueConversionError(field, err))
    } yield refinedDuration

  def fromProtoPrimitiveO(
      field: String
  )(durationPO: Option[PbDuration]): ParsingResult[D] =
    for {
      durationP <- ProtoConverter.required(field, durationPO)
      refinedDuration <- fromProtoPrimitive(field)(durationP)
    } yield refinedDuration

  def tryFromDuration(duration: Duration): D = fromDuration(duration) match {
    case Left(err) => throw new IllegalArgumentException(err)
    case Right(x) => x
  }

  def fromJavaDuration(duration: java.time.Duration): Either[String, D] =
    fromDuration(Duration.fromNanos(duration.toNanos))

  def tryFromJavaDuration(duration: java.time.Duration): D =
    tryFromDuration(Duration.fromNanos(duration.toNanos))

  def ofMillis(millis: Long): D = apply(Duration(millis, TimeUnit.MILLISECONDS))

  def ofSeconds(secs: Long): D = apply(Duration(secs, TimeUnit.SECONDS))

  def ofMinutes(minutes: Long): D = apply(Duration(minutes, TimeUnit.MINUTES))

  def ofHours(hours: Long): D = apply(Duration(hours, TimeUnit.HOURS))

  def ofDays(days: Long): D = apply(Duration(days, TimeUnit.DAYS))
}

object RefinedNonNegativeDuration {
  def strToFiniteDuration(str: String): Either[String, FiniteDuration] =
    Either
      .catchOnly[NumberFormatException](Duration.apply(str))
      .leftMap(_.getMessage)
      .flatMap(duration =>
        Some(duration)
          .collect { case d: FiniteDuration => d }
          .toRight("Duration is not a finite duration")
      )
}

/** Duration class used for non-negative durations.
  *
  * There are two options: either it's a non-negative duration or an infinite duration
  */
final case class NonNegativeDuration(duration: Duration)
    extends RefinedNonNegativeDuration[NonNegativeDuration] {
  require(duration >= Duration.Zero, s"Expecting non-negative duration, found: $duration")

  def update(newDuration: Duration): NonNegativeDuration = NonNegativeDuration(newDuration)

  def asFiniteApproximation: FiniteDuration = duration match {
    case fd: FiniteDuration => fd
    case _: Duration.Infinite => NonNegativeDuration.maxTimeout
  }

  private[canton] def toInternal: NonNegativeFiniteDurationInternal =
    checked(NonNegativeFiniteDurationInternal.tryCreate(asJavaApproximation))
}

object NonNegativeDuration extends RefinedNonNegativeDurationCompanion[NonNegativeDuration] {
  val maxTimeout: FiniteDuration = 100000.days
  val Zero: NonNegativeDuration = NonNegativeDuration(Duration.Zero)

  def fromDuration(duration: Duration): Either[String, NonNegativeDuration] = duration match {
    case x: FiniteDuration =>
      Either.cond(x.length >= 0, NonNegativeDuration(x), s"Duration ${x} is negative!")
    case Duration.Inf => Right(NonNegativeDuration(Duration.Inf))
    case x => Left(s"Duration ${x} is not a valid duration that can be used for timeouts.")
  }
}

/** Duration class used for non-negative finite durations. */
final case class NonNegativeFiniteDuration(underlying: FiniteDuration)
    extends RefinedNonNegativeDuration[NonNegativeFiniteDuration] {

  require(underlying >= Duration.Zero, s"Duration ${duration} is negative")

  def duration: Duration = underlying
  def asJava: JDuration = JDuration.ofNanos(duration.toNanos)

  def update(newDuration: Duration): NonNegativeFiniteDuration = newDuration match {
    case _: Duration.Infinite =>
      throw new IllegalArgumentException(s"Duration must be finite, but is Duration.Inf")
    case duration: FiniteDuration => NonNegativeFiniteDuration(duration)
  }

  def asFiniteApproximation: FiniteDuration = underlying

  def toInternal: NonNegativeFiniteDurationInternal = checked(
    NonNegativeFiniteDurationInternal.tryCreate(asJava)
  )
}

object NonNegativeFiniteDuration
    extends RefinedNonNegativeDurationCompanion[NonNegativeFiniteDuration] {
  val Zero: NonNegativeFiniteDuration = NonNegativeFiniteDuration(Duration.Zero)

  def apply(duration: Duration): NonNegativeFiniteDuration = NonNegativeFiniteDuration
    .fromDuration(duration)
    .fold(err => throw new IllegalArgumentException(err), identity)

  def apply(duration: JDuration): NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryFromJavaDuration(duration)

  implicit val forgetRefinementJDuration: Transformer[NonNegativeFiniteDuration, JDuration] =
    _.asJava
  implicit val forgetRefinementFDuration: Transformer[NonNegativeFiniteDuration, FiniteDuration] =
    _.underlying

  def fromDuration(duration: Duration): Either[String, NonNegativeFiniteDuration] = duration match {
    case x: FiniteDuration =>
      Either.cond(x.length >= 0, NonNegativeFiniteDuration(x), s"Duration $x is negative!")
    case Duration.Inf => Left(s"Expecting finite duration but found Duration.Inf")
    case x => Left(s"Duration $x is not a valid duration that can be used for timeouts.")
  }

  private[canton] final case class NonNegativeFiniteDurationError(input: String, reason: String)
      extends FailureReason {
    override def description: String =
      s"Cannot convert `$input` to a non-negative finite duration: $reason"
  }

  private[canton] implicit val nonNegativeFiniteDurationReader
      : ConfigReader[NonNegativeFiniteDuration] =
    ConfigReader.fromString[NonNegativeFiniteDuration] { str =>
      (for {
        duration <- strToFiniteDuration(str)
        nonNegativeFiniteDuration <- fromDuration(duration)
      } yield nonNegativeFiniteDuration).leftMap(NonNegativeFiniteDurationError(str, _))
    }

  private[canton] implicit val nonNegativeFiniteDurationWriter
      : ConfigWriter[NonNegativeFiniteDuration] =
    // avoid pretty printing by converting the underlying value to string
    ConfigWriter.toString(_.underlying.toString)
}

/** Duration class used for positive finite durations. */
final case class PositiveFiniteDuration(underlying: FiniteDuration)
    extends RefinedNonNegativeDuration[PositiveFiniteDuration] {

  require(underlying > Duration.Zero, s"Duration ${duration} is not positive")

  def duration: Duration = underlying
  def asJava: JDuration = JDuration.ofNanos(duration.toNanos)

  def update(newDuration: Duration): PositiveFiniteDuration = newDuration match {
    case _: Duration.Infinite =>
      throw new IllegalArgumentException(s"Duration must be finite, but is Duration.Inf")
    case duration: FiniteDuration => PositiveFiniteDuration(duration)
  }

  def asFiniteApproximation: FiniteDuration = underlying

  def toInternal: PositiveFiniteDurationInternal = checked(
    PositiveFiniteDurationInternal.tryCreate(asJava)
  )
}

object PositiveFiniteDuration extends RefinedNonNegativeDurationCompanion[PositiveFiniteDuration] {
  def apply(duration: Duration): PositiveFiniteDuration = PositiveFiniteDuration
    .fromDuration(duration)
    .fold(err => throw new IllegalArgumentException(err), identity)

  def fromDuration(duration: Duration): Either[String, PositiveFiniteDuration] = duration match {
    case x: FiniteDuration =>
      Either.cond(x.length > 0, PositiveFiniteDuration(x), s"Duration $x is not positive!")
    case Duration.Inf => Left(s"Expecting finite duration but found Duration.Inf")
    case x => Left(s"Duration $x is not a valid duration that can be used for timeouts.")
  }

  private[canton] final case class PositiveFiniteDurationError(input: String, reason: String)
      extends FailureReason {
    override def description: String =
      s"Cannot convert `$input` to a positive finite duration: $reason"
  }

  private[canton] implicit val positiveFiniteDurationReader
      : ConfigReader[PositiveFiniteDuration] = {
    ConfigReader.fromString[PositiveFiniteDuration] { str =>
      (for {
        duration <- strToFiniteDuration(str)
        positiveFiniteDuration <- PositiveFiniteDuration.fromDuration(duration)
      } yield positiveFiniteDuration).leftMap(PositiveFiniteDurationError(str, _))
    }
  }

  private[canton] implicit val positiveFiniteDurationWriter: ConfigWriter[PositiveFiniteDuration] =
    // avoid pretty printing by converting the underlying value to string
    ConfigWriter.toString(_.underlying.toString)
}

/** Duration class used for positive durations that are rounded to the second. */
final case class PositiveDurationSeconds(underlying: FiniteDuration)
    extends RefinedNonNegativeDuration[PositiveDurationSeconds] {

  require(underlying > Duration.Zero, s"Duration ${duration} is not positive")
  require(
    PositiveDurationSeconds.isRoundedToTheSecond(underlying),
    s"Duration ${duration} is not rounded to the second",
  )

  def duration: Duration = underlying
  def asJava: JDuration = JDuration.ofNanos(duration.toNanos)

  def update(newDuration: Duration): PositiveDurationSeconds = newDuration match {
    case _: Duration.Infinite =>
      throw new IllegalArgumentException(s"Duration must be finite, but is Duration.Inf")
    case duration: FiniteDuration => PositiveDurationSeconds(duration)
  }

  def asFiniteApproximation: FiniteDuration = underlying

  private[canton] def toInternal: PositiveSecondsInternal = checked(
    PositiveSecondsInternal.tryCreate(
      asJava
    )
  )
}

object PositiveDurationSeconds
    extends RefinedNonNegativeDurationCompanion[PositiveDurationSeconds] {
  private def isRoundedToTheSecond(duration: FiniteDuration): Boolean =
    duration == Duration(duration.toSeconds, SECONDS)

  def apply(duration: Duration): PositiveDurationSeconds = PositiveDurationSeconds
    .fromDuration(duration)
    .fold(err => throw new IllegalArgumentException(err), identity)

  def apply(duration: JDuration): PositiveDurationSeconds =
    PositiveDurationSeconds.tryFromJavaDuration(duration)

  def fromDuration(duration: Duration): Either[String, PositiveDurationSeconds] =
    duration match {
      case x: FiniteDuration =>
        for {
          _ <- Either.cond(x.length > 0, (), s"Duration $x is not positive")
          _ <- Either.cond(
            isRoundedToTheSecond(x),
            (),
            s"Duration ${duration} is not rounded to the second",
          )
        } yield PositiveDurationSeconds(x)
      case Duration.Inf => Left(s"Expecting finite duration but found Duration.Inf")
      case x => Left(s"Duration $x is not a valid duration that can be used for timeouts.")
    }

  def fromProtoPrimitive(durationP: PbDuration): Either[String, PositiveDurationSeconds] =
    fromJavaDuration(JDuration.of(durationP.seconds, java.time.temporal.ChronoUnit.SECONDS))

}
