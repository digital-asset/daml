// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric}
import com.digitalasset.canton.config.{
  NonNegativeFiniteDuration as NonNegativeFiniteDurationConfig,
  PositiveDurationSeconds as PositiveDurationSecondsConfig,
  PositiveFiniteDuration as PositiveFiniteDurationConfig,
}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{DurationConverter, ParsingResult}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.duration.Duration as PbDuration
import io.circe.Encoder
import io.scalaland.chimney.Transformer
import slick.jdbc.{GetResult, SetParameter}

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*

sealed trait RefinedDuration extends Ordered[RefinedDuration] {
  def duration: Duration
  def unwrap: Duration = duration

  def toProtoPrimitive: com.google.protobuf.duration.Duration =
    DurationConverter.toProtoPrimitive(duration)

  def toScala: FiniteDuration = duration.toScala

  override def compare(that: RefinedDuration): Int = this.duration.compareTo(that.duration)
}

trait RefinedDurationCompanion[RD <: RefinedDuration] {

  /** Factory method for creating the [[RefinedDuration]] from a [[java.time.Duration]]
    * @throws java.lang.IllegalArgumentException if the duration does not satisfy the refinement predicate
    */
  def tryCreate(duration: Duration): RD =
    create(duration).valueOr(err => throw new IllegalArgumentException(err))

  def create(duration: Duration): Either[String, RD]

  def between(from: CantonTimestamp, to: CantonTimestamp): Either[String, RD] =
    create(Duration.between(from.toInstant, to.toInstant))

  def between(from: CantonTimestampSecond, to: CantonTimestampSecond): Either[String, RD] =
    create(Duration.between(from.toInstant, to.toInstant))

  implicit val orderingRefinedDuration: Ordering[RD] = Ordering.by(_.duration)

  def fromProtoPrimitive(
      field: String
  )(durationP: PbDuration): ParsingResult[RD] =
    for {
      duration <- DurationConverter.fromProtoPrimitive(durationP)
      refinedDuration <- create(duration).leftMap(err => ValueConversionError(field, err))
    } yield refinedDuration

  def fromProtoPrimitiveO(
      field: String
  )(durationPO: Option[PbDuration]): ParsingResult[RD] =
    for {
      durationP <- ProtoConverter.required(field, durationPO)
      refinedDuration <- fromProtoPrimitive(field)(durationP)
    } yield refinedDuration

  def fromBytes(field: String)(bytes: Array[Byte]): ParsingResult[RD] =
    for {
      durationP <- ProtoConverter.protoParserArray(PbDuration.parseFrom)(bytes)
      res <- fromProtoPrimitive(field)(durationP)
    } yield res

  private def getResultFromBytes(bytes: Array[Byte]) =
    fromBytes("database field")(bytes).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize the duration: $err")
    )

  // JSON encoding using circe
  implicit val refinedDurationEncoder: Encoder[RD] =
    Encoder.forProduct1("duration")(_.duration)

  implicit val getResultRefinedDuration: GetResult[RD] =
    GetResult(r => getResultFromBytes(r.nextBytes()))

  implicit val getResultRefinedDurationOption: GetResult[Option[RD]] =
    GetResult(r => r.nextBytesOption().map(getResultFromBytes))

  implicit def setParameterRefinedDuration(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[RD] =
    (d, pp) => pp >> d.toProtoPrimitive.toByteArray

  implicit def setParameterRefinedDurationOption(implicit
      setParameterByteArrayO: SetParameter[Option[Array[Byte]]]
  ): SetParameter[Option[RD]] = (d, pp) => pp >> d.map(_.toProtoPrimitive.toByteArray)

  def tryOfMicros(micros: Long): RD =
    tryCreate(Duration.ofSeconds(micros / 1000000).withNanos((micros % 1000000L).toInt * 1000))

  def tryOfMillis(millis: Long): RD = tryCreate(Duration.ofMillis(millis))

  def tryOfSeconds(secs: Long): RD = tryCreate(Duration.ofSeconds(secs))

  def tryOfMinutes(minutes: Long): RD = tryCreate(Duration.ofMinutes(minutes))

  def tryOfHours(hours: Long): RD = tryCreate(Duration.ofHours(hours))

  def tryOfDays(days: Long): RD = tryCreate(Duration.ofDays(days))
}

final case class PositiveFiniteDuration private (duration: Duration)
    extends RefinedDuration
    with PrettyPrinting {
  require(!duration.isNegative && !duration.isZero, s"Duration $duration must not be non-negative")

  override def pretty: Pretty[PositiveFiniteDuration] = prettyOfParam(_.duration)

  /** Returns the duration in seconds truncated to the size of Int, returns as a maximum Int.MaxValue.
    *
    * Usage: On the database/jdbc level many timeouts require to be specified in seconds as an integer, not a long.
    */
  def toSecondsTruncated(
      logger: TracedLogger
  )(implicit traceContext: TraceContext): PositiveNumeric[Int] = {
    val seconds = duration.getSeconds

    val result = if (seconds > Int.MaxValue) {
      logger.info(s"Truncating $duration to integer")
      Int.MaxValue
    } else
      seconds.toInt

    // Result must be positive due to assertion on duration
    checked(PositiveNumeric.tryCreate(result))
  }

  def toConfig: PositiveFiniteDurationConfig = checked(
    PositiveFiniteDurationConfig.tryFromJavaDuration(duration)
  )
}

object PositiveFiniteDuration extends RefinedDurationCompanion[PositiveFiniteDuration] {
  override def create(duration: Duration): Either[String, PositiveFiniteDuration] =
    Either.cond(
      !duration.isNegative && !duration.isZero,
      PositiveFiniteDuration(duration),
      s"Duration should be positive, found: $duration",
    )
}

final case class NonNegativeFiniteDuration private (duration: Duration)
    extends RefinedDuration
    with PrettyPrinting {
  require(!duration.isNegative, s"Duration $duration must not be negative")

  override def pretty: Pretty[NonNegativeFiniteDuration] = prettyOfParam(_.duration)

  def +(other: NonNegativeFiniteDuration): NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    duration.plus(other.duration)
  )

  def *(multiplicand: NonNegativeInt): NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    duration.multipliedBy(multiplicand.value.toLong)
  )

  def /(divisor: NonNegativeInt): NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    duration.dividedBy(divisor.value.toLong)
  )

  def toConfig: NonNegativeFiniteDurationConfig = checked(
    NonNegativeFiniteDurationConfig.tryFromJavaDuration(duration)
  )
}

object NonNegativeFiniteDuration extends RefinedDurationCompanion[NonNegativeFiniteDuration] {
  implicit val forgetRefinementJDuration: Transformer[NonNegativeFiniteDuration, Duration] =
    _.duration
  implicit val forgetRefinementFDuration: Transformer[NonNegativeFiniteDuration, FiniteDuration] =
    _.toScala

  implicit val toNonNegativeDurationConfig
      : Transformer[NonNegativeFiniteDuration, NonNegativeFiniteDurationConfig] = _.toConfig

  val Zero: NonNegativeFiniteDuration = NonNegativeFiniteDuration(Duration.ZERO)

  override def create(duration: Duration): Either[String, NonNegativeFiniteDuration] =
    Either.cond(
      !duration.isNegative,
      NonNegativeFiniteDuration(duration),
      s"Duration should be non-negative, found: $duration",
    )

  def apply(duration: PositiveSeconds): NonNegativeFiniteDuration = checked(
    NonNegativeFiniteDuration.tryCreate(duration.duration)
  )
}

final case class NonNegativeSeconds private (duration: Duration)
    extends RefinedDuration
    with PrettyPrinting {
  require(!duration.isNegative, s"Duration $duration must not be negative")
  require(duration.getNano == 0, s"Duration $duration must be rounded to the second")

  override def pretty: Pretty[NonNegativeSeconds.this.type] = prettyOfParam(_.duration)
}

object NonNegativeSeconds extends RefinedDurationCompanion[NonNegativeSeconds] {
  val Zero: NonNegativeSeconds = NonNegativeSeconds(Duration.ZERO)

  override def create(duration: Duration): Either[String, NonNegativeSeconds] =
    Either.cond(
      !duration.isNegative && duration.getNano == 0,
      NonNegativeSeconds(duration),
      s"Duration should be non-negative and rounded to the second, found: $duration",
    )
}

final case class PositiveSeconds private (duration: Duration)
    extends RefinedDuration
    with PrettyPrinting {
  require(!duration.isNegative && !duration.isZero, s"Duration $duration must be positive")
  require(duration.getNano == 0, s"Duration $duration must be rounded to the second")

  override def pretty: Pretty[PositiveSeconds.this.type] = prettyOfParam(_.duration)

  def toConfig: PositiveDurationSecondsConfig = checked(
    PositiveDurationSecondsConfig.tryFromJavaDuration(duration)
  )

  def toFiniteDuration: FiniteDuration =
    FiniteDuration(duration.toNanos, TimeUnit.NANOSECONDS).toCoarsest

  def add(i: NonNegativeSeconds): PositiveSeconds = {
    val newDuration = duration.plus(i.duration)
    checked(PositiveSeconds(newDuration))
  }
}

object PositiveSeconds extends RefinedDurationCompanion[PositiveSeconds] {
  implicit val toPositiveSecondsConfig
      : Transformer[PositiveSeconds, PositiveDurationSecondsConfig] =
    _.toConfig

  implicit val getResultPositiveSeconds: GetResult[PositiveSeconds] =
    GetResult(r => tryOfSeconds(r.nextLong()))

  implicit def setParameterPositiveSeconds(implicit
      setParameterLong: SetParameter[Long]
  ): SetParameter[PositiveSeconds] =
    (d, pp) => pp >> d.duration.getSeconds

  override def create(duration: Duration): Either[String, PositiveSeconds] =
    Either.cond(
      !duration.isNegative && !duration.isZero && duration.getNano == 0,
      PositiveSeconds(duration),
      s"Duration should be positive and rounded to the second, found: $duration",
    )
}

object EnrichedDurations {
  import com.digitalasset.canton.config

  implicit class RichNonNegativeFiniteDurationConfig(duration: config.NonNegativeFiniteDuration) {
    def toInternal: NonNegativeFiniteDuration = checked(
      NonNegativeFiniteDuration.tryCreate(duration.asJava)
    )
  }

  implicit class RichPositiveFiniteDurationConfig(duration: config.PositiveFiniteDuration) {
    def toInternal: PositiveFiniteDuration = checked(
      PositiveFiniteDuration.tryCreate(duration.asJava)
    )
  }
}
