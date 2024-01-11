// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.syntax.either.*
import pureconfig.error.{CannotConvert, FailureReason}
import pureconfig.{ConfigReader, ConfigWriter}
import slick.jdbc.{GetResult, SetParameter}

import java.io.File

/** Encapsulates those classes and their utility methods which enforce a given invariant via the use of require. */
object RequireTypes {
  final case class InvariantViolation(message: String)

  sealed abstract case class Port(private val n: Int) extends Ordered[Port] {
    def unwrap: Int = n
    require(
      n >= Port.minValidPort && n <= Port.maxValidPort,
      s"Unable to create Port as value $n was given, but only values between ${Port.minValidPort} and ${Port.maxValidPort} (inclusive) are allowed.",
    )

    def +(n: Int): Port = Port.tryCreate(this.unwrap + n)
    override def compare(that: Port): Int = this.unwrap compare that.unwrap

    override def toString: String = n.toString
  }

  object Port {
    val (minValidPort, maxValidPort) = (0, scala.math.pow(2, 16).toInt - 1)
    private[this] def apply(n: Int): Port =
      throw new UnsupportedOperationException("Use create or tryCreate methods")

    def create(n: Int): Either[InvariantViolation, Port] = {
      Either.cond(
        n >= Port.minValidPort && n <= Port.maxValidPort,
        new Port(n) {},
        InvariantViolation(
          s"Unable to create Port as value $n was given, but only values between ${Port.minValidPort} and ${Port.maxValidPort} are allowed."
        ),
      )
    }

    def tryCreate(n: Int): Port = {
      new Port(n) {}
    }

    lazy implicit val portReader: ConfigReader[Port] = {
      ConfigReader.fromString[Port] { str =>
        def err(message: String) =
          CannotConvert(str, Port.getClass.getName, message)

        Either
          .catchOnly[NumberFormatException](str.toInt)
          .leftMap[FailureReason](error => err(error.getMessage))
          .flatMap(n => create(n).leftMap(_ => InvalidPort(n)))
      }
    }

    implicit val portWriter: ConfigWriter[Port] = ConfigWriter.toString(x => x.unwrap.toString)

    final case class InvalidPort(n: Int) extends FailureReason {
      override def description: String =
        s"Unable to create Port as value $n was given, but only values between ${Port.minValidPort} and ${Port.maxValidPort} are allowed"
    }

    /** This instructs the server to automatically choose a free port.
      */
    lazy val Dynamic = Port.tryCreate(0)
  }

  sealed trait RefinedNumeric[T] extends Ordered[RefinedNumeric[T]] {
    protected def value: T
    implicit def num: Numeric[T]

    def unwrap: T = value

    override def compare(that: RefinedNumeric[T]): Int = num.compare(value, that.value)

    override def toString: String = value.toString
  }

  final case class NonNegativeNumeric[T] private (value: T)(implicit val num: Numeric[T])
      extends RefinedNumeric[T] {
    import num.*

    def increment: PositiveNumeric[T] = PositiveNumeric.tryCreate(value + num.one)

    def +(other: NonNegativeNumeric[T]): NonNegativeNumeric[T] =
      NonNegativeNumeric.tryCreate(value + other.value)
    def *(other: NonNegativeNumeric[T]): NonNegativeNumeric[T] =
      NonNegativeNumeric.tryCreate(value * other.value)
    def tryAdd(other: T): NonNegativeNumeric[T] = NonNegativeNumeric.tryCreate(value + other)
  }

  object NonNegativeNumeric {
    def tryCreate[T](t: T)(implicit num: Numeric[T]): NonNegativeNumeric[T] =
      create(t).valueOr(err => throw new IllegalArgumentException(err.message))

    def create[T](
        t: T
    )(implicit num: Numeric[T]): Either[InvariantViolation, NonNegativeNumeric[T]] = {
      Either.cond(
        num.compare(t, num.zero) >= 0,
        NonNegativeNumeric(t),
        InvariantViolation(
          s"Received the negative $t as argument, but we require a non-negative value here."
        ),
      )
    }

    implicit val readNonNegativeLong: GetResult[NonNegativeLong] = GetResult { r =>
      NonNegativeLong.tryCreate(r.nextLong())
    }

    implicit val readNonNegativeInt: GetResult[NonNegativeInt] = GetResult { r =>
      NonNegativeInt.tryCreate(r.nextInt())
    }

    implicit val readNonNegativeIntOption: GetResult[Option[NonNegativeInt]] = GetResult { r =>
      r.nextIntOption().map(NonNegativeInt.tryCreate)
    }

    implicit def writeNonNegativeNumeric[T](implicit
        f: SetParameter[T]
    ): SetParameter[NonNegativeNumeric[T]] =
      (s, pp) => {
        pp >> s.unwrap
      }

    implicit def writeNonNegativeNumericOption[T](implicit
        f: SetParameter[Option[T]]
    ): SetParameter[Option[NonNegativeNumeric[T]]] = (s, pp) => {
      pp >> s.map(_.unwrap)
    }

    implicit def nonNegativeNumericReader[T](implicit
        num: Numeric[T]
    ): ConfigReader[NonNegativeNumeric[T]] = {
      ConfigReader.fromString[NonNegativeNumeric[T]] { str =>
        def err(message: String) =
          CannotConvert(str, NonNegativeNumeric.getClass.getName, message)

        num
          .parseString(str)
          .toRight[FailureReason](err("Cannot convert `str` to numeric"))
          .flatMap(n => Either.cond(num.compare(n, num.zero) >= 0, tryCreate(n), NegativeValue(n)))
      }
    }

    implicit def nonNegativeNumericWriter[T]: ConfigWriter[NonNegativeNumeric[T]] =
      ConfigWriter.toString(x => x.unwrap.toString)

    final case class NegativeValue[T](t: T) extends FailureReason {
      override def description: String =
        s"The value you gave for this configuration setting ($t) was negative, but we require a non-negative value for this configuration setting"
    }
  }

  type NonNegativeInt = NonNegativeNumeric[Int]

  object NonNegativeInt {
    lazy val zero: NonNegativeInt = NonNegativeInt.tryCreate(0)
    lazy val one: NonNegativeInt = NonNegativeInt.tryCreate(1)
    lazy val maxValue: NonNegativeInt = NonNegativeInt.tryCreate(Int.MaxValue)

    def create(n: Int): Either[InvariantViolation, NonNegativeInt] = NonNegativeNumeric.create(n)
    def tryCreate(n: Int): NonNegativeInt = NonNegativeNumeric.tryCreate(n)
  }

  type NonNegativeLong = NonNegativeNumeric[Long]

  object NonNegativeLong {
    lazy val zero: NonNegativeLong = NonNegativeLong.tryCreate(0)
    lazy val one: NonNegativeLong = NonNegativeLong.tryCreate(1)
    lazy val maxValue: NonNegativeLong = NonNegativeLong.tryCreate(Long.MaxValue)

    def create(n: Long): Either[InvariantViolation, NonNegativeLong] = NonNegativeNumeric.create(n)

    def tryCreate(n: Long): NonNegativeLong = NonNegativeNumeric.tryCreate(n)
  }

  final case class PositiveNumeric[T] private (value: T)(implicit val num: Numeric[T])
      extends RefinedNumeric[T] {
    import num.*

    def +(other: PositiveNumeric[T]): PositiveNumeric[T] =
      PositiveNumeric.tryCreate(value + other.value)

    def +(other: NonNegativeNumeric[T]): PositiveNumeric[T] =
      PositiveNumeric.tryCreate(value + other.value)

    def *(other: PositiveNumeric[T]): PositiveNumeric[T] =
      PositiveNumeric.tryCreate(value * other.value)

    def increment: PositiveNumeric[T] = PositiveNumeric.tryCreate(value + num.one)
    def decrement: NonNegativeNumeric[T] = NonNegativeNumeric.tryCreate(value - num.one)

    def tryAdd(other: T): PositiveNumeric[T] = PositiveNumeric.tryCreate(value + other)

    def toNonNegative: NonNegativeNumeric[T] =
      NonNegativeNumeric.tryCreate(value) // always possible to convert positive to non negative num
  }

  type PositiveInt = PositiveNumeric[Int]

  object PositiveInt {
    def create(n: Int): Either[InvariantViolation, PositiveInt] = PositiveNumeric.create(n)
    def tryCreate(n: Int): PositiveInt = PositiveNumeric.tryCreate(n)

    lazy val one: PositiveInt = PositiveInt.tryCreate(1)
    lazy val two: PositiveInt = PositiveInt.tryCreate(2)
    lazy val MaxValue: PositiveInt = PositiveInt.tryCreate(Int.MaxValue)
  }

  type PositiveLong = PositiveNumeric[Long]

  object PositiveLong {
    def create(n: Long): Either[InvariantViolation, PositiveLong] = PositiveNumeric.create(n)

    def tryCreate(n: Long): PositiveLong = PositiveNumeric.tryCreate(n)

    lazy val one: PositiveLong = PositiveLong.tryCreate(1)
    lazy val MaxValue: PositiveLong = PositiveLong.tryCreate(Long.MaxValue)
  }

  type PositiveDouble = PositiveNumeric[Double]
  object PositiveDouble {
    def create(n: Double): Either[InvariantViolation, PositiveDouble] = PositiveNumeric.create(n)
    def tryCreate(n: Double): PositiveDouble = PositiveNumeric.tryCreate(n)
  }

  object PositiveNumeric {
    def tryCreate[T](t: T)(implicit num: Numeric[T]): PositiveNumeric[T] =
      create(t).valueOr(err => throw new IllegalArgumentException(err.message))

    def create[T](
        t: T
    )(implicit num: Numeric[T]): Either[InvariantViolation, PositiveNumeric[T]] = {
      Either.cond(
        num.compare(t, num.zero) > 0,
        PositiveNumeric(t),
        InvariantViolation(
          s"Received the non-positive $t as argument, but we require a positive value here."
        ),
      )
    }

    implicit def positiveNumericReader[T](implicit
        num: Numeric[T]
    ): ConfigReader[PositiveNumeric[T]] = {
      ConfigReader.fromString[PositiveNumeric[T]] { str =>
        def err(message: String) =
          CannotConvert(str, PositiveNumeric.getClass.getName, message)

        num
          .parseString(str)
          .toRight[FailureReason](err("Cannot convert `str` to numeric"))
          .flatMap(n =>
            Either.cond(num.compare(n, num.zero) >= 0, tryCreate(n), NonPositiveValue(n))
          )
      }
    }

    implicit def readPositiveDouble: GetResult[PositiveDouble] = GetResult { r =>
      PositiveNumeric.tryCreate(r.nextDouble())
    }

    implicit def writePositiveNumeric[T](implicit
        f: SetParameter[T]
    ): SetParameter[PositiveNumeric[T]] =
      (s, pp) => {
        pp >> s.unwrap
      }

    implicit def positiveNumericWriter[T]: ConfigWriter[PositiveNumeric[T]] =
      ConfigWriter.toString(x => x.unwrap.toString)

    final case class NonPositiveValue[T](t: T) extends FailureReason {
      override def description: String =
        s"The value you gave for this configuration setting ($t) was non-positive, but we require a positive value for this configuration setting"
    }
  }

  final case class NegativeNumeric[T] private (value: T)(implicit val num: Numeric[T])
      extends RefinedNumeric[T]

  object NegativeNumeric {
    def tryCreate[T: Numeric](t: T): NegativeNumeric[T] = NegativeNumeric(t)
  }

  type NegativeLong = NegativeNumeric[Long]

  object NegativeLong {
    val MinValue: NegativeLong = NegativeNumeric.tryCreate(Long.MinValue)

    def tryCreate(v: Long): NegativeLong = NegativeNumeric.tryCreate(v)
    def create(n: Long): Either[InvariantViolation, NonNegativeLong] = NonNegativeNumeric.create(n)
  }

  sealed abstract case class ExistingFile(private val file: File) {
    def unwrap: File = file
    require(file.exists(), s"Unable to create ExistingFile as non-existing file $file was given.")
  }

  object ExistingFile {
    private[this] def apply(file: File): ExistingFile =
      throw new UnsupportedOperationException("Use create or tryCreate methods")

    def create(file: File): Either[InvariantViolation, ExistingFile] = {
      Either.cond(
        file.exists,
        new ExistingFile(file) {},
        InvariantViolation(
          s"The specified file $file does not exist/was not found. Please specify an existing file"
        ),
      )
    }

    def tryCreate(file: File): ExistingFile = {
      new ExistingFile(file) {}
    }

    def tryCreate(path: String): ExistingFile = {
      new ExistingFile(new File(path)) {}
    }

    lazy implicit val existingFileReader: ConfigReader[ExistingFile] = {
      ConfigReader.fromString[ExistingFile] { str =>
        def err(message: String) =
          CannotConvert(str, ExistingFile.getClass.getName, message)

        Either
          .catchOnly[NullPointerException](new File(str))
          .leftMap[FailureReason](error => err(error.getMessage))
          .flatMap(f => create(f).leftMap(_ => NonExistingFile(f)))
      }
    }

    final case class NonExistingFile(file: File) extends FailureReason {
      override def description: String =
        s"The specified file $file does not exist/was not found. Please specify an existing file"
    }
  }

}
