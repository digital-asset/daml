// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.time.{Duration, Instant}

import scala.language.implicitConversions

sealed trait LoggingValue

object LoggingValue {
  final object Empty extends LoggingValue

  final case class OfString(value: String) extends LoggingValue

  final case class OfInt(value: Int) extends LoggingValue

  final case class OfLong(value: Long) extends LoggingValue

  final case class OfIterable(sequence: Iterable[LoggingValue]) extends LoggingValue

  trait ToLoggingValue[-T] {
    def apply(value: T): LoggingValue
  }

  @inline
  implicit def from[T](value: T)(implicit toLoggingValue: ToLoggingValue[T]): LoggingValue =
    toLoggingValue(value)

  // This is not implicit because we only want to expose it for specific types.
  val ToStringToLoggingValue: ToLoggingValue[Any] = value => OfString(value.toString)

  implicit val `String to LoggingValue`: ToLoggingValue[String] = OfString(_)

  implicit val `Int to LoggingValue`: ToLoggingValue[Int] = OfInt(_)

  implicit val `Long to LoggingValue`: ToLoggingValue[Long] = OfLong(_)

  implicit val `Instant to LoggingValue`: ToLoggingValue[Instant] = ToStringToLoggingValue

  implicit val `Duration to LoggingValue`: ToLoggingValue[Duration] = ToStringToLoggingValue

  implicit def `Option[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Option[T]] = {
    case None => Empty
    case Some(value) => elementToLoggingValue(value)
  }

  implicit def `Iterable[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Iterable[T]] =
    sequence => OfIterable(sequence.view.map(elementToLoggingValue.apply))
}
