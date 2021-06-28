// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.time.{Duration, Instant}

import com.fasterxml.jackson.core.JsonGenerator

import scala.collection.SeqView
import scala.language.implicitConversions

final case class LoggingValue(value: String) {
  def writeTo(generator: JsonGenerator): Unit = {
    generator.writeString(value)
  }
}

object LoggingValue {
  trait ToLoggingValue[-T] {
    def apply(value: T): LoggingValue
  }

  @inline
  implicit def from[T](value: T)(implicit toLoggingValue: ToLoggingValue[T]): LoggingValue =
    toLoggingValue(value)

  implicit object `String to LoggingValue` extends ToLoggingValue[String] {
    override def apply(value: String): LoggingValue =
      new LoggingValue(value)
  }

  class ToStringToLoggingValue[T] extends ToLoggingValue[T] {
    override def apply(value: T): LoggingValue =
      new LoggingValue(value.toString)
  }

  implicit val `Int to LoggingValue`: ToLoggingValue[Int] = new ToStringToLoggingValue
  implicit val `Long to LoggingValue`: ToLoggingValue[Long] = new ToStringToLoggingValue
  implicit val `Instant to LoggingValue`: ToLoggingValue[Instant] = new ToStringToLoggingValue
  implicit val `Duration to LoggingValue`: ToLoggingValue[Duration] = new ToStringToLoggingValue

  implicit def `Option[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Option[T]] = {
    case None => ""
    case Some(value) => elementToLoggingValue(value)
  }

  implicit def `SeqView[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[SeqView[T]] =
    (sequence: SeqView[T]) =>
      new LoggingValue(
        sequence.map(element => elementToLoggingValue.apply(element).value).mkString("[", ", ", "]")
      )

  implicit def `Seq[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Seq[T]] =
    (sequence: Seq[T]) => `SeqView[T] to LoggingValue`(elementToLoggingValue)(sequence.view)
}
