// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.time.{Duration, Instant}

import com.fasterxml.jackson.core.JsonGenerator

import scala.collection.SeqView
import scala.language.implicitConversions

trait LoggingValue {
  def writeTo(generator: JsonGenerator): Unit
}

object LoggingValue {
  final class FromString(value: String) extends LoggingValue {
    override def writeTo(generator: JsonGenerator): Unit =
      generator.writeString(value)
  }

  trait ToLoggingValue[-T] {
    def apply(value: T): LoggingValue
  }

  @inline
  implicit def from[T](value: T)(implicit toLoggingValue: ToLoggingValue[T]): LoggingValue =
    toLoggingValue(value)

  private final class ToStringToLoggingValue[T] extends ToLoggingValue[T] {
    override def apply(value: T): LoggingValue =
      new FromString(value.toString)
  }

  implicit val `String to LoggingValue`: ToLoggingValue[String] = new FromString(_)

  implicit val `Int to LoggingValue`: ToLoggingValue[Int] =
    value => generator => generator.writeNumber(value)

  implicit val `Long to LoggingValue`: ToLoggingValue[Long] =
    value => generator => generator.writeNumber(value)

  implicit val `Instant to LoggingValue`: ToLoggingValue[Instant] = new ToStringToLoggingValue

  implicit val `Duration to LoggingValue`: ToLoggingValue[Duration] = new ToStringToLoggingValue

  implicit def `Option[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Option[T]] = {
    case None => generator => generator.writeNull()
    case Some(value) => elementToLoggingValue(value)
  }

  implicit def `SeqView[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[SeqView[T]] =
    (sequence: SeqView[T]) => `Iterable[T] to LoggingValue`(elementToLoggingValue).apply(sequence)

  implicit def `Iterable[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Iterable[T]] =
    sequence =>
      generator => {
        generator.writeStartArray()
        sequence.foreach { element =>
          elementToLoggingValue.apply(element).writeTo(generator)
        }
        generator.writeEndArray()
      }
}
