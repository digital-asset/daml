// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.time.{Duration, Instant}

import com.fasterxml.jackson.core.JsonGenerator

import scala.collection.SeqView
import scala.language.implicitConversions

sealed trait LoggingValue {
  def writeTo(generator: JsonGenerator): Unit
}

object LoggingValue {
  trait ToLoggingValue[-T] {
    def apply(value: T): LoggingValue
  }

  @inline
  implicit def from[T](value: T)(implicit toLoggingValue: ToLoggingValue[T]): LoggingValue =
    toLoggingValue(value)

  private final class ToStringToLoggingValue[T] extends ToLoggingValue[T] {
    override def apply(value: T): LoggingValue =
      `String to LoggingValue`(value.toString)
  }

  implicit val `String to LoggingValue`: ToLoggingValue[String] = value =>
    new LoggingValue {
      override def writeTo(generator: JsonGenerator): Unit =
        generator.writeString(value)
    }

  implicit val `Int to LoggingValue`: ToLoggingValue[Int] = value =>
    new LoggingValue {
      override def writeTo(generator: JsonGenerator): Unit =
        generator.writeNumber(value)
    }

  implicit val `Long to LoggingValue`: ToLoggingValue[Long] = value =>
    new LoggingValue {
      override def writeTo(generator: JsonGenerator): Unit =
        generator.writeNumber(value)
    }

  implicit val `Instant to LoggingValue`: ToLoggingValue[Instant] = new ToStringToLoggingValue

  implicit val `Duration to LoggingValue`: ToLoggingValue[Duration] = new ToStringToLoggingValue

  implicit def `Option[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Option[T]] = {
    case None =>
      new LoggingValue {
        override def writeTo(generator: JsonGenerator): Unit =
          generator.writeNull()
      }
    case Some(value) => elementToLoggingValue(value)
  }

  implicit def `SeqView[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[SeqView[T]] =
    (sequence: SeqView[T]) =>
      new LoggingValue {
        override def writeTo(generator: JsonGenerator): Unit = {
          generator.writeStartArray()
          sequence.foreach { element =>
            elementToLoggingValue.apply(element).writeTo(generator)
          }
          generator.writeEndArray()
        }
      }

  implicit def `Seq[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Seq[T]] =
    (sequence: Seq[T]) => `SeqView[T] to LoggingValue`(elementToLoggingValue)(sequence.view)
}
