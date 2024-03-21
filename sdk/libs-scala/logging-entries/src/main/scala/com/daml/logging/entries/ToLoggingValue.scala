// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging.entries

import spray.json.JsValue

import java.time.{Duration, Instant}

trait ToLoggingValue[-T] {
  def toLoggingValue(value: T): LoggingValue
}

object ToLoggingValue {
  // This is not implicit because we only want to expose it for specific types.
  val ToStringToLoggingValue: ToLoggingValue[Any] = value => LoggingValue.OfString(value.toString)

  implicit val `JsValue to LoggingValue`: ToLoggingValue[JsValue] = LoggingValue.OfJson(_)

  implicit val `String to LoggingValue`: ToLoggingValue[String] = LoggingValue.OfString(_)

  implicit val `Boolean to LoggingValue`: ToLoggingValue[Boolean] = {
    case false => LoggingValue.False
    case true => LoggingValue.True
  }

  implicit val `Int to LoggingValue`: ToLoggingValue[Int] = LoggingValue.OfInt(_)

  implicit val `Long to LoggingValue`: ToLoggingValue[Long] = LoggingValue.OfLong(_)

  implicit val `Instant to LoggingValue`: ToLoggingValue[Instant] = ToStringToLoggingValue

  implicit val `Duration to LoggingValue`: ToLoggingValue[Duration] = ToStringToLoggingValue

  implicit def `Option[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Option[T]] = {
    case None => LoggingValue.Empty
    case Some(value) => elementToLoggingValue.toLoggingValue(value)
  }

  implicit def `Iterable[T] to LoggingValue`[T](implicit
      elementToLoggingValue: ToLoggingValue[T]
  ): ToLoggingValue[Iterable[T]] =
    sequence => LoggingValue.OfIterable(sequence.view.map(elementToLoggingValue.toLoggingValue))
}
