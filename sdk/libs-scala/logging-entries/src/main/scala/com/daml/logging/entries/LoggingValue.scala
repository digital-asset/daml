// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging.entries

import spray.json.JsValue

import scala.language.implicitConversions

sealed trait LoggingValue

object LoggingValue {
  object Empty extends LoggingValue

  object False extends LoggingValue

  object True extends LoggingValue

  final case class OfString(value: String) extends LoggingValue {
    def truncated(maxLength: Int): OfString =
      if (value.length > maxLength)
        OfString(value.substring(0, maxLength - 1) + "…")
      else
        this
  }

  final case class OfInt(value: Int) extends LoggingValue

  final case class OfLong(value: Long) extends LoggingValue

  final case class OfIterable(sequence: Iterable[LoggingValue]) extends LoggingValue

  final case class Nested(entries: LoggingEntries) extends LoggingValue

  object Nested {
    def fromEntries(entries: LoggingEntry*): Nested = Nested(LoggingEntries(entries: _*))
  }

  final case class OfJson(json: JsValue) extends LoggingValue

  @inline
  implicit def from[T](value: T)(implicit toLoggingValue: ToLoggingValue[T]): LoggingValue =
    toLoggingValue.toLoggingValue(value)
}
