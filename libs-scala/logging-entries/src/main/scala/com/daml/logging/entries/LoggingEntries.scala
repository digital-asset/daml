// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging.entries

import scala.annotation.nowarn

final case class LoggingEntries(contents: Map[LoggingKey, LoggingValue]) extends AnyVal {
  def isEmpty: Boolean =
    contents.isEmpty

  def +:(entry: LoggingEntry): LoggingEntries =
    new LoggingEntries(contents + entry)

  def :+(entry: LoggingEntry): LoggingEntries =
    new LoggingEntries(contents + entry)

  def ++(other: LoggingEntries): LoggingEntries =
    new LoggingEntries(contents ++ other.contents)
}

object LoggingEntries {
  val empty: LoggingEntries = new LoggingEntries(Map.empty)

  // Private so that it doesn't conflict with the other `apply` method.
  @nowarn("msg=never used")
  private def apply(entries: Map[LoggingKey, LoggingValue]): LoggingEntries =
    new LoggingEntries(entries)

  def apply(entries: LoggingEntry*): LoggingEntries =
    new LoggingEntries(entries.toMap)

  def fromMap(entries: Map[LoggingKey, LoggingValue]): LoggingEntries =
    new LoggingEntries(entries)
}
