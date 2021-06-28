// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import net.logstash.logback.argument.StructuredArgument
import org.slf4j.Marker

final class LoggingEntries private (
    private[logging] val contents: Map[LoggingKey, LoggingValue]
) extends AnyVal {
  def isEmpty: Boolean =
    contents.isEmpty

  def :+(entry: LoggingEntry): LoggingEntries =
    new LoggingEntries(contents + entry)

  def ++(other: LoggingEntries): LoggingEntries =
    new LoggingEntries(contents ++ other.contents)

  private[logging] def loggingMarker: Marker with StructuredArgument =
    new LoggingMarker(contents)
}

object LoggingEntries {
  val empty: LoggingEntries = new LoggingEntries(Map.empty)

  def apply(entries: LoggingEntry*): LoggingEntries =
    new LoggingEntries(entries.toMap)

  def fromIterator(entries: Iterator[LoggingEntry]): LoggingEntries =
    new LoggingEntries(entries.toMap)
}
