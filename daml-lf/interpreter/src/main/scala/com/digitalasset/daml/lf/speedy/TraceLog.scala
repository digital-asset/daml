// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.Ref.Location
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import org.apache.commons.text.StringEscapeUtils

private[lf] trait TraceLog {
  def add(message: String, optLocation: Option[Location])(implicit
      loggingContext: LoggingContext
  ): Unit
  def iterator: Iterator[(String, Option[Location])]
}

private[lf] final class RingBufferTraceLog(logger: ContextualizedLogger, capacity: Int)
    extends TraceLog {

  private[this] val buffer = Array.ofDim[(String, Option[Location])](capacity)
  private[this] var pos: Int = 0
  private[this] var size: Int = 0

  def add(message: String, optLocation: Option[Location])(implicit
      loggingContext: LoggingContext
  ): Unit = {
    logger.debug(
      StringEscapeUtils.escapeJava(Pretty.prettyLoc(optLocation).renderWideStream.mkString) + ": " +
        StringEscapeUtils.escapeJava(message)
    )
    buffer(pos) = (message, optLocation)
    pos = (pos + 1) % capacity
    if (size < capacity)
      size += 1
  }

  def iterator: Iterator[(String, Option[Location])] =
    new RingIterator(if (size < capacity) 0 else pos, size, buffer)
}

private[this] final class RingIterator[A](ringStart: Int, ringSize: Int, buffer: Array[A])
    extends Iterator[A] {
  private[this] var pos: Int = ringStart
  private[this] var first = true
  private[this] def nextPos: Int = (pos + 1) % ringSize
  def hasNext: Boolean = ringSize != 0 && (first || pos != ringStart)
  def next(): A = {
    val x = buffer(pos)
    first = false
    pos = nextPos
    x
  }
}
