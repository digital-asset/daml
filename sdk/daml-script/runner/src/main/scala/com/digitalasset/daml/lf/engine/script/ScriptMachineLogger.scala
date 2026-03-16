// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import com.digitalasset.canton.logging.LoggerNameFromClass
import com.digitalasset.daml.lf.speedy.Pretty
import com.digitalasset.daml.lf.data.Ref.Location
import com.digitalasset.daml.lf.speedy.MachineLogger
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[lf] final class ScriptMachineLogger(
    traceLogger: Logger,
    warningLogger: Logger,
    traceCapacity: Int,
) extends MachineLogger {

  private val traceBuffer = new RingBuffer[(String, Option[Location])](traceCapacity)
  private val warningBuffer = new ArrayBuffer[(String, Option[Location])](initialSize = 10)

  def trace(message: String, location: Option[Location])(implicit ln: LoggerNameFromClass): Unit = {
    lazy val prettyLocation = Pretty.prettyLoc(location).renderWideStream.mkString
    traceLogger.debug(s"$prettyLocation: ${StringEscapeUtils.escapeJava(message)}")
    traceBuffer += (message -> location)
  }

  def warn(message: String, location: Option[Location])(implicit ln: LoggerNameFromClass): Unit = {
    lazy val prettyLocation = Pretty.prettyLoc(location).renderWideStream.mkString
    warningLogger.warn(s"$prettyLocation: $message")
    warningBuffer += (message -> location)
  }

  def traceIterator: Iterator[(String, Option[Location])] = traceBuffer.iterator
  def warningIterator: Iterator[(String, Option[Location])] = warningBuffer.iterator
}

object ScriptMachineLogger {
  def apply(): ScriptMachineLogger =
    new ScriptMachineLogger(
      traceLogger = LoggerFactory.getLogger("daml.tracelog"),
      warningLogger = LoggerFactory.getLogger("daml.warnings"),
      traceCapacity = 100,
    )
}

private final class RingBuffer[A: ClassTag](capacity: Int) {
  private val underlying = Array.ofDim[A](capacity)
  private var pos: Int = 0
  private var size: Int = 0

  def +=(value: A): Unit = {
    underlying(pos) = value
    pos = (pos + 1) % capacity
    if (size < capacity)
      size += 1
  }

  def iterator: Iterator[A] = new RingIterator(if (size < capacity) 0 else pos, size, underlying)
}

private final class RingIterator[A](ringStart: Int, ringSize: Int, buffer: Array[A])
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
