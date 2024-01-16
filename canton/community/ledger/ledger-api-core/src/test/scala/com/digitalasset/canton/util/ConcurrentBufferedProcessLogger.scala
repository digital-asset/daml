// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import java.util.concurrent.ConcurrentLinkedQueue
import scala.sys.process.ProcessLogger

// to be merged with BufferedProcessLogger from canton
class ConcurrentBufferedLogger extends ProcessLogger {
  private val buffer = new ConcurrentLinkedQueue[String]

  override def out(s: => String): Unit = buffer.add(s)
  override def err(s: => String): Unit = buffer.add(s)
  override def buffer[T](f: => T): T = f

  def output(linePrefix: String = ""): String =
    buffer.toArray.map(l => s"$linePrefix$l").mkString(System.lineSeparator)
}
