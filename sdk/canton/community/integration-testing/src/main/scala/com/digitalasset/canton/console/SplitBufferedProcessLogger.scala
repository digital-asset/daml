// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import scala.collection.mutable
import scala.sys.process.ProcessLogger

@SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
class SplitBufferedProcessLogger extends ProcessLogger {
  private val stdoutBuffer = mutable.Buffer[String]()
  private val stderrBuffer = mutable.Buffer[String]()

  override def out(s: => String): Unit = synchronized(stdoutBuffer.append(s))
  override def err(s: => String): Unit = synchronized(stderrBuffer.append(s))
  override def buffer[T](f: => T): T = f

  /** Output the buffered stdout content to a String applying an optional line prefix.
    */
  def output(linePrefix: String = ""): String = synchronized(
    stdoutBuffer.map(l => s"$linePrefix$l").mkString(System.lineSeparator)
  )

  /** Output the buffered stderr content to a String applying an optional line prefix.
    */
  def error(linePrefix: String = ""): String = synchronized(
    stderrBuffer.map(l => s"$linePrefix$l").mkString(System.lineSeparator)
  )
}
