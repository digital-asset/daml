// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.util.Mutex

import scala.collection.mutable
import scala.sys.process.ProcessLogger

@SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
class BufferedProcessLogger extends ProcessLogger {
  private val buffer = mutable.Buffer[String]()
  private val lock = new Mutex()

  override def out(s: => String): Unit = lock.exclusive(buffer.append(s))
  override def err(s: => String): Unit = lock.exclusive(buffer.append(s))
  override def buffer[T](f: => T): T = f

  /** Output the buffered content to a String applying an optional line prefix.
    */
  def output(linePrefix: String = ""): String = lock.exclusive(
    buffer.map(l => s"$linePrefix$l").mkString(System.lineSeparator)
  )

  def outputLines(): Seq[String] = buffer.toSeq

}
