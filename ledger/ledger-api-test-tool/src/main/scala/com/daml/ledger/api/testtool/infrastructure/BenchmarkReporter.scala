// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.concurrent.atomic.AtomicReference

trait BenchmarkReporter {

  protected def addReport(key: String, value: Double): Unit

  def forKey(parentKey: String)(key: String, value: Double): Unit =
    addReport(parentKey + "." + key, value)

}

object BenchmarkReporter {
  val toFile = new FileOutputBenchmarkReporter("benchmark.dat")
}

class FileOutputBenchmarkReporter(filename: String) extends BenchmarkReporter {

  import java.io._

  private val outfile = new AtomicReference[Option[PrintWriter]](None)

  override def addReport(key: String, value: Double): Unit = synchronized {
    val pw = outfile.get() match {
      case None =>
        val pw = new PrintWriter(new File(filename))
        outfile.set(Some(pw))
        pw
      case Some(pw) => pw
    }
    pw.write(key)
    pw.write("=")
    pw.write(value.toString)
    pw.write("\n")
    pw.flush()
  }
}
