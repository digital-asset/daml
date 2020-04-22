// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.io.PrintStream
import java.nio.file.{Files, Path, StandardOpenOption}

import collection.JavaConverters._

trait BenchmarkReporter {

  def addReport(key: String, value: Double): Unit

  def forKey(parentKey: String)(key: String, value: Double): Unit =
    addReport(parentKey + "." + key, value)
}

object BenchmarkReporter {
  def toFile(path: Path) = new FileOutputBenchmarkReporter(path)
  def toStream(printStream: PrintStream) = new PrintStreamBenchmarkReporter(printStream)
}

class FileOutputBenchmarkReporter(path: Path) extends BenchmarkReporter {

  override def addReport(key: String, value: Double): Unit = synchronized {
    val _ = Files
      .write(path, Seq(s"$key=$value").asJava, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
}

class PrintStreamBenchmarkReporter(printStream: PrintStream) extends BenchmarkReporter {

  override def addReport(key: String, value: Double): Unit = synchronized {
    printStream.println(s"$key=$value")
  }
}
