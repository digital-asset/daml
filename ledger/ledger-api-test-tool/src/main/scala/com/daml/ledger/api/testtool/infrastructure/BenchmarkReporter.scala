// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import collection.JavaConverters._

trait BenchmarkReporter {

  protected def addReport(key: String, value: Double): Unit

  def forKey(parentKey: String)(key: String, value: Double): Unit =
    addReport(parentKey + "." + key, value)

}

object BenchmarkReporter {
  val toFile = new FileOutputBenchmarkReporter(Paths.get("benchmark.dat"))
}

class FileOutputBenchmarkReporter(path: Path) extends BenchmarkReporter {

  override def addReport(key: String, value: Double): Unit = synchronized {
    val _ = Files.write(path, Seq(s"$key=$value").asJava, StandardOpenOption.APPEND)
  }
}
