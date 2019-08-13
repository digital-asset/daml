// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.test.runner

import com.digitalasset.damlc.{Runner => Damlc}
import java.io.File
import java.nio.file.Files

import com.digitalasset.navigator.test.runner.Runner.LazyProcessLogger
import com.typesafe.scalalogging.LazyLogging

import scala.sys.error

/**
  * Run a packaged version of damlc to create a DAR file from a DAML file.
  * Update the project dependencies to change the damlc version.
  */
object PackagedDamlc extends LazyLogging {
  private val packageName = "Main"

  private def damlToDar(damlFile: File, darFile: File): Unit = {
    val damlcBinary = Damlc.extract()
    val command = List(
      damlcBinary.toString,
      "package",
      damlFile.toString,
      packageName,
      "--output",
      darFile.toString
    )
    val exitCode = Runner.execute(command, Some(new LazyProcessLogger("[damlc] ")))
    if (exitCode != 0) {
      error(s"Error while running DAMLC for ${damlFile.toString}: exit code $exitCode")
    }
  }

  def run(damlPath: String): (List[File], Unit => Unit) = {
    val damlFile = new File(damlPath)

    val tempDirectory = Files.createTempDirectory("navigator-integration-test").toFile
    val darFile = new File(tempDirectory, packageName + ".dar")

    tempDirectory.deleteOnExit()
    val shutdown: Unit => Unit = _ => { tempDirectory.delete(); () }

    damlToDar(damlFile, darFile)

    (tempDirectory.listFiles().toList, shutdown)
  }
}
