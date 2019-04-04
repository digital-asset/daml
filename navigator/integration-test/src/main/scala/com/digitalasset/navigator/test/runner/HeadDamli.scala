// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.test.runner

import java.io.File
import java.nio.file.Files

import scala.sys.error
import scala.sys.process.Process

/**
  * Run the HEAD version of damli from source, to create a DAR file from a DAML file.
  */
object HeadDamli {
  private val packageName = "Test"

  def run(damlPath: String): (File, Unit => Unit) = {
    val damlFile = new File(damlPath)

    val tempDirectory = Files.createTempDirectory("navigator-integration-test").toFile
    val darFile = new File(tempDirectory, s"$packageName.dar")

    tempDirectory.mkdirs()
    tempDirectory.deleteOnExit()
    val shutdown: Unit => Unit = _ => { tempDirectory.delete(); () }

    // DAML -> DAR
    val exitCode = Process(
      s"bazel run damli -- package $damlPath $packageName --output ${darFile.getAbsolutePath}").!
    if (exitCode != 0) {
      shutdown(())
      error(s"Dar packager: error while running DAMLI package for $damlPath: exit code $exitCode")
    }

    (darFile, shutdown)
  }
}
