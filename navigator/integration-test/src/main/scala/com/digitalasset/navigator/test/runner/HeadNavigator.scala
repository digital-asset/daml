// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test.runner

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import com.daml.navigator.test.runner.Runner.LazyProcessLogger

/**
  * Run Navigator from source.
  */
object HeadNavigator extends LazyLogging {
  def findNavigatorJar(navigatorDir: String): File = {
    val navigatorPattern = """navigator-.*jar""".r

    val distDir = new File(s"$navigatorDir/dist")
    require(
      distDir.exists && distDir.isDirectory,
      s"Navigator dist directory does not exist. Run the navigator build first.")

    val files = distDir.listFiles
      .filter(f => f.isFile && f.canRead)
      .filter(f => navigatorPattern.findFirstIn(f.getName).isDefined)
    require(
      files.length > 0,
      s"No navigator jar file found in $distDir. Run the navigator build first.")
    require(
      files.length < 2,
      s"Multiple navigator jar files found in $distDir: ${files.mkString(" ")}. Delete old ones.")
    files(0)
  }

  def runAsync(
      navConfPAth: String,
      navigatorDir: String,
      navigatorPort: Int,
      sandboxPort: Int): Unit => Unit = {
    val navigatorJar = findNavigatorJar(navigatorDir)
    val commands = List(
      "java",
      "-jar",
      navigatorJar.toString,
      "server",
      "localhost",
      s"$sandboxPort",
      "--config-file",
      navConfPAth,
      "--port",
      s"$navigatorPort"
    )
    val process = Runner.executeAsync(commands, Some(new LazyProcessLogger("[navigator] ")))

    val shutdown = (_: Unit) => {
      if (process.isAlive()) {
        logger.info("Shutting down Navigator")
        process.destroy()
      }
    }

    shutdown
  }
}
