// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.bazeltools.BazelRunfiles.rlocation

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.sys.process.{Process, ProcessLogger}

object CantonConsole {

  val cantonPath = Paths.get(rlocation("canton/community_app_deploy.jar"))

  val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
  val java = s"${System.getenv("JAVA_HOME")}/bin/java${exe}"

  def run(configuration: String, bootstrapScript: String, debug: Boolean = false): Unit = {
    val cantonTmpDir = Files.createTempDirectory("canton_console")
    val configFilePath = cantonTmpDir.resolve("canton.conf")
    val bootstrapFilePath = cantonTmpDir.resolve("bootstrap.canton")
    val logFilePath = cantonTmpDir.resolve("canton.log")

    val _ = Files.write(configFilePath, configuration.getBytes(StandardCharsets.UTF_8))
    val _ = Files.write(bootstrapFilePath, bootstrapScript.getBytes(StandardCharsets.UTF_8))

    val debugOptions =
      if (debug) List("--log-file-name", logFilePath.toString, "--verbose")
      else List.empty

    val cmd =
      java ::
        "-jar" ::
        cantonPath.toString ::
        "run" ::
        bootstrapFilePath.toString ::
        "--config" ::
        configFilePath.toString ::
        debugOptions

    val process = Process(
      cmd,
      None,
    ).run(ProcessLogger { str =>
      if (debug) println(str)
    })

    if (process.exitValue() != 0) {
      throw new RuntimeException(
        s"Error running canton, logs in ${logFilePath} if debug was enabled"
      )
    }
  }
}
