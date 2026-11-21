// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.console.BufferedProcessLogger
import org.scalatest.matchers.should.Matchers.*

import java.io.File
import scala.sys.process.{Process, ProcessLogger}

class CommandRunner(command: String) {

  private val processLogger = new BufferedProcessLogger()

  private def checkReturn(res: Int, log: BufferedProcessLogger): Unit =
    if (res != 0)
      fail(s"""
              |$command failed:
              |${log.output("  ")}
        """.stripMargin.trim)

  def run(argument: String, cwd: Option[File], extraEnv: Seq[(String, String)] = Seq()): Unit = {
    val res = Process(s"$command $argument", cwd, extraEnv = extraEnv*) ! processLogger

    checkReturn(res, processLogger)
  }

  def run(argument: String, cwd: Option[File], processLogger: ProcessLogger): Unit = {
    val res = Process(s"$command $argument", cwd) ! processLogger
    if (res != 0) {
      fail(s"$command failed to run $argument")
    }
  }

}

/** Utility to run damlc */
class DamlBuilder()
    extends CommandRunner(
      command = (DamlBuilder.localDamlArtifactCache / "damlc" / "damlc").pathAsString
    ) {
  def build(cwd: File): Unit = {
    DamlBuilder.checkArtifactCacheExists()
    run("build ", Some(cwd), extraEnv = DamlBuilder.damlLibsEnvSpecification)
  }
}

object DamlBuilder {
  private[DamlBuilder] lazy val localDamlArtifactCache =
    better.files.File(
      System.getProperty("user.home")
    ) / ".cache" / "daml-build" / BuildInfo.damlLibrariesVersion
  private[DamlBuilder] lazy val damlLibsEnvSpecification = Seq(
    "DAML_SDK" -> localDamlArtifactCache.pathAsString
  )
  private[DamlBuilder] def checkArtifactCacheExists(): Unit =
    if (localDamlArtifactCache.notExists())
      fail(
        s"""
          |About to run the Daml compiler, but the expected Daml artifact cache directory does not exist:
          |
          |   $localDamlArtifactCache
          |
          |This usually happens when Daml was bumped to a new version, but the new Docker image has not yet been generated.
          |To fix it, please generate a new Docker image by running the `manual_update_canton_build_docker_image` job on CI.
          |""".stripMargin
      )
}
