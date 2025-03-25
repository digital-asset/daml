// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File as BFile
import cats.syntax.either.*
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.nio.file.{Path, Paths}
import scala.sys.process.*
import scala.util.Try

/** A utility to run an external console for integration tests. Can be used to run a script or a
  * bootstrap script.
  */
final case class UseExternalConsole(
    remoteConfigOrConfigFile: Either[CantonConfig, BFile],
    cantonBin: String,
    configDirectory: String = "tmp/",
    logDirectory: String = "log/",
    resourceDirectory: String = "enterprise/app/src/test/resources",
    extraConfigs: Seq[String] = Nil,
    extraEnv: Seq[(String, String)] = Nil,
    fileNameHint: String,
    removeConfigPaths: Set[(String, Option[(String, Any)])] = Set.empty,
) {
  val logFile: Path = Paths.get(logDirectory).resolve(s"external-console-$fileNameHint.log")
  def getConfigFile(): Path =
    remoteConfigOrConfigFile match {
      case Left(config) =>
        val configFile: Path =
          Paths.get(configDirectory).resolve(s"external-console-$fileNameHint.conf")
        CantonConfig.save(config, configFile.toString, removeConfigPaths)
        configFile
      case Right(configFile) => configFile.path
    }

  def runScript(script: String)(implicit elc: ErrorLoggingContext): String = {
    val configFile = getConfigFile()
    val scriptFile = Paths.get(resourceDirectory).resolve(script)
    run(
      UseExternalConsole.runScriptCommand(
        cantonBin,
        configFile.toString,
        logFile.toString,
        scriptFile.toString,
        extraConfigs,
      )
    )
  }

  def runBootstrapScript(
      bootstrapScript: String
  )(implicit elc: ErrorLoggingContext): Unit = {
    val configFile = getConfigFile()
    run(
      UseExternalConsole.bootstrapCommand(
        cantonBin,
        configFile.toString,
        logFile.toString,
        extraConfigs = extraConfigs,
        bootstrapScript,
      )
    )
  }

  private def run(command: String)(implicit
      elc: ErrorLoggingContext
  ): String = {
    implicit val traceContext: TraceContext = elc.traceContext

    val cantonBinFile = BFile(cantonBin)

    ErrorUtil.requireArgument(
      cantonBinFile.exists,
      s"$cantonBinFile doesn't exist. Before being able to run these tests locally, you need to execute `sbt bundle`",
    )
    val processLogger = new BufferedProcessLogger
    elc.logger.info(s"Running $command")

    val process = Process(command, None, extraEnv*)

    val result = Try(process.!!(processLogger)).toEither.valueOr { _ =>
      throw new RuntimeException(
        s"Failed to run bootstrap script because: \n ${processLogger.output()}"
      )
    }

    elc.logger.info(s"Running command has succeeded and returned $result")
    result
  }

}

object UseExternalConsole {
  // turn off cache-dir to avoid compilation errors due to concurrent cache access
  private val cacheTurnOff =
    s"community/app/src/test/resources/config-snippets/disable-ammonite-cache.conf"

  private def bootstrapCommand(
      cantonBin: String,
      configFile: String,
      logFile: String,
      extraConfigs: Seq[String],
      bootstrapScript: String,
  ): String =
    Seq(
      s"$cantonBin",
      // turn off cache-dir to avoid compilation errors due to concurrent cache access
      s"--config ${UseExternalConsole.cacheTurnOff}",
      s"${extraConfigs.mkString(" ")}",
      s"--log-truncate --log-file-appender flat --log-file-name $logFile",
      s"--config $configFile",
      "--no-tty",
      s"--bootstrap $bootstrapScript",
      "--log-level-canton DEBUG",
    ).mkString(" ")

  private def runScriptCommand(
      cantonBin: String,
      configFile: String,
      logFile: String,
      script: String,
      extraConfigs: Seq[String],
  ): String =
    Seq(
      s"$cantonBin",
      s"run $script",
      s"${extraConfigs.mkString(" ")}",
      s"--log-file-appender flat --log-file-name $logFile",
      s"--config $configFile",
      "--log-level-canton DEBUG",
    ).mkString(" ")
}
