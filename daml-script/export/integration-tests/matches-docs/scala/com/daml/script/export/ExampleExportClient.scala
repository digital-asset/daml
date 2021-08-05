// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.Duration

import com.daml.fs.Utils.deleteRecursively
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.engine.script.{RunnerConfig, RunnerMain}

case class ExampleExportClientConfig(
    darPath: File,
    targetPort: Int,
    outputExportDaml: Path,
    outputArgsJson: Path,
    outputDamlYaml: Path,
)

object ExampleExportClientConfig {
  def parse(args: Array[String]): Option[ExampleExportClientConfig] =
    parser.parse(
      args,
      ExampleExportClientConfig(
        darPath = null,
        targetPort = -1,
        outputExportDaml = null,
        outputArgsJson = null,
        outputDamlYaml = null,
      ),
    )

  private def parseExportOut(
      envVar: String
  ): Either[String, ExampleExportClientConfig => ExampleExportClientConfig] = {
    envVar.split(" ").map(s => Paths.get(s)) match {
      case Array(export_daml, args_json, daml_yaml) =>
        Right(c =>
          c.copy(
            outputExportDaml = export_daml,
            outputArgsJson = args_json,
            outputDamlYaml = daml_yaml,
          )
        )
      case _ => Left("Environment variable EXPORT_OUT must contain three paths")
    }
  }

  private val parser = new scopt.OptionParser[ExampleExportClientConfig]("script-export") {
    help("help")
      .text("Show this help message.")
    opt[Int]("target-port")
      .required()
      .action((x, c) => c.copy(targetPort = x))
      .text("Daml ledger port to connect to.")
    opt[String]("output")
      .hidden()
      .withFallback(() => sys.env.getOrElse("EXPORT_OUT", ""))
      .validate(x => parseExportOut(x).map(_ => ()))
      .action { (x, c) =>
        parseExportOut(x) match {
          case Left(msg) =>
            throw new RuntimeException(s"Failed to validate EXPORT_OUT environment variable: $msg")
          case Right(f) => f(c)
        }
      }
    arg[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the initialization script")
  }
}

object ExampleExportClient {
  def main(args: Array[String]): Unit = {
    ExampleExportClientConfig.parse(args) match {
      case Some(clientConfig) => main(clientConfig)
      case None => sys.exit(1)
    }
  }
  def main(clientConfig: ExampleExportClientConfig): Unit = {
    RunnerMain.main(
      RunnerConfig(
        darPath = clientConfig.darPath,
        scriptIdentifier = "ScriptExample:initializeFixed",
        ledgerHost = Some("localhost"),
        ledgerPort = Some(clientConfig.targetPort),
        participantConfig = None,
        timeMode = Some(RunnerConfig.DefaultTimeMode),
        commandTtl = Duration.ofSeconds(30L),
        inputFile = None,
        outputFile = None,
        accessTokenFile = None,
        tlsConfig = TlsConfiguration(false, None, None, None),
        jsonApi = false,
        maxInboundMessageSize = RunnerConfig.DefaultMaxInboundMessageSize,
        applicationId = None,
      )
    )
    withTemporaryDirectory { outputPath =>
      Main.main(
        Config.Empty.copy(
          ledgerHost = "localhost",
          ledgerPort = clientConfig.targetPort,
          parties = Seq("Alice", "Bob"),
          exportType = Some(
            Config.EmptyExportScript.copy(
              sdkVersion = "0.0.0",
              outputPath = outputPath,
            )
          ),
        )
      )
      moveFile(outputPath.resolve("Export.daml").toFile, clientConfig.outputExportDaml.toFile)
      moveFile(outputPath.resolve("args.json").toFile, clientConfig.outputArgsJson.toFile)
      moveFile(outputPath.resolve("daml.yaml").toFile, clientConfig.outputDamlYaml.toFile)
    }
  }

  private def moveFile(src: File, dst: File): Unit = {
    if (!dst.getParentFile.exists()) {
      if (!dst.getParentFile.mkdirs())
        throw new RuntimeException(
          s"Failed to move $src to $dst. Could not create parent directory ${dst.getParent}."
        )
    }
    val _ = Files.move(src.toPath, dst.toPath, StandardCopyOption.REPLACE_EXISTING)
  }

  private def withTemporaryDirectory(f: Path => Unit): Unit = {
    val tmpDir = Files.createTempDirectory("daml-ledger-export")
    try {
      f(tmpDir)
    } finally {
      deleteRecursively(tmpDir)
    }
  }
}
