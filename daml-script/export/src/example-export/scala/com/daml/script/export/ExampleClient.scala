// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.Duration

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.engine.script.{RunnerConfig, RunnerMain}

case class ExampleClientConfig(
    darPath: File,
    targetPort: Int,
    outputPath: Path,
)

object ExampleClientConfig {
  def parse(args: Array[String]): Option[ExampleClientConfig] =
    parser.parse(
      args,
      ExampleClientConfig(
        darPath = null,
        targetPort = -1,
        outputPath = null,
      ),
    )

  private def parseExportOut(
      envVar: String
  ): Either[String, ExampleClientConfig => ExampleClientConfig] = {
    if (envVar.isEmpty) Left("Environment variable EXPORT_OUT must not be empty")
    else
      envVar.split(" ").map(s => Paths.get(s)) match {
        case Array(export_daml, args_json, daml_yaml) =>
          if (export_daml.getParent == null) {
            Left("First component in environment variable EXPORT_OUT has no parent")
          } else if (export_daml.getParent != args_json.getParent) {
            Left(
              "First and second component in environment variable EXPORT_OUT have different parent"
            )
          } else if (export_daml.getParent != daml_yaml.getParent) {
            Left(
              "First and third component in environment variable EXPORT_OUT have different parent"
            )
          } else {
            Right(c => c.copy(outputPath = export_daml.getParent))
          }
        case _ => Left("Environment variable EXPORT_OUT must contain three paths")
      }
  }

  private val parser = new scopt.OptionParser[ExampleClientConfig]("script-export") {
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

object ExampleClient {
  def main(args: Array[String]): Unit = {
    ExampleClientConfig.parse(args) match {
      case Some(clientConfig) => main(clientConfig)
      case None => sys.exit(1)
    }
  }
  def main(clientConfig: ExampleClientConfig): Unit = {
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
    Main.main(
      Config.Empty.copy(
        ledgerHost = "localhost",
        ledgerPort = clientConfig.targetPort,
        parties = Seq("Alice", "Bob"),
        exportType = Some(
          Config.EmptyExportScript.copy(
            sdkVersion = "0.0.0",
            outputPath = clientConfig.outputPath,
          )
        ),
      )
    )
  }
}
