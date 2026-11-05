// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.assistant.config.PackageConfig
import com.digitalasset.daml.lf.codegen._
import io.circe.Json
import io.circe.yaml

import java.nio.file.Paths
import scala.util.{Failure, Success, Try}
import scopt.OptionParser

object CodegenMain {
  private lazy val isDpm = !sys.env.contains("DAML_SDK")

  sealed abstract class ExitCode(val code: Int)
  object OK extends ExitCode(0)
  object UsageError extends ExitCode(101)
  object CodegenError extends ExitCode(201)

  def main(args: Array[String]): Unit = {
    val codegenRunner = commandParser.parse(args.headOption.toList, JavaCodegenRunner)
    val exitCode: ExitCode = codegenRunner match {
      case Some(runner) =>
        readConfiguration(runner, args.tail) match {
          case Some(config) =>
            val assemblyVersion = sys.env
              .get("DPM_SDK_VERSION")
              .orElse(sys.env.get("DAML_SDK_VERSION"))
              .getOrElse("0.0.0")
            // Try get component version from resolution file, fallback to assembly version
            val componentVersion = tryGetComponentVersion().getOrElse(assemblyVersion)
            Try(runner.generateCode(config, assemblyVersion, componentVersion)) match {
              case Success(_) => OK
              case Failure(t) =>
                println(s"Error generating code: ${t.getMessage}")
                CodegenError
            }
          case None => displayUsage(runner.configParser(isDpm))
        }
      case None => displayUsage(commandParser)
    }
    sys.exit(exitCode.code)
  }

  // Try read codegen component version from the resolution file
  def tryGetComponentVersion(): Option[String] =
    Option(System.getenv("DPM_RESOLUTION_FILE")).map { resolutionPath =>
      // Need own running path to find resolution (in case its run from a package)
      val runPath = Paths.get(".").toAbsolutePath.normalize
      val source = scala.io.Source.fromFile(resolutionPath)
      val content =
        try source.mkString
        finally source.close()

      val eComponentVersion = for {
        json <- yaml.parser.parse(content).left.map(e => e.getMessage)
        // Get the packages field to try find package specific resolution
        allPackages <-
          json.hcursor
            .downField("packages")
            .as[Map[String, Json]]
            .left
            .map(e => e.getMessage)
        // get the default-sdk field in case there is no package specific resolution
        // Default resolution is under `default-sdk.<version>`, where there will only ever be one key for `<version>`
        // We don't care what the version itself actually is, so we just select the first key in the object
        defaultResolution <-
          json.hcursor
            .downField("default-sdk")
            .as[Map[String, Json]]
            .map(m =>
              m(
                m.keys.headOption.getOrElse(
                  throw new IllegalArgumentException(
                    "No damlc version found under default-sdk in DPM resolution file"
                  )
                )
              )
            )
            .left
            .map(e => e.getMessage)
        // Search for package in resolution, must use `Path.equals` else windows will break
        packageResolution = allPackages.toList
          .map { case (p, j) => (Paths.get(p), j) }
          .find { case (p, _) => runPath.equals(p) }
          .fold(defaultResolution)(_._2)
        // Grab the codegen version, only support componentsV2
        componentVersion <-
          packageResolution.hcursor
            .downField("componentsV2")
            .downField("codegen")
            .downField("version")
            .as[String]
            .left
            .map(e => e.getMessage)
      } yield componentVersion
      eComponentVersion.fold(
        identity,
        err => throw new IllegalArgumentException(s"Failed to parse resolution file: $err"),
      )
    }

  private def displayUsage(parser: OptionParser[_]): ExitCode = {
    println("\n")
    parser.displayToOut(parser.usage)
    UsageError
  }

  private def readConfiguration(runner: CodegenRunner, args: Array[String]): Option[runner.Config] =
    if (args.nonEmpty) {
      println(s"Reading configuration from command line input: ${args.mkString(",")}")
      runner.configureFromArgs(args, isDpm)
    } else {
      println(s"Reading configuration from package configuration file")
      PackageConfig.loadFromEnv().flatMap(runner.configureFromPackageConfig) match {
        case Left(e) =>
          println(s"Error reading package configuration file: ${e.reason}")
          None
        case Right(c) => Some(c)
      }
    }
  private val commandParser = new scopt.OptionParser[CodegenRunner]("codegen") {
    head("Daml Codegen")

    override val showUsageOnError = Some(false)

    cmd("java")
      .action((_, _) => JavaCodegenRunner)
      .text("To generate Java code:\n")
      .children(help("help").text("Java codegen help"))
    note("\n")

    cmd("js")
      .action((_, _) => JsCodegenRunner)
      .text("To generate Javascript code and Typescript declarations:\n")
      .children(help("help").text("JS codegen help"))
    note("\n")
  }
}
