// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.assistant.config.PackageConfig
import com.digitalasset.daml.lf.codegen._

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
            val damlVersion = sys.env
              .get("DPM_SDK_VERSION")
              .orElse(sys.env.get("DAML_SDK_VERSION"))
              .getOrElse("0.0.0")
            Try(runner.generateCode(config, damlVersion)) match {
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
