// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.lf.codegen.conf.CodegenConfigReader.{CodegenDest, Java}
import com.daml.lf.codegen.conf.{CodegenConfigReader, Conf}
import com.daml.lf.codegen.{CodeGenRunner => JavaCodegen}

import scala.util.{Failure, Success, Try}

object CodegenMain {

  sealed abstract class ExitCode(val code: Int)
  object OK extends ExitCode(0)
  object UsageError extends ExitCode(101)
  object CodegenError extends ExitCode(201)

  private final case class FrontEndConfig(mode: Option[CodegenDest])

  def main(args: Array[String]): Unit = {
    val exitCode: ExitCode = parseFrontEndConfig(args) match {
      case Some(FrontEndConfig(Some(Java))) =>
        javaCodegen(args.tail)
      case Some(FrontEndConfig(None)) | None =>
        println("\n")
        cliConfigParser.displayToOut(cliConfigParser.usage)
        UsageError
    }
    sys.exit(exitCode.code)
  }

  private def javaCodegen(args: Array[String]): ExitCode = {
    println("Java codegen")
    runCodegen(JavaCodegen.run, codegenConfig(args, Java))
  }

  private def runCodegen(generate: Conf => Unit, configO: Option[Conf]): ExitCode =
    configO match {
      case None =>
        println("\n")
        Conf.parser.displayToOut(Conf.parser.usage)
        UsageError
      case Some(conf) =>
        Try(generate(conf)) match {
          case Success(_) =>
            OK
          case Failure(t) =>
            println(s"Error generating code: ${t.getMessage}")
            t.printStackTrace()
            CodegenError
        }
    }

  private def codegenConfig(args: Array[String], mode: CodegenDest): Option[Conf] =
    if (args.nonEmpty) {
      println(s"Reading configuration from command line input: ${args.mkString(",")}")
      Conf.parse(args)
    } else {
      println(s"Reading configuration from project configuration file")
      CodegenConfigReader.readFromEnv(mode) match {
        case Left(e) => println(s"Error reading project configuration file: $e"); None
        case Right(c) => Some(c)
      }
    }

  private def parseFrontEndConfig(args: collection.Seq[String]): Option[FrontEndConfig] =
    args match {
      case h +: _ => cliConfigParser.parse(Seq(h), FrontEndConfig(None))
      case _ => None
    }

  private val cliConfigParser = new scopt.OptionParser[FrontEndConfig]("codegen-front-end") {
    head("Codegen front end")

    override val showUsageOnError = Some(false)

    help("help").text("Prints this usage text")
    note("\n")

    cmd("java")
      .action((_, c) => c.copy(mode = Some(Java)))
      .text("To generate Java code:\n")
      .children(help("help").text("Java codegen help"))
    note("\n")

  }
}
