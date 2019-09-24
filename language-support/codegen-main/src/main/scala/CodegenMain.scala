// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.codegen.{Main => ScalaCodegen}
import com.digitalasset.daml.lf.codegen.conf.CodegenConfigReader.{CodegenDest, Java, Scala}
import com.digitalasset.daml.lf.codegen.conf.{CodegenConfigReader, Conf}
import com.digitalasset.daml.lf.codegen.{CodeGenRunner => JavaCodegen}

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
      case Some(FrontEndConfig(Some(Scala))) =>
        scalaCodegen(args.tail)
      case Some(FrontEndConfig(None)) | None =>
        println("\n")
        cliConfigParser.showUsage()
        UsageError
    }
    sys.exit(exitCode.code)
  }

  private def javaCodegen(args: Array[String]): ExitCode = {
    println("Java codegen")
    runCodegen(JavaCodegen.run, codegenConfig(args, Java))
  }

  private def scalaCodegen(args: Array[String]): ExitCode = {
    println("Scala codegen")
    runCodegen(ScalaCodegen.generateCode, codegenConfig(args, Scala))
  }

  private def runCodegen(generate: Conf => Unit, configO: Option[Conf]): ExitCode =
    configO match {
      case None =>
        println("\n")
        Conf.parser.showUsage
        UsageError
      case Some(conf) =>
        Try(generate(conf)) match {
          case Success(_) =>
            OK
          case Failure(t) =>
            println(s"Error generating code: ${t.getMessage}")
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

  private def parseFrontEndConfig(args: Seq[String]): Option[FrontEndConfig] = args match {
    case h +: _ => cliConfigParser.parse(Seq(h), FrontEndConfig(None))
    case _ => None
  }

  private val cliConfigParser = new scopt.OptionParser[FrontEndConfig]("codegen-front-end") {
    head("Codegen front end")

    override def showUsageOnError = false

    help("help").text("Prints this usage text")
    note("\n")

    cmd("java")
      .action((_, c) => c.copy(mode = Some(Java)))
      .text("To generate Java code:\n")
      .children(help("help").text("Java codegen help"))
    note("\n")

    cmd("scala")
      .action((_, c) => c.copy(mode = Some(Scala)))
      .text("To generate Scala code:\n")
      .children(help("help").text("Scala codegen help"))
    note("\n")
  }
}
