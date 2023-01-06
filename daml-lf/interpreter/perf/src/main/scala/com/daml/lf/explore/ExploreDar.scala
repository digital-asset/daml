// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.bazeltools.BazelRunfiles.{rlocation}
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.Ref.{DefinitionRef, Identifier, QualifiedName}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.Speedy._
import com.daml.logging.LoggingContext

import java.io.File

// Explore the execution of speedy machine on small examples taken from a dar file

object ExploreDar extends App {
  PlaySpeedy.main(args.toList)
}

object PlaySpeedy {

  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  def usage(): Unit = {
    println("""
     |usage: explore-dar [NAME] [--arg INT]
    """.stripMargin)
  }

  def parseArgs(args0: List[String]): Config = {
    var moduleName: String = "Examples"
    var funcName: String = "triangle"
    var argValue: Long = 10
    var stacktracing: Compiler.StackTraceMode = Compiler.NoStackTrace
    def loop(args: List[String]): Unit = args match {
      case Nil => {}
      case "-h" :: _ => usage()
      case "--help" :: _ => usage()
      case "--arg" :: x :: args =>
        argValue = x.toLong
        loop(args)
      case "--stacktracing" :: args =>
        stacktracing = Compiler.FullStackTrace
        loop(args)
      case "--base" :: x :: args =>
        moduleName = x
        loop(args)
      case x :: args =>
        funcName = x
        loop(args)
    }
    loop(args0)
    Config(moduleName, funcName, argValue, stacktracing)
  }

  final case class Config(
      moduleName: String,
      funcName: String,
      argValue: Long,
      stacktracing: Compiler.StackTraceMode,
  )

  def main(args0: List[String]) = {

    println("Start...")
    val config = parseArgs(args0)
    val base = config.moduleName
    val dar = s"daml-lf/interpreter/perf/${base}.dar"
    val darFile = new File(rlocation(dar))

    println("Loading dar...")
    val packages = UniversalArchiveDecoder.assertReadFile(darFile)

    println(s"Compiling packages... ${config.stacktracing}")
    val compilerConfig = Compiler.Config.Default.copy(stacktracing = config.stacktracing)
    val compiledPackages =
      PureCompiledPackages.build(packages.all.toMap, compilerConfig) match {
        case Right(x) => x
        case Left(x) =>
          throw new MachineProblem(s"Unexpecteded result when compiling $x")
      }

    val expr = {
      val ref: DefinitionRef =
        Identifier(
          packages.main._1,
          QualifiedName.assertFromString(s"${base}:${config.funcName}"),
        )
      val func = SEVal(LfDefRef(ref))
      val arg = SInt64(config.argValue)
      SEApp(func, Array(arg))
    }
    println("Run...")
    val result = Machine.runPureSExpr(compiledPackages, expr).toTry.get
    println(s"Final-value: $result")
  }

  final case class MachineProblem(s: String) extends RuntimeException(s, null, false, false)

}
