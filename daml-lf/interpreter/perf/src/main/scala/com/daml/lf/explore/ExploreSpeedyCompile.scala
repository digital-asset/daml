// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.bazeltools.BazelRunfiles.{rlocation}
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.Ref.DottedName

import java.io.File

// Explore the speedy compilation pipeline

object ExploreSpeedyCompile extends App {

  main(args.toList)

  def usage(): Unit = {
    println("""
     |usage: explore-dar [FUNC_NAME] [--base DAR_NAME] [--stacktracing] [--profiling]
     |
     |DAR_NAME is the name of the dar file in `daml-lf/interpreter/perf/`
    """.stripMargin)
    System.exit(0)
  }

  def parseArgs(args0: List[String]): Config = {
    var moduleName: String = "Examples"
    var funcNameOpt: Option[String] = None
    var stacktracing: Boolean = false
    var profiling: Boolean = false
    var funcNameContains: Boolean = false
    def loop(args: List[String]): Unit = args match {
      case Nil => {}
      case "-h" :: _ => usage()
      case "--help" :: _ => usage()
      case "--base" :: x :: args =>
        moduleName = x
        loop(args)
      case "--stacktracing" :: args =>
        stacktracing = true
        loop(args)
      case "--profiling" :: args =>
        profiling = true
        loop(args)
      case "--contains-name" :: args =>
        funcNameContains = true
        loop(args)
      case x :: args =>
        funcNameOpt = Some(x)
        loop(args)
    }
    loop(args0)
    Config(moduleName, funcNameOpt, stacktracing, profiling, funcNameContains)
  }

  final case class Config(
      moduleName: String,
      funcNameOpt: Option[String],
      stacktracing: Boolean,
      profiling: Boolean,
      funcNameContains: Boolean,
  )

  def main(args0: List[String]) = {

    val config = parseArgs(args0)
    val base = config.moduleName
    val dar = s"daml-lf/interpreter/perf/${base}.dar"
    val darFile = new File(rlocation(dar))
    val packages = UniversalArchiveDecoder.assertReadFile(darFile)

    def debugExamples(m: DottedName, d: DottedName): Option[String] = {
      val selectM: Boolean = m.toString == config.moduleName
      val selectF: Boolean = config.funcNameOpt match {
        case None => true
        case Some(funcName) =>
          if (config.funcNameContains)
            d.toString contains funcName
          else d.toString == funcName
      }
      if (selectM && selectF) {
        Some(d.toString)
      } else {
        None
      }
    }
    val stackTraceMode = if (config.stacktracing) Compiler.FullStackTrace else Compiler.NoStackTrace
    val profilingMode = if (config.profiling) Compiler.FullProfile else Compiler.NoProfile
    val compilerConfig = Compiler.Config.Default.copy(
      debug = debugExamples,
      stacktracing = stackTraceMode,
      profiling = profilingMode,
    )
    val _ = PureCompiledPackages.build(packages.all.toMap, compilerConfig)
  }
}
