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
     |usage: explore-dar [NAME] [--arg INT]
    """.stripMargin)
  }

  def parseArgs(args0: List[String]): Config = {
    var moduleName: String = "Examples"
    var funcNameOpt: Option[String] = None
    def loop(args: List[String]): Unit = args match {
      case Nil => {}
      case "-h" :: _ => usage()
      case "--help" :: _ => usage()
      case "--base" :: x :: args =>
        moduleName = x
        loop(args)
      case x :: args =>
        funcNameOpt = Some(x)
        loop(args)
    }
    loop(args0)
    Config(moduleName, funcNameOpt)
  }

  final case class Config(
      moduleName: String,
      funcNameOpt: Option[String],
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
        case Some(funcName) => d.toString == funcName
      }
      if (selectM && selectF) {
        Some(d.toString)
      } else {
        None
      }
    }

    val compilerConfig = Compiler.Config.Default.copy(debug = debugExamples)
    val _ = PureCompiledPackages.build(packages.all.toMap, compilerConfig)
  }
}
