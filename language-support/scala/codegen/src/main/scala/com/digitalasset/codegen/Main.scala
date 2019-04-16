// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import java.io.File

import scopt.OptionParser

object Main {

  case class Config(
      inputFiles: Seq[File] = Seq(),
      packageName: String = "",
      outputDir: File = new File("."),
      codeGenMode: CodeGen.Mode = CodeGen.Novel)

  private val parser = new OptionParser[Config]("codegen") {
    help("help").text("prints this usage text")

    opt[Seq[File]]("input-files").required
      .abbr("i")
      .action((d, c) => c.copy(inputFiles = d))
      .text("input DAR or DALF files")

    opt[String]("package-name")
      .required()
      .abbr("p")
      .action((d, c) => c.copy(packageName = d))
      .text("package name e.g. com.digitalasset.mypackage")

    opt[File]("output-dir")
      .required()
      .abbr("o")
      .action((d, c) => c.copy(outputDir = d))
      .text("output directory for Scala files")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>
        CodeGen.generateCode(
          config.inputFiles.toList,
          config.packageName,
          config.outputDir,
          config.codeGenMode)
      case None => // arguments are bad, error message will have been displayed
    }
  }
}
