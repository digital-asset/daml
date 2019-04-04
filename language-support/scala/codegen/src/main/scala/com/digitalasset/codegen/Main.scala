// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import java.io.File
import scopt.OptionParser

import java.net.URL

object Main {

  case class Config(
      inputFile: File = new File("."),
      otherDalfInputs: Seq[URL] = Seq(),
      packageName: String = "",
      outputDir: File = new File("."),
      codeGenMode: CodeGen.Mode = CodeGen.Novel)

  val parser = new OptionParser[Config]("codegen") {
    help("help").text("prints this usage text")

    opt[File]("input-file").required
      .abbr("i")
      .action((d, c) => c.copy(inputFile = d))
      .text("input top-level DAML module")

    opt[Seq[File]]("dependencies").optional
      .abbr("d")
      .action((d, c) => c.copy(otherDalfInputs = d.map(toURL)))
      .text("list of DAML module dependencies")

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

  private def toURL(f: File): URL = f.toURI.toURL

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>
        CodeGen.generateCode(
          config.inputFile,
          config.otherDalfInputs,
          config.packageName,
          config.outputDir,
          config.codeGenMode)
      case None => // arguments are bad, error message will have been displayed
    }
  }
}
