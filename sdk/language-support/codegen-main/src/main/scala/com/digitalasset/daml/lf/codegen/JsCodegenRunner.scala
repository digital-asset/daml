// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import ch.qos.logback.classic.Level
import com.daml.assistant.config.{ConfigLoadingError, PackageConfig}
import io.circe.ACursor
import scopt.OptionParser

object JsCodegenRunner extends CodegenRunner {
  type Config = JsCodeGenConf

  override def configParser(isDpm: Boolean): OptionParser[JsCodeGenConf] =
    JsCodeGenConf.parser(parserName(isDpm))

  override def configureFromArgs(args: Array[String], isDpm: Boolean): Option[JsCodeGenConf] =
    JsCodeGenConf.parse(args, parserName(isDpm))

  override def configureFromPackageConfig(
      sdkConf: PackageConfig
  ): Either[ConfigLoadingError, JsCodeGenConf] =
    for {
      dar <- ConfigReader.darPath(sdkConf)
      codegenCursor = ConfigReader.codegen(sdkConf, target = "js")
      outputDirectory <- ConfigReader.outputDirectory(codegenCursor)
      scope <- scope(codegenCursor, "daml.js")
      verbosity <- ConfigReader.verbosity(codegenCursor, Level.ERROR)
    } yield JsCodeGenConf(
      darFiles = Seq(dar),
      outputDirectory = outputDirectory,
      scope = scope,
      verbosity = verbosity,
    )

  override def generateCode(config: JsCodeGenConf, damlVersion: String): Unit =
    JsCodeGen.run(config, damlVersion)

  private def parserName(isDpm: Boolean): String = if (isDpm) "codegen-js" else "codegen"

  private def scope(codegenCursor: ACursor, defaultScope: String): ConfigReader.Result[String] =
    codegenCursor
      .downField("scope")
      .as[Option[String]]
      .map(_.getOrElse(defaultScope))
      .left
      .map(ConfigReader.toConfigParseError)
}
