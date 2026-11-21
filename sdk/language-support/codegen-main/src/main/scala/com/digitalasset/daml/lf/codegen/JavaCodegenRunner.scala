// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import ch.qos.logback.classic.Level
import com.daml.assistant.config._
import com.digitalasset.daml.lf.data.Ref.{PackageName, PackageVersion}
import io.circe.{ACursor, Decoder, KeyDecoder}
import scopt.OptionParser

object JavaCodegenRunner extends CodegenRunner {
  type Config = JavaCodeGenConf

  override def configParser(isDpm: Boolean): OptionParser[JavaCodeGenConf] =
    JavaCodeGenConf.parser(parserName(isDpm))

  override def configureFromArgs(args: Array[String], isDpm: Boolean): Option[JavaCodeGenConf] =
    JavaCodeGenConf.parse(args, parserName(isDpm))

  override def configureFromPackageConfig(
      sdkConf: PackageConfig
  ): Either[ConfigLoadingError, JavaCodeGenConf] =
    for {
      dar <- ConfigReader.darPath(sdkConf)
      packagePrefix <- packagePrefix(sdkConf)
      modulePrefixes <- modulePrefixes(sdkConf)
      codegenCursor = ConfigReader.codegen(sdkConf, target = "java")
      outputDirectory <- ConfigReader.outputDirectory(codegenCursor)
      decoderPkgAndClass <- decoderPkgAndClass(codegenCursor)
      verbosity <- ConfigReader.verbosity(codegenCursor, Level.ERROR)
      root <- root(codegenCursor)
    } yield JavaCodeGenConf(
      darFiles = Map(dar -> packagePrefix),
      modulePrefixes = modulePrefixes,
      outputDirectory = outputDirectory,
      decoderPkgAndClass = decoderPkgAndClass,
      verbosity = verbosity,
      roots = root.getOrElse(Nil),
    )

  override def generateCode(config: JavaCodeGenConf, damlVersion: String): Unit =
    JavaCodeGen.run(config)

  private def parserName(isDpm: Boolean): String = if (isDpm) "codegen-java" else "codegen"

  private def packagePrefix(sdkConf: PackageConfig): ConfigReader.Result[Option[String]] =
    ConfigReader
      .codegen(sdkConf, "java")
      .downField("package-prefix")
      .as[Option[String]]
      .left
      .map(ConfigReader.toConfigParseError)

  private def modulePrefixes(
      sdkConf: PackageConfig
  ): ConfigReader.Result[Map[PackageReference, String]] =
    sdkConf.content.hcursor
      .downField("module-prefixes")
      .as[Option[Map[PackageReference, String]]]
      .map(_.getOrElse(Map.empty))
      .left
      .map(ConfigReader.toConfigParseError)

  private implicit val decodePackageReference: KeyDecoder[PackageReference] =
    key =>
      for {
        (rawName, rawVersion) <- splitNameAndVersion(key)
        name <- PackageName.fromString(rawName).toOption
        version <- PackageVersion.fromString(rawVersion).toOption
      } yield PackageReference.NameVersion(name, version)

  private def decoderPkgAndClass(
      codegenCursor: ACursor
  ): ConfigReader.Result[Option[(String, String)]] =
    codegenCursor
      .downField("decoderClass")
      .as[Option[(String, String)]](
        Decoder[Option[String]].map(_.map(JavaCodeGenConf.readClassName.reads))
      )
      .left
      .map(ConfigReader.toConfigParseError)

  private def root(codegenCursor: ACursor): ConfigReader.Result[Option[List[String]]] =
    codegenCursor
      .downField("root")
      .as[Option[List[String]]]
      .left
      .map(ConfigReader.toConfigParseError)

  private[codegen] def splitNameAndVersion(string: String): Option[(String, String)] = {
    val separatorIndex = string.lastIndexOf('-'.toInt)
    if (separatorIndex < 0) {
      None
    } else {
      // `splitAt` doesn't allow to cleanly drop the separator
      val name = string.take(separatorIndex)
      val version = string.drop(separatorIndex + 1)
      if (name.nonEmpty && version.nonEmpty) {
        Some((name, version))
      } else {
        None
      }
    }
  }
}
