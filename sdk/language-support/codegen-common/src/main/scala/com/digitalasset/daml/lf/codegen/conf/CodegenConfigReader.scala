// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.conf

import java.io.File
import java.nio.file.{Path, Paths}

import ch.qos.logback.classic.Level
import com.digitalasset.daml.lf.data.Ref.{PackageName, PackageVersion}
import com.daml.assistant.config._
import io.circe.{ACursor, KeyDecoder}

import scala.util.Try

object CodegenConfigReader {

  sealed trait CodegenDest
  object Java extends CodegenDest

  type Result[A] = Either[ConfigLoadingError, A]

  def readFromEnv(dest: CodegenDest): Result[Conf] =
    for {
      sdkConf <- ProjectConfig.loadFromEnv()
      codegenConf <- codegenConf(sdkConf, dest)
    } yield codegenConf

  def codegenConf(sdkConf: ProjectConfig, dest: CodegenDest): Result[Conf] =
    for {
      dar <- darPath(sdkConf)
      packagePrefix <- packagePrefix(sdkConf, dest)
      modulePrefixes <- modulePrefixes(sdkConf)
      outputDirectory <- outputDirectory(sdkConf, dest)
      decoderPkgAndClass <- decoderPkgAndClass(sdkConf, dest)
      verbosity <- verbosity(sdkConf, dest): Result[Option[Int]]
      logLevel <- logLevel(verbosity, Level.ERROR)
      root <- root(sdkConf, dest): Result[Option[List[String]]]
    } yield Conf(
      darFiles = Map(dar -> packagePrefix),
      modulePrefixes = modulePrefixes,
      outputDirectory = outputDirectory,
      decoderPkgAndClass = decoderPkgAndClass,
      verbosity = logLevel,
      roots = root.getOrElse(Nil),
    )

  private def darPath(sdkConf: ProjectConfig): Result[Path] =
    for {
      name <- name(sdkConf)
      version <- version(sdkConf)
      dar <- darPath(sdkConf.projectPath, name, version)
    } yield dar

  private def name(sdkConf: ProjectConfig): Result[String] =
    sdkConf.name.flatMap {
      case Some(a) => Right(a)
      case None => Left(ConfigMissing("name"))
    }

  private def version(sdkConf: ProjectConfig): Result[String] =
    sdkConf.version.flatMap {
      case Some(a) => Right(a)
      case None => Left(ConfigMissing("version"))
    }

  private def darPath(projectPath: Path, name: String, version: String): Result[Path] =
    result(projectPath.resolve(darDirectory).resolve(s"$name-$version.dar"))

  private val darDirectory = Paths.get(".daml/dist")

  private def packagePrefix(sdkConf: ProjectConfig, mode: CodegenDest): Result[Option[String]] =
    codegen(sdkConf, mode)
      .downField("package-prefix")
      .as[Option[String]]
      .left
      .map(configParseError)

  private def modulePrefixes(sdkConf: ProjectConfig): Result[Map[PackageReference, String]] =
    sdkConf.content.hcursor
      .downField("module-prefixes")
      .as[Option[Map[PackageReference, String]]]
      .map(_.getOrElse(Map.empty))
      .left
      .map(configParseError)

  private def outputDirectory(sdkConf: ProjectConfig, mode: CodegenDest): Result[Path] =
    codegen(sdkConf, mode)
      .downField("output-directory")
      .as[String]
      .left
      .map(configParseError)
      .flatMap(path)

  private def decoderPkgAndClass(
      sdkConf: ProjectConfig,
      mode: CodegenDest,
  ): Result[Option[(String, String)]] =
    codegen(sdkConf, mode)
      .downField("decoderClass")
      .as[Option[String]]
      .left
      .map(configParseError)
      .flatMap(decoderClass)

  private def decoderClass(fa: Option[String]): Result[Option[(String, String)]] =
    fa match {
      case Some(a) => decoderClass(a).map(Some(_))
      case None => resultR(None)
    }

  private def decoderClass(s: String): Result[(String, String)] =
    result(Conf.readClassName.reads(s))

  private def verbosity(sdkConf: ProjectConfig, mode: CodegenDest): Result[Option[Int]] =
    codegen(sdkConf, mode)
      .downField("verbosity")
      .as[Option[Int]]
      .left
      .map(configParseError)

  private def logLevel(fa: Option[Int], default: Level): Result[Level] =
    fa.fold(resultR(default))(readVerbosity)

  private def readVerbosity(a: Int): Result[Level] =
    result(Conf.readVerbosity.reads(a.toString))

  private def root(sdkConf: ProjectConfig, mode: CodegenDest): Result[Option[List[String]]] =
    codegen(sdkConf, mode)
      .downField("root")
      .as[Option[List[String]]]
      .left
      .map(configParseError)

  private def codegen(sdkConf: ProjectConfig, mode: CodegenDest): ACursor =
    sdkConf.content.hcursor
      .downField("codegen")
      .downField(dest(mode))

  private def dest(a: CodegenDest): String = a match {
    case Java => "java"
  }

  private def path(a: String): Result[Path] =
    result(new File(a).toPath)

  private def configParseError(e: Exception): ConfigParseError = ConfigParseError(e.getMessage)

  private def result[A](a: => A): Result[A] =
    result(Try(a))

  private def result[A](fa: Try[A]): Result[A] =
    fa.toEither.left.map(e => ConfigLoadError(e.getMessage))

  private def resultR[A](a: A): Result[A] =
    Right(a): Result[A]

  private[conf] def splitNameAndVersion(
      string: String,
      separator: Char,
  ): Option[(String, String)] = {
    val separatorIndex = string.lastIndexOf(separator.toInt)
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

  private[conf] val PackageReferenceSeparator = '-'

  implicit val decodePackageReference: KeyDecoder[PackageReference] =
    key =>
      for {
        (rawName, rawVersion) <- splitNameAndVersion(key, PackageReferenceSeparator)
        name <- PackageName.fromString(rawName).toOption
        version <- PackageVersion.fromString(rawVersion).toOption
      } yield PackageReference.NameVersion(name, version)

}
