// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import java.nio.file.Path

import ch.qos.logback.classic.Level
import com.daml.assistant.config._
import io.circe.{ACursor, Decoder, DecodingFailure}

private object ConfigReader {
  implicit val pathDecoder: Decoder[Path] = Decoder[String].map(Path.of(_))
  implicit val logLevelDecoder: Decoder[Level] = Decoder[Int].map {
    case 0 => Level.ERROR
    case 1 => Level.WARN
    case 2 => Level.INFO
    case 3 => Level.DEBUG
    case 4 => Level.TRACE
    case _ =>
      throw new IllegalArgumentException(
        "Expected a verbosity value between 0 (least verbose) and 4 (most verbose)"
      )
  }

  type Result[A] = Either[ConfigLoadingError, A]

  def darPath(sdkConf: PackageConfig): Result[Path] =
    for {
      name <- name(sdkConf)
      version <- version(sdkConf)
    } yield darPath(sdkConf.packagePath, name, version)

  def outputDirectory(codegenCursor: ACursor): Result[Path] =
    codegenCursor.downField("output-directory").as[Path].left.map(toConfigParseError)

  def codegen(sdkConf: PackageConfig, target: String): ACursor =
    sdkConf.content.hcursor.downField("codegen").downField(target)

  def verbosity(codegenCursor: ACursor, default: Level): Result[Level] =
    codegenCursor
      .downField("verbosity")
      .as[Option[Level]]
      .map(_.getOrElse(default))
      .left
      .map(toConfigParseError)

  def toConfigParseError(error: DecodingFailure): ConfigParseError =
    ConfigParseError(error.getMessage)

  private def name(sdkConf: PackageConfig): Result[String] =
    sdkConf.name.flatMap {
      case Some(a) => Right(a)
      case None => Left(ConfigParseError("missing field name"))
    }

  private def version(sdkConf: PackageConfig): Result[String] =
    sdkConf.version.flatMap {
      case Some(a) => Right(a)
      case None => Left(ConfigParseError("missing field version"))
    }

  private def darPath(packagePath: Path, name: String, version: String): Path =
    packagePath.resolve(".daml/dist").resolve(s"$name-$version.dar")
}