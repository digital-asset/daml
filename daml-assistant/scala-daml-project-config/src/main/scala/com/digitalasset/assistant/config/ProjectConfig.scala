// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.assistant.config

import java.io.File
import java.nio.file.{Files, Path}

import io.circe.{Json, yaml}
import io.circe.optics.JsonOptics._
import monocle.function.Plated

import scala.collection.immutable.Map
import scala.io.Source
import scala.util.Try
import scala.util.matching.Regex

/** Base class for all errors encountered while loading the config file */
sealed abstract class ConfigLoadingError extends Product with Serializable {
  def reason: String
}

/** Config file does not exist */
final case class ConfigMissing(reason: String) extends ConfigLoadingError

/** Config file exists, but could not read the config file */
final case class ConfigLoadError(reason: String) extends ConfigLoadingError

/** Config file is readable, but content could not be parsed */
final case class ConfigParseError(reason: String) extends ConfigLoadingError

/** Note: The SDK project configuration does not have an explicit schema.
  * The original parsed Yaml/Json object is therefore kept, with additional
  * getters for commonly used properties.
  *
  * The helper methods return:
  * - Left(ConfigParseError) if there was an error parsing the property
  * - Right(None) if the property is missing
  * - Right(Some(_)) if the property is present and valid
  */
case class ProjectConfig(
    content: Json,
    projectPath: Path,
) {
  type Result[A] = Either[ConfigParseError, A]
  type OptionalResult[A] = Either[ConfigParseError, Option[A]]

  /** The SDK version is the only non-optional property. */
  def sdkVersion: Result[String] =
    content.hcursor
      .downField("sdk-version")
      .as[String]
      .left
      .map(e => ConfigParseError(e.getMessage()))

  def name: OptionalResult[String] =
    content.hcursor
      .downField("name")
      .as[Option[String]]
      .left
      .map(e => ConfigParseError(e.getMessage()))
  def source: OptionalResult[String] =
    content.hcursor
      .downField("source")
      .as[Option[String]]
      .left
      .map(e => ConfigParseError(e.getMessage()))
  def scenario: OptionalResult[String] =
    content.hcursor
      .downField("scenario")
      .as[Option[String]]
      .left
      .map(e => ConfigParseError(e.getMessage()))
  def parties: OptionalResult[List[String]] =
    content.hcursor
      .downField("parties")
      .as[Option[List[String]]]
      .left
      .map(e => ConfigParseError(e.getMessage()))
  def version: OptionalResult[String] =
    content.hcursor
      .downField("version")
      .as[Option[String]]
      .left
      .map(e => ConfigParseError(e.getMessage()))
  def exposedModules: OptionalResult[List[String]] =
    content.hcursor
      .downField("exposed-modules")
      .as[Option[List[String]]]
      .left
      .map(e => ConfigParseError(e.getMessage()))
  def dependencies: OptionalResult[List[String]] =
    content.hcursor
      .downField("dependencies")
      .as[Option[List[String]]]
      .left
      .map(e => ConfigParseError(e.getMessage()))
}

object ProjectConfig {

  /** The DAML_PROJECT environment variable determines the path of
    * the current daml project. By default, this is done by traversing
    * up the directory structure until we find a "daml.yaml" file.
    */
  val envVarProjectPath = "DAML_PROJECT"

  /** File name of config file in DAML_PROJECT (the project path). */
  val projectConfigName = "daml.yaml"

  /** Returns the path of the current daml project, if any.
    * The path is given by environment variables set by the SDK Assistant.
    */
  def projectPath(): Either[ConfigLoadingError, String] =
    sys.env
      .get(envVarProjectPath)
      .toRight(ConfigMissing(s"Environment variable $envVarProjectPath not found"))

  /** Returns the path of the current daml project config file, if any.
    * The path is given by environment variables set by the SDK Assistant.
    */
  def projectConfigPath(): Either[ConfigLoadingError, File] =
    projectPath().flatMap(path =>
      Try(new File(path, projectConfigName)).toEither.left.map(t => ConfigMissing(t.getMessage))
    )

  val envVarMatch: Regex = """(^|[^\\])(\\*)\$\{([^\}]+)\}""".r
  def interpolateEnvironmentVariable(str: String, env: Map[String, String]): String =
    envVarMatch.replaceAllIn(str, (m: Regex.Match) => Regex.quoteReplacement {
      val prefix = m.group(1) + m.group(2).take(m.group(2).length / 2)
      if (m.group(2).length % 2 == 0) {
        val varName = m.group(3).replace(".", "_")
        prefix + env.get(varName).getOrElse(
          throw new IllegalArgumentException(s"Couldn't find environment variable $varName in value $str")
        )
      } else prefix + "${" + m.group(3) + "}"
    })

  def interpolateEnvironmentVariables(json: Json, env: Map[String, String]): Json =
    Plated.transform[Json](_.mapString(interpolateEnvironmentVariable(_, env)))(json)

    /** Loads a project configuration from a string */
  def loadFromString(
      projectPath: Path,
      content: String,
  ): Either[ConfigLoadingError, ProjectConfig] =
    loadFromStringWithEnv(projectPath, content, sys.env)

  /** Loads a project configuration from a string, with an explicit environment variable map */
  def loadFromStringWithEnv(
      projectPath: Path,
      content: String,
      env: Map[String, String],
  ): Either[ConfigLoadingError, ProjectConfig] = {
    for {
      json <- yaml.parser.parse(content).left.map(e => ConfigParseError(e.getMessage))
      interpolatedJson <- Try(interpolateEnvironmentVariables(json, env)).fold(e => Left(ConfigParseError(e.getMessage)), Right(_))
    } yield ProjectConfig(interpolatedJson, projectPath)
  }

  /** Loads a project configuration from a file */
  def loadFromFile(file: File): Either[ConfigLoadingError, ProjectConfig] = {
    for {
      _ <- Either.cond(
        Files.exists(file.toPath),
        true,
        ConfigMissing(s"Config file ${file.toPath} does not exist"),
      )
      source <- Try(Source.fromFile(file, "UTF-8")).toEither.left.map(e =>
        ConfigLoadError(e.getMessage)
      )
      content <- Try(
        try source.mkString
        finally source.close()
      ).toEither.left.map(e => ConfigLoadError(e.getMessage))
      result <- loadFromString(file.getParentFile.toPath, content)
    } yield result
  }

  /** Loads the project configuration from a config file,
    * with the path to the config file given by environment variables set by the SDK Assistant.
    * This is the preferred way of loading the SDK project configuration.
    */
  def loadFromEnv(): Either[ConfigLoadingError, ProjectConfig] = {
    for {
      path <- projectConfigPath()
      result <- loadFromFile(path)
    } yield result
  }

}
