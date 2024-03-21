// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.config

import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Path}

import com.daml.assistant.config.{
  ProjectConfig,
  ConfigLoadError => SdkConfigLoadError,
  ConfigMissing => SdkConfigMissing,
  ConfigParseError => SdkConfigParseError,
}
import com.daml.ledger.api.refinements.ApiTypes
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.slf4j.LoggerFactory
import pureconfig.{ConfigConvert, ConfigSource, ConfigWriter}
import pureconfig.generic.auto._
import scalaz.Tag

import scala.annotation.nowarn

final case class UserConfig(party: ApiTypes.Party, role: Option[String], useDatabase: Boolean)

/* The configuration has an empty map as default list of users because you can login as party too */
final case class Config(users: Map[String, UserConfig] = Map.empty[String, UserConfig]) {

  def userIds: Set[String] = users.keySet

  def roles: Set[String] = users.values.view.flatMap(_.role.toList).toSet
}

sealed abstract class ConfigReadError extends Product with Serializable {
  def reason: String
}

final case class ConfigNotFound(reason: String) extends ConfigReadError
final case class ConfigInvalid(reason: String) extends ConfigReadError
final case class ConfigParseFailed(reason: String) extends ConfigReadError

sealed abstract class ConfigOption {
  def path: Path
}
final case class DefaultConfig(path: Path) extends ConfigOption
final case class ExplicitConfig(path: Path) extends ConfigOption

object Config {

  private[this] val logger = LoggerFactory.getLogger(this.getClass)
  private[this] def userFacingLogger = LoggerFactory.getLogger("user-facing-logs")

  def load(configOpt: ConfigOption, useDatabase: Boolean): Either[ConfigReadError, Config] = {
    configOpt match {
      case ExplicitConfig(configFile) =>
        // If users specified a config file explicitly, we ignore the SDK config.
        loadNavigatorConfig(configFile, useDatabase)
      case DefaultConfig(configFile) =>
        loadSdkConfig(useDatabase).left
          .flatMap {
            case ConfigNotFound(_) =>
              logger.info("SDK config does not exist. Falling back to Navigator config file.")
              loadNavigatorConfig(configFile, useDatabase) match {
                case Left(ConfigNotFound(_)) =>
                  logger.info("No config file found. Using default config")
                  Right(Config())
                case r => r
              }
            case e: ConfigReadError =>
              logger.warn(s"SDK config exists, but is not usable: ${e.reason}")
              Left(e)
          }
    }
  }

  def loadNavigatorConfig(
      configFile: Path,
      useDatabase: Boolean,
  ): Either[ConfigReadError, Config] = {
    @nowarn(
      "msg=local val userConfigConvert .* is never used"
    ) // false positive; macro uses aren't seen
    implicit val userConfigConvert: ConfigConvert[UserConfig] =
      mkUserConfigConvert(useDatabase = useDatabase)

    if (Files.exists(configFile)) {
      logger.info(s"Loading Navigator config file from $configFile")
      val config = ConfigFactory.parseFileAnySyntax(configFile.toAbsolutePath.toFile)
      ConfigSource
        .fromConfig(config)
        .load[Config]
        .left
        .map(e => ConfigParseFailed(e.toList.mkString(", ")))
    } else {
      Left(ConfigNotFound(s"File $configFile not found"))
    }
  }

  def loadSdkConfig(useDatabase: Boolean): Either[ConfigReadError, Config] = {
    val partiesE = for {
      projectConfigPath <- ProjectConfig.projectConfigPath()
      _ = logger.info(s"Loading SDK config file from $projectConfigPath")
      projectConfig <- ProjectConfig.loadFromFile(projectConfigPath)
      result <- projectConfig.parties
    } yield result

    partiesE match {
      case Right(Some(parties)) =>
        Right(
          Config(
            parties
              .map(p => p -> UserConfig(ApiTypes.Party(p), None, useDatabase))
              .toMap
          )
        )
      case Right(None) =>
        // Pick up parties from party management service
        Right(Config())
      case Left(SdkConfigMissing(reason)) =>
        Left(ConfigNotFound(reason))
      case Left(SdkConfigParseError(reason)) =>
        val message = s"Found a SDK project config file, but it could not be parsed: $reason."
        Left(ConfigParseFailed(message))
      case Left(SdkConfigLoadError(reason)) =>
        val message = s"Found a SDK project config file, but it could not be loaded: $reason."
        Left(ConfigParseFailed(message))
    }
  }

  def template(useDatabase: Boolean): Config =
    Config(
      Map(
        "OPERATOR" -> UserConfig(ApiTypes.Party("party"), None, useDatabase)
      )
    )

  def writeTemplateToPath(configFile: Path, useDatabase: Boolean): Unit = {
    @nowarn(
      "msg=local val userConfigConvert .* is never used"
    ) // false positive; macro uses aren't seen
    implicit val userConfigConvert: ConfigConvert[UserConfig] = mkUserConfigConvert(
      useDatabase = useDatabase
    )
    val config = ConfigWriter[Config].to(template(useDatabase))
    val cro = ConfigRenderOptions
      .defaults()
      .setComments(false)
      .setOriginComments(false)
      .setFormatted(true)
      .setJson(false)
    Files.write(configFile, config.render(cro).getBytes, CREATE_NEW)
    ()
  }

  final case class UserConfigHelper(password: Option[String], party: String, role: Option[String])

  private[this] def mkUserConfigConvert(useDatabase: Boolean): ConfigConvert[UserConfig] =
    implicitly[ConfigConvert[UserConfigHelper]].xmap(
      helper => {
        helper.password.foreach { _ =>
          userFacingLogger.warn(s"password field set for user ${helper.party} is deprecated")
        }
        UserConfig(ApiTypes.Party(helper.party), helper.role, useDatabase)
      },
      conf => UserConfigHelper(None, Tag.unwrap(conf.party), conf.role),
    )
}
