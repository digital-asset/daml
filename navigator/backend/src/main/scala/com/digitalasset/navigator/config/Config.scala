// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.config

import java.nio.file.{Files, Path}
import java.nio.file.StandardOpenOption._

import com.digitalasset.navigator.model.PartyState
import com.digitalasset.ledger.api.refinements.ApiTypes
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import pureconfig.{ConfigConvert, ConfigWriter}
import pureconfig.error.ConfigReaderFailures
import scalaz.Tag

final case class UserConfig(password: Option[String], party: PartyState, role: Option[String])
/* The configuration has an empty map as default list of users because you can login as party too */
@SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
final case class Config(users: Map[String, UserConfig] = Map.empty[String, UserConfig]) {

  def userIds: Set[String] = users.keySet

  def roles: Set[String] = users.values.flatMap(_.role)(collection.breakOut)
  def parties: Set[PartyState] = users.values.map(_.party)(collection.breakOut)
}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Option2Iterable",
    "org.wartremover.warts.ExplicitImplicitTypes"))
object Config {

  def load(configFile: Path, useDatabase: Boolean): Either[ConfigReaderFailures, Config] = {
    implicit val partyConfigConvert = ConfigConvert.viaNonEmptyString[PartyState](
      str => _ => Right(new PartyState(ApiTypes.Party(str), useDatabase)),
      t => Tag.unwrap(t.name))
    val config = ConfigFactory.parseFileAnySyntax(configFile.toAbsolutePath.toFile)
    pureconfig.loadConfig[Config](config)
  }

  def template(useDatabase: Boolean): Config =
    Config(
      Map(
        "OPERATOR" -> UserConfig(
          Some("password"),
          new PartyState(ApiTypes.Party("party"), useDatabase),
          None)
      ))

  def writeTemplateToPath(configFile: Path, useDatabase: Boolean): Unit = {
    implicit val partyConfigConvert = ConfigConvert.viaNonEmptyString[PartyState](
      str => _ => Right(new PartyState(ApiTypes.Party(str), useDatabase)),
      t => Tag.unwrap(t.name))
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
}
