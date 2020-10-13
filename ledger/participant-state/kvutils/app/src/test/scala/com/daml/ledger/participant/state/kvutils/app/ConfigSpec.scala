// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.ledger.participant.state.v1.ParticipantId
import org.scalatest.{FlatSpec, Matchers, OptionValues}
import scopt.OptionParser

final class ConfigSpec extends FlatSpec with Matchers with OptionValues {

  private val dumpIndexMetadataCommand = "dump-index-metadata"
  private val participantOption = "--participant"
  private val participantId: ParticipantId = ParticipantId.assertFromString("dummy-participant")
  private val fixedParticipantSubOptions = s"participant-id=$participantId,port=123"
  private val jdbcUrlSubOption = "server-jdbc-url"
  private val jdbcUrlEnvSubOption = "server-jdbc-url-env"
  private def configParser(
      parameters: Seq[String],
      getEnvVar: String => Option[String] = (_ => None)): Option[Config[Unit]] =
    Config.parse("Test", (_: OptionParser[Config[Unit]]) => (), (), parameters, getEnvVar)

  behavior of "Runner"

  it should "fail if a participant is not provided in run mode" in {
    configParser(Seq.empty) shouldEqual None
  }

  it should "fail if a participant is not provided when dumping the index metadata" in {
    configParser(Seq(dumpIndexMetadataCommand)) shouldEqual None
  }

  it should "succeed if a participant is provided when dumping the index metadata" in {
    configParser(Seq(dumpIndexMetadataCommand, "some-jdbc-url"))
  }

  it should "succeed if more than one participant is provided when dumping the index metadata" in {
    configParser(Seq(dumpIndexMetadataCommand, "some-jdbc-url", "some-other-jdbc-url"))
  }

  it should "get the jdbc string from the command line argument when provided" in {
    val jdbcFromCli = "command-line-jdbc"
    val config = configParser(
      Seq(participantOption, s"$fixedParticipantSubOptions,$jdbcUrlSubOption=$jdbcFromCli"))
      .getOrElse(fail())
    config.participants.head.serverJdbcUrl should be(jdbcFromCli)
  }

  it should "get the jdbc string from the environment when provided" in {
    val jdbcEnvVar = "JDBC_ENV_VAR"
    val jdbcFromEnv = "env-jdbc"
    val config = configParser(
      Seq(participantOption, s"$fixedParticipantSubOptions,$jdbcUrlEnvSubOption=$jdbcEnvVar"),
      { case `jdbcEnvVar` => Some(jdbcFromEnv) }
    ).getOrElse(fail())
    config.participants.head.serverJdbcUrl should be(jdbcFromEnv)
  }

  it should "return the default when env variable not provided" in {
    val jdbcEnvVar = "JDBC_ENV_VAR"
    val defaultJdbc = ParticipantConfig.defaultIndexJdbcUrl(participantId)
    val config = configParser(
      Seq(participantOption, s"$fixedParticipantSubOptions,$jdbcUrlEnvSubOption=$jdbcEnvVar")
    ).getOrElse(fail())
    config.participants.head.serverJdbcUrl should be(defaultJdbc)
  }

}
