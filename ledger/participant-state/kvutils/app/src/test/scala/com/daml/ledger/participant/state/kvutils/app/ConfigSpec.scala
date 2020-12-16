// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.ledger.participant.state.v1.ParticipantId
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scopt.OptionParser

final class ConfigSpec extends AnyFlatSpec with Matchers with OptionValues {

  private val dumpIndexMetadataCommand = "dump-index-metadata"
  private val participantOption = "--participant"
  private val participantId: ParticipantId = ParticipantId.assertFromString("dummy-participant")
  private val fixedParticipantSubOptions = s"participant-id=$participantId,port=123"

  private val jdbcUrlSubOption = "server-jdbc-url"
  private val jdbcUrlEnvSubOption = "server-jdbc-url-env"
  private val jdbcEnvVar = "JDBC_ENV_VAR"

  private val certRevocationChecking = "--cert-revocation-checking"

  object TestJdbcValues {
    val jdbcFromCli = "command-line-jdbc"
    val jdbcFromEnv = "env-jdbc"
  }

  private val minimalValidOptions = List(
    participantOption,
    s"$fixedParticipantSubOptions,$jdbcUrlSubOption=${TestJdbcValues.jdbcFromCli}")

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
      Seq(
        participantOption,
        s"$fixedParticipantSubOptions,$jdbcUrlSubOption=${TestJdbcValues.jdbcFromCli}"))
      .getOrElse(fail())
    config.participants.head.serverJdbcUrl should be(jdbcFromCli)
  }

  it should "get the jdbc string from the environment when provided" in {
    val config = configParser(
      Seq(participantOption, s"$fixedParticipantSubOptions,$jdbcUrlEnvSubOption=$jdbcEnvVar"),
      { case `jdbcEnvVar` => Some(TestJdbcValues.jdbcFromEnv) }
    ).getOrElse(parsingFailure())
    config.participants.head.serverJdbcUrl should be(TestJdbcValues.jdbcFromEnv)
  }

  it should "return the default when env variable not provided" in {
    val defaultJdbc = ParticipantConfig.defaultIndexJdbcUrl(participantId)
    val config = configParser(
      Seq(participantOption, s"$fixedParticipantSubOptions,$jdbcUrlEnvSubOption=$jdbcEnvVar")
    ).getOrElse(parsingFailure())

    config.participants.head.serverJdbcUrl should be(defaultJdbc)
  }

  it should "get the certificate revocation checking parameter when provided" in {
    val config =
      configParser(parameters = minimalValidOptions ++ List(s"$certRevocationChecking", "true"))
        .getOrElse(parsingFailure())

    config.tlsConfig.value.enableCertRevocationChecking should be(true)
  }

  private def parsingFailure(): Nothing = fail("Config parsing failed.")
}
