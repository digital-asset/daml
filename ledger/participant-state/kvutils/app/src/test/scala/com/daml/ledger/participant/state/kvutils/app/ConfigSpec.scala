// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.util.concurrent.TimeUnit

import com.daml.ledger.participant.state.v1.ParticipantId
import io.netty.handler.ssl.ClientAuth
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import scopt.OptionParser

import scala.concurrent.duration.FiniteDuration

final class ConfigSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with TableDrivenPropertyChecks {

  private val dumpIndexMetadataCommand = "dump-index-metadata"
  private val participantOption = "--participant"
  private val participantId: ParticipantId = ParticipantId.assertFromString("dummy-participant")
  private val fixedParticipantSubOptions = s"participant-id=$participantId,port=123"

  private val jdbcUrlSubOption = "server-jdbc-url"
  private val jdbcUrlEnvSubOption = "server-jdbc-url-env"
  private val jdbcEnvVar = "JDBC_ENV_VAR"

  private val certRevocationChecking = "--cert-revocation-checking"
  private val trackerRetentionPeriod = "--tracker-retention-period"
  private val clientAuth = "--client-auth"

  object TestJdbcValues {
    val jdbcFromCli = "command-line-jdbc"
    val jdbcFromEnv = "env-jdbc"
  }

  private val minimalValidOptions = List(
    participantOption,
    s"$fixedParticipantSubOptions,$jdbcUrlSubOption=${TestJdbcValues.jdbcFromCli}",
  )

  private def configParser(
      parameters: Seq[String],
      getEnvVar: String => Option[String] = (_ => None),
  ): Option[Config[Unit]] =
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
        s"$fixedParticipantSubOptions,$jdbcUrlSubOption=${TestJdbcValues.jdbcFromCli}",
      )
    )
      .getOrElse(fail())
    config.participants.head.serverJdbcUrl should be(jdbcFromCli)
  }

  it should "get the jdbc string from the environment when provided" in {
    val config = configParser(
      Seq(participantOption, s"$fixedParticipantSubOptions,$jdbcUrlEnvSubOption=$jdbcEnvVar"),
      { case `jdbcEnvVar` => Some(TestJdbcValues.jdbcFromEnv) },
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

  it should "get the tracker retention period when provided" in {
    val periodStringRepresentation = "P0DT1H2M3S"
    val expectedPeriod =
      FiniteDuration(1, TimeUnit.HOURS) +
        FiniteDuration(2, TimeUnit.MINUTES) +
        FiniteDuration(3, TimeUnit.SECONDS)
    val config =
      configParser(parameters =
        minimalValidOptions ++ List(trackerRetentionPeriod, periodStringRepresentation)
      )
        .getOrElse(parsingFailure())

    config.trackerRetentionPeriod should be(expectedPeriod)
  }

  it should "set the client-auth parameter when provided" in {
    val cases = Table(
      ("clientAuthParam", "expectedParsedValue"),
      ("none", ClientAuth.NONE),
      ("optional", ClientAuth.OPTIONAL),
      ("require", ClientAuth.REQUIRE),
    )
    forAll(cases) { (param, expectedValue) =>
      val config =
        configParser(parameters = minimalValidOptions ++ List(clientAuth, param))
          .getOrElse(parsingFailure())

      config.tlsConfig.value.clientAuth shouldBe expectedValue
    }
  }

  it should "set REQUIRE client-auth when the parameter is not explicitly provided" in {
    val aValidTlsOptions = List(s"$certRevocationChecking", "false")
    val config =
      configParser(parameters = minimalValidOptions ++ aValidTlsOptions).getOrElse(parsingFailure())

    config.tlsConfig.value.clientAuth shouldBe ClientAuth.REQUIRE
  }

  private def parsingFailure(): Nothing = fail("Config parsing failed.")
}
