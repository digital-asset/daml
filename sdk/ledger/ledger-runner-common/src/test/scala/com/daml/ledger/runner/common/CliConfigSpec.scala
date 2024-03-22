// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration, TlsVersion}
import com.daml.lf.data.Ref
import io.netty.handler.ssl.ClientAuth
import org.scalatest.{Assertion, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import scopt.OptionParser
import java.io.File
import java.time.Duration

final class CliConfigSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with TableDrivenPropertyChecks {

  private val dumpIndexMetadataCommand = "dump-index-metadata"
  private val participantOption = "--participant"
  private val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("dummy-participant")
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
  ): Option[CliConfig[Unit]] =
    CliConfig.parse(
      name = "Test",
      extraOptions = (_: OptionParser[CliConfig[Unit]]) => (),
      defaultExtra = (),
      args = parameters,
      getEnvVar = getEnvVar,
    )

  private def configParserSimple(
      parameters: Iterable[String] = Seq.empty
  ): Option[CliConfig[Unit]] =
    configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
      ) ++ parameters
    )

  private def checkOptionFail(parameters: Iterable[String]): Assertion = {
    configParserSimple(parameters) shouldBe None
  }

  behavior of "Runner"

  it should "succeed when server's private key is encrypted and secret-url is provided" in {
    val actual = configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
        "--pem",
        "key.enc",
        "--tls-secrets-url",
        "http://aaa",
      )
    )

    actual should not be None
    actual.get.tlsConfig shouldBe Some(
      TlsConfiguration(
        enabled = true,
        secretsUrl = Some(SecretsUrl.fromString("http://aaa")),
        keyFile = Some(new File("key.enc")),
        keyCertChainFile = None,
        trustCertCollectionFile = None,
      )
    )
  }

  it should "fail when server's private key is encrypted but secret-url is not provided" in {
    configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
        "--pem",
        "key.enc",
      )
    ) shouldBe None
  }

  it should "fail parsing a bogus TLS version" in {
    configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
        "--min-tls-version",
        "111",
      )
    ) shouldBe None
  }

  it should "succeed parsing a supported TLS version" in {
    val actual = configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
        "--min-tls-version",
        "1.3",
      )
    )

    actual should not be None
    actual.get.tlsConfig shouldBe Some(
      TlsConfiguration(
        enabled = true,
        minimumServerProtocolVersion = Some(TlsVersion.V1_3),
      )
    )
  }

  it should "succeed when server's private key is in plaintext and secret-url is not provided" in {
    val actual = configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
        "--pem",
        "key.txt",
      )
    )

    actual should not be None
    actual.get.tlsConfig shouldBe Some(
      TlsConfiguration(
        enabled = true,
        secretsUrl = None,
        keyFile = Some(new File("key.txt")),
        keyCertChainFile = None,
        trustCertCollectionFile = None,
      )
    )
  }

  it should "fail if a participant is not provided in run mode" in {
    configParser(Seq.empty) shouldEqual None
  }

  it should "fail if a participant is not provided when dumping the index metadata" in {
    configParser(Seq(dumpIndexMetadataCommand)) shouldEqual None

  }
  it should "succeed if a participant is provided when dumping the index metadata" in {
    configParser(Seq(dumpIndexMetadataCommand, "some-jdbc-url")) should not be empty
  }

  it should "succeed if more than one participant is provided when dumping the index metadata" in {
    configParser(
      Seq(dumpIndexMetadataCommand, "some-jdbc-url", "some-other-jdbc-url")
    ) should not be empty
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
    val expectedPeriod = Duration.ofHours(1).plusMinutes(2).plusSeconds(3)
    val config =
      configParser(parameters =
        minimalValidOptions ++ List(trackerRetentionPeriod, periodStringRepresentation)
      )
        .getOrElse(parsingFailure())

    config.commandConfig.trackerRetentionPeriod should be(expectedPeriod)
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

  it should "handle '--enable-user-management' flag correctly" in {
    configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
        "--enable-user-management",
      )
    ) shouldBe None

    configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
        "--enable-user-management",
        "false",
      )
    ).value.userManagementConfig.enabled shouldBe false

    configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
        "--enable-user-management",
        "true",
      )
    ).value.userManagementConfig.enabled shouldBe true

    configParser(
      Seq(
        dumpIndexMetadataCommand,
        "some-jdbc-url",
      )
    ).value.userManagementConfig.enabled shouldBe false
  }

  it should "set REQUIRE client-auth when the parameter is not explicitly provided" in {
    val aValidTlsOptions = List(s"$certRevocationChecking", "false")
    val config =
      configParser(parameters = minimalValidOptions ++ aValidTlsOptions).getOrElse(parsingFailure())

    config.tlsConfig.value.clientAuth shouldBe ClientAuth.REQUIRE
  }

  it should "handle '--user-management-max-cache-size' flag correctly" in {
    // missing cache size value
    configParserSimple(
      Seq("--user-management-max-cache-size")
    ) shouldBe None
    // default
    configParserSimple().value.userManagementConfig.maxCacheSize shouldBe 100
    // custom value
    configParserSimple(
      Seq(
        "--user-management-max-cache-size",
        "123",
      )
    ).value.userManagementConfig.maxCacheSize shouldBe 123
  }

  it should "handle '--user-management-cache-expiry' flag correctly" in {
    // missing cache size value
    configParserSimple(
      Seq("--user-management-cache-expiry")
    ) shouldBe None
    // default
    configParserSimple().value.userManagementConfig.cacheExpiryAfterWriteInSeconds shouldBe 5
    // custom value
    configParserSimple(
      Seq(
        "--user-management-cache-expiry",
        "123",
      )
    ).value.userManagementConfig.cacheExpiryAfterWriteInSeconds shouldBe 123
  }

  it should "handle '--user-management-max-users-page-size' flag correctly" in {
    // missing value
    configParserSimple(
      Seq("--user-management-max-users-page-size")
    ) shouldBe None
    // default
    configParserSimple().value.userManagementConfig.maxUsersPageSize shouldBe 1000
    // custom value
    configParserSimple(
      Seq(
        "--user-management-max-users-page-size",
        "123",
      )
    ).value.userManagementConfig.maxUsersPageSize shouldBe 123
    // values in range [1, 99] are disallowed
    checkOptionFail(
      Array(
        "--user-management-max-users-page-size",
        "1",
      )
    )
    checkOptionFail(
      Array(
        "--user-management-max-users-page-size",
        "99",
      )
    )
    // negative values are disallowed
    checkOptionFail(
      Array(
        "--user-management-max-users-page-size",
        "-1",
      )
    )
  }

  private def parsingFailure(): Nothing = fail("Config parsing failed.")
}
