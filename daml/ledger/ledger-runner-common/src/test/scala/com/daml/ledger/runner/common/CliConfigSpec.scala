// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration, TlsVersion}
import com.daml.ledger.runner.common.CliConfigSpec.TestScope
import com.daml.lf.data.Ref
import com.daml.platform.config.ParticipantConfig
import io.netty.handler.ssl.ClientAuth
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import scopt.OParser

import java.io.File
import java.time.Duration

final class CliConfigSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with TableDrivenPropertyChecks {

  behavior of "CliConfig with RunLegacy mode"

  it should "succeed when server's private key is encrypted and secret-url is provided" in new TestScope {
    val actual = configParser(
      Seq(
        "run-legacy-cli-config",
        "--participant=participant-id=example,port=0",
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
        privateKeyFile = Some(new File("key.enc")),
        certChainFile = None,
        trustCollectionFile = None,
      )
    )
  }

  it should "fail when server's private key is encrypted but secret-url is not provided" in new TestScope {
    configParser(
      Seq(
        "run-legacy-cli-config",
        "--participant=participant-id=example,port=0",
        "--pem",
        "key.enc",
      )
    ) shouldBe None
  }

  it should "fail parsing a bogus TLS version" in new TestScope {
    configParser(
      Seq(
        "run-legacy-cli-config",
        "--participant=participant-id=example,port=0",
        "--min-tls-version",
        "111",
      )
    ) shouldBe None
  }

  it should "succeed parsing a supported TLS version" in new TestScope {
    val actual = configParser(
      Seq(
        "run-legacy-cli-config",
        "--participant=participant-id=example,port=0",
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

  it should "succeed when server's private key is in plaintext and secret-url is not provided" in new TestScope {
    val actual = configParser(
      Seq(
        "run-legacy-cli-config",
        "--participant=participant-id=example,port=0",
        "--pem",
        "key.txt",
      )
    )

    actual should not be None
    actual.get.tlsConfig shouldBe Some(
      TlsConfiguration(
        enabled = true,
        secretsUrl = None,
        privateKeyFile = Some(new File("key.txt")),
        certChainFile = None,
        trustCollectionFile = None,
      )
    )
  }

  it should "be running in Run mode if parameters list is empty" in new TestScope {
    configParser(Seq()).value.mode shouldBe Mode.Run
  }

  it should "fail if a participant is not provided in run mode" in new TestScope {
    configParser(Seq("run-legacy-cli-config")) shouldEqual None
  }

  it should "fail if a participant is not provided when dumping the index metadata" in new TestScope {
    configParser(Seq(dumpIndexMetadataCommand)) shouldEqual None

  }
  it should "succeed if a participant is provided when dumping the index metadata" in new TestScope {
    configParser(Seq(dumpIndexMetadataCommand, "some-jdbc-url")) should not be empty
  }

  it should "succeed if more than one participant is provided when dumping the index metadata" in new TestScope {
    configParser(
      Seq(dumpIndexMetadataCommand, "some-jdbc-url", "some-other-jdbc-url")
    ) should not be empty
  }

  it should "accept single default participant" in new TestScope {
    val config =
      configParser(Seq("run-legacy-cli-config", "--participant", "participant-id=p1,port=123"))
        .getOrElse(fail())
    config.participants(0).participantId should be(Ref.ParticipantId.assertFromString("p1"))
  }

  it should "accept multiple participant, with unique id" in new TestScope {
    val config = configParser(
      Seq(
        "run-legacy-cli-config",
        "--participant",
        "participant-id=p1,port=123",
        "--participant",
        "participant-id=p2,port=123",
      )
    ).getOrElse(fail())
    config.participants(0).participantId should be(Ref.ParticipantId.assertFromString("p1"))
    config.participants(1).participantId should be(Ref.ParticipantId.assertFromString("p2"))
  }

  it should "fail to accept multiple participants with non-unique ids" in new TestScope {
    configParser(
      Seq(
        "run-legacy-cli-config",
        "--participant",
        "participant-id=p1,port=123",
        "--participant",
        "participant-id=p1,port=123",
      )
    ) shouldBe None
  }

  it should "get the jdbc string from the command line argument when provided" in new TestScope {
    val jdbcFromCli = "command-line-jdbc"
    val config = configParser(
      Seq(
        "run-legacy-cli-config",
        participantOption,
        s"$fixedParticipantSubOptions,$jdbcUrlSubOption=${TestJdbcValues.jdbcFromCli}",
      )
    )
      .getOrElse(fail())
    config.participants.head.serverJdbcUrl should be(jdbcFromCli)
  }

  it should "get the jdbc string from the environment when provided" in new TestScope {
    val config = configParser(
      Seq(
        "run-legacy-cli-config",
        participantOption,
        s"$fixedParticipantSubOptions,$jdbcUrlEnvSubOption=$jdbcEnvVar",
      ),
      { case `jdbcEnvVar` => Some(TestJdbcValues.jdbcFromEnv) },
    ).getOrElse(parsingFailure())
    config.participants.head.serverJdbcUrl should be(TestJdbcValues.jdbcFromEnv)
  }

  it should "return the default when env variable not provided" in new TestScope {
    val defaultJdbc = ParticipantConfig.defaultIndexJdbcUrl(participantId)
    val config = configParser(
      Seq(
        "run-legacy-cli-config",
        participantOption,
        s"$fixedParticipantSubOptions,$jdbcUrlEnvSubOption=$jdbcEnvVar",
      )
    ).getOrElse(parsingFailure())

    config.participants.head.serverJdbcUrl should be(defaultJdbc)
  }

  it should "get the certificate revocation checking parameter when provided" in new TestScope {
    val config =
      configParser(parameters = minimumRunLegacyOptions ++ List(s"$certRevocationChecking", "true"))
        .getOrElse(parsingFailure())

    config.tlsConfig.value.enableCertRevocationChecking should be(true)
  }

  it should "get the tracker retention period when provided" in new TestScope {
    val periodStringRepresentation = "P0DT1H2M3S"
    val expectedPeriod = Duration.ofHours(1).plusMinutes(2).plusSeconds(3)
    val config =
      configParser(parameters =
        minimumRunLegacyOptions ++ List(trackerRetentionPeriod, periodStringRepresentation)
      )
        .getOrElse(parsingFailure())

    config.commandConfig.trackerRetentionPeriod should be(expectedPeriod)
  }

  it should "set the client-auth parameter when provided" in new TestScope {
    val cases = Table(
      ("clientAuthParam", "expectedParsedValue"),
      ("none", ClientAuth.NONE),
      ("optional", ClientAuth.OPTIONAL),
      ("require", ClientAuth.REQUIRE),
    )
    forAll(cases) { (param, expectedValue) =>
      val config =
        configParser(parameters = minimumRunLegacyOptions ++ List(clientAuth, param))
          .getOrElse(parsingFailure())

      config.tlsConfig.value.clientAuth shouldBe expectedValue
    }
  }

  it should "handle '--enable-user-management' flag correctly" in new TestScope {
    configParser(
      minimumRunLegacyOptions ++ Seq(
        "--enable-user-management"
      )
    ) shouldBe None

    configParser(
      minimumRunLegacyOptions ++ Seq(
        "--enable-user-management",
        "false",
      )
    ).value.userManagementConfig.enabled shouldBe false

    configParser(
      minimumRunLegacyOptions ++ Seq(
        "--enable-user-management",
        "true",
      )
    ).value.userManagementConfig.enabled shouldBe true

    configParser(
      minimumRunLegacyOptions
    ).value.userManagementConfig.enabled shouldBe false
  }

  it should "set REQUIRE client-auth when the parameter is not explicitly provided" in new TestScope {
    val aValidTlsOptions = List(s"$certRevocationChecking", "false")
    val config =
      configParser(parameters = minimumRunLegacyOptions ++ aValidTlsOptions)
        .getOrElse(parsingFailure())

    config.tlsConfig.value.clientAuth shouldBe ClientAuth.REQUIRE
  }

  it should "handle '--user-management-max-cache-size' flag correctly" in new TestScope {
    // missing cache size value
    configParserRunLegacy(
      Seq("--user-management-max-cache-size")
    ) shouldBe None
    // default
    configParserRunLegacy().value.userManagementConfig.maxCacheSize shouldBe 100
    // custom value
    configParserRunLegacy(
      Seq(
        "--user-management-max-cache-size",
        "123",
      )
    ).value.userManagementConfig.maxCacheSize shouldBe 123
  }

  it should "handle '--user-management-cache-expiry' flag correctly" in new TestScope {
    // missing cache size value
    configParserRunLegacy(
      Seq("--user-management-cache-expiry")
    ) shouldBe None
    // default
    configParserRunLegacy().value.userManagementConfig.cacheExpiryAfterWriteInSeconds shouldBe 5
    // custom value
    configParserRunLegacy(
      Seq(
        "--user-management-cache-expiry",
        "123",
      )
    ).value.userManagementConfig.cacheExpiryAfterWriteInSeconds shouldBe 123
  }

  it should "handle '--user-management-max-users-page-size' flag correctly" in new TestScope {
    // missing value
    configParserRunLegacy(
      Seq("--user-management-max-users-page-size")
    ) shouldBe None
    // default
    configParserRunLegacy().value.userManagementConfig.maxUsersPageSize shouldBe 1000
    // custom value
    configParserRunLegacy(
      Seq(
        "--user-management-max-users-page-size",
        "123",
      )
    ).value.userManagementConfig.maxUsersPageSize shouldBe 123
    // values in range [1, 99] are disallowed
    configParserRunLegacy(
      Array(
        "--user-management-max-users-page-size",
        "1",
      )
    ) shouldBe None
    configParserRunLegacy(
      Array(
        "--user-management-max-users-page-size",
        "99",
      )
    ) shouldBe None
    // negative values are disallowed
    configParserRunLegacy(
      Array(
        "--user-management-max-users-page-size",
        "-1",
      )
    ) shouldBe None
  }

  behavior of "CliConfig with Run mode"

  it should "support empty cli parameters" in new TestScope {
    configParserRun().value.configFiles shouldBe Seq.empty
    configParserRun().value.configMap shouldBe Map.empty
  }

  it should "support key-value map via -C option" in new TestScope {
    configParserRun(Seq("-C", "key1=value1,key2=value2")).value.configMap shouldBe Map(
      "key1" -> "value1",
      "key2" -> "value2",
    )
  }

  it should "support key-value map via multiple -C options" in new TestScope {
    configParserRun(Seq("-C", "key1=value1", "-C", "key2=value2")).value.configMap shouldBe Map(
      "key1" -> "value1",
      "key2" -> "value2",
    )
  }

  it should "support key-value map with complex hocon path" in new TestScope {
    configParserRun(
      Seq("-C", "ledger.participant.api-server.host=localhost")
    ).value.configMap shouldBe Map(
      "ledger.participant.api-server.host" -> "localhost"
    )
  }

  it should "support existing config file" in new TestScope {
    configParserRun(Seq("-c", confFilePath)).value.configFiles shouldBe Seq(
      confFile
    )
  }

  it should "support multiple existing config files" in new TestScope {
    configParserRun(Seq("-c", confFilePath, "-c", confFilePath2)).value.configFiles shouldBe Seq(
      confFile,
      confFile2,
    )

    configParserRun(Seq("-c", s"$confFilePath,$confFilePath2")).value.configFiles shouldBe Seq(
      confFile,
      confFile2,
    )
  }

  it should "fail for non-existing config file" in new TestScope {
    configParserRun(Seq("-c", "somefile.conf")) shouldBe None
  }

}

object CliConfigSpec {

  trait TestScope extends Matchers {

    val dumpIndexMetadataCommand = "dump-index-metadata"
    val participantOption = "--participant"
    val participantId: Ref.ParticipantId =
      Ref.ParticipantId.assertFromString("dummy-participant")
    val fixedParticipantSubOptions = s"participant-id=$participantId,port=123"

    val jdbcUrlSubOption = "server-jdbc-url"
    val jdbcUrlEnvSubOption = "server-jdbc-url-env"
    val jdbcEnvVar = "JDBC_ENV_VAR"

    val certRevocationChecking = "--cert-revocation-checking"
    val trackerRetentionPeriod = "--tracker-retention-period"
    val clientAuth = "--client-auth"

    object TestJdbcValues {
      val jdbcFromCli = "command-line-jdbc"
      val jdbcFromEnv = "env-jdbc"
    }

    val minimumRunLegacyOptions = List(
      "run-legacy-cli-config",
      participantOption,
      s"$fixedParticipantSubOptions,$jdbcUrlSubOption=${TestJdbcValues.jdbcFromCli}",
    )

    val minimumRunOptions = List(
      "run"
    )

    def configParser(
        parameters: Seq[String],
        getEnvVar: String => Option[String] = (_ => None),
    ): Option[CliConfig[Unit]] =
      CliConfig.parse(
        name = "Test",
        extraOptions = OParser.builder[CliConfig[Unit]].note(""),
        defaultExtra = (),
        args = parameters,
        getEnvVar = getEnvVar,
      )

    def configParserRunLegacy(
        parameters: Iterable[String] = Seq.empty
    ): Option[CliConfig[Unit]] =
      configParser(
        minimumRunLegacyOptions ++ parameters
      )

    def configParserRun(
        parameters: Iterable[String] = Seq.empty
    ): Option[CliConfig[Unit]] =
      configParser(
        minimumRunOptions ++ parameters
      )

    def parsingFailure(): Nothing = fail("Config parsing failed.")

    def confStringPath = "ledger/ledger-runner-common/src/test/resources/test.conf"
    def confFilePath = rlocation(confStringPath)
    def confFile = requiredResource(confStringPath)

    def confStringPath2 = "ledger/ledger-runner-common/src/test/resources/test2.conf"
    def confFilePath2 = rlocation(confStringPath2)
    def confFile2 = requiredResource(confStringPath2)

  }
}
