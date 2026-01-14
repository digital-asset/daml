// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.AuthServiceConfig.UnsafeJwtHmac256
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ConfigErrors.{
  CannotParseFilesError,
  CannotReadFilesError,
  CantonConfigError,
  GenericConfigError,
  NoConfigFiles,
  SubstitutionError,
}
import com.digitalasset.canton.config.InitConfigBase.NodeIdentifierConfig
import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel
import com.digitalasset.canton.crypto.SigningAlgorithmSpec.EcDsaSha256
import com.digitalasset.canton.crypto.SigningKeySpec.EcP256
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.digitalasset.canton.version.{ProtocolVersionCompatibility, ReleaseVersion}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.io.File as JFile
import java.nio.file.Path

class CantonConfigTest extends AnyWordSpec with BaseTest {

  import scala.jdk.CollectionConverters.*

  private lazy val baseDir: File = "community" / "app" / "src" / "test" / "resources"
  private lazy val examplesDir: File = "community" / "app" / "src" / "pack" / "examples"
  private lazy val confDir: File = "community" / "app" / "src" / "pack" / "config"
  private lazy val node: File = confDir / "participant.conf"
  private lazy val env: File =
    "community" / "app" / "src" / "test" / "resources" / "advancedConfDef.env"
  private lazy val perf: File = "scripts" / "canton-testing"

  lazy val files: Seq[JFile] = Seq(env, node).map(_.toJava)

  private lazy val deprecatedConfigs: File =
    "community" / "app" / "src" / "test" / "resources" / "deprecated-configs"

  private lazy val remoteSequencerHAConfig: File =
    baseDir / "external-ha-sequencers-remote-admin-parsing.conf"

  private lazy val distributedConf: File = baseDir / "distributed-single-synchronizer-topology.conf"

  private lazy val simpleConf: File = examplesDir / "01-simple-topology" / "simple-topology.conf"
  private lazy val simpleConfPath: String = simpleConf.pathAsString

  private lazy val duplicateStorageWithoutReplicationConfig: File =
    baseDir / "duplicate-storage-without-replication.conf"

  private lazy val duplicateStorageWithReplicationConfig: File =
    baseDir / "duplicate-storage-with-replication.conf"

  private lazy val unsupportedMinProtocolVersionConfig: File =
    baseDir / "unsupported-minimum-protocol-version.conf"

  private def loadFile(resourcePath: String): Either[CantonConfigError, CantonConfig] =
    loadFiles(Seq(resourcePath))

  private def loadFiles(
      resourcePaths: Seq[String]
  ): Either[CantonConfigError, CantonConfig] = {
    val files = resourcePaths.map(r => (baseDir.toString / r).toJava)
    CantonConfig.parseAndLoad(files, Some(DefaultPorts.create()))
  }

  "the example simple topology configuration" should {
    lazy val config =
      loadFile(simpleConfPath).valueOrFail("failed to load simple-topology.conf")

    "contain a couple of participants" in {
      config.participants should have size 2
    }

    "contain a single sequencer" in {
      config.sequencers should have size 1
    }

    "contain a single mediator" in {
      config.mediators should have size 1
    }

    "produce a port definition message" in {
      config.portDescription.split(";") should contain theSameElementsAs List(
        "participant1:admin-api=5012,ledger-api=5011",
        "participant2:admin-api=5022,ledger-api=5021",
        "sequencer1:admin-api=5002,public-api=5001",
        "mediator1:admin-api=5202",
      )
    }
    "check startup memory checker config" in {
      config.parameters.startupMemoryCheckConfig shouldBe StartupMemoryCheckConfig(
        ReportingLevel.Warn
      )
    }

  }

  "the invalid node names configuration" should {
    "return an error" in {
      loggerFactory.assertLogs(
        {
          val result = loadFile("invalid-configs/invalid-node-names.conf")
          inside(result.left.value) { case GenericConfigError.Error(cause) =>
            cause should include(
              "Node name is too long. Max length: 30. Length: 43. Name: \"myparticipant01234567890123456...\""
            )
            cause should include(
              "Node name contains invalid characters (allowed: [a-zA-Z0-9_-]): \"my`participant\""
            )
            cause should include(
              "Node name contains invalid characters (allowed: [a-zA-Z0-9_-]): \"my participant\""
            )
          }
        },
        entry => {
          entry.shouldBeCantonErrorCode(GenericConfigError.code)
          val cause = entry.errorMessage
          cause should include(
            "Node name is too long. Max length: 30. Length: 43. Name: \"myparticipant01234567890123456...\""
          )
          // The other causes get truncated away, unfortunately.
          // See https://github.com/digital-asset/daml/issues/12785
        },
      )
    }
  }

  // test that fails because we misspelled 'port' as 'bort'
  "the bort configuration" should {
    "return an error mentioning the bort issue" in {
      val result = loggerFactory.assertLogs(
        loadFiles(Seq(simpleConfPath, "invalid-configs/bort.conf")),
        _.errorMessage should (include("bort.conf") and include("Unknown key")),
      )
      result.left.value shouldBe a[GenericConfigError.Error]
    }
  }

  // test that fails because of missing '{' in .conf-file
  "the missing-bracket configuration" should {
    "return a CannotParseFilesError during loading when combined with simple config" in {
      val result =
        loggerFactory.assertLogs(
          loadFiles(Seq(simpleConfPath, "invalid-configs/missing-bracket.conf")),
          _.mdc("err-context") should (include("missing-bracket.conf") and include(
            "expecting a close parentheses ')' here, not: end of file"
          )),
        )
      result.left.value shouldBe a[CannotParseFilesError.Error]
    }

    "return a CannotParseFilesError during loading when not combined with simple config" in {
      val result =
        loggerFactory.assertLogs(
          loadFiles(Seq(simpleConfPath, "invalid-configs/missing-bracket.conf")),
          _.mdc("err-context") should (include("missing-bracket.conf") and include(
            "expecting a close parentheses ')' here, not: end of file"
          )),
        )
      result.left.value shouldBe a[CannotParseFilesError.Error]
    }
  }

  "the negative-port configuration" should {

    "return a sensible error message during loading" in {
      val result =
        loggerFactory.assertLogs(
          loadFiles(Seq(simpleConfPath, "invalid-configs/negative-port.conf")),
          _.errorMessage should (include("negative-port.conf") and include("Unable to create Port")),
        )
      result.left.value shouldBe a[GenericConfigError.Error]
    }
  }

  // test that fails because of using env variable substitution with a non-existent env variable
  "the undefined-env-var configuration" should {

    "return an error during loading" in {
      // defined like this because instantiating the error will automatically lead to another error message being logged
      val code = loggerFactory.assertLogs(
        SubstitutionError.Error(Seq()).code.id,
        _.message should include(""),
      )
      val result = loggerFactory.assertLogs(
        loadFile("invalid-configs/undefined-env-var.conf"),
        logEntry => {
          logEntry.mdc("err-context") should (include("UNDEFINED_ENV_VARIABLE") and include(
            "undefined-env-var.conf"
          ))
          logEntry.errorMessage should include(code)
        },
      )
      result.left.value shouldBe a[SubstitutionError.Error]
    }
  }

  // confs with missing files for includes
  // no error despite missing include
  "the include-missing-file configuration" should {
    lazy val config =
      loadFiles(Seq(simpleConfPath, "invalid-configs/include-missing-file.conf"))
        .valueOrFail("failed to load include-missing-file.conf")

    "contain a couple of participants2" in {
      config.participants should have size 2
    }
  }

  // tests that fails because of a `include required` of a missing file
  "the require-missing-file configuration" should {
    "throw a meaningful error message during loading" in {
      // sadly, we don't have enough information at the time the error is thrown to also include
      // `require-missing-file.conf` in the error message
      val result = loggerFactory.assertLogs(
        loadFiles(Seq("invalid-configs/require-missing-file.conf", simpleConfPath)),
        _.mdc("err-context") should (include("this-file-does-not-exist.conf") and include(
          "resource not found"
        )),
      )
      result.left.value shouldBe a[CannotParseFilesError.Error]
    }
  }

  "configuration file with unknown keys" should {
    "should return an error" in {
      val result =
        loggerFactory.assertLogs(
          loadFile("invalid-configs/unknown-key-in-nested-config.conf"),
          _.errorMessage should include("canton.monitoring.this-is-not-a-key"),
        )
      result.left.value shouldBe a[GenericConfigError.Error]
    }
  }

  "load with multiple config files" should {
    lazy val config1: Config = ConfigFactory.parseMap(
      Map(
        "item1" -> "config1",
        "item2" -> "config1",
      ).asJava
    )
    lazy val config2: Config = ConfigFactory.parseMap(
      Map(
        "item2" -> "config2",
        "item3" -> "config2",
      ).asJava
    )
    lazy val combinedConfig = CantonConfig.mergeConfigs(config1, Seq(config2))
    "prefer the right hand config where multiple keys are defined" in {
      combinedConfig.getString("item1") shouldBe "config1"
      // this is defined in both, but as config2 was provided last it should provide the value
      combinedConfig.getString("item2") shouldBe "config2"
      // this is missing from config1
      combinedConfig.getString("item3") shouldBe "config2"
    }

    "load with no config files" should {
      "return None" in {
        val result =
          loggerFactory.assertLogs(
            loadFiles(Seq()),
            _.errorMessage should include("No config files"),
          )

        result.left.value shouldBe a[NoConfigFiles.Error]

      }
    }

    "load with files that cannot be read" should {
      "will log errors for all files that can't be read" in {
        val result = loggerFactory.assertLogs(
          loadFiles(Seq("file-1", "file-2")),
          _.mdc("err-context") should (include("file-1") and include("file-2")),
        )
        result.left.value shouldBe a[CannotReadFilesError.Error]
      }
    }

    "config validation on duplicate storage" should {

      "not log the password when url or jdbcUrl is set" in {
        val result = loggerFactory.assertLogs(
          loadFiles(Seq(simpleConfPath, "invalid-configs/storage-url-with-password.conf")),
          _.errorMessage should (include("Failed to validate the configuration")
            and include("participant1") and include("participant2")
            and not include "password=" and not include "supersafe"),
        )
        result.left.value shouldBe a[ConfigErrors.ValidationError.Error]
      }
    }
  }

  "parsing our config example snippets" should {
    "succeed on all examples" in {
      val inputDir = baseDir / "documentation-snippets"

      inputDir
        .list(_.extension.contains(".conf"))
        .foreach(file =>
          loggerFactory.assertLogsUnorderedOptional(
            loadFiles(Seq(simpleConfPath, "documentation-snippets/" + file.name))
              .valueOrFail(
                "failed to load " + file.name
              ),
            LogEntryOptionality.Optional -> (entry =>
              entry.shouldBeCantonErrorCode(DeprecatedProtocolVersion)
            ),
          )
        )
    }
  }

  "dumping config" should {
    def assertNoLeakage(configMapOverride: Map[String, String] = Map.empty) = {
      val config =
        CantonConfig.parseAndLoadOrExit(
          files,
          Some(DefaultPorts.create()),
        )

      val configFromCliOnly = CantonConfig
        .parseAndMergeJustCLIConfigs(NonEmpty.from(files).value)
        .valueOrFail("config should be loaded")
      val configFromMap = ConfigFactory.parseMap(configMapOverride.asJava)
      val finalConfig = CantonConfig.mergeConfigs(configFromCliOnly, Seq(configFromMap))
      val rawConfig = CantonConfig.renderForLoggingOnStartup(finalConfig)
      val dump = config.dumpString
      val participant1Config = config.participantsByString("participant")
      val secret = participant1Config.ledgerApi.authServices.head match {
        case UnsafeJwtHmac256(secret, _, _, _, _, _, _) => secret
        case x =>
          fail(s"Invalid participant config: $x from files $files")
      }
      val storageConfig = participant1Config.storage.config
      // test db password
      storageConfig.getString("properties.password") shouldBe "supersafe"

      // test jwt secret
      dump should not include storageConfig.getString("properties.password")
      dump should not include secret.unwrap

      rawConfig should not include storageConfig.getString("properties.password")
      rawConfig should not include secret.unwrap
    }

    "not leak confidential information" in {
      assertNoLeakage()
      clue("with password override") {
        assertNoLeakage(
          // Test with specifically overridden password as well (typically provided via -C on the CLI)
          Map(
            "canton.participants.participant.storage.config.properties.password" -> "supersafe"
          )
        )
      }
    }
  }

  "config file" should {

    "load / save / load" in {
      val config = CantonConfig.parseAndLoadOrExit(
        files,
        Some(DefaultPorts.create()),
      )

      File.usingTemporaryFile("reload-test", ".canton") { file =>
        CantonConfig.save(config, file.toString)
        val loaded = CantonConfig.parseAndLoadOrExit(
          Seq(file.toJava),
          Some(DefaultPorts.create()),
        )
        config shouldBe loaded
      }
    }

    "remote sequencer ha onboarding config" in {
      val configE =
        CantonConfig.parseAndLoad(
          Seq(remoteSequencerHAConfig.toJava),
          Some(DefaultPorts.create()),
        )
      valueOrFail(configE)("loading remote sequencer ha config")
    }

    "distributed topology produce a port definition message" in {
      val configE = CantonConfig.parseAndLoad(
        Seq(distributedConf.toJava),
        Some(DefaultPorts.create()),
      )
      val config = valueOrFail(configE)("loading distributed synchronizer topology config")

      val expectedValues = Set(
        "participant1:admin-api=5012,ledger-api=5011",
        "participant2:admin-api=5022,ledger-api=5021",
        "participant3:admin-api=5032,ledger-api=5031",
        "sequencer1:admin-api=5002,public-api=5001",
        "mediator1:admin-api=6002",
      )

      config.portDescription.split(";").toSet shouldBe expectedValues
    }

    "parse all node identifier configs" in {
      forAll(
        Table[String, NodeIdentifierConfig](
          ("configName", "expected"),
          ("explicit-node-identifier.conf", NodeIdentifierConfig.Explicit("MyName")),
          ("random-node-identifier.conf", NodeIdentifierConfig.Random),
          ("config-node-identifier.conf", NodeIdentifierConfig.Config),
        )
      ) { case (configName, expectedConfig) =>
        val file = "community" / "app" / "src" / "test" / "resources" / configName
        val config = CantonConfig
          .parseAndLoad(
            Seq(simpleConf, file).map(_.toJava),
            Some(DefaultPorts.create()),
          )
          .value
        config
          .participantsByString("participant1")
          .init
          .identity should matchPattern { case IdentityConfig.Auto(`expectedConfig`) => }

      }
    }
  }

  "config validation on duplicate storage" should {
    "return a ValidationError during loading" in {
      val result = loggerFactory.assertLogs(
        CantonConfig.parseAndLoad(
          Seq(simpleConf.toJava, duplicateStorageWithoutReplicationConfig.toJava),
          Some(DefaultPorts.create()),
        ),
        _.errorMessage should (include("Failed to validate the configuration") and include(
          "participant1"
        ) and include("participant2")),
      )
      result.left.value shouldBe a[ConfigErrors.ValidationError.Error]
    }

    "pass when replication flag is enabled" in {
      CantonConfig
        .parseAndLoad(
          Seq(simpleConf.toJava, duplicateStorageWithReplicationConfig.toJava),
          Some(DefaultPorts.create()),
        )
        .valueOrFail("loading config")
    }
  }

  "config validation on protocol version" should {
    "return an error during loading a config with a minimum protocol version not supported by the participant" in {
      val result = loggerFactory.assertLogs(
        CantonConfig.parseAndLoad(
          Seq(simpleConf.toJava, unsupportedMinProtocolVersionConfig.toJava),
          Some(DefaultPorts.create()),
        ),
        _.errorMessage should (include("unsupported-minimum-protocol-version.conf") and include(
          "42"
        ) and include(
          ProtocolVersionCompatibility
            .supportedProtocols(
              includeAlphaVersions = false,
              includeBetaVersions = false,
              release = ReleaseVersion.current,
            )
            .headOption
            .value
            .toString
        )),
      )
      val resVal = result.left.value

      resVal shouldBe a[ConfigErrors.GenericConfigError.Error]
      resVal.cause should (include("unsupported-minimum-protocol-version.conf") and include(
        "42"
      ))
    }
  }

  "performance test configs" should {
    Seq(
      "synchronizers.conf",
      "synchronizers-sec.conf",
      "participants.conf",
      "participants-sec.conf",
      "performance-runners.conf",
    ).foreach { file =>
      s"parse $file" in {
        CantonConfig
          .parseAndLoad(
            Seq(
              perf / "config-test.env",
              perf / "config" / "db-sequencer" / "_sequencer_type.conf",
              perf / "config" / "postgres" / "_persistence.conf",
              perf / "config" / file,
            ).map(_.toJava),
            Some(DefaultPorts.create()),
          )
          .valueOrFail("loading config")
      }
    }
  }

  "deprecated configs" should {

    def deprecatedConfigChecks(config: CantonConfig) = {
      // verify that `http-ledger-api` is configured with the expected values
      val participant = config.participants.headOption.value._2
      participant.httpLedgerApi.address shouldBe "127.0.0.101"
      participant.httpLedgerApi.port.unwrap shouldBe 102
      participant.httpLedgerApi.portFile shouldBe Some(Path.of("103"))
      participant.httpLedgerApi.pathPrefix shouldBe Some("104")
      participant.httpLedgerApi.requestTimeout.toMinutes shouldBe 105

      // verify that `crypto.sessionSigningKeys` is configured with the expected values
      participant.crypto.sessionSigningKeys.enabled shouldBe true
      participant.crypto.sessionSigningKeys.keyValidityDuration shouldBe PositiveFiniteDuration
        .ofMinutes(5)
      participant.crypto.sessionSigningKeys.toleranceShiftDuration shouldBe NonNegativeFiniteDuration
        .ofMinutes(2)
      participant.crypto.sessionSigningKeys.cutOffDuration shouldBe NonNegativeFiniteDuration
        .ofSeconds(30)
      participant.crypto.sessionSigningKeys.keyEvictionPeriod shouldBe PositiveFiniteDuration
        .ofMinutes(15)
      participant.crypto.sessionSigningKeys.signingAlgorithmSpec shouldBe EcDsaSha256
      participant.crypto.sessionSigningKeys.signingKeySpec shouldBe EcP256
    }

    // In this test case, both deprecated and new fields are set with opposite values, we make sure the new fields
    // are used
    "load with new fields set" in {
      val deprecatedConfigLogs = Seq[(LogEntry => Assertion, String)](
        (
          _.message should (include("Config field") and include("is deprecated")),
          "moved field",
        )
      )
      loggerFactory.assertLogsSeq(SuppressionRule.Level(org.slf4j.event.Level.INFO))(
        {
          val parsed = CantonConfig
            .parseAndLoad(
              Seq(
                confDir / "storage" / "postgres.conf",
                deprecatedConfigs / "new-config-fields-take-precedence.conf",
              )
                .map(
                  _.toJava
                ),
              Some(DefaultPorts.create()),
            )
            .value
          deprecatedConfigChecks(parsed)
        },
        LogEntry.assertLogSeq(deprecatedConfigLogs, Seq.empty),
      )
    }

    // In this test case, only the deprecated fields are set, we make sure they get used as fallbacks
    "be backwards compatible" in {
      val deprecatedConfigLogs = Seq[(LogEntry => Assertion, String)](
        (
          _.message should include(
            "Config field at http-ledger-api.server is deprecated since 3.4.0"
          ),
          "deprecated field 'http-ledger-api.server'",
        ),
        (
          _.message should include(
            "Config field at kms.session-signing-keys is deprecated since 3.5.0"
          ),
          "deprecated field 'kms.session-signing-keys'",
        ),
      )
      loggerFactory.assertLogsSeq(SuppressionRule.Level(org.slf4j.event.Level.INFO))(
        {
          val parsed = CantonConfig
            .parseAndLoad(
              Seq(
                confDir / "storage" / "postgres.conf",
                deprecatedConfigs / "backwards-compatible.conf",
              ).map(
                _.toJava
              ),
              Some(DefaultPorts.create()),
            )
            .value
          deprecatedConfigChecks(parsed)
        },
        LogEntry.assertLogSeq(deprecatedConfigLogs, Seq.empty),
      )
    }

    "tolerate and log removed fields" in {
      val deprecatedConfigLogs = Seq[(LogEntry => Assertion, String)](
        (
          _.message should include(
            "Config path 'package-dependency-cache' is deprecated since 3.5.0"
          ),
          "deprecated path",
        )
      )
      loggerFactory.assertLogsSeq(SuppressionRule.Level(org.slf4j.event.Level.INFO))(
        {
          val parsed = CantonConfig.parseAndLoad(
            Seq(
              confDir / "storage" / "postgres.conf",
              deprecatedConfigs / "removed-fields.conf",
            ).map(_.toJava),
            Some(DefaultPorts.create()),
          )
          parsed.value.participants.keySet should contain(InstanceName.tryCreate("participant1"))
        },
        LogEntry.assertLogSeq(deprecatedConfigLogs, Seq.empty),
      )
    }
  }

}
