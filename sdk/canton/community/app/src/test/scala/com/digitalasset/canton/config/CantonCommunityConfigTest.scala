// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.{File, *}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ConfigErrors.{
  CannotParseFilesError,
  CannotReadFilesError,
  CantonConfigError,
  GenericConfigError,
  NoConfigFiles,
  SubstitutionError,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec

class CantonCommunityConfigTest extends AnyWordSpec with BaseTest {

  import scala.jdk.CollectionConverters.*
  private val simpleConf = "examples/01-simple-topology/simple-topology.conf"

  "the example simple topology configuration" should {
    lazy val config =
      loadFile(simpleConf).valueOrFail("failed to load simple-topology.conf")

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
      config.portDescription shouldBe "participant1:admin-api=5012,ledger-api=5011;participant2:admin-api=5022,ledger-api=5021;sequencer1:admin-api=5002,public-api=5001;mediator1:admin-api=5202"
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
        loadFiles(Seq(simpleConf, "invalid-configs/bort.conf")),
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
          loadFiles(Seq(simpleConf, "invalid-configs/missing-bracket.conf")),
          _.mdc("err-context") should (include("missing-bracket.conf") and include(
            "expecting a close parentheses ')' here, not: end of file"
          )),
        )
      result.left.value shouldBe a[CannotParseFilesError.Error]
    }

    "return a CannotParseFilesError during loading when not combined with simple config" in {
      val result =
        loggerFactory.assertLogs(
          loadFiles(Seq(simpleConf, "invalid-configs/missing-bracket.conf")),
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
          loadFiles(Seq(simpleConf, "invalid-configs/negative-port.conf")),
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
      loadFiles(Seq(simpleConf, "invalid-configs/include-missing-file.conf"))
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
        loadFiles(Seq("invalid-configs/require-missing-file.conf", simpleConf)),
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
      "return a ValidationError during loading" in {
        val result = loggerFactory.assertLogs(
          loadFiles(Seq(simpleConf, "invalid-configs/duplicate-storage.conf")),
          _.errorMessage should (include("Failed to validate the configuration")
            and include("participant1") and include("participant2")),
        )
        result.left.value shouldBe a[ConfigErrors.ValidationError.Error]
      }
      "not log the password when url or jdbcUrl is set" in {
        val result = loggerFactory.assertLogs(
          loadFiles(Seq(simpleConf, "invalid-configs/storage-url-with-password.conf")),
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

      val exclude = List(
        "enforce-protocol-version-domain-2.5.conf" // Does not build anymore but needed in the docs
      )

      inputDir
        .list(_.extension.contains(".conf"))
        .filterNot(file => exclude.contains(file.name))
        .foreach(file =>
          loggerFactory.assertLogsUnorderedOptional(
            loadFiles(Seq(simpleConf, "documentation-snippets/" + file.name))
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

  private def loadFile(resourcePath: String): Either[CantonConfigError, CantonCommunityConfig] =
    loadFiles(Seq(resourcePath))

  private def loadFiles(
      resourcePaths: Seq[String]
  ): Either[CantonConfigError, CantonCommunityConfig] = {
    val files = resourcePaths.map(r => (baseDir.toString / r).toJava)
    CantonCommunityConfig.parseAndLoad(files)
  }

  private lazy val baseDir: File = "community" / "app" / "src" / "test" / "resources"

}
