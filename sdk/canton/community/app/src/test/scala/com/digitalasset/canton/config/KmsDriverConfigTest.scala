// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.{File, *}
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName

class KmsDriverConfigTest extends BaseTestWordSpec {
  lazy val baseDir: File = "community" / "app" / "src" / "test" / "resources"
  lazy val testKmsDriverConfigFile: File = baseDir / "test-kms-driver-config.conf"
  val driverName = "test-kms"
  val driverFactory = new TestKmsDriverFactory
  val username = "kmsuser"
  val password = "kmssecretpass"

  "KMS driver configs" should {
    "load KMS driver config and be able to parse driver specific config" in {
      val cantonConfig = CantonConfig
        .parseAndLoad(
          Seq(testKmsDriverConfigFile.toJava),
          Some(DefaultPorts.create()),
        )
        .valueOrFail("loading test KMS driver config")

      val participantConfig =
        cantonConfig.participants
          .get(InstanceName.tryCreate("participant1"))
          .valueOrFail("retrieve participant config")

      participantConfig.crypto.kms.valueOrFail("no KMS config") match {
        case KmsConfig.Driver(_, config, _, _) =>
          val driverConfig =
            driverFactory.configReader.from(config).valueOrFail("failed to parse driver config")
          driverConfig shouldBe TestKmsDriverConfig(username = username, password = password)
        case kmsConfig => fail(s"unexpected KMS config type: $kmsConfig")
      }
    }

    "respect confidentiality of passwords when writing" in {
      val config = CantonConfig
        .parseAndLoad(
          Seq(testKmsDriverConfigFile.toJava),
          Some(DefaultPorts.create()),
        )
        .valueOrFail("loading test KMS driver config")
      val confString = CantonConfig.makeConfidentialString(config)

      confString should include(username)
      confString should not include password
    }
  }
}
