// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoSchemeConfig,
  EncryptionSchemeConfig,
  StorageConfig,
}
import com.digitalasset.canton.crypto.{EncryptionAlgorithmSpec, EncryptionKeySpec}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

/** Runs a ping with one participant using deterministic ECIES with AES-CBC */
trait SimplestPingWithCbcIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  // Uncomment if you want to test the deterministic encryption-check
  // System.setProperty("canton.encryption-check", "true")

  lazy private val cbcCryptoConfig: CryptoConfig = CryptoConfig(
    encryption = EncryptionSchemeConfig(
      algorithms = CryptoSchemeConfig(Some(EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc)),
      keys = CryptoSchemeConfig(Some(EncryptionKeySpec.EcP256)),
    )
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1.addConfigTransform(
      ConfigTransforms.setCrypto(cbcCryptoConfig, (name: String) => name == "participant1")
    )

  "we can run a trivial ping" in { implicit env =>
    import env.*

    clue("participant1 connect") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connect") {
      participant2.synchronizers.connect_local(sequencer1, daName)
    }

    participant1.crypto.pureCrypto.defaultEncryptionAlgorithmSpec shouldBe EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc
    participant2.crypto.pureCrypto.defaultEncryptionAlgorithmSpec shouldBe EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm

    clue("maybe ping") {
      participant1.health.maybe_ping(
        participant2,
        timeout = 30.seconds,
      ) shouldBe defined
    }
  }
}

class SimplestPingWithCbcIntegrationTestInMemory extends SimplestPingWithCbcIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(ConfigTransforms.allInMemory)
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  registerPlugin(new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](loggerFactory))

}
