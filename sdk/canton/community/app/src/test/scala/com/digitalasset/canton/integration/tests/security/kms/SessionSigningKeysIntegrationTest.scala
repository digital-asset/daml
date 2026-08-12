// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.digitalasset.canton.config.{DbConfig, SessionSigningKeysConfig}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}

/** Test a scenario where we have a combination of non-KMS, KMS and KMS with session signing keys'
  * nodes and make sure communication is correct among all of them.
  */
trait SessionSigningKeysIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KmsCryptoIntegrationTestBase {

  s"ping succeeds with nodes $protectedNodes using session signing keys" in { implicit env =>
    import env.*

    env.nodes.local.foreach { node =>
      if (protectedNodes.contains(node.name)) {
        val sessionSigningKeysConfig =
          node.config.crypto.kms.valueOrFail("no kms config").sessionSigningKeys
        if (nodesWithSessionSigningKeysDisabled.contains(node.name))
          sessionSigningKeysConfig shouldBe SessionSigningKeysConfig.disabled
        else sessionSigningKeysConfig shouldBe SessionSigningKeysConfig.default
      } else node.config.crypto.kms shouldBe empty
    }

    assertPingSucceeds(participant1, participant2)
  }
}

class AwsKmsSessionSigningKeysIntegrationTestPostgres
    extends SessionSigningKeysIntegrationTest
    with AwsKmsCryptoIntegrationTestBase {
  override protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] =
    Set("participant2")

  override protected lazy val protectedNodes: Set[String] =
    Set("participant1", "participant2", "mediator1")

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )
}

class GcpKmsSessionSigningKeysIntegrationTestPostgres
    extends SessionSigningKeysIntegrationTest
    with GcpKmsCryptoIntegrationTestBase {
  override protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] =
    Set("participant2")

  override protected lazy val protectedNodes: Set[String] =
    Set("participant1", "participant2", "mediator1")

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )
}

class MockKmsDriverSessionSigningKeysIntegrationTestPostgres
    extends SessionSigningKeysIntegrationTest
    with MockKmsDriverCryptoIntegrationTestBase {
  override protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] =
    Set.empty

  override protected lazy val protectedNodes: Set[String] =
    Set("sequencer1")

  setupPlugins(
    // TODO(#25069): Add persistence to mock KMS driver to support auto-init = false
    withAutoInit = true,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}
