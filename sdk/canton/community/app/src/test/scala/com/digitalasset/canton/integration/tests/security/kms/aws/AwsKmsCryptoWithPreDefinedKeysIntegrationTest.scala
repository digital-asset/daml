// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.EnvironmentDefinition.allNodeNames
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.security.CryptoIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.KmsCryptoWithPreDefinedKeysIntegrationTest

/** Runs a crypto integration tests with ALL nodes using an AWS KMS provider and pre-generated keys.
  * Consequently keys must be registered in Canton and nodes MUST be manually initialized.
  */
class AwsKmsCryptoWithPreDefinedKeysReferenceIntegrationTestAllNodes
    extends CryptoIntegrationTest(
      AwsKmsCryptoIntegrationTestBase.defaultAwsKmsCryptoConfig
    )
    with AwsKmsCryptoIntegrationTestBase {

  // all nodes are protected using KMS as a provider
  override lazy protected val protectedNodes: Set[String] = allNodeNames(
    environmentDefinition.baseConfig
  )

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )
}

/** Runs a crypto integration tests with one participant using an AWS KMS provider and pre-generated
  * CROSS-ACCOUNT keys. Because we are using cross-account keys the IAM user has less permissions,
  * but should still be able to run Canton. Consequently keys must be registered in Canton and the
  * participant node MUST be manually initialized.
  */
class AwsKmsCryptoWithPreDefinedCrossAccountKeysReferenceIntegrationTest
    extends CryptoIntegrationTest(
      AwsKmsCryptoIntegrationTestBase.defaultAwsKmsCryptoConfig
    )
    with AwsKmsCryptoIntegrationTestBase {
  override val topologyPreDefinedKeys: TopologyKmsKeys = TopologyKmsKeys(
    namespaceKeyId =
      Some(s"arn:aws:kms:us-east-1:577087714890:key/c94045e5-ef8d-44d9-bc75-54733aeb5df0"),
    sequencerAuthKeyId =
      Some(s"arn:aws:kms:us-east-1:577087714890:key/91b6e88e-cb70-4b90-97a8-023c2955ae61"),
    signingKeyId =
      Some(s"arn:aws:kms:us-east-1:577087714890:key/bbf85eb2-bfed-48b7-9d3f-3e0ddfb0c0d7"),
    encryptionKeyId =
      Some(s"arn:aws:kms:us-east-1:577087714890:key/b9e2b349-0682-4446-9e9a-36db172a993b"),
  )

  override protected def getTopologyKeysForNode(name: String): TopologyKmsKeys =
    topologyPreDefinedKeys

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )
}

/** Runs a crypto integration tests with one participant using an AWS KMS provider with
  * pre-generated keys. Runs with persistence so we also check that it is able to recover from an
  * unexpected shutdown.
  */
class AwsKmsCryptoWithPreDefinedKeysReferenceIntegrationTestPostgres
    extends CryptoIntegrationTest(
      AwsKmsCryptoIntegrationTestBase.defaultAwsKmsCryptoConfig
    )
    with AwsKmsCryptoIntegrationTestBase
    with KmsCryptoWithPreDefinedKeysIntegrationTest {
  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )
}
