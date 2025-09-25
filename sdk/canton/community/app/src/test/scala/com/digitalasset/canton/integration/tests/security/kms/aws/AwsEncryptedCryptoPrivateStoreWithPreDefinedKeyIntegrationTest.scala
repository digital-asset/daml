// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.EnvironmentDefinition.allNodeNames
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.security.kms.EncryptedCryptoPrivateStoreIntegrationTest

/** Tests the encrypted private store in a setting where the AWS KMS key IS pre-defined:
  * "alias/canton-kms-test-key" where (a) only participant1 has an encrypted private store
  */
class AwsEncryptedCryptoPrivateStoreWithPreDefinedKeyReferenceIntegrationTestPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )

}

/** (b) all nodes have an encrypted private store
  */
class AwsEncryptedCryptoPrivateStoreWithPreDefinedKeyReferenceIntegrationTestAllPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  override protected val protectedNodes: Set[String] = allNodeNames(
    environmentDefinition.baseConfig
  )

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )

}
