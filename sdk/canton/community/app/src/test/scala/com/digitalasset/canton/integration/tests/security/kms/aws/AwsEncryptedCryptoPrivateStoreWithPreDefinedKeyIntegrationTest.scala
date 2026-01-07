// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.integration.EnvironmentDefinition.allNodeNames
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.EncryptedCryptoPrivateStoreIntegrationTest

/** Tests the encrypted private store in a setting where the AWS KMS key IS pre-defined:
  * "alias/canton-kms-test-key" where (a) only participant1 has an encrypted private store
  */
class AwsEncryptedCryptoPrivateStoreWithPreDefinedKeyBftOrderingIntegrationTestPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )

}

/** (b) all nodes have an encrypted private store
  */
class AwsEncryptedCryptoPrivateStoreWithPreDefinedKeyBftOrderingIntegrationTestAllPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  override protected val protectedNodes: Set[String] = allNodeNames(
    environmentDefinition.baseConfig
  )

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )

}
