// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.mock

import com.digitalasset.canton.integration.EnvironmentDefinition.allNodeNames
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.EncryptedCryptoPrivateStoreIntegrationTest

/** Tests the encrypted private store with a Mock KMS diver where (a) only participant1 has an
  * encrypted private store
  */
class MockEncryptedCryptoPrivateStoreBftOrderingIntegrationTestPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with MockEncryptedCryptoPrivateStoreTestBase {

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )

}

/** (b) all nodes have an encrypted private store
  */
class MockEncryptedCryptoPrivateStoreBftOrderingIntegrationTestAllPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with MockEncryptedCryptoPrivateStoreTestBase {

  override protected val protectedNodes: Set[String] = allNodeNames(
    environmentDefinition.baseConfig
  )

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )

}
