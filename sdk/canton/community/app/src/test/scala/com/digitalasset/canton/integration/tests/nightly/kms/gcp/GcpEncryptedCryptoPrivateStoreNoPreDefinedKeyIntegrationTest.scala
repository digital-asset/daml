// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.kms.gcp

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.EncryptedCryptoPrivateStoreIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpEncryptedCryptoPrivateStoreTestBase

/** Tests the encrypted private store in a setting where the GCP KMS key IS NOT pre-defined. Canton
  * will (a) create a new temporary SINGLE-REGION key that is scheduled for deletion at the end of
  * the test. Creating a new key costs a small fee so this test is only run nightly.
  */
class GcpEncryptedCryptoPrivateStoreNoPreDefinedKeyBftOrderingIntegrationTestPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with GcpEncryptedCryptoPrivateStoreTestBase {

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
    withPreGenKey = false,
  )

}

/** In GCP to use a multi-region key we need to use a different keyring
  */
class GcpEncryptedCryptoPrivateStoreNoPreDefinedKeyMultiRegionBftOrderingIntegrationTestPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with GcpEncryptedCryptoPrivateStoreTestBase {

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
    withPreGenKey = false,
    multiRegion = true,
  )

}
