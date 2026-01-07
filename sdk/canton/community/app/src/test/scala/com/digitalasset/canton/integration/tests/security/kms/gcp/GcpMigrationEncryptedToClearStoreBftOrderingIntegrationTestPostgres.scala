// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.gcp

import com.digitalasset.canton.integration.plugins.{
  EncryptedPrivateStoreStatus,
  UseBftSequencer,
  UseGcpKms,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.security.kms.MigrationEncryptedToClearStoreIntegrationTest

/** Tests a migration from an encrypted private store that uses GCP KMS to a clear crypto private
  * store.
  */
class GcpMigrationEncryptedToClearStoreBftOrderingIntegrationTestPostgres
    extends MigrationEncryptedToClearStoreIntegrationTest
    with GcpEncryptedCryptoPrivateStoreTestBase {

  override protected val kmsRevertPlugin = new UseGcpKms(
    nodes = protectedNodes,
    enableEncryptedPrivateStore = EncryptedPrivateStoreStatus.Revert,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )

}
