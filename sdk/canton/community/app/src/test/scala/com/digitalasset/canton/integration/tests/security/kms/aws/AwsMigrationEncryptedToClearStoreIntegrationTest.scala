// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.tests.security.kms.MigrationEncryptedToClearStoreIntegrationTest

/** Tests a migration from an encrypted private store that uses AWS KMS to a clear crypto private
  * store.
  */
class AwsMigrationEncryptedToClearStoreReferenceIntegrationTestPostgres
    extends MigrationEncryptedToClearStoreIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  override protected val kmsRevertPlugin = new UseAwsKms(
    nodes = protectedNodes,
    enableEncryptedPrivateStore = EncryptedPrivateStoreStatus.Revert,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )

}
