// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.gcp

import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.RotateWrapperKeyFailureIntegrationTest

/** Tests erroneous calls to RotateWrapperKey console command.
  */
class GcpRotateWrapperKeyFailureBftOrderingIntegrationTestPostgres
    extends RotateWrapperKeyFailureIntegrationTest
    with GcpEncryptedCryptoPrivateStoreTestBase {

  override protected val disabledKeyId: KmsKeyId =
    KmsKeyId(String300.tryCreate("canton-kms-test-key-disabled"))

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )

}
