// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.gcp

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseGcpKms, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.RotateWrapperKeyIntegrationTest

/** Tests a manual rotation of the wrapper key, where a GCP KMS key is SPECIFIED -
  * "canton-kms-rotation-test-key" - and selected to be the new wrapper key.
  */
class GcpRotateWrapperKeyWithPreDefinedKeyBftOrderingIntegrationTestPostgres
    extends RotateWrapperKeyIntegrationTest
    with GcpEncryptedCryptoPrivateStoreTestBase {

  override protected val preDefinedKey: Option[String] = Some(
    UseGcpKms.DefaultCantonRotationTestKeyId.unwrap
  )

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}
