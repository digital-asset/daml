// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.integration.plugins.{UseAwsKms, UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.RotateWrapperKeyIntegrationTest

/** Tests a manual rotation of the wrapper key, where an AWS KMS key is SPECIFIED -
  * "alias/canton-kms-rotation-test-key" - and selected to be the new wrapper key.
  */
class AwsRotateWrapperKeyWithPreDefinedKeyReferenceIntegrationTestPostgres
    extends RotateWrapperKeyIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  override protected val preDefinedKey: Option[String] = Some(
    UseAwsKms.DefaultCantonRotationTestKeyId.unwrap
  )

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )

}
