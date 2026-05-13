// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{
  UseAwsKms,
  UsePostgres,
  UseReferenceBlockSequencer,
}
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
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )

}
