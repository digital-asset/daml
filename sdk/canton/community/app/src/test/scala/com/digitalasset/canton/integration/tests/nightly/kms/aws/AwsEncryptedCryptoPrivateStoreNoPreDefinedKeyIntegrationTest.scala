// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.kms.aws

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.security.kms.EncryptedCryptoPrivateStoreIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsEncryptedCryptoPrivateStoreTestBase

/** Tests the encrypted private store in a setting where the AWS KMS key IS NOT pre-defined. Canton
  * will (a) create a new temporary SINGLE-REGION key that is scheduled for deletion at the end of
  * the test. Creating a new key costs a small fee so this test is only run nightly.
  */
class AwsEncryptedCryptoPrivateStoreNoPreDefinedKeyReferenceIntegrationTestPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
    withPreGenKey = false,
  )

}

/** (b) create a new temporary MULTI-REGION key that is scheduled for deletion at the end of the
  * test.
  */
class AwsEncryptedCryptoPrivateStoreNoPreDefinedKeyMultiRegionReferenceIntegrationTestPostgres
    extends EncryptedCryptoPrivateStoreIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
    withPreGenKey = false,
    multiRegion = true,
  )

}
