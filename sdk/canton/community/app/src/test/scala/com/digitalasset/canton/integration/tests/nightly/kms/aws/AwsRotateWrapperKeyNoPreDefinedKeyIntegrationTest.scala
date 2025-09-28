// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.kms.aws

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.security.kms.RotateWrapperKeyIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsEncryptedCryptoPrivateStoreTestBase

/** Tests a manual rotation of an AWS wrapper key, where NO KEY is SPECIFIED as the new wrapper key
  * and, as such, Canton will automatically generate a new one.
  */
class AwsRotateWrapperKeyNoPreDefinedKeyReferenceIntegrationTestPostgres
    extends RotateWrapperKeyIntegrationTest
    with AwsEncryptedCryptoPrivateStoreTestBase {

  override protected val preDefinedKey: Option[String] = None

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )

}
