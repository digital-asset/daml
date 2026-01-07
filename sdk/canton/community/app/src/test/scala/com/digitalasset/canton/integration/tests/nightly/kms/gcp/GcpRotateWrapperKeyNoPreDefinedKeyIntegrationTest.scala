// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.kms.gcp

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.RotateWrapperKeyIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpEncryptedCryptoPrivateStoreTestBase

/** Tests a manual rotation of a GCP wrapper key, where NO KEY is SPECIFIED as the new wrapper key
  * and, as such, Canton will automatically generate a new one.
  */
class GcpRotateWrapperKeyNoPreDefinedKeyBftOrderingIntegrationTestPostgres
    extends RotateWrapperKeyIntegrationTest
    with GcpEncryptedCryptoPrivateStoreTestBase {

  override protected val preDefinedKey: Option[String] = None

  setupPlugins(
    protectedNodes,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )

}
