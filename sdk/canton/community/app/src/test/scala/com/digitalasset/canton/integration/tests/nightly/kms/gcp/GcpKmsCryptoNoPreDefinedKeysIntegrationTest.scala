// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.kms.gcp

import com.digitalasset.canton.integration.tests.nightly.kms.KmsCryptoNoPreDefinedKeysIntegrationTest
import com.digitalasset.canton.integration.tests.security.CryptoIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpKmsCryptoIntegrationTestBase

/** Runs a ping while one participant is using a GCP KMS provider and letting Canton generate its
  * own keys (i.e. auto-init == true)
  */
class GcpKmsCryptoNoPreDefinedKeysReferenceIntegrationTest
    extends CryptoIntegrationTest(
      GcpKmsCryptoIntegrationTestBase.defaultGcpKmsCryptoConfig
    )
    with GcpKmsCryptoIntegrationTestBase
    with KmsCryptoNoPreDefinedKeysIntegrationTest {

  override def afterAll(): Unit = {
    deleteAllGenerateKeys()
    super.afterAll()
  }

}
