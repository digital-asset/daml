// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.kms.aws

import com.digitalasset.canton.integration.tests.nightly.kms.KmsCryptoNoPreDefinedKeysIntegrationTest
import com.digitalasset.canton.integration.tests.security.CryptoIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsKmsCryptoIntegrationTestBase

/** Runs a ping while one participant is using an AWS KMS provider and letting Canton generate its
  * own keys (i.e. auto-init == true)
  */
class AwsKmsCryptoNoPreDefinedKeysReferenceIntegrationTest
    extends CryptoIntegrationTest(
      AwsKmsCryptoIntegrationTestBase.defaultAwsKmsCryptoConfig
    )
    with AwsKmsCryptoIntegrationTestBase
    with KmsCryptoNoPreDefinedKeysIntegrationTest {

  override def afterAll(): Unit = {
    deleteAllGenerateKeys()
    super.afterAll()
  }

}
