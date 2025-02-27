// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.mock.v1

import com.digitalasset.canton.crypto.kms.driver.api.v1.KmsDriver
import com.digitalasset.canton.crypto.kms.driver.testing.v1.KmsDriverTest

class MockKmsDriverTest extends KmsDriverTest {

  override protected def newKmsDriver(): KmsDriver = {
    val factory = new MockKmsDriverFactory

    factory.create(MockKmsDriverConfig(), simpleLoggerFactory, parallelExecutionContext)
  }

  "Mock KMS Driver" must {
    behave like kmsDriver(allowKeyGeneration = true)
  }
}
