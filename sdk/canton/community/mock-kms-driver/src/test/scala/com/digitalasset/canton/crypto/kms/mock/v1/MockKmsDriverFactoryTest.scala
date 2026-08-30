// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.mock.v1

import com.digitalasset.canton.crypto.kms.driver.testing.v1.KmsDriverFactoryTest

class MockKmsDriverFactoryTest extends KmsDriverFactoryTest {

  override type Factory = MockKmsDriverFactory

  override protected val factory: MockKmsDriverFactory = new MockKmsDriverFactory()

  override protected val config: MockKmsDriverConfig = MockKmsDriverConfig()

  "Mock KMS Driver Factory" must {
    behave like kmsDriverFactory()
  }
}
