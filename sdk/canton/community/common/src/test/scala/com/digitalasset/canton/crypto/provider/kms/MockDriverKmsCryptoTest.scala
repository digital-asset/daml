// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.digitalasset.canton.config.KmsConfig
import com.digitalasset.canton.crypto.kms.Kms
import com.digitalasset.canton.crypto.kms.mock.v1.MockKmsDriver
import com.typesafe.config.ConfigValueFactory

import scala.jdk.CollectionConverters.*

class MockDriverKmsCryptoTest extends KmsCryptoTest {

  private lazy val kmsConfigDriver = KmsConfig.Driver(
    "mock-kms",
    ConfigValueFactory.fromMap(Map.empty[String, AnyRef].asJava),
  )

  override protected def kmsConfig: Option[KmsConfig.Driver] = Some(kmsConfigDriver)
  override protected def supportedSchemes: Kms.SupportedSchemes = MockKmsDriver

}
