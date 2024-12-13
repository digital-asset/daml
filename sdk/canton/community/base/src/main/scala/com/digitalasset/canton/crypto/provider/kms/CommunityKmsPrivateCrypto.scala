// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoFactory.CryptoStoresAndSchemes
import com.digitalasset.canton.crypto.kms.Kms
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.crypto.store.KmsCryptoPrivateStore
import com.digitalasset.canton.logging.NamedLoggerFactory

import scala.concurrent.ExecutionContext

/** Factory to create a [[KmsPrivateCrypto]] based on a community KMS. */
object CommunityKmsPrivateCrypto {

  def create(
      kms: Kms,
      storesAndSchemes: CryptoStoresAndSchemes,
      kmsCryptoPrivateStore: KmsCryptoPrivateStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): Either[String, KmsPrivateCrypto] = kms match {
    case driverKms: DriverKms =>
      DriverKmsPrivateCrypto.create(
        driverKms,
        storesAndSchemes,
        kmsCryptoPrivateStore,
        timeouts,
        loggerFactory,
      )

    case unknown => Left(s"Unknown KMS type: $unknown")
  }

}
