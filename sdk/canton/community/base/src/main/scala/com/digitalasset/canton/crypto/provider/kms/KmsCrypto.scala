// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.crypto.CryptoFactory.{CryptoStoresAndSchemes, selectSchemes}
import com.digitalasset.canton.crypto.provider.jce.JcePureCrypto
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.Security
import scala.concurrent.ExecutionContext

/** Factory to create a KMS-backed [[Crypto]] instance based on a [[KmsPrivateCrypto]] and a
  * software JCE-based pure crypto.
  */
object KmsCrypto {

  def create(
      cryptoConfig: CryptoConfig,
      storesAndSchemes: CryptoStoresAndSchemes,
      privateCrypto: KmsPrivateCrypto,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): Either[String, Crypto] = {

    // Register BC as security provider for software-based verification in KMS providers
    Security.addProvider(new BouncyCastleProvider).discard

    for {
      pbkdfSchemes <- cryptoConfig.provider.pbkdf
        .toRight("PBKDF schemes must be set for JCE provider")
      pbkdfScheme <- selectSchemes(cryptoConfig.pbkdf, pbkdfSchemes)
        .map(_.default)

      pureCrypto =
        new JcePureCrypto(
          storesAndSchemes.symmetricKeyScheme,
          storesAndSchemes.signingAlgorithmSpec,
          storesAndSchemes.supportedSigningAlgorithmSpecs,
          storesAndSchemes.encryptionAlgorithmSpec,
          storesAndSchemes.supportedEncryptionAlgorithmSpecs,
          storesAndSchemes.hashAlgorithm,
          pbkdfScheme,
          loggerFactory,
        )
    } yield new Crypto(
      pureCrypto,
      privateCrypto,
      storesAndSchemes.cryptoPrivateStore,
      storesAndSchemes.cryptoPublicStore,
      timeouts,
      loggerFactory,
    )
  }

}
