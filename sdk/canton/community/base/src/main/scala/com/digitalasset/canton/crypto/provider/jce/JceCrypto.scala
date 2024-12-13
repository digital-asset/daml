// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.crypto.CryptoFactory.{CryptoStoresAndSchemes, selectSchemes}
import com.digitalasset.canton.logging.NamedLoggerFactory
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.Security
import scala.concurrent.{ExecutionContext, Future}

object JceCrypto {
  def create(
      config: CryptoConfig,
      storesAndSchemes: CryptoStoresAndSchemes,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, String, Crypto] =
    for {
      cryptoPrivateStoreExtended <- storesAndSchemes.cryptoPrivateStore.toExtended
        .toRight(
          s"The crypto private store does not implement all the functions necessary " +
            s"for the chosen provider ${config.provider}"
        )
        .toEitherT[Future]
      _ = Security.addProvider(new BouncyCastleProvider)
      pbkdfSchemes <- config.provider.pbkdf
        .toRight("PBKDF schemes must be set for JCE provider")
        .toEitherT[Future]
      pbkdfScheme <- selectSchemes(config.pbkdf, pbkdfSchemes).map(_.default).toEitherT[Future]
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
      privateCrypto =
        new JcePrivateCrypto(
          pureCrypto,
          storesAndSchemes.signingAlgorithmSpec,
          storesAndSchemes.signingKeySpec,
          storesAndSchemes.encryptionKeySpec,
          cryptoPrivateStoreExtended,
          timeouts,
          loggerFactory,
        )
      crypto = new Crypto(
        pureCrypto,
        privateCrypto,
        storesAndSchemes.cryptoPrivateStore,
        storesAndSchemes.cryptoPublicStore,
        timeouts,
        loggerFactory,
      )
    } yield crypto

}
