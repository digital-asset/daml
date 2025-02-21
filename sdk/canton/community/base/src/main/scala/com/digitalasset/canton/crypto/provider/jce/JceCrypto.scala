// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.digitalasset.canton.config.{CryptoConfig, CryptoProvider, ProcessingTimeout}
import com.digitalasset.canton.crypto.store.{CryptoPrivateStore, CryptoPublicStore}
import com.digitalasset.canton.crypto.{Crypto, CryptoSchemes}
import com.digitalasset.canton.logging.NamedLoggerFactory

import scala.concurrent.ExecutionContext

object JceCrypto {

  def create(
      config: CryptoConfig,
      cryptoPrivateStore: CryptoPrivateStore,
      cryptoPublicStore: CryptoPublicStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): Either[String, Crypto] =
    for {
      _ <- Either
        .cond(config.provider == CryptoProvider.Jce, (), "JCE provider must be configured")
      cryptoSchemes <- CryptoSchemes.fromConfig(config)
      cryptoPrivateStoreExtended <- cryptoPrivateStore.toExtended
        .toRight(
          s"The crypto private store does not implement all the functions necessary " +
            s"for the chosen provider ${config.provider}"
        )
      pureCrypto <- JcePureCrypto.create(config, loggerFactory)
      privateCrypto =
        new JcePrivateCrypto(
          pureCrypto,
          defaultSigningAlgorithmSpec = cryptoSchemes.signingAlgoSpecs.default,
          defaultSigningKeySpec = cryptoSchemes.signingKeySpecs.default,
          defaultEncryptionKeySpec = cryptoSchemes.encryptionKeySpecs.default,
          store = cryptoPrivateStoreExtended,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )
      crypto = new Crypto(
        pureCrypto,
        privateCrypto,
        cryptoPrivateStore,
        cryptoPublicStore,
        timeouts,
        loggerFactory,
      )
    } yield crypto

}
