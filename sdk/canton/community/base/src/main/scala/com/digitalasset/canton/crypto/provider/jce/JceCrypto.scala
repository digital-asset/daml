// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.digitalasset.canton.config.{
  CacheConfig,
  CryptoConfig,
  CryptoProvider,
  ProcessingTimeout,
  SessionEncryptionKeyCacheConfig,
}
import com.digitalasset.canton.crypto.store.{CryptoPrivateStore, CryptoPublicStore}
import com.digitalasset.canton.crypto.{Crypto, CryptoSchemes}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.util.EitherUtil

import scala.concurrent.ExecutionContext

object JceCrypto {

  def create(
      config: CryptoConfig,
      cryptoSchemes: CryptoSchemes,
      sessionEncryptionKeyCacheConfig: SessionEncryptionKeyCacheConfig,
      publicKeyConversionCacheConfig: CacheConfig,
      cryptoPrivateStore: CryptoPrivateStore,
      cryptoPublicStore: CryptoPublicStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): Either[String, Crypto] =
    for {
      _ <- EitherUtil.condUnit(
        config.provider == CryptoProvider.Jce,
        "JCE provider must be configured",
      )
      cryptoPrivateStoreExtended <- cryptoPrivateStore.toExtended
        .toRight(
          s"The crypto private store does not implement all the functions necessary " +
            s"for the chosen provider ${config.provider}"
        )
      pureCrypto <- JcePureCrypto.create(
        config,
        sessionEncryptionKeyCacheConfig,
        publicKeyConversionCacheConfig,
        cryptoSchemes,
        loggerFactory,
      )
      privateCrypto =
        new JcePrivateCrypto(
          pureCrypto,
          signingSchemes = cryptoSchemes.signingSchemes,
          encryptionSchemes = cryptoSchemes.encryptionSchemes,
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
