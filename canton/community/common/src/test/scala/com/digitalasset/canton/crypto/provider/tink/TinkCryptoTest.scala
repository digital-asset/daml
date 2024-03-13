// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import com.digitalasset.canton.config.CommunityCryptoConfig
import com.digitalasset.canton.config.CommunityCryptoProvider.{Jce, Tink}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.jce.JceJavaConverter
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class TinkCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with PrivateKeySerializationTest
    with HkdfTest
    with RandomTest
    with JavaPublicKeyConverterTest
    with PublicKeyValidationTest {

  "TinkCrypto" can {

    def tinkCrypto(): Future[Crypto] =
      new CommunityCryptoFactory()
        .create(
          CommunityCryptoConfig(provider = Tink),
          new MemoryStorage(loggerFactory, timeouts),
          new CommunityCryptoPrivateStoreFactory,
          testedReleaseProtocolVersion,
          timeouts,
          loggerFactory,
          NoReportingTracerProvider,
        )
        .valueOrFail("create crypto")

    behave like signingProvider(Tink.signing.supported, tinkCrypto())
    behave like encryptionProvider(
      Tink.encryption.supported,
      Tink.symmetric.supported,
      tinkCrypto(),
    )
    behave like privateKeySerializerProvider(
      Tink.signing.supported,
      Tink.encryption.supported,
      tinkCrypto(),
    )
    behave like hkdfProvider(tinkCrypto().map(_.pureCrypto))
    behave like randomnessProvider(tinkCrypto().map(_.pureCrypto))

    behave like javaPublicKeyConverterProvider(
      Tink.signing.supported,
      Tink.encryption.supported,
      tinkCrypto(),
      "Tink",
    )

    behave like javaPublicKeyConverterProviderOther(
      Tink.signing.supported,
      Tink.encryption.supported,
      tinkCrypto(),
      "JCE",
      new JceJavaConverter(Jce.signing.supported, Jce.encryption.supported),
    )

    behave like publicKeyValidationProvider(
      Tink.signing.supported,
      Tink.encryption.supported,
      Tink.supportedCryptoKeyFormats,
      tinkCrypto(),
    )

  }

}
