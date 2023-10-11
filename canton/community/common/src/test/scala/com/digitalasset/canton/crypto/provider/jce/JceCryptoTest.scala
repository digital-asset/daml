// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.digitalasset.canton.config.CommunityCryptoConfig
import com.digitalasset.canton.config.CommunityCryptoProvider.{Jce, Tink}
import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.tink.TinkJavaConverter
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class JceCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with PrivateKeySerializationTest
    with HkdfTest
    with RandomTest
    with JavaPublicKeyConverterTest {

  "JceCrypto" can {

    def jceCrypto(): Future[Crypto] = {
      new CommunityCryptoFactory()
        .create(
          CommunityCryptoConfig(provider = Jce),
          new MemoryStorage(loggerFactory, timeouts),
          new CommunityCryptoPrivateStoreFactory,
          testedReleaseProtocolVersion,
          timeouts,
          loggerFactory,
          NoReportingTracerProvider,
        )
        .valueOr(err => throw new RuntimeException(s"failed to create crypto: $err"))
    }

    behave like signingProvider(Jce.signing.supported, jceCrypto())
    behave like encryptionProvider(
      Jce.encryption.supported,
      Jce.symmetric.supported,
      jceCrypto(),
    )
    behave like privateKeySerializerProvider(
      Jce.signing.supported,
      Jce.encryption.supported,
      jceCrypto(),
    )

    forAll(Jce.encryption.supported.filter(_.supportDeterministicEncryption)) { keyScheme =>
      s"Deterministic hybrid encrypt " +
        s"with $keyScheme" should {

          val newCrypto = jceCrypto()

          behave like hybridEncrypt(
            keyScheme,
            (message, publicKey, version) =>
              newCrypto.map(crypto =>
                crypto.pureCrypto.encryptDeterministicWith(message, publicKey, version)
              ),
            newCrypto,
          )

          "yield the same ciphertext for the same encryption" in {
            val message = TestMessage(ByteString.copyFromUtf8("foobar"))
            for {
              crypto <- jceCrypto()
              publicKey <- getEncryptionPublicKey(crypto, keyScheme)
              encrypted1 = crypto.pureCrypto
                .encryptDeterministicWith(message, publicKey, testedProtocolVersion)
                .valueOrFail("encrypt")
              _ = assert(message.bytes != encrypted1.ciphertext)
              encrypted2 = crypto.pureCrypto
                .encryptDeterministicWith(message, publicKey, testedProtocolVersion)
                .valueOrFail("encrypt")
              _ = assert(message.bytes != encrypted2.ciphertext)
            } yield encrypted1.ciphertext shouldEqual encrypted2.ciphertext
          }
        }
    }

    behave like hkdfProvider(jceCrypto().map(_.pureCrypto))
    behave like randomnessProvider(jceCrypto().map(_.pureCrypto))
    behave like javaPublicKeyConverterProvider(
      Jce.signing.supported,
      // TODO(i13896): Support conversion for ECIES public key with AES-128-CBC
      Jce.encryption.supported.filterNot(_ == EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc),
      jceCrypto(),
      "JCE",
    )

    // Also test the conversion from JCE to Tink, limited to Tink supported algorithms
    behave like javaPublicKeyConverterProviderOther(
      Tink.signing.supported,
      Tink.encryption.supported,
      jceCrypto(),
      "Tink",
      new TinkJavaConverter,
    )

  }
}
