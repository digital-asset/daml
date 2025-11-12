// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.digitalasset.canton.config
import com.digitalasset.canton.config.CryptoProvider.Jce
import com.digitalasset.canton.config.{CachingConfigs, CryptoConfig, PositiveFiniteDuration}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.digitalasset.canton.crypto.SigningKeySpec.EcSecp256k1
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AsyncWordSpec

import java.util.Base64

class JceCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with HmacTest
    with PrivateKeySerializationTest
    with PasswordBasedEncryptionTest
    with RandomTest
    with PublicKeyValidationTest
    with PrivateKeyValidationTest
    with CryptoKeyFormatMigrationTest {

  "JceCrypto" must {

    // use a short duration to verify that a Java key is removed from the cache promptly
    lazy val javaKeyCacheDuration = PositiveFiniteDuration.ofSeconds(4)

    def jceCrypto(): FutureUnlessShutdown[Crypto] =
      Crypto
        .create(
          CryptoConfig(provider = Jce),
          CachingConfigs.defaultKmsMetadataCache,
          CachingConfigs.defaultSessionEncryptionKeyCacheConfig
            .focus(_.senderCache.expireAfterTimeout)
            .replace(javaKeyCacheDuration),
          CachingConfigs.defaultPublicKeyConversionCache.copy(expireAfterAccess =
            config.NonNegativeFiniteDuration(javaKeyCacheDuration.underlying)
          ),
          new MemoryStorage(loggerFactory, timeouts),
          Option.empty[ReplicaManager],
          testedReleaseProtocolVersion,
          futureSupervisor,
          wallClock,
          executionContext,
          timeouts,
          loggerFactory,
          NoReportingTracerProvider,
        )
        .valueOrFail("failed to create crypto")

    behave like migrationTest(
      // No legacy keys for secp256k1
      Jce.signingKeys.supported.filterNot(_ == EcSecp256k1),
      Jce.encryptionKeys.supported,
      jceCrypto(),
    )

    behave like signingProvider(
      Jce.signingKeys.supported,
      Jce.signingAlgorithms.supported,
      Jce.supportedSignatureFormats,
      jceCrypto(),
    )
    behave like encryptionProvider(
      Jce.encryptionAlgorithms.supported,
      Jce.symmetric.supported,
      jceCrypto(),
    )
    behave like privateKeySerializerProvider(
      Jce.signingKeys.supported,
      Jce.encryptionKeys.supported,
      jceCrypto(),
    )

    forAll(
      Jce.encryptionAlgorithms.supported.filter(_.supportDeterministicEncryption)
    ) { encryptionAlgorithmSpec =>
      forAll(encryptionAlgorithmSpec.supportedEncryptionKeySpecs.forgetNE) { keySpec =>
        s"Deterministic hybrid encrypt " +
          s"with $encryptionAlgorithmSpec and a $keySpec key" should {

            val newCrypto = jceCrypto()

            behave like hybridEncrypt(
              keySpec,
              (message, publicKey) =>
                newCrypto.map(crypto =>
                  crypto.pureCrypto.encryptDeterministicWith(
                    message,
                    publicKey,
                    encryptionAlgorithmSpec,
                  )
                ),
              newCrypto,
            )

            "yield the same ciphertext for the same encryption" in {
              val message = TestMessage(ByteString.copyFromUtf8("foobar"))
              for {
                crypto <- jceCrypto()
                publicKey <- getEncryptionPublicKey(crypto, keySpec)
                encrypted1 = crypto.pureCrypto
                  .encryptDeterministicWith(
                    message,
                    publicKey,
                    encryptionAlgorithmSpec,
                  )
                  .valueOrFail("encrypt")
                _ = assert(message.bytes != encrypted1.ciphertext)
                encrypted2 = crypto.pureCrypto
                  .encryptDeterministicWith(
                    message,
                    publicKey,
                    encryptionAlgorithmSpec,
                  )
                  .valueOrFail("encrypt")
                _ = assert(message.bytes != encrypted2.ciphertext)
              } yield encrypted1.ciphertext shouldEqual encrypted2.ciphertext
            }

            /* Ensures that changes in the crypto provider (e.g. a different version) do not break deterministic
             * encryption. A fixed set of public keys is embedded in the code, along with the ciphertexts obtained
             * by deterministically encrypting a test string with those keys. This test compares the provider’s output
             * against these reference values. If a newer version or provider produces different ciphertexts,
             * the mismatch will be detected.
             */
            "generate the same ciphertext as the reference value" in {
              val message = TestMessage(ByteString.copyFromUtf8("foobar"))

              val (publicKeyDer, expectedCiphertext) = keySpec match {
                case EncryptionKeySpec.EcP256 =>
                  val publicKeyEcP256Base64 =
                    "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEoArYy5JnyIzu4iYdGmnLDOeDY9bSqtHDYFJFnMQXQXqfAErcD/b6ObfaM+neZcNo/ejLu5GwAZWFYMiQ5z7dQQ=="
                  val expectedCiphertextBase64 =
                    "XoQZEdUTCcf3B5xAkwgJZARfG+TrRn8hK+UbKvhhwlWx3jfgTvNwPkQDn2qs/bn4Glu0lKrb1pzofxc0ZZ6SY5JU1Mr1Q0P38ZTqaY2STz3rSew000qt8mCBS1HyJgjC0DAH4kt62NFex/xKcWjA9iktRPqaPfwgjfsnFqaaDvNk"
                  (
                    ByteString.copyFrom(Base64.getDecoder.decode(publicKeyEcP256Base64)),
                    ByteString.copyFrom(Base64.getDecoder.decode(expectedCiphertextBase64)),
                  )
                case EncryptionKeySpec.Rsa2048 =>
                  val publicKeyRsa2048 =
                    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyIv/MMU8KfhQSZZwmg++01ZT5Kc8jYO99LoMei2DhFa6tcoPbvvcEWdm6bdmdR4Leampe0ZO7RWIexK/hsSTQhFVJAPngxGB0ESicbfg858+S3q4FFixqJm1A3NGwBWWUnjUwMuJ+yQkXdaeQu4hMRtNYZ/QpptXjXIy4KbyFb6YeP+FOscY/LTAGAinfgGy4/uSeoldHDUxkcfrSbesbLOAbiHmMhB12+Ayx7X8Rh5loakqWpjrF4l+wwegT9dapDTrwKFzUUTnXMX8Y0aTooIy/JWjdSF3nG8ftOYkD8mq1uXwuevN7T8se5L+aq/rKED0AibnwSDfehaKMjmbPQIDAQAB"
                  val expectedCiphertextBase64 =
                    "wvpK3CLngaeai6QQZGvhczdzW/b8Emg6HT+NdB1lzJvLHaJnUFrG5oXn9ty/GzbYPOvWdFXVHqBfwwxircyWsXYN2+s2Q57Zzcok/nRoJ/l/PfgPi3UPy34fxxrA1n007imTNmOpkhoUu+PxfTfPbcFg01XH5sla8hg58MgkQaW5v1ahXPW0aOOq6N4JYztwu476/vqHys6wFy+m+8PHSZJoTbalbSsDUhbolPs2jkBrpbEEU0sJ6Bd1jTvNCADVsEMP+Va7rOPCYMmcIbMc0+fyKxJBm9EPHXNuO93S4pGyhrqiRiGBhFrbCCKzt93bhRanaqH+MfqkQ1d77UkaiQ=="
                  (
                    ByteString.copyFrom(Base64.getDecoder.decode(publicKeyRsa2048)),
                    ByteString.copyFrom(Base64.getDecoder.decode(expectedCiphertextBase64)),
                  )
              }
              val publicKey = EncryptionPublicKey
                .create(
                  CryptoKeyFormat.DerX509Spki,
                  publicKeyDer,
                  keySpec,
                )
                .valueOrFail("create encryption public key")
              for {
                crypto <- jceCrypto()
                encrypted = crypto.pureCrypto
                  .encryptDeterministicWith(
                    message,
                    publicKey,
                    encryptionAlgorithmSpec,
                  )
                  .valueOrFail("encrypt")
              } yield encrypted.ciphertext shouldBe expectedCiphertext
            }
          }
      }
    }

    behave like hmacProvider(
      Set(HmacAlgorithm.HmacSha256),
      jceCrypto().map(_.pureCrypto),
    )

    behave like randomnessProvider(jceCrypto().map(_.pureCrypto))

    behave like pbeProvider(
      Jce.pbkdf.valueOrFail("no PBKDF schemes configured").supported,
      Jce.symmetric.supported,
      jceCrypto().map(_.pureCrypto),
    )

    behave like publicKeyValidationProvider(
      Jce.signingKeys.supported,
      Jce.encryptionKeys.supported,
      Jce.supportedCryptoKeyFormats,
      jceCrypto().failOnShutdown,
      javaKeyCacheDuration,
    )

    behave like privateKeyValidationProvider(
      Jce.signingKeys.supported,
      Jce.encryptionKeys.supported,
      Jce.supportedCryptoKeyFormats,
      jceCrypto().failOnShutdown,
    )
  }
}
