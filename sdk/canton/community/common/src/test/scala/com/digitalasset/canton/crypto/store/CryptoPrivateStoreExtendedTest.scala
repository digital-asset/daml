// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  EncryptionPrivateKey,
  Fingerprint,
  KeyName,
  PrivateKey,
  SigningPrivateKey,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import org.scalatest.wordspec.AsyncWordSpec

trait CryptoPrivateStoreExtendedTest extends CryptoPrivateStoreTest {
  this: AsyncWordSpec =>

  def cryptoPrivateStoreExtended(
      newStore: => CryptoPrivateStoreExtended,
      encrypted: Boolean,
  ): Unit = {

    val crypto = SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
    crypto.setRandomKeysFlag(true)

    val sigKey1Name: String = uniqueKeyName("sigKey1_")
    val encKey1Name: String = uniqueKeyName("encKey1_")

    val sigKey1: SigningPrivateKey = crypto.newSymbolicSigningKeyPair().privateKey
    val sigKey1WithName: SigningPrivateKeyWithName =
      SigningPrivateKeyWithName(sigKey1, Some(KeyName.tryCreate(sigKey1Name)))
    val sigKey1BytesWithName =
      (sigKey1.toByteString(testedReleaseProtocolVersion.v), sigKey1WithName.name)

    val sigKey2: SigningPrivateKey = crypto.newSymbolicSigningKeyPair().privateKey
    val sigKey2WithName: SigningPrivateKeyWithName = SigningPrivateKeyWithName(sigKey2, None)
    val sigKey2BytesWithName =
      (sigKey2.toByteString(testedReleaseProtocolVersion.v), sigKey2WithName.name)

    val encKey1: EncryptionPrivateKey = crypto.newSymbolicEncryptionKeyPair().privateKey
    val encKey1WithName: EncryptionPrivateKeyWithName =
      EncryptionPrivateKeyWithName(encKey1, Some(KeyName.tryCreate(encKey1Name)))
    val encKey1BytesWithName =
      (encKey1.toByteString(testedReleaseProtocolVersion.v), encKey1WithName.name)

    val encKey2: EncryptionPrivateKey = crypto.newSymbolicEncryptionKeyPair().privateKey
    val encKey2WithName: EncryptionPrivateKeyWithName = EncryptionPrivateKeyWithName(encKey2, None)
    val encKey2BytesWithName =
      (encKey2.toByteString(testedReleaseProtocolVersion.v), encKey2WithName.name)

    def storePrivateKey(
        store: CryptoPrivateStore,
        privateKey: PrivateKey,
        id: Fingerprint,
        name: Option[KeyName],
    ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
      store match {
        case extended: CryptoPrivateStoreExtended =>
          extended.storePrivateKey(privateKey, name)
        case _ =>
          EitherT.leftT[FutureUnlessShutdown, Unit](
            CryptoPrivateStoreError.FailedToInsertKey(
              id,
              "crypto private store does not implement the necessary method to store a private key",
            )
          )
      }

    behave like cryptoPrivateStore(
      newStore,
      storePrivateKey,
    )

    "store encryption keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store
          .storeDecryptionKey(encKey1, encKey1WithName.name)
          .valueOrFailShutdown("store key 1")
        _ <- store.storeDecryptionKey(encKey2, None).valueOrFailShutdown("store key 2")
        result <- store.listPrivateKeys(Encryption, encrypted).valueOrFailShutdown("list keys")
      } yield {
        result.map(storedKey => (storedKey.data, storedKey.name)) shouldEqual Set(
          encKey1BytesWithName,
          encKey2BytesWithName,
        )
      }
    }

    "store signing keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name).valueOrFailShutdown("store key 1")
        _ <- store.storeSigningKey(sigKey2, None).valueOrFailShutdown("store key 2")
        result <- store.listPrivateKeys(Signing, encrypted).valueOrFailShutdown("list keys")
      } yield {
        result.map(storedKey => (storedKey.data, storedKey.name)) shouldEqual Set(
          sigKey1BytesWithName,
          sigKey2BytesWithName,
        )
      }
    }

    "idempotent store of encryption keys" in {
      val store = newStore
      for {
        _ <- store
          .storeDecryptionKey(encKey1, encKey1WithName.name)
          .valueOrFailShutdown("store key 1 with name")

        // Should succeed
        _ <- store
          .storeDecryptionKey(encKey1, encKey1WithName.name)
          .valueOrFailShutdown("store key 1 with name again")

        // Should fail due to different name
        failedInsert <- store.storeDecryptionKey(encKey1, None).failOnShutdown.value

        result <- store
          .listPrivateKeys(Encryption, encrypted)
          .valueOrFailShutdown("list private keys")
      } yield {
        failedInsert.left.value shouldBe a[CryptoPrivateStoreError]
        result.map(storedKey => (storedKey.data, storedKey.name)) shouldEqual Set(
          encKey1BytesWithName
        )
      }
    }

    "idempotent store of signing keys" in {
      val store = newStore
      for {
        _ <- store
          .storeSigningKey(sigKey1, sigKey1WithName.name)
          .valueOrFailShutdown("store key 1 with name")

        // Should succeed
        _ <- store
          .storeSigningKey(sigKey1, sigKey1WithName.name)
          .valueOrFailShutdown("store key 1 with name again")

        // Should fail due to different name
        failedInsert <- store.storeSigningKey(sigKey1, None).failOnShutdown.value

        result <- store.listPrivateKeys(Signing, encrypted).valueOrFailShutdown("list private keys")
      } yield {
        failedInsert.left.value shouldBe a[CryptoPrivateStoreError]
        result.map(storedKey => (storedKey.data, storedKey.name)) shouldEqual Set(
          sigKey1BytesWithName
        )
      }
    }

    "check if private key is encrypted" in {
      val store = newStore
      for {
        _ <- store
          .storeDecryptionKey(encKey1, encKey1WithName.name)
          .valueOrFailShutdown("store key 1")
        encryptedRes <- store.encrypted(encKey1.id).valueOrFailShutdown("encrypted")
      } yield encryptedRes.isDefined shouldBe encrypted
    }

  }

}
