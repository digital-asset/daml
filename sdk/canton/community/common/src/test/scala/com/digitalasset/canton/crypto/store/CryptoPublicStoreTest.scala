// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import org.scalatest.wordspec.AsyncWordSpec

trait CryptoPublicStoreTest extends BaseTest { this: AsyncWordSpec =>

  def cryptoPublicStore(newStore: => CryptoPublicStore, backedByDatabase: Boolean): Unit = {

    val crypto = SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

    val sigKey1: SigningPublicKey = crypto.generateSymbolicSigningKey(Some("sigKey1"))
    val sigKey1WithName: SigningPublicKeyWithName =
      SigningPublicKeyWithName(sigKey1, Some(KeyName.tryCreate("sigKey1")))
    val sigKey2: SigningPublicKey = crypto.generateSymbolicSigningKey(Some("sigKey2"))
    val sigKey2WithName: SigningPublicKeyWithName = SigningPublicKeyWithName(sigKey2, None)

    val encKey1: EncryptionPublicKey = crypto.generateSymbolicEncryptionKey(Some("encKey1"))
    val encKey1WithName: EncryptionPublicKeyWithName =
      EncryptionPublicKeyWithName(encKey1, Some(KeyName.tryCreate("encKey1")))
    val encKey2: EncryptionPublicKey = crypto.generateSymbolicEncryptionKey(Some("encKey2"))
    val encKey2WithName: EncryptionPublicKeyWithName = EncryptionPublicKeyWithName(encKey2, None)

    "save encryption keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeEncryptionKey(encKey1, encKey1WithName.name)
        _ <- store.storeEncryptionKey(encKey2, None)
        result <- store.encryptionKeys
        result2 <- store.listEncryptionKeys
      } yield {
        result shouldEqual Set(encKey1, encKey2)
        result2 shouldEqual Set(encKey1WithName, encKey2WithName)
      }
    }.failOnShutdown

    if (backedByDatabase) {
      "not rely solely on cache" in {
        val store = newStore
        val separateStore = newStore
        for {
          _ <- store.storeEncryptionKey(encKey1, encKey1WithName.name)
          _ <- store.storeEncryptionKey(encKey2, None)
          result1 <- separateStore.encryptionKey(encKey1.fingerprint).value
          result2 <- separateStore.encryptionKey(encKey2.fingerprint).value

          _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name)
          _ <- store.storeSigningKey(sigKey2, None)
          result3 <- separateStore.signingKey(sigKey1.fingerprint).value
          result4 <- separateStore.signingKey(sigKey2.fingerprint).value
        } yield {
          result1 shouldEqual Some(encKey1)
          result2 shouldEqual Some(encKey2)

          result3 shouldEqual Some(sigKey1)
          result4 shouldEqual Some(sigKey2)
        }
      }.failOnShutdown
    }

    "save signing keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name)
        _ <- store.storeSigningKey(sigKey2, None)
        result <- store.signingKeys
        result2 <- store.listSigningKeys
      } yield {
        result shouldEqual Set(sigKey1, sigKey2)
        result2 shouldEqual Set(sigKey1WithName, sigKey2WithName)
      }
    }.failOnShutdown

    "delete public keys" in {
      val store = newStore
      for {
        _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name)
        result1 <- store.signingKeys
        _ <- store.deleteKey(sigKey1.id)
        result2 <- store.signingKeys
        _ <- store.storeSigningKey(sigKey1, None)
      } yield {
        result1 shouldEqual Set(sigKey1)
        result2 shouldEqual Set()
      }
    }.failOnShutdown

    "idempotent store of encryption keys" in {
      val store = newStore
      for {
        _ <- store.storeEncryptionKey(encKey1, encKey1WithName.name)

        // Should succeed
        _ <- store.storeEncryptionKey(encKey1, encKey1WithName.name)

        // Should fail due to different name
        _failedInsert <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          store.storeEncryptionKey(encKey1, None),
          _.getMessage shouldBe s"Existing public key for ${encKey1.id} is different than inserted key",
        )

        result <- store.listEncryptionKeys
      } yield {
        result shouldEqual Set(encKey1WithName)
      }
    }.failOnShutdown

    "idempotent store of signing keys" in {
      val store = newStore
      for {
        _ <- store
          .storeSigningKey(sigKey1, sigKey1WithName.name)

        // Should succeed
        _ <- store
          .storeSigningKey(sigKey1, sigKey1WithName.name)

        // Should fail due to different name
        _failedInsert <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          store.storeSigningKey(sigKey1, None),
          _.getMessage should startWith(
            s"Existing public key for ${sigKey1.id} is different than inserted key"
          ),
        )

        result <- store.listSigningKeys
      } yield {
        result shouldEqual Set(sigKey1WithName)
      }
    }.failOnShutdown

  }
}
