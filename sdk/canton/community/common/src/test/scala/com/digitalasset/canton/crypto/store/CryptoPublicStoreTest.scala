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
        _ <- store.storeEncryptionKey(encKey1, encKey1WithName.name).valueOrFail("store encKey1")
        _ <- store.storeEncryptionKey(encKey2, None).valueOrFail("store encKey2")
        result <- store.encryptionKeys.valueOrFail("get encryption keys")
        result2 <- store.listEncryptionKeys.valueOrFail("list keys")
      } yield {
        result shouldEqual Set(encKey1, encKey2)
        result2 shouldEqual Set(encKey1WithName, encKey2WithName)
      }
    }

    if (backedByDatabase) {
      "not rely solely on cache" in {
        val store = newStore
        val separateStore = newStore
        for {
          _ <- store.storeEncryptionKey(encKey1, encKey1WithName.name).valueOrFail("store encKey1")
          _ <- store.storeEncryptionKey(encKey2, None).valueOrFail("store encKey2")
          result1 <- separateStore.encryptionKey(encKey1.fingerprint).valueOrFail("read encKey1")
          result2 <- separateStore.encryptionKey(encKey2.fingerprint).valueOrFail("read encKey2")

          _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name).valueOrFail("store sigKey1")
          _ <- store.storeSigningKey(sigKey2, None).valueOrFail("store sigKey2")
          result3 <- separateStore.signingKey(sigKey1.fingerprint).valueOrFail("read sigKey1")
          result4 <- separateStore.signingKey(sigKey2.fingerprint).valueOrFail("read sigKey2")
        } yield {
          result1 shouldEqual Some(encKey1)
          result2 shouldEqual Some(encKey2)

          result3 shouldEqual Some(sigKey1)
          result4 shouldEqual Some(sigKey2)
        }
      }
    }

    "save signing keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name).valueOrFail("store sigKey1")
        _ <- store.storeSigningKey(sigKey2, None).valueOrFail("store sigKey2")
        result <- store.signingKeys.valueOrFail("list keys")
        result2 <- store.listSigningKeys.valueOrFail("list keys")
      } yield {
        result shouldEqual Set(sigKey1, sigKey2)
        result2 shouldEqual Set(sigKey1WithName, sigKey2WithName)
      }
    }

    "idempotent store of encryption keys" in {
      val store = newStore
      for {
        _ <- store
          .storeEncryptionKey(encKey1, encKey1WithName.name)
          .valueOrFail("store key 1 with name")

        // Should succeed
        _ <- store
          .storeEncryptionKey(encKey1, encKey1WithName.name)
          .valueOrFail("store key 1 with name again")

        // Should fail due to different name
        failedInsert <- store.storeEncryptionKey(encKey1, None).value

        result <- store.listEncryptionKeys
      } yield {
        failedInsert.left.value shouldBe a[CryptoPublicStoreError]
        result shouldEqual Set(encKey1WithName)
      }
    }

    "idempotent store of signing keys" in {
      val store = newStore
      for {
        _ <- store
          .storeSigningKey(sigKey1, sigKey1WithName.name)
          .valueOrFail("store key 1 with name")

        // Should succeed
        _ <- store
          .storeSigningKey(sigKey1, sigKey1WithName.name)
          .valueOrFail("store key 1 with name again")

        // Should fail due to different name
        failedInsert <- store.storeSigningKey(sigKey1, None).value

        result <- store.listSigningKeys
      } yield {
        failedInsert.left.value shouldBe a[CryptoPublicStoreError]
        result shouldEqual Set(sigKey1WithName)
      }
    }

  }
}
