// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.crypto.KeyPurpose.Encryption
import com.digitalasset.canton.crypto.kms.{KmsKeyId, SymbolicKms}
import com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore
import com.digitalasset.canton.crypto.{EncryptionPrivateKey, KeyName}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ByteString4096
import org.scalatest.wordspec.AsyncWordSpec

trait EncryptedCryptoPrivateStoreTest extends AsyncWordSpec with CryptoPrivateStoreExtendedTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*

    /* We delete all private keys that ARE encrypted (wrapper_key_id != NULL).
    This conditional delete is to avoid conflicts with the crypto private store tests. */
    storage.update(
      DBIO.seq(
        sqlu"delete from common_crypto_private_keys where wrapper_key_id IS NOT NULL"
      ),
      operationName = s"${this.getClass}: Delete from encrypted private crypto table",
    )
  }

  // Use null for the config as there's no KmsConfig subclass for SymbolicKms which is a test only KMS implementation
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  lazy val symbolicKms: SymbolicKms = new SymbolicKms(
    crypto,
    null,
    timeouts,
    loggerFactory,
  )

  lazy val keyId: KmsKeyId =
    symbolicKms
      .generateSymmetricEncryptionKey()
      .valueOrFailShutdown("create symbolic KMS key")
      .futureValue

  lazy val dbStore =
    new DbCryptoPrivateStore(
      storage,
      testedReleaseProtocolVersion,
      timeouts,
      BatchingConfig(),
      loggerFactory,
    )

  lazy val encryptedStore = new EncryptedCryptoPrivateStore(
    dbStore,
    symbolicKms,
    keyId,
    testedReleaseProtocolVersion,
    timeouts,
    BatchingConfig(),
    loggerFactory,
  )

  "EncryptedCryptoPrivateStore" can {
    behave like cryptoPrivateStoreExtended(encryptedStore, encrypted = true)

    "stores private keys encrypted" in {
      val encKeyName = uniqueKeyName("encKey_")
      val encKey: EncryptionPrivateKey = crypto.newSymbolicEncryptionKeyPair().privateKey
      val encKeyWithName: EncryptionPrivateKeyWithName =
        EncryptionPrivateKeyWithName(encKey, Some(KeyName.tryCreate(encKeyName)))
      val encKeyBytesWithName = encKey.toByteString(testedReleaseProtocolVersion.v)

      for {
        _ <- encryptedStore
          .storeDecryptionKey(encKey, encKeyWithName.name)
          .valueOrFailShutdown("store key 1")
        expectedEncValue <- symbolicKms
          .encryptSymmetric(
            encryptedStore.wrapperKeyId,
            ByteString4096.tryCreate(encKeyBytesWithName),
          )
          .valueOrFailShutdown("encrypt value")
        resultEncrypted <- dbStore
          .listPrivateKeys(Encryption, encrypted = true)
          .valueOrFailShutdown("list keys without decrypting")
        resultClear <- encryptedStore
          .listPrivateKeys(Encryption, encrypted = true)
          .valueOrFailShutdown("list keys after decrypting")
      } yield {
        resultEncrypted.size shouldBe 1
        // this check works because the symbolic "encryption" is deterministic
        resultEncrypted.head.data shouldBe expectedEncValue.unwrap

        resultClear.size shouldBe 1
        resultClear.head.data shouldEqual encKeyBytesWithName
      }
    }

  }
}

class EncryptedCryptoPrivateStoreTestH2 extends EncryptedCryptoPrivateStoreTest with H2Test

class EncryptedCryptoPrivateStoreTestPostgres
    extends EncryptedCryptoPrivateStoreTest
    with PostgresTest
