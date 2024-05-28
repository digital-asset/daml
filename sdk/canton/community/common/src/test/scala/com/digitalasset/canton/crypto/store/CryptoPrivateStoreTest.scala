// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  EncryptionPrivateKey,
  Fingerprint,
  KeyName,
  PrivateKey,
  SigningPrivateKey,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

trait CryptoPrivateStoreTest extends BaseTest { this: AsyncWordSpec =>

  def uniqueKeyName(name: String): String =
    name + UUID.randomUUID().toString

  val sigKey1Name: String = uniqueKeyName("sigKey1_")
  val encKey1Name: String = uniqueKeyName("encKey1_")

  val sigKey1: SigningPrivateKey = SymbolicCrypto.signingPrivateKey(sigKey1Name)
  val sigKey1WithName: SigningPrivateKeyWithName =
    SigningPrivateKeyWithName(sigKey1, Some(KeyName.tryCreate(sigKey1Name)))
  val sigKey1BytesWithName: (ByteString, Option[KeyName]) =
    (sigKey1.toByteString(testedReleaseProtocolVersion.v), sigKey1WithName.name)

  val encKey1: EncryptionPrivateKey = SymbolicCrypto.encryptionPrivateKey(encKey1Name)
  val encKey1WithName: EncryptionPrivateKeyWithName =
    EncryptionPrivateKeyWithName(encKey1, Some(KeyName.tryCreate(encKey1Name)))
  val encKey1BytesWithName: (ByteString, Option[KeyName]) =
    (encKey1.toByteString(testedReleaseProtocolVersion.v), encKey1WithName.name)

  def cryptoPrivateStore(
      newStore: => CryptoPrivateStore,
      storePrivateKey: (
          CryptoPrivateStore,
          PrivateKey,
          Fingerprint,
          Option[KeyName],
      ) => EitherT[Future, CryptoPrivateStoreError, Unit],
      encrypted: Boolean,
  ): Unit = {

    "check existence of private key" in {
      val store = newStore
      for {
        _ <- storePrivateKey(store, sigKey1, sigKey1.id, sigKey1WithName.name)
        _ <- storePrivateKey(store, encKey1, encKey1.id, encKey1WithName.name)
        signRes <- store.existsSigningKey(sigKey1.id)
        encRes <- store.existsDecryptionKey(encKey1.id)
      } yield {
        signRes shouldBe true
        encRes shouldBe true
      }

    }

    "delete key successfully" in {
      val store = newStore
      for {
        _ <- storePrivateKey(store, sigKey1, sigKey1.id, sigKey1WithName.name)
        _ <- store.removePrivateKey(sigKey1.id)
        res <- store.existsSigningKey(sigKey1.id)
      } yield res shouldBe false
    }

  }

}
