// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  EncryptionPrivateKey,
  Fingerprint,
  KeyName,
  PrivateKey,
  SigningKeyUsage,
  SigningPrivateKey,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import org.scalatest.wordspec.AsyncWordSpec

trait CryptoPrivateStoreTest extends BaseTest { this: AsyncWordSpec =>

  def uniqueKeyName(name: String): String = name + getClass.getSimpleName

  lazy val crypto: SymbolicCrypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
  crypto.setRandomKeysFlag(true)

  lazy val sigKey1Name: String = uniqueKeyName("sigKey1_")
  lazy val encKey1Name: String = uniqueKeyName("encKey1_")

  lazy val sigKey1: SigningPrivateKey =
    crypto.newSymbolicSigningKeyPair(SigningKeyUsage.ProtocolOnly).privateKey
  lazy val sigKey1WithName: SigningPrivateKeyWithName =
    SigningPrivateKeyWithName(sigKey1, Some(KeyName.tryCreate(sigKey1Name)))

  lazy val encKey1: EncryptionPrivateKey = crypto.newSymbolicEncryptionKeyPair().privateKey
  lazy val encKey1WithName: EncryptionPrivateKeyWithName =
    EncryptionPrivateKeyWithName(encKey1, Some(KeyName.tryCreate(encKey1Name)))

  def cryptoPrivateStore(
      newStore: => CryptoPrivateStore,
      storePrivateKey: (
          CryptoPrivateStore,
          PrivateKey,
          Fingerprint,
          Option[KeyName],
      ) => EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit],
  ): Unit = {

    "check existence of private key" in {
      val store = newStore
      for {
        _ <- storePrivateKey(store, sigKey1, sigKey1.id, sigKey1WithName.name).failOnShutdown
        _ <- storePrivateKey(store, encKey1, encKey1.id, encKey1WithName.name).failOnShutdown
        signRes <- store.existsSigningKey(sigKey1.id).failOnShutdown
        encRes <- store.existsDecryptionKey(encKey1.id).failOnShutdown
      } yield {
        signRes shouldBe true
        encRes shouldBe true
      }
    }

    "filter signing keys based on usage" in {
      val store = newStore

      val sigKey2: SigningPrivateKey =
        crypto
          .newSymbolicSigningKeyPair(
            NonEmpty.mk(
              Set,
              SigningKeyUsage.Namespace,
              SigningKeyUsage.IdentityDelegation,
            )
          )
          .privateKey
      val sigKey2WithName: SigningPrivateKeyWithName =
        SigningPrivateKeyWithName(sigKey2, Some(KeyName.tryCreate(uniqueKeyName("sigKey2_"))))

      val sigKeyAllUsage: SigningPrivateKey =
        crypto.newSymbolicSigningKeyPair(SigningKeyUsage.All).privateKey
      val sigKeyAllUsageWithName: SigningPrivateKeyWithName =
        SigningPrivateKeyWithName(
          sigKeyAllUsage,
          Some(KeyName.tryCreate(uniqueKeyName("sigKeyAllUsage_"))),
        )

      for {
        _ <- storePrivateKey(store, sigKey1, sigKey1.id, sigKey1WithName.name).failOnShutdown
        _ <- storePrivateKey(store, sigKey2, sigKey2.id, sigKey2WithName.name).failOnShutdown
        protocolKeys <- store
          .filterSigningKeys(
            Seq(sigKey1.id, sigKey2.id),
            SigningKeyUsage.ProtocolOnly,
          )
          .failOnShutdown
        authenticationKeys <- store
          .filterSigningKeys(
            Seq(sigKey1.id, sigKey2.id),
            SigningKeyUsage.SequencerAuthenticationOnly,
          )
          .failOnShutdown

        _ <- storePrivateKey(
          store,
          sigKeyAllUsage,
          sigKeyAllUsage.id,
          sigKeyAllUsageWithName.name,
        ).failOnShutdown
        filterProtocolWithOldAndNewKeys <- store
          .filterSigningKeys(
            Seq(sigKey1.id, sigKey2.id, sigKeyAllUsage.id),
            SigningKeyUsage.ProtocolOnly,
          )
          .failOnShutdown
        filterAuthenticationWithOldAndNewKeys <- store
          .filterSigningKeys(
            Seq(sigKey1.id, sigKey2.id, sigKeyAllUsage.id),
            SigningKeyUsage.IdentityDelegationOnly,
          )
          .failOnShutdown

        // remove the keys with usage
        _ <- store.removePrivateKey(sigKey1.id).failOnShutdown
        _ <- store.removePrivateKey(sigKey2.id).failOnShutdown
        filterWithOldKeys <- store
          .filterSigningKeys(Seq(sigKeyAllUsage.id), SigningKeyUsage.ProtocolOnly)
          .failOnShutdown
      } yield {
        protocolKeys.loneElement shouldBe sigKey1.id
        authenticationKeys shouldBe Seq.empty
        filterProtocolWithOldAndNewKeys shouldBe Seq(sigKey1.id, sigKeyAllUsage.id)
        filterAuthenticationWithOldAndNewKeys shouldBe Seq(sigKey2.id, sigKeyAllUsage.id)
        filterWithOldKeys.loneElement shouldBe sigKeyAllUsage.id
      }
    }

    "delete key successfully" in {
      val store = newStore
      for {
        _ <- storePrivateKey(store, sigKey1, sigKey1.id, sigKey1WithName.name).failOnShutdown
        _ <- store.removePrivateKey(sigKey1.id).failOnShutdown
        res <- store.existsSigningKey(sigKey1.id).failOnShutdown
      } yield res shouldBe false
    }

  }

}
