// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import cats.data.EitherT
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.ExecutionContext

class SymbolicPrivateCrypto(
    pureCrypto: SymbolicPureCrypto,
    override val store: CryptoPrivateStoreExtended,
)(
    override implicit val ec: ExecutionContext
) extends CryptoPrivateStoreApi {

  private val keyCounter = new AtomicInteger

  private val randomKeys = new AtomicBoolean(false)

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  // NOTE: These schemes are not really used by Symbolic crypto
  override val defaultSigningKeyScheme: SigningKeyScheme = SigningKeyScheme.Ed25519
  override val defaultEncryptionKeyScheme: EncryptionKeyScheme =
    EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm

  @VisibleForTesting
  def setRandomKeysFlag(newValue: Boolean): Unit = {
    randomKeys.set(newValue)
  }

  private def genKeyPair[K](keypair: (ByteString, ByteString) => K): K = {
    val key = if (randomKeys.get()) {
      PseudoRandom.randomAlphaNumericString(8)
    } else {
      s"key-${keyCounter.incrementAndGet()}"
    }
    val publicKey = ByteString.copyFromUtf8(s"pub-$key")
    val privateKey = ByteString.copyFromUtf8(s"priv-$key")
    keypair(publicKey, privateKey)
  }

  override protected[crypto] def generateSigningKeypair(scheme: SigningKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningKeyPair] =
    EitherT.rightT(
      genKeyPair((pubKey, privKey) =>
        SigningKeyPair.create(CryptoKeyFormat.Symbolic, pubKey, privKey, scheme)
      )
    )

  override protected[crypto] def generateEncryptionKeypair(scheme: EncryptionKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionKeyPair] =
    EitherT.rightT(
      genKeyPair((pubKey, privKey) =>
        EncryptionKeyPair.create(CryptoKeyFormat.Symbolic, pubKey, privKey, scheme)
      )
    )

  override def close(): Unit = ()
}
